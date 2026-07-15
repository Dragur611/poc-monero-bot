import os
import json
import logging
import secrets
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ---------- Configuración ----------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "TU_TOKEN_AQUI")
MONERO_RPC_URL = os.getenv("MONERO_RPC_URL", "http://monero-rpc-wallet:38083/json_rpc")
NGROK_URL = os.getenv("NGROK_URL", "http://ngrok-gateway:4040/api/tunnels")
RESPONSE_FILE_ENDPOINT = os.getenv(
    "RESPONSE_FILE_ENDPOINT", "http://localhost:8001/scrape"
)  # Endpoint de generación de facturas

PAYMENT_AMOUNT = 0.000000000001  # XMR
POLL_INTERVAL = 15  # segundos

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------- Estado global ----------
pending_payments = {}  # payment_id -> {user_id, product_code, amount, address, ...}
processed_txids = set()  # para no procesar la misma tx dos veces


# ---------- Cliente RPC ----------
async def rpc_call(method: str, params: dict = None) -> dict:
    """Función genérica para llamar al wallet RPC con logs detallados."""
    payload = {
        "jsonrpc": "2.0",
        "id": "0",
        "method": method,
        "params": params or {},
    }
    logger.info(f"RPC call: {method} with params: {params}")
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(MONERO_RPC_URL, json=payload)
            response.raise_for_status()
            data = response.json()
            logger.info(f"RPC response for {method}: {json.dumps(data, indent=2)}")
            return data
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error calling {method}: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error calling {method}: {e}")
            raise


async def wait_for_rpc(max_retries: int = 10, delay: float = 2.0) -> bool:
    logger.info("⏳ Esperando que el wallet RPC esté disponible...")
    for attempt in range(1, max_retries + 1):
        try:
            await rpc_call("get_balance")
            logger.info("✅ Wallet RPC está listo.")
            return True
        except Exception as e:
            logger.warning(f"Intento {attempt}/{max_retries} falló: {e}")
            await asyncio.sleep(delay)
    logger.error("❌ No se pudo conectar al wallet RPC después de varios intentos.")
    return False


async def init_monero_wallet():
    if not await wait_for_rpc():
        raise RuntimeError("Wallet RPC no disponible")

    logger.info("Inicializando wallet Monero...")
    try:
        resp = await rpc_call(
            "create_wallet",
            {
                "filename": "poc_wallet",
                "password": "poc",
                "language": "English",
            },
        )
        if "error" in resp:
            logger.info(f"create_wallet returned error: {resp['error']}")
        else:
            logger.info("Wallet creada exitosamente.")
    except Exception as e:
        logger.warning(f"create_wallet falló: {e}")

    try:
        resp = await rpc_call(
            "open_wallet",
            {
                "filename": "poc_wallet",
                "password": "poc",
            },
        )
        if "error" in resp:
            logger.error(f"open_wallet error: {resp['error']}")
            raise RuntimeError("No se pudo abrir la wallet")
        logger.info("Wallet abierta correctamente.")
    except Exception as e:
        logger.error(f"open_wallet falló: {e}")
        raise


# ---------- Generación de factura ----------
async def generate_invoice(product_code: str) -> bytes:
    """
    Llama al endpoint /scrape para generar el PDF de la factura.
    Retorna el contenido binario del PDF.
    """
    logger.info(f"Generando factura para producto: {product_code}")
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(
                RESPONSE_FILE_ENDPOINT,
                json={"query": product_code},
            )
            response.raise_for_status()
            # Asumimos que la respuesta es el PDF en binario
            return response.content
        except Exception as e:
            logger.error(
                f"Error generando factura para {product_code}: {e}", exc_info=True
            )
            raise


# ---------- Verificación de pagos ----------
async def check_payments():
    """Task en segundo plano que verifica pagos entrantes cada POLL_INTERVAL segundos."""
    global pending_payments, processed_txids

    while True:
        try:
            resp = await rpc_call(
                "get_transfers",
                {
                    "in": True,
                    "pool": True,
                    "filter_by_height": False,
                },
            )

            result = resp.get("result", {})
            all_txs = []
            for key in ["in", "pool"]:
                if key in result and isinstance(result[key], list):
                    all_txs.extend(result[key])

            if not all_txs:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            for tx in all_txs:
                txid = tx.get("txid")
                if txid in processed_txids:
                    continue

                payment_id = tx.get("payment_id")
                if not payment_id:
                    continue

                if payment_id in pending_payments:
                    pend = pending_payments[payment_id]
                    amount_piconero = tx.get("amount", 0)
                    expected_piconero = int(pend["amount"] * 1e12)
                    if amount_piconero >= expected_piconero:
                        logger.info(
                            f"✅ Pago detectado para usuario {pend['user_id']}, txid: {txid}"
                        )
                        # Enviar confirmación + factura
                        await send_payment_confirmation(
                            pend["user_id"], pend["product_code"], txid
                        )
                        del pending_payments[payment_id]
                        processed_txids.add(txid)
                    else:
                        logger.warning(
                            f"⚠️ Pago con monto incorrecto para {payment_id}: recibido {amount_piconero}, esperado {expected_piconero}"
                        )

            if len(processed_txids) > 1000:
                processed_txids = set(list(processed_txids)[-500:])

        except Exception as e:
            logger.error(f"Error en check_payments: {e}", exc_info=True)

        await asyncio.sleep(POLL_INTERVAL)


async def send_payment_confirmation(user_id: int, product_code: str, txid: str):
    """
    Envía confirmación de pago y genera/envía la factura en PDF.
    """
    global telegram_app
    if telegram_app is None:
        logger.error("Telegram app no disponible para enviar confirmación")
        return

    try:
        # 1. Mensaje de pago recibido
        mensaje_pago = (
            f"🎉 ¡Pago recibido!\n"
            f"Producto: *{product_code}*\n"
            f"ID de transacción: `{txid}`\n\n"
            f"⏳ Generando tu factura, por favor espera..."
        )
        await telegram_app.bot.send_message(
            chat_id=user_id, text=mensaje_pago, parse_mode=None
        )
        logger.info(f"Mensaje de pago enviado a usuario {user_id}")

        # 2. Generar factura (PDF)
        pdf_content = await generate_invoice(product_code)

        # 3. Enviar PDF como documento
        filename = f"{product_code}.pdf"
        await telegram_app.bot.send_document(
            chat_id=user_id,
            document=pdf_content,
            filename=filename,
            caption=f"📄 Factura para {product_code}",
        )
        logger.info(f"Factura enviada a usuario {user_id} con archivo {filename}")

        # 4. Mensaje final
        mensaje_final = (
            f"✅ Factura generada correctamente.\n"
            f"Puedes descargarla desde el archivo adjunto.\n"
            f"¡Gracias por tu compra!"
        )
        await telegram_app.bot.send_message(
            chat_id=user_id, text=mensaje_final, parse_mode=None
        )

    except Exception as e:
        logger.error(
            f"Error en send_payment_confirmation para usuario {user_id}: {e}",
            exc_info=True,
        )
        # Notificar al usuario que hubo un problema con la factura
        try:
            await telegram_app.bot.send_message(
                chat_id=user_id,
                text="❌ Ocurrió un error al generar tu factura. Por favor contacta a soporte.",
            )
        except Exception as e2:
            logger.error(f"Error adicional al notificar fallo de factura: {e2}")


# ---------- Handlers del bot ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"Usuario {update.effective_user.id} ejecutó /start")
    await update.message.reply_text(
        "¡Bienvenido! Por favor ingresa el código del producto (ejemplo: 111222333444)"
    )


async def handle_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global pending_payments

    product_code = update.message.text.strip()
    user_id = update.effective_user.id

    logger.info(f"Usuario {user_id} ingresó código: {product_code}")

    if not product_code:
        await update.message.reply_text("Por favor ingresa un código válido.")
        return


    try:
        payment_id = secrets.token_hex(8)
        logger.info(f"Generando dirección integrada con payment_id: {payment_id}")

        resp = await rpc_call("make_integrated_address", {"payment_id": payment_id})
        if "error" in resp:
            logger.error(f"Error en make_integrated_address: {resp['error']}")
            await update.message.reply_text(
                "Hubo un error al generar la dirección de pago."
            )
            return

        integrated_address = resp.get("result", {}).get("integrated_address")
        if not integrated_address:
            logger.error("La respuesta no contiene integrated_address")
            await update.message.reply_text(
                "Error: no se pudo obtener la dirección de pago."
            )
            return

        pending_payments[payment_id] = {
            "user_id": user_id,
            "product_code": product_code,
            "address": integrated_address,
            "amount": PAYMENT_AMOUNT,
            "timestamp": datetime.now().isoformat(),
        }
        logger.info(f"Pago pendiente agregado para {payment_id}")

        amount_str = f"{PAYMENT_AMOUNT:.12f}".rstrip("0").rstrip(".")

        mensaje = (
            f"✅ Para completar la compra del producto <b>{product_code}</b>:\n\n"
            f"💰 <b>Monto a pagar:</b> <code>{amount_str}</code> XMR\n"
            f"📬 <b>Dirección de pago:</b>\n"
            f"<code>{integrated_address}</code>\n\n"
            f"⏳ Una vez realizado el pago, el sistema verificará la transacción automáticamente.\n"
            f"🔗 <b>Importante:</b> Envía exactamente el monto indicado.\n"
            f"\n💡 <i>Para copiar la dirección o el monto, haz tap largo sobre el texto y selecciona.</i>"
        )

        await update.message.reply_text(
            mensaje,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

        logger.info(
            f"Mensaje de pago enviado a usuario {user_id} con address {integrated_address}"
        )

    except Exception as e:
        logger.error(
            f"Error en handle_product para usuario {user_id}: {e}", exc_info=True
        )
        await update.message.reply_text("❌ Ocurrió un error al procesar tu solicitud.")


# ---------- Crear la aplicación de telegram (global) ----------
def create_telegram_app() -> Application:
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_product))
    return app


telegram_app = None


# ---------- FastAPI con lifespan ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global telegram_app

    logger.info("🚀 Iniciando aplicación...")

    try:
        await init_monero_wallet()
        logger.info("✅ Wallet Monero inicializada correctamente.")
    except Exception as e:
        logger.error(f"❌ Fallo crítico al iniciar la wallet: {e}")
        raise

    telegram_app = create_telegram_app()
    await telegram_app.initialize()
    logger.info("✅ Aplicación de Telegram inicializada.")

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(NGROK_URL)
            tunnels = resp.json().get("tunnels", [])
            if not tunnels:
                raise RuntimeError("ngrok no está corriendo")
            public_url = tunnels[0].get("public_url")
            if not public_url:
                raise RuntimeError("URL pública no encontrada")

        webhook_url = f"{public_url}/webhook"
        logger.info(f"🔗 Configurando webhook en: {webhook_url}")

        async with httpx.AsyncClient() as client:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook"
            resp = await client.post(url, json={"url": webhook_url})
            resp.raise_for_status()
            result = resp.json()
            if not result.get("ok"):
                logger.error(f"Error al configurar webhook: {result}")
                raise RuntimeError("Fallo al configurar webhook")

        logger.info("✅ Webhook configurado exitosamente.")
    except Exception as e:
        logger.error(f"❌ Error configurando webhook: {e}")
        raise

    asyncio.create_task(check_payments())
    logger.info(
        f"🔍 Task de verificación de pagos iniciado (intervalo {POLL_INTERVAL}s)"
    )

    yield

    logger.info("🛑 Apagando aplicación...")
    if telegram_app:
        await telegram_app.shutdown()


app = FastAPI(lifespan=lifespan)


@app.post("/webhook")
async def webhook(request: Request):
    global telegram_app
    if telegram_app is None:
        logger.error("❌ Aplicación de Telegram no inicializada")
        return {"status": "error", "message": "Not initialized"}

    try:
        update_data = await request.json()
        update = Update.de_json(update_data, telegram_app.bot)
        await telegram_app.process_update(update)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Error procesando webhook: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
