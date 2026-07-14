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


# ---------- Verificación de pagos ----------
async def check_payments():
    """Task en segundo plano que verifica pagos entrantes cada POLL_INTERVAL segundos."""
    global pending_payments, processed_txids

    while True:
        try:
            # Obtener transferencias entrantes (confirmadas y en pool)
            resp = await rpc_call(
                "get_transfers",
                {
                    "in": True,  # transferencias confirmadas
                    "pool": True,  # transferencias en el pool (no confirmadas)
                    "filter_by_height": False,
                },
            )

            result = resp.get("result", {})
            # Combinar listas de transacciones
            all_txs = []
            for key in ["in", "pool"]:
                if key in result and isinstance(result[key], list):
                    all_txs.extend(result[key])

            if not all_txs:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            # Procesar cada transacción
            for tx in all_txs:
                txid = tx.get("txid")
                if txid in processed_txids:
                    continue

                payment_id = tx.get("payment_id")
                if not payment_id:
                    continue  # sin payment_id, no nos interesa

                # Buscar si este payment_id está en nuestros pendientes
                if payment_id in pending_payments:
                    pend = pending_payments[payment_id]
                    # Verificar monto (en piconeros)
                    # 1 XMR = 1e12 piconeros
                    amount_piconero = tx.get("amount", 0)
                    expected_piconero = int(pend["amount"] * 1e12)
                    if amount_piconero >= expected_piconero:
                        # Pago recibido!
                        logger.info(
                            f"✅ Pago detectado para usuario {pend['user_id']}, txid: {txid}"
                        )
                        # Enviar mensaje de confirmación al usuario
                        try:
                            await send_payment_confirmation(
                                pend["user_id"], pend["product_code"], txid
                            )
                        except Exception as e:
                            logger.error(f"Error al enviar confirmación: {e}")
                        # Eliminar pendiente
                        del pending_payments[payment_id]
                        # Marcar tx como procesada
                        processed_txids.add(txid)
                    else:
                        logger.warning(
                            f"⚠️ Pago con monto incorrecto para {payment_id}: recibido {amount_piconero}, esperado {expected_piconero}"
                        )

                # Opcional: marcar tx como procesada aunque no coincida, para no repetir
                # Pero mejor solo marcar si coincide o si ya no está pendiente.
                # Si no coincide, no la marcamos, porque podría ser para otro producto futuro.
                # Sin embargo, si ya fue procesada (coincidió) la marcamos.

            # Limpiar set de txids procesados si crece demasiado
            if len(processed_txids) > 1000:
                processed_txids = set(list(processed_txids)[-500:])

        except Exception as e:
            logger.error(f"Error en check_payments: {e}", exc_info=True)

        await asyncio.sleep(POLL_INTERVAL)


async def send_payment_confirmation(user_id: int, product_code: str, txid: str):
    """Envía mensaje de confirmación al usuario."""
    # Necesitamos una referencia al bot para enviar mensajes.
    # Usaremos la instancia global de telegram_app.
    global telegram_app
    if telegram_app is None:
        logger.error("Telegram app no disponible para enviar confirmación")
        return

    try:
        mensaje = (
            f"🎉 ¡Pago recibido!\n"
            f"Producto: *{product_code}*\n"
            f"ID de transacción: `{txid}`\n\n"
            f"✅ Tu compra ha sido confirmada. ¡Gracias!"
        )
        await telegram_app.bot.send_message(
            chat_id=user_id, text=mensaje, parse_mode=None
        )
        logger.info(f"Confirmación enviada al usuario {user_id}")
    except Exception as e:
        logger.error(f"Error enviando confirmación a {user_id}: {e}")


# ---------- Handlers del bot ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"Usuario {update.effective_user.id} ejecutó /start")
    await update.message.reply_text(
        "¡Bienvenido! Por favor ingresa el código del producto (ejemplo: pantalon_00432)"
    )


async def handle_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global pending_payments

    product_code = update.message.text.strip()
    user_id = update.effective_user.id

    logger.info(f"Usuario {user_id} ingresó código: {product_code}")

    if not product_code:
        await update.message.reply_text("Por favor ingresa un código válido.")
        return

    # Simular validación del producto (para pruebas)
    if "pantalon" not in product_code:
        await update.message.reply_text(
            "Código de producto no reconocido. Intenta con 'pantalon_XXXX'"
        )
        return

    try:
        # Generar payment_id de 8 bytes (16 caracteres hex)
        payment_id = secrets.token_hex(8)
        logger.info(f"Generando dirección integrada con payment_id: {payment_id}")

        # Llamar al wallet RPC
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

        # Guardar pago pendiente
        pending_payments[payment_id] = {
            "user_id": user_id,
            "product_code": product_code,
            "address": integrated_address,
            "amount": PAYMENT_AMOUNT,
            "timestamp": datetime.now().isoformat(),
        }
        logger.info(f"Pago pendiente agregado para {payment_id}")

        # Formatear monto sin notación científica
        # Ejemplo: 0.001 -> "0.001", 0.000000000001 -> "0.000000000001"
        amount_str = f"{PAYMENT_AMOUNT:.12f}".rstrip("0").rstrip(".")

        # Construir mensaje con HTML para mejor visualización
        mensaje = (
            f"✅ Para completar la compra del producto <b>{product_code}</b>:\n\n"
            f"💰 <b>Monto a pagar:</b> <code>{amount_str}</code> XMR\n"
            f"📬 <b>Dirección de pago:</b>\n"
            f"<code>{integrated_address}</code>\n\n"
            f"⏳ Una vez realizado el pago, el sistema verificará la transacción automáticamente.\n"
            f"🔗 <b>Importante:</b> Envía exactamente el monto indicado.\n"
            f"\n💡 <i>Para copiar la dirección o el monto, haz tap largo sobre el texto y selecciona.</i>"
        )

        # Enviar mensaje con parse_mode HTML
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

    # 1. Inicializar wallet
    try:
        await init_monero_wallet()
        logger.info("✅ Wallet Monero inicializada correctamente.")
    except Exception as e:
        logger.error(f"❌ Fallo crítico al iniciar la wallet: {e}")
        raise

    # 2. Crear e inicializar la aplicación de telegram
    telegram_app = create_telegram_app()
    await telegram_app.initialize()
    logger.info("✅ Aplicación de Telegram inicializada.")

    # 3. Configurar webhook
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

    # 4. Lanzar task de verificación de pagos
    asyncio.create_task(check_payments())
    logger.info(
        f"🔍 Task de verificación de pagos iniciado (intervalo {POLL_INTERVAL}s)"
    )

    yield  # La aplicación está activa

    # Shutdown
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
