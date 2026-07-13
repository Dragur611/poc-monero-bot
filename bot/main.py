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

PAYMENT_AMOUNT = 0.001  # Monto fijo para pruebas en stagenet

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


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
    """
    Espera hasta que el wallet RPC esté disponible, haciendo polling con get_balance.
    Retorna True si se conecta, False si se agotan los reintentos.
    """
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
    """Espera a que el RPC esté listo, luego crea la wallet si no existe, o la abre si ya existe."""
    # Primero esperar a que el RPC esté accesible
    if not await wait_for_rpc():
        raise RuntimeError("Wallet RPC no disponible")

    logger.info("Inicializando wallet Monero...")
    try:
        # Intentar crear
        resp = await rpc_call(
            "create_wallet",
            {
                "filename": "poc_wallet",
                "password": "poc",
                "language": "English",
            },
        )
        if "error" in resp:
            logger.info(
                f"create_wallet returned error (probablemente ya existe): {resp['error']}"
            )
        else:
            logger.info("Wallet creada exitosamente.")
    except Exception as e:
        logger.warning(f"create_wallet falló: {e}")

    try:
        # Abrir wallet
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
        else:
            logger.info("Wallet abierta correctamente.")
    except Exception as e:
        logger.error(f"open_wallet falló: {e}")
        raise


# ---------- Funciones del bot ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /start"""
    await update.message.reply_text(
        "¡Bienvenido! Por favor ingresa el código del producto (ejemplo: pantalon_00432)"
    )
    logger.info(f"Usuario {update.effective_user.id} inició conversación")


async def handle_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja el código de producto ingresado por el usuario."""
    product_code = update.message.text.strip()
    user_id = update.effective_user.id

    logger.info(f"Usuario {user_id} ingresó código: {product_code}")

    if not product_code:
        await update.message.reply_text("Por favor ingresa un código válido.")
        return

    # Simular que el producto existe (para pruebas)
    if "pantalon" not in product_code:
        await update.message.reply_text(
            "Código de producto no reconocido. Intenta con 'pantalon_XXXX'"
        )
        return

    try:
        # 1. Generar un payment_id aleatorio de 16 bytes en hex (32 caracteres)
        payment_id = secrets.token_hex(16)
        logger.info(f"Generando dirección integrada con payment_id: {payment_id}")

        # 2. Llamar al wallet RPC para obtener una dirección integrada
        resp = await rpc_call("make_integrated_address", {"payment_id": payment_id})

        if "error" in resp:
            logger.error(f"Error en make_integrated_address: {resp['error']}")
            await update.message.reply_text(
                "Hubo un error al generar la dirección de pago. Por favor intenta más tarde."
            )
            return

        integrated_address = resp.get("result", {}).get("integrated_address")
        if not integrated_address:
            logger.error("La respuesta no contiene integrated_address")
            await update.message.reply_text(
                "Error: no se pudo obtener la dirección de pago."
            )
            return

        # 3. Construir mensaje con la dirección y el monto
        amount_xmr = PAYMENT_AMOUNT
        mensaje = (
            f"✅ Para completar la compra del producto *{product_code}*:\n\n"
            f"💰 *Monto a pagar:* `{amount_xmr}` XMR\n"
            f"📬 *Dirección de pago:*\n`{integrated_address}`\n\n"
            f"⏳ Una vez realizado el pago, el sistema verificará la transacción automáticamente.\n"
            f"🔗 *Importante:* Envía exactamente el monto indicado para evitar problemas."
        )

        # 4. Guardar el estado en contexto (para futuras verificaciones)
        context.user_data["pending_payment"] = {
            "product_code": product_code,
            "payment_id": payment_id,
            "address": integrated_address,
            "amount": amount_xmr,
            "timestamp": datetime.now().isoformat(),
        }

        # 5. Enviar mensaje al usuario
        await update.message.reply_text(
            mensaje,
            parse_mode=None,
            disable_web_page_preview=True,
        )

        logger.info(
            f"Mensaje de pago enviado a usuario {user_id} con dirección {integrated_address}"
        )

    except Exception as e:
        logger.error(
            f"Error en handle_product para usuario {user_id}: {e}", exc_info=True
        )
        await update.message.reply_text(
            "❌ Ocurrió un error al procesar tu solicitud. Por favor intenta más tarde."
        )


# ---------- Configuración de la aplicación FastAPI con lifespan ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Contexto de vida de la aplicación.
    - Startup: inicializa wallet y configura webhook.
    - Shutdown: (opcional) tareas de limpieza.
    """
    logger.info("🚀 Iniciando aplicación...")

    # --- STARTUP ---
    # Inicializar wallet Monero (espera a que el RPC esté listo)
    try:
        await init_monero_wallet()
        logger.info("✅ Wallet Monero inicializada correctamente.")
    except Exception as e:
        logger.error(f"❌ Fallo crítico al iniciar la wallet: {e}")
        raise

    # Configurar webhook
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(NGROK_URL)
            tunnels = resp.json().get("tunnels", [])
            if not tunnels:
                logger.error("No se encontraron túneles ngrok")
                raise RuntimeError("ngrok no está corriendo")
            public_url = tunnels[0].get("public_url")
            if not public_url:
                logger.error("No se pudo obtener la URL pública de ngrok")
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

    # --- YIELD: la aplicación está activa ---
    yield

    # --- SHUTDOWN (opcional) ---
    logger.info("🛑 Apagando aplicación...")


# ---------- Crear la aplicación FastAPI con lifespan ----------
app = FastAPI(lifespan=lifespan)


# ---------- Endpoint FastAPI para webhook ----------
@app.post("/webhook")
async def webhook(request: Request):
    """Recibe updates de Telegram."""
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    await application.initialize()
    update_data = await request.json()
    update = Update.de_json(update_data, application.bot)
    await application.process_update(update)
    return {"status": "ok"}


# ---------- Punto de entrada (para ejecución directa) ----------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
