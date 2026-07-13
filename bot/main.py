import os
import httpx
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters

TOKEN = os.environ.get("TELEGRAM_TOKEN")
MONERO_RPC_URL = "http://monero-rpc-wallet:38083/json_rpc"
NGROK_API_URL = "http://ngrok-gateway:4040/api/tunnels"
XMR_AMOUNT_REQUIRED = 16000000000  # 0.016 XMR en piconeros

# Manejo de Estados
USER_STATES = {}
IDLE = "IDLE"
AWAITING_PRODUCT = "AWAITING_PRODUCT"
PAYMENT_LOCK = "PAYMENT_LOCK"

ptb = Application.builder().token(TOKEN).updater(None).build()


async def init_monero_wallet():
    """Crea la billetera (si no existe) y se asegura de abrirla"""
    async with httpx.AsyncClient() as client:
        payload_create = {
            "jsonrpc": "2.0",
            "id": "0",
            "method": "create_wallet",
            "params": {
                "filename": "poc_wallet",
                "password": "poc",
                "language": "English",
            },
        }
        try:
            await client.post(MONERO_RPC_URL, json=payload_create, timeout=10.0)
        except Exception:
            pass

        payload_open = {
            "jsonrpc": "2.0",
            "id": "0",
            "method": "open_wallet",
            "params": {"filename": "poc_wallet", "password": "poc"},
        }
        try:
            await client.post(MONERO_RPC_URL, json=payload_open, timeout=10.0)
        except Exception:
            pass


async def get_ngrok_url():
    """Extrae dinámicamente la URL HTTPS del contenedor de Ngrok"""
    async with httpx.AsyncClient() as client:
        for _ in range(15):
            try:
                res = await client.get(NGROK_API_URL, timeout=5.0)
                if res.status_code == 200:
                    tunnels = res.json().get("tunnels", [])
                    for t in tunnels:
                        if t["public_url"].startswith("https"):
                            return t["public_url"]
            except Exception:
                pass
            await asyncio.sleep(2)
    return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Damos tiempo a los servicios hermanos de levantar
    await asyncio.sleep(8)

    # Preparamos Monero y Ngrok
    await init_monero_wallet()
    public_url = await get_ngrok_url()

    if public_url:
        await ptb.bot.set_webhook(f"{public_url}/webhook")

    async with ptb:
        await ptb.start()
        yield
        await ptb.stop()


app = FastAPI(lifespan=lifespan)


@app.post("/webhook")
async def process_update(request: Request):
    """Recibe los webhooks entrantes de Telegram"""
    req_json = await request.json()
    update = Update.de_json(req_json, ptb.bot)
    await ptb.process_update(update)
    return Response(status_code=200)


async def verify_payment(chat_id: int, address_index: int):
    """Tarea en segundo plano que consulta el mempool buscando el pago"""
    async with httpx.AsyncClient() as client:
        payload = {
            "jsonrpc": "2.0",
            "id": "0",
            "method": "get_transfers",
            "params": {
                "in": True,
                "pool": True,
                "account_index": 0,
                "subaddr_indices": [address_index],
            },
        }
        while USER_STATES.get(chat_id) == PAYMENT_LOCK:
            try:
                res = await client.post(MONERO_RPC_URL, json=payload, timeout=5.0)
                data = res.json()

                # Buscamos en el pool (0-conf) y confirmadas
                pool_txs = data.get("result", {}).get("pool", []) or []
                in_txs = data.get("result", {}).get("in", []) or []

                for tx in pool_txs + in_txs:
                    if tx.get("amount", 0) >= XMR_AMOUNT_REQUIRED:
                        await ptb.bot.send_message(
                            chat_id=chat_id,
                            text="¡Pago verificado con éxito! Muchas gracias por tu compra. Puedes realizar un nuevo pedido enviando /start.",
                        )
                        USER_STATES[chat_id] = IDLE
                        return
            except Exception:
                pass

            await asyncio.sleep(10)  # Frecuencia de chequeo


# --- CONTROLADORES DEL FLUJO CONVERSACIONAL ---


async def start_command(update: Update, context):
    chat_id = update.effective_chat.id

    if USER_STATES.get(chat_id) == PAYMENT_LOCK:
        await update.message.reply_text(
            "Debes completar el pago pendiente antes de iniciar una nueva compra."
        )
        return

    USER_STATES[chat_id] = AWAITING_PRODUCT
    await update.message.reply_text(
        "¡Bienvenido! Por favor ingresa el código del producto (por ejemplo: pantalon_00432)"
    )


async def handle_text(update: Update, context):
    chat_id = update.effective_chat.id
    state = USER_STATES.get(chat_id, IDLE)

    if state == PAYMENT_LOCK:
        await update.message.reply_text(
            "El sistema no permitirá más interacción hasta que se complete la transacción. Esperando el pago de 0.016 XMR..."
        )
        return

    if state == AWAITING_PRODUCT:
        product_code = update.message.text

        # Integración RPC para crear la dirección desechable
        async with httpx.AsyncClient() as client:
            payload = {
                "jsonrpc": "2.0",
                "id": "0",
                "method": "create_address",
                "params": {"account_index": 0, "label": product_code},
            }
            try:
                res = await client.post(MONERO_RPC_URL, json=payload, timeout=10.0)
                result = res.json().get("result", {})
                address = result.get("address")
                address_index = result.get("address_index")

                if address and address_index is not None:
                    USER_STATES[chat_id] = PAYMENT_LOCK
                    await update.message.reply_text(
                        f"Para el producto {product_code}, debes pagar exactamente 0.016 XMR.\n\n"
                        f"Envía los fondos a esta dirección de Stagenet:\n`{address}`\n\n"
                        "El sistema se encuentra esperando la verificación del pago...",
                        parse_mode="Markdown",
                    )
                    # Iniciamos el worker asíncrono para verificar este índice específico
                    asyncio.create_task(verify_payment(chat_id, address_index))
                else:
                    await update.message.reply_text(
                        "Error interconectando con el nodo Stagenet. Inténtalo más tarde."
                    )
            except Exception as e:
                await update.message.reply_text(
                    "Error de red contactando al demonio de la billetera."
                )


ptb.add_handler(CommandHandler("start", start_command))
ptb.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
