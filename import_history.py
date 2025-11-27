import os
from telethon import TelegramClient
from supabase import create_client
import asyncio

# Leer las variables de entorno
api_id = int(os.environ.get("TELEGRAM_API_ID"))  # Asegúrate de que sea un entero
api_hash = os.environ.get("TELEGRAM_API_HASH")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Conectar a Supabase y Telegram
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
client = TelegramClient("session", api_id, api_hash)

# --- Función para extraer columnas ---
def extract_columns(text):
    # separar por |
    parts = [p.strip() for p in text.split("|")]

    # completar hasta 6 columnas (brand, type, extra1, extra2, extra3, extra4)
    while len(parts) < 6:
        parts.append(None)
    
    # solo tomamos las primeras 6
    return parts[:6]

# --- Función para importar mensajes de los últimos 7 días ---
async def import_history():
    # Fecha de hace 7 días
    seven_days_ago = datetime.now() - timedelta(days=7)
    seven_days_ago = seven_days_ago.replace(tzinfo=datetime.timezone.utc)

    # Obtener los mensajes desde hace 7 días, sin límite de cantidad
    batch = []
    async for msg in client.iter_messages(
        -1002520693250,  # Tu grupo
        min_date=seven_days_ago  # Solo mensajes desde hace 7 días
    ):
        if not msg.text:
            continue
        # Filtrar mensajes irrelevantes
        if msg.text.lower() in ["system approved", "test"]:
            continue

        print("Mensaje:", msg.text)
        cols = extract_columns(msg.text)

        data = {
            "id": msg.id,
            "brand": cols[0],
            "type": cols[1],
            "extra1": cols[2],
            "extra2": cols[3],
            "extra3": cols[4],
            "extra4": cols[5],
            "date": msg.date.date().isoformat()
        }

        batch.append(data)

    # Insertar o actualizar los mensajes sin duplicar
    if batch:
        try:
            supabase.table("messages").upsert(batch, on_conflict="id").execute()
            print(f"✔ Mensajes insertados o actualizados correctamente: {len(batch)}")
        except Exception as e:
            print("❌ Error al insertar mensajes:", e)

# --- Mantener el script corriendo cada 5 minutos ---
async def main_loop():
    while True:
        await import_history()  # Llamamos a la función principal que importa los mensajes
        print("Esperando 5 minutos antes de verificar nuevos mensajes...")
        await asyncio.sleep(60 * 5)  # Espera 5 minutos entre cada ejecución

asyncio.run(main_loop())
