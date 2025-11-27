from telethon import TelegramClient
from supabase import create_client
import asyncio

api_id = 32076891
api_hash = "8cacf5236a2c3f09c56fab48dcd6096c"

SUPABASE_URL = "https://vbcdlzmfysefgubthppt.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZiY2Rsem1meXNlZmd1YnRocHB0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjQyMjg0NDgsImV4cCI6MjA3OTgwNDQ0OH0.ZuzVgebJW-hQYQr2a7kacU5YvavzZ4B0VmqCAcXbl4o"

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

# --- Script principal ---
async def main():
    await client.start()
    group = -1002520693250  # tu grupo
    print("Importando últimos 10 mensajes...")

    batch = []

    async for msg in client.iter_messages(group, limit=10):
        if not msg.text:
            continue
        # opcional: filtrar mensajes irrelevantes
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

    # --- Insertar o actualizar mensajes sin romper si hay duplicados ---
    if batch:
        try:
            supabase.table("messages").upsert(batch, on_conflict="id").execute()
            print(f"✔ Mensajes insertados o actualizados correctamente: {len(batch)}")
        except Exception as e:
            print("❌ Error al insertar mensajes:", e)

asyncio.run(main())
