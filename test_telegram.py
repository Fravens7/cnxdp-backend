from telethon import TelegramClient

api_id = 32076891  # <-- pega tu api_id
api_hash = "8cacf5236a2c3f09c56fab48dcd6096c"  # <-- pega tu api_hash

client = TelegramClient("session", api_id, api_hash)

async def main():
    print("Iniciando cliente...")
    await client.start()  # primera vez te pedirá tu número y un código

    group = -1002520693250  # ej: "-1001234567890" o "https://t.me/nombre"

    print("Leyendo últimos 5 mensajes...")
    async for msg in client.iter_messages(group, limit=5):
        print(msg.sender_id, ":", msg.text)

    print("Listo.")

client.loop.run_until_complete(main())
