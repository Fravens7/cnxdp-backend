import os
import asyncio
import re
from threading import Thread
from datetime import datetime, timezone, timedelta
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from supabase import create_client
from flask import Flask # <--- NUEVO IMPORT

# --- CONFIGURACIÓN DE ENTORNO ---
api_id = int(os.environ.get("TELEGRAM_API_ID"))
api_hash = os.environ.get("TELEGRAM_API_HASH")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TARGET_GROUP = int(os.environ.get("TARGET_GROUP", "-1002520693250"))
SESSION_STRING = os.environ.get("TELEGRAM_SESSION")

# --- TRUCO RENDER: SERVIDOR FLASK (KEEP-ALIVE) ---
# Esto crea un servidor web real que responde a UptimeRobot
app = Flask('')

@app.route('/')
def home():
    return "¡Estoy vivo! Bot funcionando correctamente."

def run():
    # Render asigna un puerto dinámico o usa el 8080 por defecto
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run)
    t.start()
# -----------------------------------------------------

# --- CONFIGURACIÓN LÓGICA ---
BRANDS = ["M1", "B1", "M2", "K1", "B2", "B3", "B4"]
SYSTEM_KEYWORDS = [
    "SYSTEM", "SYS APP", "AUTO SYS", "AUTO APP", "APPROVED TEAM", "TEST",
    "AUTO SETTLE", "SETTLE", "CANCELLED", "REJECTED", "SUCCESS", "DONE",
    "WITHDRAW ALREADY", "NOTE TEAM", "ALL PENDING", "ALREADY", "PLS", "PLEASE",
    "CAX", "CANCEL", "@"
]

# Inicializar Supabase y Telegram
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), api_id, api_hash)
else:
    print("❌ ERROR: Falta TELEGRAM_SESSION")
    client = TelegramClient("session_render", api_id, api_hash)

# --- FUNCIÓN DE LIMPIEZA Y PROCESAMIENTO (CON AJUSTE DE HORA) ---
def parse_and_save(message):
    """Procesa un solo mensaje y lo guarda en Supabase con hora Local"""
    if not message.text: return False

    texto_bruto = message.text
    
    # 1. Limpieza inicial
    def limpiar_parte(texto):
        if not texto: return None
        texto = re.sub(r'[*_~`]', '', texto)
        texto = " ".join(texto.split())
        return texto.strip()

    raw_parts = re.split(r'[|\n\\]+', texto_bruto)
    parts = [limpiar_parte(p) for p in raw_parts if limpiar_parte(p)]

    if not parts: return False

    final_brand = "Otros"
    data_parts = []
    marca_encontrada = False

    # 2. PRIORIDAD A LA MARCA
    if len(parts) > 0:
        first_part_upper = parts[0].upper()
        
        for b in BRANDS:
            # Regex: Busca la marca al inicio
            if re.match(rf"^{b}(\s|-|/|$)", first_part_upper):
                final_brand = b
                marca_encontrada = True
                resto = parts[0][len(b):].lstrip(" -/")
                if resto:
                    data_parts = [resto] + parts[1:]
                else:
                    data_parts = parts[1:]
                break
    
    # 3. Si no es marca, buscamos Sistema
    if not marca_encontrada:
        es_sistema = False
        for part in parts:
            if any(k in part.upper() for k in SYSTEM_KEYWORDS):
                es_sistema = True
                break
        
        if es_sistema:
            final_brand = "SYSTEM"
            data_parts = parts
        else:
            # Búsqueda profunda
            for i, parte in enumerate(parts):
                if parte.upper() in BRANDS:
                    final_brand = part.upper()
                    marca_encontrada = True
                    data_parts = parts[:i] + parts[i+1:]
                    break
            
            if not marca_encontrada:
                final_brand = "Otros"
                data_parts = parts

    # 4. Preparar Payload
    safe_data = [None] * 5
    for i in range(min(len(data_parts), 5)):
        safe_data[i] = data_parts[i]

    # --- 🕒 AJUSTE HORARIO CRÍTICO (UTC+5:30) ---
    offset = timedelta(hours=5, minutes=30)
    fecha_local = message.date + offset
    # ---------------------------------------------------------------

    payload = {
        "id": message.id,
        "date": str(fecha_local),
        "brand": final_brand,
        "type": safe_data[0],
        "extra1": safe_data[1],
        "extra2": safe_data[2],
        "extra3": safe_data[3],
        "extra4": safe_data[4]
    }

    try:
        supabase.table("messages").upsert(payload).execute()
        # Log visual con la hora local para confirmar en Render
        print(f"⚡ [SYNC] {final_brand} | {safe_data[0] or 'N/A'} | {fecha_local.strftime('%Y-%m-%d %H:%M:%S')} (Local)")
        return True
    except Exception as e:
        print(f"❌ Error BD: {e}")
        return False

# --- FASE 1: CATCH-UP ---
async def catch_up_historico():
    print("🔄 FASE 1: Recuperando historial de las últimas 24h...")
    
    # Calculamos la fecha límite (hace 24 horas)
    fecha_limite = datetime.now(timezone.utc) - timedelta(hours=24)
    count = 0
    
    # Iteramos manualmente y frenamos si la fecha es vieja
    async for message in client.iter_messages(TARGET_GROUP):
        if message.date < fecha_limite:
            print("⏹ Límite de 24h alcanzado. Deteniendo recuperación histórica.")
            break
            
        parse_and_save(message)
        count += 1
        if count % 50 == 0: print(f"   ... procesados {count} históricos")
    
    print(f"✅ FASE 1 COMPLETADA. Historial sincronizado ({count} msgs).")

# --- FASE 2: TIEMPO REAL ---
@client.on(events.NewMessage(chats=TARGET_GROUP))
async def handler_nuevo_mensaje(event):
    print("🔔 Nuevo mensaje detectado en tiempo real:")
    parse_and_save(event.message)

# --- MAIN ---
if __name__ == "__main__":
    # 1. Arrancar servidor Flask en segundo plano (Keep-Alive)
    keep_alive()
    
    print("🚀 INICIANDO BOT CON AJUSTE HORARIO (UTC+5:30) Y SERVIDOR WEB")
    
    # 2. Conectar cliente
    client.start()
    
    # 3. Ejecutar recuperación de historial
    try:
        client.loop.run_until_complete(catch_up_historico())
    except Exception as e:
        print(f"⚠️ Error no crítico en historial: {e}")
    
    # 4. Mantenerse escuchando nuevos eventos
    print("👂 FASE 2: Escuchando nuevos mensajes en tiempo real...")
    client.run_until_disconnected()
