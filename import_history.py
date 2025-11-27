import os
import asyncio
import re
import threading
from http.server import SimpleHTTPRequestHandler, HTTPServer
from datetime import datetime, timezone, timedelta
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from supabase import create_client

# --- CONFIGURACIÓN DE ENTORNO ---
api_id = int(os.environ.get("TELEGRAM_API_ID"))
api_hash = os.environ.get("TELEGRAM_API_HASH")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TARGET_GROUP = int(os.environ.get("TARGET_GROUP", "-1002520693250"))
SESSION_STRING = os.environ.get("TELEGRAM_SESSION")

# --- TRUCO RENDER: SERVIDOR FANTASMA ---
def start_dummy_server():
    port = int(os.environ.get("PORT", 8080))
    try:
        server = HTTPServer(("", port), SimpleHTTPRequestHandler)
        print(f"🌍 Servidor Dummy activo en puerto {port}")
        server.serve_forever()
    except Exception as e:
        print(f"⚠️ Error dummy server: {e}")

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

# --- FUNCIÓN DE LIMPIEZA Y PROCESAMIENTO ---
def parse_and_save(message):
    """Procesa un solo mensaje y lo guarda en Supabase"""
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

    # 2. Detección de Sistema
    es_sistema = False
    for part in parts:
        if any(k in part.upper() for k in SYSTEM_KEYWORDS):
            es_sistema = True
            break
    
    if es_sistema:
        final_brand = "SYSTEM"
        data_parts = parts
    else:
        # 3. Detección de Marca
        marca_encontrada = False
        for i, parte in enumerate(parts):
            p_upper = parte.upper()
            if p_upper in BRANDS:
                final_brand = p_upper
                marca_encontrada = True
                data_parts = parts[:i] + parts[i+1:]
                break
            
            if i == 0:
                for b in BRANDS:
                    if re.match(rf"^{b}(\s|-|/|$)", p_upper):
                        final_brand = b
                        marca_encontrada = True
                        resto = parts[i][len(b):].lstrip(" -/")
                        if resto:
                            data_parts = [resto] + parts[1:]
                        else:
                            data_parts = parts[1:]
                        break
                if marca_encontrada: break

        if not marca_encontrada:
            final_brand = "Otros"
            data_parts = parts

    # 4. Preparar Payload
    safe_data = [None] * 5
    for i in range(min(len(data_parts), 5)):
        safe_data[i] = data_parts[i]

    payload = {
        "id": message.id,
        "date": str(message.date),
        "brand": final_brand,
        "type": safe_data[0],
        "extra1": safe_data[1],
        "extra2": safe_data[2],
        "extra3": safe_data[3],
        "extra4": safe_data[4]
    }

    try:
        supabase.table("messages").upsert(payload).execute()
        # Log corto para ver actividad en tiempo real
        print(f"⚡ [SYNC] {final_brand} | {safe_data[0] or 'N/A'} | {message.date}")
        return True
    except Exception as e:
        print(f"❌ Error BD: {e}")
        return False

# --- FASE 1: CATCH-UP (CORREGIDA) ---
async def catch_up_historico():
    print("🔄 FASE 1: Recuperando historial de las últimas 24h...")
    
    # Calculamos la fecha límite (hace 24 horas)
    fecha_limite = datetime.now(timezone.utc) - timedelta(hours=24)
    count = 0
    
    # CORRECCIÓN: Quitamos min_date y filtramos MANUALMENTE
    async for message in client.iter_messages(TARGET_GROUP):
        # Si el mensaje es más viejo que 24h, detenemos el bucle
        if message.date < fecha_limite:
            print("⏹ Límite de 24h alcanzado. Deteniendo recuperación histórica.")
            break
            
        # Si es reciente, lo procesamos
        parse_and_save(message)
        count += 1
        if count % 50 == 0: print(f"   ... procesados {count} históricos")
    
    print(f"✅ FASE 1 COMPLETADA. Historial sincronizado ({count} msgs).")

# --- FASE 2: TIEMPO REAL (Event Listener) ---
@client.on(events.NewMessage(chats=TARGET_GROUP))
async def handler_nuevo_mensaje(event):
    # Esta función se dispara AUTOMÁTICAMENTE cuando llega un mensaje nuevo
    print("🔔 Nuevo mensaje detectado en tiempo real:")
    parse_and_save(event.message)

# --- MAIN ---
if __name__ == "__main__":
    # 1. Arrancar servidor dummy para Render
    threading.Thread(target=start_dummy_server, daemon=True).start()
    
    print("🚀 INICIANDO BOT EN MODO HÍBRIDO (Catch-up + Realtime)")
    
    # 2. Conectar cliente
    client.start()
    
    # 3. Ejecutar recuperación de historial (solo una vez al inicio)
    try:
        client.loop.run_until_complete(catch_up_historico())
    except Exception as e:
        print(f"⚠️ Error no crítico en historial: {e}")
    
    # 4. Mantenerse escuchando nuevos eventos para siempre
    print("👂 FASE 2: Escuchando nuevos mensajes en tiempo real...")
    client.run_until_disconnected()
