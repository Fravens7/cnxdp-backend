import os
import asyncio
import re
from datetime import datetime, timezone, timedelta
from telethon import TelegramClient
from supabase import create_client

# --- CONFIGURACIÓN DE ENTORNO (Render) ---
api_id = int(os.environ.get("TELEGRAM_API_ID"))
api_hash = os.environ.get("TELEGRAM_API_HASH")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TARGET_GROUP = int(os.environ.get("TARGET_GROUP", "-1002520693250"))

# --- CONFIGURACIÓN LÓGICA ---
BRANDS = ["M1", "B1", "M2", "K1", "B2", "B3", "B4"]
SYSTEM_KEYWORDS = [
    "SYSTEM", "SYS APP", "AUTO SYS", "AUTO APP", "APPROVED TEAM", "TEST",
    "AUTO SETTLE", "SETTLE", "CANCELLED", "REJECTED", "SUCCESS", "DONE",
    "WITHDRAW ALREADY", "NOTE TEAM", "ALL PENDING", "ALREADY", "PLS", "PLEASE",
    "CAX", "CANCEL", "@"
]

# Inicializar clientes
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
client = TelegramClient("session_render", api_id, api_hash)

def limpiar_parte(texto):
    """Limpia markdown (*, _), espacios y caracteres invisibles"""
    if not texto: return None
    texto = re.sub(r'[*_~`]', '', texto) # Eliminar markdown
    texto = " ".join(texto.split())      # Eliminar espacios múltiples/nulos
    return texto.strip()

async def procesar_mensajes():
    print(f"🔄 Iniciando ciclo de sincronización...")
    
    # Rango: Miramos las últimas 24 horas para asegurar cobertura de 'hoy'
    # Esto corrige cualquier retraso si el servidor se reinicia.
    fecha_limite = datetime.now(timezone.utc) - timedelta(hours=24)
    
    batch_data = []
    total_procesados = 0
    
    # Iteramos mensajes desde hace 24 horas hasta ahora
    async for message in client.iter_messages(TARGET_GROUP, min_date=fecha_limite):
        if not message.text: continue

        texto_bruto = message.text
        
        # --- LÓGICA DE LIMPIEZA (Tu lógica maestra) ---
        raw_parts = re.split(r'[|\n\\]+', texto_bruto)
        parts = [limpiar_parte(p) for p in raw_parts if limpiar_parte(p)]

        if not parts: continue

        final_brand = "Otros"
        data_parts = []

        # 1. Detección de Sistema
        es_sistema = False
        for part in parts:
            part_upper = part.upper()
            if any(k in part_upper for k in SYSTEM_KEYWORDS):
                es_sistema = True
                break
        
        if es_sistema:
            final_brand = "SYSTEM"
            data_parts = parts
        else:
            # 2. Búsqueda de Marca
            marca_encontrada = False
            for i, parte in enumerate(parts):
                p_upper = parte.upper()
                
                # Coincidencia Exacta
                if p_upper in BRANDS:
                    final_brand = p_upper
                    marca_encontrada = True
                    data_parts = parts[:i] + parts[i+1:]
                    break
                
                # Coincidencia con Prefijo (solo en la primera parte)
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

        # 3. Preparar Payload
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
        
        batch_data.append(payload)
        total_procesados += 1

    # --- GUARDADO EN LOTE (Más eficiente para Render) ---
    if batch_data:
        try:
            # upsert maneja duplicados automáticamente por el ID
            supabase.table("messages").upsert(batch_data).execute()
            print(f"✅ Sincronizados {len(batch_data)} mensajes recientes.")
        except Exception as e:
            print(f"❌ Error guardando en Supabase: {e}")
    else:
        print("💤 No hay mensajes nuevos en las últimas 24h.")

async def main_loop():
    print("🚀 Bot iniciado en Render.")
    await client.start()
    
    while True:
        try:
            await procesar_mensajes()
        except Exception as e:
            print(f"⚠️ Error en el ciclo principal: {e}")
        
        # Esperar 2 minutos antes de volver a revisar (tiempo real)
        print("⏳ Esperando 2 minutos...")
        await asyncio.sleep(120)

if __name__ == "__main__":
    client.loop.run_until_complete(main_loop())
