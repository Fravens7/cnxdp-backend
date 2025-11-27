import os
import asyncio
import re
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient
from supabase import create_client

# --- 1. CONFIGURACIÓN DE ENTORNO (CLOUD) ---
# Intentamos leer de variables de entorno (Render), si no existen, usa valores por defecto o lanza error
api_id = os.environ.get("TELEGRAM_API_ID")
api_hash = os.environ.get("TELEGRAM_API_HASH")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TARGET_GROUP = int(os.environ.get("TARGET_GROUP_ID", "-1002520693250")) # Tu grupo por defecto

# Validación básica
if not api_id or not api_hash:
    # Si estás probando local, puedes descomentar y poner tus claves aquí temporalmente
    # api_id = 32076891
    # api_hash = "..."
    print("⚠️ ADVERTENCIA: Faltan variables de entorno TELEGRAM_API_ID o HASH")

# Conversión a entero para Telethon
try:
    api_id = int(api_id)
except:
    pass

# --- 2. LISTAS DE DETECCIÓN INTELIGENTE ---
BRANDS = ["M1", "B1", "M2", "K1", "B2", "B3", "B4"]

SYSTEM_KEYWORDS = [
    "SYSTEM", "SYS APP", "AUTO SYS", "AUTO APP", "APPROVED TEAM", "TEST",
    "AUTO SETTLE", "SETTLE", "CANCELLED", "REJECTED", "SUCCESS", "DONE", 
    "WITHDRAW ALREADY", "NOTE TEAM", "ALL PENDING", "ALREADY", "PLS", "PLEASE",
    "CAX", "CANCEL", "@"
]

# --- 3. CONEXIÓN ---
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
# "session" buscará el archivo session.session en la carpeta de Render
client = TelegramClient("session", api_id, api_hash)

# --- 4. FUNCIONES DE LIMPIEZA ---
def limpiar_parte(texto):
    """Limpia markdown (*, _), espacios y caracteres invisibles"""
    if not texto: return None
    texto = re.sub(r'[*_~`]', '', texto) # Quitar Markdown
    texto = " ".join(texto.split())      # Quitar espacios dobles/saltos
    return texto.strip()

async def sincronizar_hoy():
    print(f"📂 Iniciando cliente Telegram...")
    await client.start()

    try:
        # --- 5. LÓGICA DE FECHAS DINÁMICAS (CRON) ---
        # Calculamos el rango "Ahora" vs "Hace 2 días"
        ahora = datetime.now(timezone.utc)
        hace_dos_dias = ahora - timedelta(days=2)
        
        print(f"🔗 Conectando al grupo {TARGET_GROUP}...")
        entity = await client.get_entity(TARGET_GROUP)
        print(f"✅ Grupo detectado: '{entity.title}'")
        
        print(f"⏳ Buscando mensajes desde: {hace_dos_dias.strftime('%Y-%m-%d %H:%M')} hasta Ahora")

        stats = {"procesados": 0, "actualizados": 0, "rescatados": 0, "sistema": 0}

        # Iteramos solo los mensajes recientes
        async for message in client.iter_messages(entity):
            # Filtro de fecha (Optimización crítica)
            if not message.date: continue
            
            # Si el mensaje es más viejo que 2 días, PARAMOS el script.
            # Esto hace que el Cron sea rápido y eficiente.
            if message.date < hace_dos_dias:
                print(f"⏹ Límite de tiempo alcanzado ({message.date}). Finalizando ejecución.")
                break 

            if not message.text: continue

            # --- 6. PROCESAMIENTO Y LIMPIEZA (Igual que tu versión local) ---
            texto_bruto = message.text
            raw_parts = re.split(r'[|\n\\]+', texto_bruto)
            parts = [limpiar_parte(p) for p in raw_parts if limpiar_parte(p)]

            if not parts: continue

            final_brand = "Otros"
            data_parts = []

            # Detección Sistema
            es_sistema = False
            for part in parts:
                if any(k in part.upper() for k in SYSTEM_KEYWORDS):
                    es_sistema = True
                    break
            
            if es_sistema:
                final_brand = "SYSTEM"
                data_parts = parts
                stats["sistema"] += 1
            else:
                # Detección Marcas
                marca_encontrada = False
                for i, parte in enumerate(parts):
                    p_upper = parte.upper()
                    
                    if p_upper in BRANDS:
                        final_brand = p_upper
                        marca_encontrada = True
                        data_parts = parts[:i] + parts[i+1:]
                        if i > 0: stats["rescatados"] += 1 
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

            # --- 7. UPSERT A SUPABASE ---
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
                stats["actualizados"] += 1
            except Exception as e:
                print(f"❌ Error Supabase ID {message.id}: {e}")

            stats["procesados"] += 1

        print("\n" + "="*40)
        print(f"✅ CRON JOB FINALIZADO CON ÉXITO")
        print(f"📊 Revisados (últimos 2 días): {stats['procesados']}")
        print(f"💾 Guardados/Actualizados: {stats['actualizados']}")
        print(f"✨ Marcas Rescatadas: {stats['rescatados']}")
        print(f"🤖 Sistema Detectado: {stats['sistema']}")
        print("="*40)

    except Exception as e:
        print(f"❌ Error Crítico en el Job: {e}")

    await client.disconnect()

if __name__ == "__main__":
    client.loop.run_until_complete(sincronizar_hoy())
