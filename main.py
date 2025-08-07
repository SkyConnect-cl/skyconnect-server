from fastapi import FastAPI, Request
from shapely.geometry import Point, Polygon
from utils.database import supabase
import traceback
from datetime import datetime, timezone
from httpx import AsyncClient
from dotenv import load_dotenv
import os

app = FastAPI()

load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
async def consulta_google_geolocation(wifi_access_points):
    url = f"https://www.googleapis.com/geolocation/v1/geolocate?key={GOOGLE_API_KEY}"
    payload = {
        "considerIp": "false",
        "wifiAccessPoints": wifi_access_points
    }
    async with AsyncClient() as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()

@app.post("/ttn-webhook")
async def recibir_datos_ttn(request: Request):
    try:
        data = await request.json()
        ahora_utc = datetime.now(timezone.utc)

        device_id = data.get("end_device_ids", {}).get("device_id", "desconocido")
        decoded_payload = data.get("uplink_message", {}).get("decoded_payload", {})
        messages = decoded_payload.get("messages", [[]])[0]

        latitude = None
        longitude = None
        battery = None
        wifi_aps = []

        for msg in messages:
            tipo = msg.get("type")
            valor = msg.get("measurementValue")
            if tipo == "Latitude":
                latitude = valor
            elif tipo == "Longitude":
                longitude = valor
            elif tipo == "Battery":
                battery = valor
            elif tipo == "Wi-Fi Scan":
                wifi_aps = [
                    {"macAddress": ap["mac"], "signalStrength": int(ap["rssi"])}
                    for ap in valor
                ]

        rx_metadata = data.get("uplink_message", {}).get("rx_metadata", [])
        snr = rx_metadata[0].get("snr") if rx_metadata else None
        rssi = rx_metadata[0].get("rssi") if rx_metadata else None

        # Si no hay lat/lon pero hay wifi, consulta a Google
        if (latitude is None or longitude is None) and wifi_aps:
            try:
                geo_resp = await consulta_google_geolocation(wifi_aps)
                latitude = geo_resp.get("location", {}).get("lat")
                longitude = geo_resp.get("location", {}).get("lng")
            except Exception as e:
                print(f"Error al consultar Google Geolocation: {e}")
                traceback.print_exc()

        if latitude is None or longitude is None:
            print("No hay coordenadas disponibles, no se procesa el registro")
            return {"status": "ok", "mensaje": "No coordenadas"}

        datos = supabase.table("device").select("empresas(geocercas)").eq("device_id", device_id).single().execute()
        if datos.error or not datos.data or 'empresas' not in datos.data or 'geocercas' not in datos.data['empresas']:
            print(f"No se encontró geocerca para el dispositivo {device_id}")
            return {"status": "ok", "mensaje": "No geocerca"}

        poligono = Polygon(datos.data['empresas']['geocercas'])
        punto = Point(longitude, latitude)

        if poligono.contains(punto):
            print(f"Está dentro del perímetro {latitude} - {longitude}")

            # Inserta historial
            supabase.table('device_position_history').insert({
                "device_id": device_id,
                "battery": battery,
                "rssi": rssi,
                "snr": snr,
                "lat": latitude,
                "lon": longitude,
                "timestamp": ahora_utc.isoformat()
            }).execute()

            # Actualiza o inserta posición actual
            existe = supabase.table('device_position').select("*").eq("device_id", device_id).execute()
            if existe.data:
                supabase.table("device_position").update({
                    "battery": battery,
                    "last_seen": ahora_utc.isoformat(),
                    "rssi": rssi,
                    "snr": snr,
                    "lat": latitude,
                    "lon": longitude
                }).eq("device_id", device_id).execute()
            else:
                supabase.table("device_position").insert({
                    "device_id": device_id,
                    "last_seen": ahora_utc.isoformat(),
                    "battery": battery,
                    "rssi": rssi,
                    "snr": snr,
                    "type": "Gps",
                    "dev_eui": device_id.upper(),
                    "lat": latitude,
                    "lon": longitude
                }).execute()
        else:
            print("Fuera del perímetro")
            supabase.table('alertas').insert({
                "desc": f"El dispositivo {device_id} está fuera del perímetro, calibrando GPS",
                "type": "notify",
                "timestamp": ahora_utc.isoformat()
            }).execute()

        return {"status": "ok"}

    except ValueError as ve:
        print(f"ValueError capturado: {ve}")
        traceback.print_exc()
        return {"status": "ok", "mensaje": f"Error ValueError: {ve}"}
    except Exception as e:
        print(f"Error capturado: {e}")
        traceback.print_exc()
        return {"mensaje": "Error interno, pero recibido"}
