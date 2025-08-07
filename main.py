from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from shapely.geometry import Point, Polygon
from utils.database import supabase
import traceback
from datetime import datetime, timezone
from httpx import AsyncClient
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()
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
        macs = []
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
                macs.append({
                    "macAddress": msg.get("mac"),
                    "signalStrength": msg.get("rssi")
                })
        rx_metadata = data.get("uplink_message", {}).get("rx_metadata", [])
        snr = rx_metadata[0].get("snr") if rx_metadata else None
        rssi = rx_metadata[0].get("rssi") if rx_metadata else None

        if len(macs) > 0:
            response = await consulta_google_geolocation(macs)
            latitude = response["location"]["lat"]
            longitude = response["location"]["lon"]
        datos = supabase.table("device").select("empresas(geocercas)").eq("device_id", device_id).single().execute()
        poligono = Polygon(datos.data['empresas']['geocercas'])
        punto = Point(longitude, latitude)
        if poligono.contains(punto):
            print(f"Está dentro del perímetro {latitude} - {longitude}")
            history_response = supabase.table('device_position_history').insert({
                "device_id":device_id,
                "battery":battery,
                "rssi":rssi,
                "snr":snr,
                "lat":latitude,
                "lon":longitude
            }).execute()
            existe = supabase.table('device_position').select("*").eq("device_id", device_id).execute()
            if existe.data:
                response = supabase.table("device_position").update({
                "battery": battery,
                "last_seen": ahora_utc.isoformat(),
                "rssi": rssi,
                "snr":snr,
                "lat":latitude,
                "lon":longitude}).eq("device_id", device_id).execute()
                print(f"Resultado de UPDATE: {response}")
                return ""
            else:
                response = supabase.table("device_position").insert({
                    "device_id": device_id,
                    "last_seen":ahora_utc.isoformat(),
                    "battery": battery,
                    "rssi": rssi,
                    "snr":snr,
                    "type":"Gps",
                    "dev_eui": device_id.upper(),
                    "lat":latitude,
                    "lon":longitude}).execute()
                print(f"Resultado de UPDATE: {response}")
                return "" 
            
        else:
            print("Fuera del perimetro ahhhhhhhhhhhhhhhhhhh")
            alert_response = supabase.table('alertas').insert({
                "desc": f"El dispositivo {device_id} esta fuera del perimetro, calibrando GPS",
                "type":"notify"
            }).execute()
        
    except ValueError as ve:
        print(str(ve))
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        print(f"Error capturado: {e}")      # Muestra el mensaje del error
        traceback.print_exc()                # Imprime la traza completa del error en consola
        return {"mensaje": "Error interno, pero recibido"}
