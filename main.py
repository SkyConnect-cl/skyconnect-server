# main.py
from fastapi import FastAPI, Request, HTTPException, Query
from shapely.geometry import Point, Polygon
from utils.database import supabase
from datetime import datetime, timezone
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
import traceback
import json
import hashlib, hmac, base64, requests, os
from email.utils import formatdate
from typing import Any, List, Dict, Optional


load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],   # ajusta según tu frontend
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================== FUNCIONES AUXILIARES ==================

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def get_geocerca(device_id):
    datos = supabase.table("device").select("empresas(geocercas)").eq("device_id", device_id).single().execute()
    geocerca_raw = datos.data["empresas"]["geocercas"]
    poligono = Polygon(geocerca_raw)
    return poligono

# ================== TTN WEBHOOK ==================

@app.post("/ttn-webhook")
async def recibir_datos_ttn(request: Request):
    try:
        data = await request.json()
        ahora_utc = datetime.now(timezone.utc)

        device_id = data.get("end_device_ids", {}).get("device_id")
        if not device_id:
            raise HTTPException(status_code=400, detail="No se encontró device_id")

        uplink = data.get("uplink_message", {}) or {}
        decoded_payload = uplink.get("decoded_payload", {}) or {}

        messages = decoded_payload.get("messages", [])
        if messages and isinstance(messages[0], list):
            messages = messages[0]

        latitude = None
        longitude = None
        battery = None

        for msg in messages:
            tipo = msg.get("type")
            valor = msg.get("measurementValue")
            if tipo == "Latitude":
                latitude = valor
            elif tipo == "Longitude":
                longitude = valor
            elif tipo == "Battery":
                battery = valor

        rx_metadata = uplink.get("rx_metadata", []) or []
        snr = rx_metadata[0].get("snr") if rx_metadata else None
        rssi = rx_metadata[0].get("rssi") if rx_metadata else None

        # ===== GNSS =====
        if latitude is not None and longitude is not None:
            try:
                lon = float(longitude)
                lat = float(latitude)
            except:
                raise HTTPException(status_code=400, detail="Coordenadas inválidas")

            poligono = get_geocerca(device_id)
            punto = Point(lon, lat)

            if poligono.contains(punto):
                print(f"[POS] {device_id} dentro del perímetro ({lat}, {lon})")

                supabase.table('device_position_history').insert({
                    "device_id": device_id,
                    "battery": battery,
                    "rssi": rssi,
                    "snr": snr,
                    "lat": lat,
                    "lon": lon,
                    "observed_at": ahora_utc.isoformat()
                }).execute()

                existe = supabase.table('device_position').select("*").eq("device_id", device_id).execute()
                data_pos = {
                    "battery": battery,
                    "last_seen": ahora_utc.isoformat(),
                    "rssi": rssi,
                    "snr": snr,
                    "lat": lat,
                    "lon": lon
                }
                if existe.data:
                    supabase.table("device_position").update(data_pos).eq("device_id", device_id).execute()
                else:
                    data_pos.update({
                        "device_id": device_id,
                        "type": "Gps",
                        "dev_eui": device_id.upper()
                    })
                    supabase.table("device_position").insert(data_pos).execute()
            else:
                print(f"[POS] {device_id} fuera del perímetro (GNSS)")
                supabase.table('alertas').insert({
                    "desc": f"El dispositivo {device_id} está fuera del perímetro (GNSS)",
                    "type": "notify",
                    "created_at": ahora_utc.isoformat(),
                    "resumen": f"{device_id} fuera de perimetro",
                    "guilty": "Tracker",
                    "coords": [lat,lon]
                }).execute()
            return {"status": "ok"}

        # ===== BLE =====
        ble_hits = []
        for msg in messages:
            tipo = msg.get("type", "")
            if "BLE" in tipo.upper():
                valores = msg.get("measurementValue") or []
                for v in valores:
                    mac = v.get("mac") or v.get("id")
                    if mac:
                        try:
                            rssi_ble = int(v.get("rssi")) if v.get("rssi") else None
                        except:
                            rssi_ble = None
                        ble_hits.append({"mac": mac, "rssi": rssi_ble})

        if ble_hits:
            ble_hits.sort(key=lambda h: (h["rssi"] if h["rssi"] is not None else -9999), reverse=True)
            lat_ble = lon_ble = beacon_mac = None

            for hit in ble_hits:
                mac = hit["mac"]
                res = supabase.table("beacons").select("lat, lon").eq("mac", mac).limit(1).execute()
                row = res.data[0] if res.data else None
                if row and row.get("lat") and row.get("lon"):
                    lat_ble = float(row["lat"])
                    lon_ble = float(row["lon"])
                    beacon_mac = mac
                    break

            if lat_ble and lon_ble:
                poligono = get_geocerca(device_id)
                punto = Point(lon_ble, lat_ble)

                if poligono.contains(punto):
                    print(f"[POS] {device_id} dentro del perímetro (BLE→{beacon_mac}) ({lat_ble}, {lon_ble})")

                    supabase.table('device_position_history').insert({
                        "device_id": device_id,
                        "battery": battery,
                        "rssi": rssi,
                        "snr": snr,
                        "lat": lat_ble,
                        "lon": lon_ble,
                        "observed_at": ahora_utc.isoformat()
                    }).execute()

                    existe = supabase.table('device_position').select("*").eq("device_id", device_id).execute()
                    data_pos = {
                        "battery": battery,
                        "last_seen": ahora_utc.isoformat(),
                        "rssi": rssi,
                        "snr": snr,
                        "lat": lat_ble,
                        "lon": lon_ble
                    }
                    if existe.data:
                        supabase.table("device_position").update(data_pos).eq("device_id", device_id).execute()
                    else:
                        data_pos.update({
                            "device_id": device_id,
                            "type": "Gps",
                            "dev_eui": device_id.upper()
                        })
                        supabase.table("device_position").insert(data_pos).execute()
                else:
                    print(f"[POS] {device_id} fuera del perímetro (BLE→{beacon_mac})")
                    supabase.table('alertas').insert({
                        "desc": f"El dispositivo {device_id} está fuera del perímetro (BLE→{beacon_mac})",
                        "type": "notify",
                        "created_at": ahora_utc.isoformat(),
                        "resumen": f"{device_id} fuera de perimetro",
                        "guilty": "Tracker",
                        "coords": [lat,lon]
                    }).execute()
                return {"status": "ok"}

            print(f"[BLE] {device_id} sin match en beacons, actualizando heartbeat.")
            base = {
                "battery": battery,
                "last_seen": ahora_utc.isoformat(),
                "rssi": rssi,
                "snr": snr
            }
            existe = supabase.table('device_position').select("*").eq("device_id", device_id).execute()
            if existe.data:
                supabase.table("device_position").update(base).eq("device_id", device_id).execute()
            else:
                base.update({
                    "device_id": device_id,
                    "type": "Gps",
                    "dev_eui": device_id.upper()
                })
                supabase.table("device_position").insert(base).execute()
            return {"status": "ok"}

        raise HTTPException(status_code=400, detail="Faltan coordenadas o BLE en el payload")

    except Exception as e:
        print(f"[ERR] /ttn-webhook: {e}")
        traceback.print_exc()
        return {"mensaje": "Error interno, pero recibido"}

# ================== ABEE TTN ==================

@app.post("/abee-ttn")
async def abee_ttn(request: Request):
    try:
        data = await request.json()
        ahora_utc = datetime.now(timezone.utc)

        # === Identificadores ===
        end_ids = data.get("end_device_ids") or data.get("data", {}).get("end_device_ids") or {}
        device_id = end_ids.get("device_id")
        dev_eui = end_ids.get("dev_eui") or end_ids.get("devEui")
        if not device_id:
            raise HTTPException(status_code=400, detail="No se encontró device_id")

        # === Uplink y payload ===
        uplink = (data.get("uplink_message") or data.get("data", {}).get("uplink_message") or {})
        decoded = uplink.get("decoded_payload") or {}

        # === BLE primero (prioridad) ===
        ble_list = decoded.get("ble") or []
        battery_percent = decoded.get("battery_percent")

        # Señal LoRaWAN
        rx_metadata = uplink.get("rx_metadata") or []
        rssi = rx_metadata[0].get("rssi") if rx_metadata else None
        snr = rx_metadata[0].get("snr") if rx_metadata else None

        # --- Si trae BLE, ignoramos GNSS ---
        if ble_list:
            print(f"[BLE] {device_id} detectó {len(ble_list)} balizas")

            # Lista de hits BLE
            ble_hits = []
            for b in ble_list:
                mac = b.get("id")
                if mac:
                    try:
                        rssi_val = int(b.get("rssi"))
                    except:
                        rssi_val = None
                    ble_hits.append({"mac": mac, "rssi": rssi_val})

            # Ordenar por RSSI descendente
            ble_hits.sort(key=lambda h: (h["rssi"] if h["rssi"] is not None else -9999), reverse=True)

            # Buscar beacon más cercano
            lat_ble = lon_ble = beacon_mac = None
            for hit in ble_hits:
                mac = hit["mac"]
                res = supabase.table("beacons").select("lat, lon").eq("mac", mac).limit(1).execute()
                row = res.data[0] if res.data else None
                if row and row.get("lat") is not None and row.get("lon") is not None:
                    lat_ble = float(row["lat"])
                    lon_ble = float(row["lon"])
                    beacon_mac = mac
                    break

            if lat_ble and lon_ble:
                # Verificar geocerca
                datos = supabase.table("device").select("empresas(geocercas)").eq("device_id", device_id).single().execute()
                geocerca_raw = datos.data['empresas']['geocercas']
                if isinstance(geocerca_raw, str):
                    geocerca_raw = json.loads(geocerca_raw)
                poligono = Polygon(geocerca_raw)
                punto = Point(lon_ble, lat_ble)

                if poligono.contains(punto):
                    print(f"[POS] {device_id} dentro del perímetro (BLE→{beacon_mac}) ({lat_ble}, {lon_ble})")

                    supabase.table('device_position_history').insert({
                        "device_id": device_id,
                        "battery": battery_percent,
                        "rssi": rssi,
                        "snr": snr,
                        "lat": lat_ble,
                        "lon": lon_ble,
                        "observed_at": ahora_utc.isoformat()
                    }).execute()

                    data_pos = {
                        "battery": battery_percent,
                        "last_seen": ahora_utc.isoformat(),
                        "rssi": rssi,
                        "snr": snr,
                        "lat": lat_ble,
                        "lon": lon_ble
                    }
                    existe = supabase.table('device_position').select("device_id").eq("device_id", device_id).execute()
                    if existe.data:
                        supabase.table("device_position").update(data_pos).eq("device_id", device_id).execute()
                    else:
                        data_pos.update({
                            "device_id": device_id,
                            "type": "Gps",
                            "dev_eui": (dev_eui or device_id).upper()
                        })
                        supabase.table("device_position").insert(data_pos).execute()
                else:
                    print(f"[POS] {device_id} fuera del perímetro (BLE→{beacon_mac})")
                    supabase.table('alertas').insert({
                        "desc": f"El dispositivo {device_id} está fuera del perímetro (BLE→{beacon_mac})",
                        "type": "notify",
                        "created_at": ahora_utc.isoformat()
                    }).execute()
                return {"status": "ok"}

            print(f"[BLE] {device_id} sin coincidencias en tabla beacons. {mac}")
            return {"status": "ok"}

        # === Si NO hay BLE, usar GNSS ===
        loc = (uplink.get("locations") or {}).get("frm-payload") or {}
        lat = loc.get("latitude")
        lon = loc.get("longitude")

        if lat is not None and lon is not None:
            datos = supabase.table("device").select("empresas(geocercas)").eq("device_id", device_id).single().execute()
            geocerca_raw = datos.data['empresas']['geocercas']
            if isinstance(geocerca_raw, str):
                geocerca_raw = json.loads(geocerca_raw)
            poligono = Polygon(geocerca_raw)
            punto = Point(float(lon), float(lat))

            if poligono.contains(punto):
                print(f"[POS] {device_id} dentro del perímetro (GNSS) ({lat}, {lon})")
                supabase.table('device_position_history').insert({
                    "device_id": device_id,
                    "battery": battery_percent,
                    "rssi": rssi,
                    "snr": snr,
                    "lat": lat,
                    "lon": lon,
                    "observed_at": ahora_utc.isoformat()
                }).execute()
            else:
                print(f"[POS] {device_id} fuera del perímetro (GNSS)")
                supabase.table('alertas').insert({
                    "desc": f"El dispositivo {device_id} está fuera del perímetro (GNSS)",
                    "type": "notify",
                    "created_at": ahora_utc.isoformat()
                }).execute()
            return {"status": "ok"}

        # === Sin BLE ni GNSS ===
        raise HTTPException(status_code=400, detail="Faltan coordenadas BLE y GNSS en el payload")

    except Exception as e:
        print(f"[ERR] /abee-ttn: {e}")
        traceback.print_exc()
        return {"mensaje": "Error interno, pero recibido"}


# ================== EMQX WEBHOOK ==================

@app.post("/emqx-webhook")
async def emqx_webhook(req: Request):
    data = await req.json()
    try:
        topic_id = data['clientid']
        print(topic_id)
        if data.get('payload'):
            datos = json.loads(data['payload'])
            if "contact" in datos:
                contacto = {"estado": "Cerrado" if datos["contact"] else "Abierto", "battery": datos.get('battery')}
                supabase.table("tower_value").update({"sensor_apertura": contacto}).eq("client_id", topic_id).execute()
                print(contacto)
            elif "illuminance" in datos:
                contacto = {"iluminancia": datos["illuminance"], "battery": datos.get('battery')}
                supabase.table("tower_value").update({"sensor_luz": contacto}).eq("client_id", topic_id).execute()
                print(contacto)
            return {"ok": True}
    except Exception as e:
        print(f"Error EMQX: {e}")

# ================== INVERTER (SOLISCLOUD) ==================

API_ID = os.getenv("API_ID")
API_SECRET = os.getenv("API_SECRET").strip()
BASE = os.getenv("BASE")
INV_ID = os.getenv("INV_ID")

def sign_headers(path: str, body_str: str):
    md5 = base64.b64encode(hashlib.md5(body_str.encode()).digest()).decode()
    ct = "application/json"
    date = formatdate(timeval=None, localtime=False, usegmt=True)
    s2s = f"POST\n{md5}\n{ct}\n{date}\n{path}"
    sig = base64.b64encode(hmac.new(API_SECRET.encode(), s2s.encode(), hashlib.sha1).digest()).decode()
    return {"Content-MD5": md5, "Content-Type": ct, "Date": date, "Authorization": f"API {API_ID}:{sig}"}

def post(path: str, body: dict):
    body_str = json.dumps(body, separators=(',', ':'))
    r = requests.post(f"{BASE}{path}", headers=sign_headers(path, body_str), data=body_str, timeout=20)
    r.raise_for_status()
    return r.json()

@app.get("/api/inverter")
def inverter(sn: str = Query(INV_ID, description="Número de serie del inversor")):
    detail = post("/v1/api/inverterDetail", {"sn": sn})
    inv = detail.get("data") or {}

    potencia_kw = inv.get("pac")
    consumo_red_hoy_kwh = inv.get("gridPurchasedTodayEnergy")
    carga_actual_kw = inv.get("familyLoadPower") or inv.get("totalLoadPower")
    bat_charge_kwh = inv.get("batteryTodayChargeEnergy")

    return {
        "sn": sn,
        "potencia_kw": potencia_kw,
        "consumo_red_hoy_kwh": consumo_red_hoy_kwh,
        "carga_actual_kw": carga_actual_kw,
        "bat_charge_kwh": bat_charge_kwh
    }

# ================== HANDLE LIGHT ==================

IOT_URL = os.getenv("IOT_URL")
IOT_USER = os.getenv("IOT_USER")
IOT_PASS = os.getenv("IOT_PASS")

@app.post("/handle-light")
async def handle_light(request: Request):
    data = await request.json()
    state = data.get("state", "").upper()
    if state not in ["ON", "OFF"]:
        return {"error": "Estado inválido"}

    payload = {
        "payload_encoding": "plain",
        "topic": "zigbee2mqtt/smart_switch/set",
        "payload": f'{{"state": "{state}"}}'
    }

    try:
        res = requests.post(
            IOT_URL,
            json=payload,
            auth=(IOT_USER, IOT_PASS),
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        print(f"Luz {state} enviada correctamente")
        return {"status": res.status_code, "response": res.text}
    except Exception as e:
        print(f"Error al enviar luz: {e}")
        return {"error": str(e)}
    

# ================== GPS ==================

def normalize_ignition(raw: Any) -> Optional[bool]:
    """Normaliza ignition a True/False/None."""
    if raw in (True, "true", "TRUE", 1, "1", "on", "ON"):
        return True
    if raw in (False, "false", "FALSE", 0, "0", "off", "OFF"):
        return False
    return None

@app.post("/teltonika-hook")
async def teltonikaHook(request: Request):
    data: Any = await request.json()
    ahora_utc = datetime.now(timezone.utc)

    # Normaliza a lista de mensajes
    if isinstance(data, dict) and "messages" in data and isinstance(data["messages"], list):
        messages: List[Dict] = data["messages"]
    elif isinstance(data, list):
        messages = data
    else:
        return {"ok": False, "reason": "payload format not recognized", "sample": data}

    received = 0

    for msg in messages:
        lat = msg.get("position.latitude")
        lon = msg.get("position.longitude")
        imei = msg.get("ident")

        if imei is None or lat is None or lon is None:
            continue

        # Normaliza coordenadas
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except (TypeError, ValueError):
            continue

        # Ignition normalizado
        ignition = normalize_ignition(msg.get("engine.ignition.status"))

        # Extra payload (puedes agregar más campos si quieres)
        extra_payload = {
            "battery_voltage": msg.get("external.powersource.voltage"),
            "mileage": msg.get("vehicle.mileage"),
            "raw_ignition": msg.get("engine.ignition.status"),
        }

        observed_at = ahora_utc.isoformat()

        # 1) Leer estado actual del vehículo (device_state)
        state_res = (
            supabase.table("vehicle_state")
            .select("device_id, ignition, current_trip_id")
            .eq("device_id", imei)
            .execute()
        )
        device_state = state_res.data[0] if state_res.data else None
        current_trip_id = device_state["current_trip_id"] if device_state else None

        # 2) Lógica: crear/cerrar viaje según ignition
        # Caso A: motor encendido
        if ignition is True:
            # Si no hay trip activo, crear uno
            if current_trip_id is None:
                trip_res = (
                    supabase.table("trips")
                    .insert(
                        {
                            "device_id": imei,
                            "started_at": observed_at,
                            "status": "active",
                        }
                    )
                    .execute()
                )
                current_trip_id = trip_res.data[0]["id"]

            # Upsert device_state (1 fila por IMEI)
            supabase.table("vehicle_state").upsert(
                {
                    "device_id": imei,
                    "ignition": True,
                    "current_trip_id": current_trip_id,
                    "last_seen": observed_at,
                    "last_lat": lat_f,
                    "last_lon": lon_f,
                }
            ).execute()

        # Caso B: motor apagado
        elif ignition is False:
            # Si hay trip activo, cerrarlo
            if current_trip_id is not None:
                supabase.table("trips").update(
                    {
                        "ended_at": observed_at,
                        "status": "closed",
                        "close_reason": "ignition_off",
                    }
                ).eq("id", current_trip_id).execute()

            # Actualizar estado y limpiar trip activo
            supabase.table("vehicle_state").upsert(
                {
                    "device_id": imei,
                    "ignition": False,
                    "current_trip_id": None,
                    "last_seen": observed_at,
                    "last_lat": lat_f,
                    "last_lon": lon_f,
                }
            ).execute()

            # Para guardar el último punto asociado al viaje que se cerró,
            # usamos una variable auxiliar:
            # (guardamos el trip_id anterior en history)
            # OJO: current_trip_id lo vamos a dejar como el anterior solo para history.
            # Después de insertar history, lo dejamos "null" para el resto.
            trip_id_for_history = current_trip_id
            current_trip_id = None  # ya no hay viaje activo

        # Caso C: ignition no viene (None)
        else:
            # No cambiamos el estado; si existe trip activo, lo usamos para history
            trip_id_for_history = current_trip_id

        # Determinar trip_id para guardar en history
        if ignition is False:
            # en apagado, usamos el trip anterior (si existía)
            history_trip_id = trip_id_for_history
        elif ignition is None:
            history_trip_id = trip_id_for_history
        else:
            # ignition True
            history_trip_id = current_trip_id

        # 3) Insertar coordenada en vehicle_position_history (tu tabla nueva)
        supabase.table("vehicle_position_history").insert(
            {
                "device_id": imei,
                "trip_id": history_trip_id,  # puede ser NULL si no hay viaje
                "lat": lat_f,
                "lon": lon_f,
                "observed_at": observed_at,
                "ignition": ignition,
                "extra": extra_payload,
            }
        ).execute()

        # 4) Actualizar posición actual (device_position) como ya lo hacías
        payload_current = {
            "device_id": imei,
            "lat": lat_f,
            "lon": lon_f,
            "last_seen": observed_at,
            "extra": {**extra_payload, "ignition": ignition, "trip_id": history_trip_id},
        }

        existe = (
            supabase.table("device_position")
            .select("device_id")
            .eq("device_id", imei)
            .execute()
        )

        if existe.data:
            supabase.table("device_position").update(payload_current).eq("device_id", imei).execute()
        else:
            payload_current.update(
                {
                    "type": "Vehicle",
                    "dev_eui": str(imei).upper(),
                }
            )
            supabase.table("device_position").insert(payload_current).execute()

        received += 1

    return {"ok": True, "received": received}
