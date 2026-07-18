"""
SmartWatt ingest Function + API Function, in one Function App.

IngestReading: unchanged from before -- IoT Hub trigger, writes readings/alerts.

API endpoints (HTTP triggered):
  GET /api/readings?deviceId=  -> recent readings, optionally filtered by device
  GET /api/alerts              -> recent anomaly alerts
  GET /api/devices             -> device list with zone + rolling average
  GET /api/settings?deviceId=  -> get threshold for a device
  POST /api/settings           -> set threshold for a device (JSON body: {deviceId, thresholdWatts})
"""

import os
import json
import logging
import datetime
import uuid

import azure.functions as func
import requests
from azure.cosmos import CosmosClient

app = func.FunctionApp()

# --- Configuration ---
COSMOS_ENDPOINT = os.environ["COSMOS_ENDPOINT"]
COSMOS_KEY = os.environ["COSMOS_KEY"]
LOGIC_APP_URL = os.environ.get("LOGIC_APP_URL", "")
DEFAULT_THRESHOLD_MULTIPLIER = 3.0
DEFAULT_RATE_EUR_PER_KWH = 0.25

cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
database = cosmos_client.get_database_client("SmartWattDB")
readings_container = database.get_container_client("Readings")
alerts_container = database.get_container_client("Alerts")
devices_container = database.get_container_client("Devices")
settings_container = database.get_container_client("UserSettings")


# ============================================================
# Ingest Function (unchanged logic from Part 3)
# ============================================================

def get_or_create_device(device_id: str) -> dict:
    try:
        return devices_container.read_item(item=device_id, partition_key=device_id)
    except Exception:
        new_device = {
            "id": device_id,
            "deviceId": device_id,
            "userId": device_id,
            "zone": "Unassigned",
            "name": device_id,
            "avgWatts": None,
            "readingCount": 0,
        }
        devices_container.create_item(new_device)
        return new_device


def get_threshold_for_device(device_id: str, fallback_avg: float) -> float:
    try:
        query = "SELECT * FROM c WHERE c.deviceId = @deviceId"
        params = [{"name": "@deviceId", "value": device_id}]
        results = list(settings_container.query_items(
            query=query, parameters=params, enable_cross_partition_query=True
        ))
        if results and "thresholdWatts" in results[0]:
            return float(results[0]["thresholdWatts"])
    except Exception as e:
        logging.warning(f"Could not fetch custom threshold for {device_id}: {e}")

    if fallback_avg is None:
        return 500.0
    return fallback_avg * DEFAULT_THRESHOLD_MULTIPLIER


def update_rolling_average(device_doc: dict, watts: float):
    count = device_doc.get("readingCount", 0)
    current_avg = device_doc.get("avgWatts")
    new_avg = watts if current_avg is None else (current_avg * count + watts) / (count + 1)
    device_doc["avgWatts"] = new_avg
    device_doc["readingCount"] = count + 1
    devices_container.upsert_item(device_doc)


def send_alert_email(device_id: str, watts: float, threshold: float, timestamp: str):
    if not LOGIC_APP_URL:
        logging.warning("LOGIC_APP_URL not configured -- skipping email call.")
        return
    try:
        payload = {"deviceId": device_id, "watts": watts, "threshold": threshold, "timestamp": timestamp}
        response = requests.post(LOGIC_APP_URL, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to call Logic App for {device_id}: {e}")


@app.function_name(name="IngestReading")
@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="%IOTHUB_EVENTHUB_NAME%",
    connection="IOTHUB_EVENTHUB_CONNECTION"
)
def ingest_reading(event: func.EventHubEvent):
    body = event.get_body().decode("utf-8")
    logging.info(f"Received message: {body}")

    try:
        data = json.loads(body)
        device_id = data["deviceId"]
        watts = float(data["watts"])
        timestamp = data.get("timestamp", datetime.datetime.utcnow().isoformat())
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        logging.error(f"Malformed message, skipping: {e}")
        return

    device_doc = get_or_create_device(device_id)
    threshold = get_threshold_for_device(device_id, device_doc.get("avgWatts"))
    is_anomaly = watts > threshold

    reading_doc = {
        "id": str(uuid.uuid4()),
        "deviceId": device_id,
        "timestamp": timestamp,
        "watts": watts,
        "isAnomaly": is_anomaly,
    }
    readings_container.create_item(reading_doc)

    if is_anomaly:
        alert_doc = {
            "id": str(uuid.uuid4()),
            "deviceId": device_id,
            "timestamp": timestamp,
            "watts": watts,
            "threshold": threshold,
        }
        alerts_container.create_item(alert_doc)
        send_alert_email(device_id, watts, threshold, timestamp)
        logging.info(f"ANOMALY detected for {device_id}: {watts}W > {threshold}W")
    else:
        update_rolling_average(device_doc, watts)


# ============================================================
# API Functions (new -- Part 5)
# ============================================================

def _cors_response(body, status_code=200):
    """Helper: JSON response with permissive CORS so the dashboard can call it."""
    return func.HttpResponse(
        json.dumps(body, default=str),
        status_code=status_code,
        mimetype="application/json",
        headers={"Access-Control-Allow-Origin": "*"},
    )


@app.function_name(name="GetReadings")
@app.route(route="readings", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_readings(req: func.HttpRequest) -> func.HttpResponse:
    device_id = req.params.get("deviceId")
    try:
        if device_id:
            query = "SELECT TOP 100 * FROM c WHERE c.deviceId = @deviceId ORDER BY c._ts DESC"
            params = [{"name": "@deviceId", "value": device_id}]
            items = list(readings_container.query_items(
                query=query, parameters=params, enable_cross_partition_query=True
            ))
        else:
            query = "SELECT TOP 200 * FROM c ORDER BY c._ts DESC"
            items = list(readings_container.query_items(
                query=query, enable_cross_partition_query=True
            ))
        return _cors_response(items)
    except Exception as e:
        logging.error(f"GetReadings failed: {e}")
        return _cors_response({"error": str(e)}, 500)


@app.function_name(name="GetAlerts")
@app.route(route="alerts", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_alerts(req: func.HttpRequest) -> func.HttpResponse:
    try:
        query = "SELECT TOP 100 * FROM c ORDER BY c._ts DESC"
        items = list(alerts_container.query_items(
            query=query, enable_cross_partition_query=True
        ))
        return _cors_response(items)
    except Exception as e:
        logging.error(f"GetAlerts failed: {e}")
        return _cors_response({"error": str(e)}, 500)


@app.function_name(name="GetDevices")
@app.route(route="devices", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_devices(req: func.HttpRequest) -> func.HttpResponse:
    try:
        query = "SELECT * FROM c"
        devices = list(devices_container.query_items(
            query=query, enable_cross_partition_query=True
        ))

        settings_query = "SELECT * FROM c"
        all_settings = list(settings_container.query_items(
            query=settings_query, enable_cross_partition_query=True
        ))
        rate_by_device = {s["deviceId"]: s.get("rateEurPerKwh", DEFAULT_RATE_EUR_PER_KWH) for s in all_settings}

        for d in devices:
            rate = rate_by_device.get(d["deviceId"], DEFAULT_RATE_EUR_PER_KWH)
            d["rateEurPerKwh"] = rate
            avg = d.get("avgWatts") or 0
            d["costPerHourEur"] = round((avg / 1000) * rate, 4)
            d.setdefault("zone", "Unassigned")

        return _cors_response(devices)
    except Exception as e:
        logging.error(f"GetDevices failed: {e}")
        return _cors_response({"error": str(e)}, 500)


@app.function_name(name="Settings")
@app.route(route="settings", methods=["GET", "POST"], auth_level=func.AuthLevel.ANONYMOUS)
def settings(req: func.HttpRequest) -> func.HttpResponse:
    try:
        if req.method == "GET":
            device_id = req.params.get("deviceId")
            if not device_id:
                return _cors_response({"error": "deviceId query param required"}, 400)
            query = "SELECT * FROM c WHERE c.deviceId = @deviceId"
            params = [{"name": "@deviceId", "value": device_id}]
            items = list(settings_container.query_items(
                query=query, parameters=params, enable_cross_partition_query=True
            ))
            return _cors_response(items[0] if items else {
                "deviceId": device_id, "thresholdWatts": None, "rateEurPerKwh": DEFAULT_RATE_EUR_PER_KWH
            })

        # POST -- create or update threshold and/or electricity rate
        body = req.get_json()
        device_id = body.get("deviceId")
        if not device_id:
            return _cors_response({"error": "deviceId required"}, 400)

        # Merge with existing doc so partial updates (just threshold, or just rate) don't wipe the other field
        existing = None
        try:
            existing = settings_container.read_item(item=device_id, partition_key=device_id)
        except Exception:
            pass

        doc = existing or {"id": device_id, "userId": device_id, "deviceId": device_id}
        if "thresholdWatts" in body and body["thresholdWatts"] is not None:
            doc["thresholdWatts"] = float(body["thresholdWatts"])
        if "rateEurPerKwh" in body and body["rateEurPerKwh"] is not None:
            doc["rateEurPerKwh"] = float(body["rateEurPerKwh"])

        settings_container.upsert_item(doc)
        return _cors_response(doc)
    except Exception as e:
        logging.error(f"Settings failed: {e}")
        return _cors_response({"error": str(e)}, 500)


@app.function_name(name="DeviceMeta")
@app.route(route="devicemeta", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def device_meta(req: func.HttpRequest) -> func.HttpResponse:
    """Update a device's zone and/or display name."""
    try:
        body = req.get_json()
        device_id = body.get("deviceId")
        if not device_id:
            return _cors_response({"error": "deviceId required"}, 400)

        device_doc = get_or_create_device(device_id)
        if "zone" in body and body["zone"]:
            device_doc["zone"] = body["zone"]
        if "name" in body and body["name"]:
            device_doc["name"] = body["name"]

        devices_container.upsert_item(device_doc)
        return _cors_response(device_doc)
    except Exception as e:
        logging.error(f"DeviceMeta failed: {e}")
        return _cors_response({"error": str(e)}, 500)
