import asyncio
import json
import os
from contextlib import asynccontextmanager
from typing import Optional

import aiomqtt
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
BASE_TOPIC = os.environ.get("ZIGBEE2MQTT_BASE_TOPIC", "zigbee2mqtt")

# In-memory device state cache — updated by the MQTT listener task
_devices: dict[str, dict] = {}
_listener_task: Optional[asyncio.Task] = None


async def _mqtt_listener():
    """Subscribe to zigbee2mqtt bridge/devices and all device state updates."""
    async with aiomqtt.Client(MQTT_HOST, MQTT_PORT) as client:
        await client.subscribe(f"{BASE_TOPIC}/bridge/devices")
        await client.subscribe(f"{BASE_TOPIC}/+")
        async for message in client.messages:
            topic = str(message.topic)
            try:
                payload = json.loads(message.payload)
            except Exception:
                continue

            if topic == f"{BASE_TOPIC}/bridge/devices":
                # Populate device registry from bridge announcement
                for dev in payload:
                    friendly = dev.get("friendly_name")
                    if friendly and dev.get("type") == "EndDevice":
                        _devices.setdefault(friendly, {})["info"] = dev
            else:
                # State update for a specific device
                friendly = topic.removeprefix(f"{BASE_TOPIC}/")
                if friendly in _devices and isinstance(payload, dict):
                    _devices[friendly].update(payload)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _listener_task
    _listener_task = asyncio.create_task(_mqtt_listener())
    # Give the listener a moment to populate initial state
    await asyncio.sleep(1)
    yield
    _listener_task.cancel()
    try:
        await _listener_task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Zigbee Lights API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class LightState(BaseModel):
    state: Optional[str] = Field(None, pattern="^(ON|OFF|TOGGLE)$")
    brightness: Optional[int] = Field(None, ge=0, le=254)
    color_temp: Optional[int] = Field(None, ge=150, le=500)


async def _publish(friendly_name: str, payload: dict):
    async with aiomqtt.Client(MQTT_HOST, MQTT_PORT) as client:
        await client.publish(
            f"{BASE_TOPIC}/{friendly_name}/set",
            json.dumps(payload),
        )


def _device_response(name: str, data: dict) -> dict:
    info = data.get("info", {})
    return {
        "name": name,
        "ieee_address": info.get("ieee_address"),
        "model": info.get("definition", {}).get("model"),
        "description": info.get("definition", {}).get("description"),
        "state": data.get("state"),
        "brightness": data.get("brightness"),
        "color_temp": data.get("color_temp"),
        "linkquality": data.get("linkquality"),
    }


@app.get("/lights")
async def list_lights():
    return [_device_response(name, data) for name, data in _devices.items()]


@app.get("/lights/{name}")
async def get_light(name: str):
    if name not in _devices:
        raise HTTPException(status_code=404, detail="Device not found")
    return _device_response(name, _devices[name])


@app.put("/lights/{name}")
async def set_light(name: str, body: LightState):
    payload = body.model_dump(exclude_none=True)
    if not payload:
        raise HTTPException(status_code=400, detail="No fields provided")
    # zigbee2mqtt expects state as ON/OFF string
    await _publish(name, payload)
    return {"ok": True}


@app.put("/lights/{name}/on")
async def turn_on(name: str):
    await _publish(name, {"state": "ON"})
    return {"ok": True}


@app.put("/lights/{name}/off")
async def turn_off(name: str):
    await _publish(name, {"state": "OFF"})
    return {"ok": True}


@app.put("/lights/{name}/toggle")
async def toggle(name: str):
    await _publish(name, {"state": "TOGGLE"})
    return {"ok": True}


@app.put("/lights/{name}/brightness/{value}")
async def set_brightness(name: str, value: int):
    if not 0 <= value <= 254:
        raise HTTPException(status_code=400, detail="brightness must be 0–254")
    await _publish(name, {"brightness": value})
    return {"ok": True}


@app.put("/lights/{name}/color_temp/{value}")
async def set_color_temp(name: str, value: int):
    if not 150 <= value <= 500:
        raise HTTPException(status_code=400, detail="color_temp must be 150–500 (mireds)")
    await _publish(name, {"color_temp": value})
    return {"ok": True}


@app.put("/lights/all/on")
async def all_on():
    for name in _devices:
        await _publish(name, {"state": "ON"})
    return {"ok": True, "count": len(_devices)}


@app.put("/lights/all/off")
async def all_off():
    for name in _devices:
        await _publish(name, {"state": "OFF"})
    return {"ok": True, "count": len(_devices)}


@app.get("/health")
async def health():
    return {"status": "ok", "devices_seen": len(_devices)}
