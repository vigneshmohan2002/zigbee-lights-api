"""
Zigbee Lights API
-----------------
FastAPI + aiomqtt bridge to Zigbee2MQTT.

Effect system
~~~~~~~~~~~~~
User-defined effects are sequences of steps, each a small command:
  brightness  – fade to a brightness value over `duration` seconds
  color       – change hue/saturation over `duration` seconds
  wait        – hold current state for `duration` seconds
  on / off    – turn lamps on/off over `duration` seconds (transition)

Steps loop (or run once) until cancelled.  Any API call that affects
lights calls `_stop_all_effects()` first — that function is the single
place responsible for tearing down every running effect type.
"""

import asyncio
import json
import os
import re
from contextlib import asynccontextmanager
from typing import Any, Literal, Optional

import aiofiles
import aiomqtt
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, model_validator

# ── Config ────────────────────────────────────────────────────────────────────

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
BASE_TOPIC = os.environ.get("ZIGBEE2MQTT_BASE_TOPIC", "zigbee2mqtt")

SCENES_FILE         = os.environ.get("SCENES_FILE",         "scenes.json")
PROFILES_FILE       = os.environ.get("PROFILES_FILE",       "profiles.json")
CUSTOM_EFFECTS_FILE = os.environ.get("CUSTOM_EFFECTS_FILE", "custom_effects.json")

# Extra sleep added after each effect step so the next command lands only
# after the bulb finishes its transition (Zigbee round-trip latency buffer).
TRANSITION_BUFFER = 0.05   # seconds

# How long to wait between the base-state and the firmware-effect command
# when applying an effect payload that contains both (e.g. brightness + blink).
EFFECT_BASE_PAUSE = 0.3    # seconds

# Brightness below which we treat a bulb as "off" for dim-off behaviour.
BRIGHTNESS_OFF_THRESHOLD = 12

# ── Validation helper ─────────────────────────────────────────────────────────

_SAFE_NAME = re.compile(r'^[^/\\?#&%]+$')

def _validate_name(name: str, label: str = "Name") -> str:
    """Reject empty strings or names containing URL-unsafe characters."""
    name = name.strip()
    if not name:
        raise HTTPException(400, f"{label} cannot be empty")
    if not _SAFE_NAME.match(name):
        raise HTTPException(400, f"{label} must not contain / \\ ? # & % characters")
    return name

# ── Static data ───────────────────────────────────────────────────────────────

GROUPS: dict[str, list[str]] = {
    "bedroom": ["desk-lamp", "dress-table-lamp", "nighttable-lamp", "corner-lamp", "reading-lamp"],
    "desk":    ["desk-lamp"],
    "bedside": ["nighttable-lamp"],
    "ambient": ["corner-lamp", "dress-table-lamp"],
    "reading": ["reading-lamp"],
}

COLORS: dict[str, dict] = {
    "red":    {"state": "ON", "color": {"hue": 0,   "saturation": 90}, "transition": 1},
    "orange": {"state": "ON", "color": {"hue": 25,  "saturation": 90}, "transition": 1},
    "yellow": {"state": "ON", "color": {"hue": 55,  "saturation": 90}, "transition": 1},
    "green":  {"state": "ON", "color": {"hue": 120, "saturation": 80}, "transition": 1},
    "cyan":   {"state": "ON", "color": {"hue": 180, "saturation": 80}, "transition": 1},
    "blue":   {"state": "ON", "color": {"hue": 240, "saturation": 90}, "transition": 1},
    "purple": {"state": "ON", "color": {"hue": 280, "saturation": 85}, "transition": 1},
    "pink":   {"state": "ON", "color": {"hue": 320, "saturation": 85}, "transition": 1},
}

# Firmware effects passed directly to Zigbee bulbs via the `effect` field.
# Keys are the user-facing API names; values are the Zigbee2MQTT MQTT values.
FIRMWARE_EFFECTS: dict[str, str] = {
    "colorloop":      "colorloop",
    "stop_colorloop": "stop_colorloop",
    "breathe":        "breathe",
    "blink":          "blink",
    "okay":           "okay",
    "finish":         "finish_effect",
    "stop":           "stop_effect",
}

# Remote arrow cycling palette (whites + colours)
COLOR_SCENES: list[dict] = [
    {"color_temp": 250, "transition": 0.5},
    {"color_temp": 370, "transition": 0.5},
    {"color_temp": 454, "transition": 0.5},
    *[{"color": {"hue": h, "saturation": s}, "transition": 0.5} for h, s in [
        (0, 90), (25, 90), (55, 90), (120, 80),
        (180, 80), (240, 90), (280, 85), (320, 85),
    ]],
]

REMOTE_ACTION_MAP: dict[str, dict] = {
    "on":                  {"state": "ON"},
    "off":                 {"state": "OFF"},
    "brightness_move_up":  {"brightness_move": 40},
    "brightness_move_down":{"brightness_move": -40},
    "brightness_stop":     {"brightness_move": 0},
    "arrow_right_hold":    {"color_temp_move": 40},
    "arrow_left_hold":     {"color_temp_move": -40},
    "arrow_right_release": {"color_temp_move": 0},
    "arrow_left_release":  {"color_temp_move": 0},
}

REMOTE_GROUP_MAP: dict[str, str] = {"Remote": "bedroom"}

# Built-in effects seeded into custom_effects.json on first run.
# Adding ignore_master=True exempts an effect from the master brightness cap.
BUILTIN_EFFECTS: dict[str, dict] = {
    # ── Brightness breathing ───────────────────────────────────────────────────
    "slow breathe": {
        "steps": [
            {"type": "brightness", "brightness": 8,   "duration": 2.0},
            {"type": "brightness", "brightness": 180, "duration": 2.0},
        ],
        "loop": True,
    },
    "slow breathe long": {
        "steps": [
            {"type": "brightness", "brightness": 8,   "duration": 4.0},
            {"type": "brightness", "brightness": 180, "duration": 4.0},
        ],
        "loop": True,
    },
    "fast pulse": {
        "steps": [
            {"type": "brightness", "brightness": 20,  "duration": 0.4},
            {"type": "brightness", "brightness": 200, "duration": 0.4},
        ],
        "loop": True,
    },
    "heartbeat": {
        # double-bump: two quick pulses then a long rest
        "steps": [
            {"type": "brightness", "brightness": 220, "duration": 0.15},
            {"type": "brightness", "brightness": 60,  "duration": 0.15},
            {"type": "brightness", "brightness": 220, "duration": 0.15},
            {"type": "brightness", "brightness": 20,  "duration": 1.4},
        ],
        "loop": True,
    },
    # ── Colour cycling ─────────────────────────────────────────────────────────
    "rainbow slow": {
        "steps": [
            {"type": "color", "hue": 0,   "saturation": 90, "duration": 2.0},
            {"type": "color", "hue": 45,  "saturation": 90, "duration": 2.0},
            {"type": "color", "hue": 90,  "saturation": 90, "duration": 2.0},
            {"type": "color", "hue": 150, "saturation": 90, "duration": 2.0},
            {"type": "color", "hue": 200, "saturation": 90, "duration": 2.0},
            {"type": "color", "hue": 240, "saturation": 90, "duration": 2.0},
            {"type": "color", "hue": 290, "saturation": 90, "duration": 2.0},
            {"type": "color", "hue": 330, "saturation": 90, "duration": 2.0},
        ],
        "loop": True,
    },
    "rainbow fast": {
        "steps": [
            {"type": "color", "hue": 0,   "saturation": 90, "duration": 0.5},
            {"type": "color", "hue": 60,  "saturation": 90, "duration": 0.5},
            {"type": "color", "hue": 120, "saturation": 90, "duration": 0.5},
            {"type": "color", "hue": 180, "saturation": 90, "duration": 0.5},
            {"type": "color", "hue": 240, "saturation": 90, "duration": 0.5},
            {"type": "color", "hue": 300, "saturation": 90, "duration": 0.5},
        ],
        "loop": True,
    },
    "red blue strobe": {
        "steps": [
            {"type": "color",      "hue": 0,   "saturation": 95, "duration": 0.4},
            {"type": "brightness", "brightness": 220,             "duration": 0.4},
            {"type": "color",      "hue": 240, "saturation": 95, "duration": 0.4},
            {"type": "brightness", "brightness": 220,             "duration": 0.4},
        ],
        "loop": True,
    },
    # ── Colour breathe ─────────────────────────────────────────────────────────
    "blue breathe": {
        "steps": [
            {"type": "color",      "hue": 240, "saturation": 90, "duration": 0.5},
            {"type": "brightness", "brightness": 10,              "duration": 2.5},
            {"type": "brightness", "brightness": 180,             "duration": 2.5},
        ],
        "loop": True,
    },
    "red breathe": {
        "steps": [
            {"type": "color",      "hue": 0,   "saturation": 90, "duration": 0.5},
            {"type": "brightness", "brightness": 10,              "duration": 2.5},
            {"type": "brightness", "brightness": 180,             "duration": 2.5},
        ],
        "loop": True,
    },
    "purple breathe": {
        "steps": [
            {"type": "color",      "hue": 280, "saturation": 85, "duration": 0.5},
            {"type": "brightness", "brightness": 10,              "duration": 2.5},
            {"type": "brightness", "brightness": 180,             "duration": 2.5},
        ],
        "loop": True,
    },
    # ── Ambience / mood ────────────────────────────────────────────────────────
    "candle flicker": {
        # Uneven durations simulate the random quality of real candlelight
        "steps": [
            {"type": "brightness", "brightness": 80,  "duration": 0.1},
            {"type": "brightness", "brightness": 120, "duration": 0.2},
            {"type": "brightness", "brightness": 60,  "duration": 0.15},
            {"type": "brightness", "brightness": 110, "duration": 0.3},
            {"type": "brightness", "brightness": 45,  "duration": 0.1},
            {"type": "brightness", "brightness": 100, "duration": 0.25},
            {"type": "brightness", "brightness": 130, "duration": 0.4},
            {"type": "brightness", "brightness": 70,  "duration": 0.15},
        ],
        "loop": True,
    },
    "sunset fade": {
        # cool white → warm amber → deep red over ~2 min, run once
        "steps": [
            {"type": "color", "hue": 45,  "saturation": 60, "duration": 20.0},
            {"type": "color", "hue": 30,  "saturation": 75, "duration": 20.0},
            {"type": "color", "hue": 20,  "saturation": 85, "duration": 20.0},
            {"type": "color", "hue": 10,  "saturation": 90, "duration": 20.0},
            {"type": "color", "hue": 5,   "saturation": 95, "duration": 20.0},
            {"type": "brightness", "brightness": 40,         "duration": 20.0},
        ],
        "loop": False,
    },
    "ocean": {
        "steps": [
            {"type": "color",      "hue": 200, "saturation": 80, "duration": 3.0},
            {"type": "brightness", "brightness": 80,              "duration": 3.0},
            {"type": "color",      "hue": 220, "saturation": 85, "duration": 3.0},
            {"type": "brightness", "brightness": 160,             "duration": 3.0},
            {"type": "color",      "hue": 185, "saturation": 75, "duration": 3.0},
            {"type": "brightness", "brightness": 110,             "duration": 3.0},
        ],
        "loop": True,
    },
    "northern lights": {
        "steps": [
            {"type": "color",      "hue": 160, "saturation": 80, "duration": 4.0},
            {"type": "brightness", "brightness": 60,              "duration": 2.0},
            {"type": "color",      "hue": 200, "saturation": 85, "duration": 4.0},
            {"type": "brightness", "brightness": 130,             "duration": 2.0},
            {"type": "color",      "hue": 280, "saturation": 80, "duration": 4.0},
            {"type": "brightness", "brightness": 80,              "duration": 2.0},
            {"type": "color",      "hue": 140, "saturation": 75, "duration": 4.0},
            {"type": "brightness", "brightness": 110,             "duration": 2.0},
        ],
        "loop": True,
    },
    # ── Wake-up / wind-down ────────────────────────────────────────────────────
    "wake up": {
        # Breathes in warm sunny yellow-orange. ignore_master so it always
        # reaches full brightness regardless of the master slider setting.
        "steps": [
            {"type": "on",         "duration": 1.0},
            {"type": "color",      "hue": 40, "saturation": 80, "duration": 1.0},
            {"type": "brightness", "brightness": 30,  "duration": 2.0},
            {"type": "brightness", "brightness": 220, "duration": 2.0},
        ],
        "loop": True,
        "ignore_master": True,
    },
    "wind down": {
        # Dims from warm orange to deep red then off over ~10 minutes, run once.
        "steps": [
            {"type": "color",      "hue": 30,  "saturation": 70, "duration": 60.0},
            {"type": "brightness", "brightness": 160,              "duration": 60.0},
            {"type": "color",      "hue": 15,  "saturation": 85, "duration": 60.0},
            {"type": "brightness", "brightness": 100,              "duration": 60.0},
            {"type": "color",      "hue": 5,   "saturation": 95, "duration": 60.0},
            {"type": "brightness", "brightness": 50,               "duration": 60.0},
            {"type": "brightness", "brightness": 15,               "duration": 120.0},
            {"type": "off",        "duration": 3.0},
        ],
        "loop": False,
    },
}

DEFAULT_SCENES: dict[str, dict] = {
    "evening":     {"state": "ON", "brightness": 100, "color_temp": 380, "transition": 2},
    "sunrise":     {"state": "ON", "brightness": 60,  "color_temp": 454, "transition": 30},
    "party":       {"state": "ON", "brightness": 200, "effect": "colorloop", "transition": 1},
    "pulse":       {"state": "ON", "brightness": 180, "color_temp": 330, "effect": "breathe", "transition": 1},
    "candlelight": {"state": "ON", "brightness": 40,  "color_temp": 454, "transition": 2},
    "cinema":      {"state": "ON", "brightness": 30,  "color_temp": 454, "transition": 3},
    "energise":    {"state": "ON", "brightness": 254, "color_temp": 250, "transition": 5},
}

DEFAULT_PROFILES: dict[str, dict] = {
    "bright":  {"state": "ON", "brightness": 254, "color_temp": 250, "transition": 1},
    "focus":   {"state": "ON", "brightness": 220, "color_temp": 280, "transition": 1},
    "reading": {"state": "ON", "brightness": 180, "color_temp": 330, "transition": 1},
    "relax":   {"state": "ON", "brightness": 120, "color_temp": 380, "transition": 1},
    "dim":     {"state": "ON", "brightness": 60,  "color_temp": 420, "transition": 1},
    "night":   {"state": "ON", "brightness": 15,  "color_temp": 454, "transition": 1},
    "off":     {"state": "OFF", "transition": 1},
}

# ── In-memory state ───────────────────────────────────────────────────────────

_devices: dict[str, dict] = {}
_listener_task:  Optional[asyncio.Task] = None
_publisher_task: Optional[asyncio.Task] = None
_scenes:          dict[str, dict] = {}
_profiles:        dict[str, dict] = {}
_custom_effects:  dict[str, dict] = {}
_group_scene_index: dict[str, int] = {}
_effect_tasks:    dict[str, asyncio.Task] = {}   # effect_key → running coroutine
_master_brightness: int = 254                    # ceiling applied to effect brightness steps
_mqtt_client: Optional[aiomqtt.Client] = None    # persistent publisher connection

# Cache for lamp name list — invalidated when _devices changes.
_lamp_names_cache: Optional[list[str]] = None

# ── Persistence ───────────────────────────────────────────────────────────────

def _save_json_sync(path: str, data: dict) -> None:
    """Synchronous write — used only at startup before the event loop yields."""
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def _load_json(path: str, default: dict) -> dict:
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass  # malformed JSON → fall through to default
    _save_json_sync(path, dict(default))
    return dict(default)

async def _save_json(path: str, data: dict) -> None:
    """Async write — does not block the event loop."""
    async with aiofiles.open(path, "w") as f:
        await f.write(json.dumps(data, indent=2))

# ── MQTT helpers ──────────────────────────────────────────────────────────────

def _lamp_names() -> list[str]:
    """Return all device names that expose a brightness or color_temp feature.
    Result is cached; invalidated whenever _devices is mutated via _invalidate_lamp_cache()."""
    global _lamp_names_cache
    if _lamp_names_cache is not None:
        return _lamp_names_cache
    names = [
        name for name, data in _devices.items()
        if any(
            f.get("name") in ("brightness", "color_temp")
            for ep in data.get("info", {}).get("definition", {}).get("exposes", [])
            for f in (ep.get("features", []) or [ep])
        )
    ]
    _lamp_names_cache = names
    return names

def _invalidate_lamp_cache() -> None:
    global _lamp_names_cache
    _lamp_names_cache = None

async def _pub(topic: str, payload: dict) -> None:
    """Send one MQTT message on the persistent publisher connection.
    Silently drops the message if the publisher is not yet connected."""
    if _mqtt_client is None:
        return
    await _mqtt_client.publish(topic, json.dumps(payload))

async def _pub_lamp(name: str, payload: dict) -> None:
    """Convenience wrapper: publish to a single lamp's /set topic."""
    await _pub(f"{BASE_TOPIC}/{name}/set", payload)

async def _publish_many(names: list[str], payload: dict) -> None:
    """Publish to multiple lamps.
    When the payload includes an `effect` key, the base state is sent first,
    then EFFECT_BASE_PAUSE later the effect command is sent, so the bulbs
    are already in the right state when the effect starts."""
    effect = payload.get("effect")
    if effect:
        base = {k: v for k, v in payload.items() if k != "effect"}
        if base:
            for n in names:
                await _pub_lamp(n, base)
            await asyncio.sleep(EFFECT_BASE_PAUSE)
        for n in names:
            await _pub_lamp(n, {"effect": effect})
    else:
        for n in names:
            await _pub_lamp(n, payload)

def _device_response(name: str, data: dict) -> dict:
    info = data.get("info", {})
    return {
        "name":         name,
        "ieee_address": info.get("ieee_address"),
        "model":        info.get("definition", {}).get("model"),
        "state":        data.get("state"),
        "brightness":   data.get("brightness"),
        "color_temp":   data.get("color_temp"),
        "color":        data.get("color"),
        "linkquality":  data.get("linkquality"),
    }

# ── Effect engine ─────────────────────────────────────────────────────────────

def _effect_key(names: list[str]) -> str:
    return "\x00".join(sorted(names))  # null-byte separator avoids comma-in-name collisions

async def _run_sequence(
    names: list[str],
    steps: list[dict],
    loop: bool = True,
    ignore_master: bool = False,
) -> None:
    """Execute a step sequence on a set of lamps, looping until cancelled.

    Step types:
      brightness  {"type":"brightness", "brightness":<1-254>, "duration":<s>}
      color       {"type":"color", "hue":<0-360>, "saturation":<0-100>, "duration":<s>}
      wait        {"type":"wait", "duration":<s>}
      on          {"type":"on",  "duration":<s>}
      off         {"type":"off", "duration":<s>}

    To add a new step type: add a branch below and update the docstring.
    """
    try:
        while True:
            for step in steps:
                stype    = step.get("type")
                duration = float(step.get("duration", 1.0))

                if stype == "brightness":
                    bri = int(step["brightness"])
                    scaled = bri if ignore_master else max(1, round(bri * _master_brightness / 254))
                    cmd: dict = {"brightness": scaled, "transition": duration}
                elif stype == "color":
                    cmd = {
                        "color": {
                            "hue":        int(step["hue"]),
                            "saturation": int(step.get("saturation", 90)),
                        },
                        "transition": duration,
                    }
                elif stype == "wait":
                    await asyncio.sleep(duration)
                    continue
                elif stype == "on":
                    cmd = {"state": "ON",  "transition": duration}
                elif stype == "off":
                    cmd = {"state": "OFF", "transition": duration}
                else:
                    continue

                for n in names:
                    await _pub_lamp(n, cmd)
                # Wait for the transition to finish before sending the next command.
                await asyncio.sleep(duration + TRANSITION_BUFFER)

            if not loop:
                break
    except asyncio.CancelledError:
        pass


def _start_effect(names: list[str], effect_def: dict) -> None:
    """Cancel any running effect on these lamps, then start the new one."""
    key = _effect_key(names)
    existing = _effect_tasks.get(key)
    if existing and not existing.done():
        existing.cancel()
    task = asyncio.create_task(
        _run_sequence(
            names,
            effect_def["steps"],
            loop=effect_def.get("loop", True),
            ignore_master=effect_def.get("ignore_master", False),
        )
    )
    _effect_tasks[key] = task


async def _stop_all_effects() -> None:
    """Stop everything:
    - Cancel all running Python-side sequence tasks
    - Send firmware stop commands to every lamp

    NOTE: If you add a new effect category that needs extra teardown, add it here.
    """
    for task in list(_effect_tasks.values()):
        task.cancel()
    _effect_tasks.clear()

    lamps = _lamp_names()
    for n in lamps:
        await _pub_lamp(n, {"effect": "stop_colorloop"})
    for n in lamps:
        await _pub_lamp(n, {"effect": "stop_effect"})


def _resolve_effect(name: str) -> Optional[dict]:
    """Look up an effect by name. Returns a dict describing the effect, or None."""
    if name in _custom_effects:
        return {"kind": "custom",   "def": _custom_effects[name]}
    if name in FIRMWARE_EFFECTS:
        return {"kind": "firmware", "value": FIRMWARE_EFFECTS[name]}
    return None


async def _apply_effect(names: list[str], effect: dict) -> None:
    """Apply a resolved effect dict (from _resolve_effect) to the given lamps."""
    if effect["kind"] == "custom":
        _start_effect(names, effect["def"])
    else:
        await _publish_many(names, {"effect": effect["value"]})

# ── Remote control ────────────────────────────────────────────────────────────

def _advance_scene(group_name: str, direction: int) -> dict:
    idx = (_group_scene_index.get(group_name, 0) + direction) % len(COLOR_SCENES)
    _group_scene_index[group_name] = idx
    return COLOR_SCENES[idx]


async def _handle_remote_action(remote_name: str, action: str) -> None:
    group_name = REMOTE_GROUP_MAP.get(remote_name)
    if not group_name:
        return
    members = GROUPS.get(group_name, [])
    await _stop_all_effects()

    if action == "brightness_stop":
        on_members = [n for n in members if _devices.get(n, {}).get("state") == "ON"]
        at_min = all(
            _devices.get(n, {}).get("brightness", 255) <= BRIGHTNESS_OFF_THRESHOLD
            for n in on_members
        )
        cmd = {"state": "OFF"} if (at_min and on_members) else {"brightness_move": 0}
        for n in members:
            await _pub_lamp(n, cmd)
        return

    if action in ("arrow_right_click", "arrow_left_click"):
        direction = 1 if action == "arrow_right_click" else -1
        payload   = _advance_scene(group_name, direction)
        for n in members:
            await _pub_lamp(n, payload)
        return

    if cmd := REMOTE_ACTION_MAP.get(action):
        for n in members:
            await _pub_lamp(n, cmd)

# ── MQTT background tasks ─────────────────────────────────────────────────────

async def _mqtt_listener() -> None:
    """Subscribe to device state and remote action topics. Reconnects automatically."""
    while True:
        try:
            async with aiomqtt.Client(MQTT_HOST, MQTT_PORT) as c:
                await c.subscribe(f"{BASE_TOPIC}/bridge/devices")
                await c.subscribe(f"{BASE_TOPIC}/+")
                async for msg in c.messages:
                    topic = str(msg.topic)
                    try:
                        payload = json.loads(msg.payload)
                    except Exception:
                        continue

                    if topic == f"{BASE_TOPIC}/bridge/devices":
                        # Evict stale raw-address entries (e.g. "0xf84477...") that
                        # linger in _devices after a device is renamed in Zigbee2MQTT.
                        ieee_to_friendly = {
                            dev.get("ieee_address"): dev.get("friendly_name")
                            for dev in payload
                            if dev.get("friendly_name") and dev.get("ieee_address")
                        }
                        friendly_names = set(ieee_to_friendly.values())
                        stale = [
                            k for k in list(_devices)
                            if k.startswith("0x") and ieee_to_friendly.get(k) in friendly_names
                        ]
                        for k in stale:
                            del _devices[k]
                            _invalidate_lamp_cache()

                        for dev in payload:
                            name = dev.get("friendly_name")
                            if name and dev.get("type") in ("EndDevice", "Router"):
                                _devices.setdefault(name, {})["info"] = dev
                                _invalidate_lamp_cache()
                                await _pub(
                                    f"{BASE_TOPIC}/{name}/get",
                                    {"state": "", "brightness": "", "color_temp": "", "color": ""},
                                )
                    else:
                        name = topic.removeprefix(f"{BASE_TOPIC}/")
                        if isinstance(payload, dict) and payload.get("action"):
                            asyncio.create_task(_handle_remote_action(name, payload["action"]))
                        if name in _devices and isinstance(payload, dict):
                            _devices[name].update(payload)
        except asyncio.CancelledError:
            return
        except Exception:
            await asyncio.sleep(2)   # brief pause before reconnect


async def _mqtt_publisher() -> None:
    """Maintain a long-lived MQTT connection used by all publish calls."""
    global _mqtt_client
    while True:
        try:
            async with aiomqtt.Client(MQTT_HOST, MQTT_PORT) as c:
                _mqtt_client = c
                # Hold the connection open until cancelled or the broker drops it.
                await asyncio.get_running_loop().create_future()
        except asyncio.CancelledError:
            _mqtt_client = None
            return
        except Exception:
            _mqtt_client = None
            await asyncio.sleep(2)

# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _listener_task, _publisher_task, _scenes, _profiles, _custom_effects

    _scenes   = _load_json(SCENES_FILE,   DEFAULT_SCENES)
    _profiles = _load_json(PROFILES_FILE, DEFAULT_PROFILES)

    # Seed built-in effects only for keys that are absent (won't overwrite user edits).
    raw = _load_json(CUSTOM_EFFECTS_FILE, {})
    changed = False
    for name, defn in BUILTIN_EFFECTS.items():
        if name not in raw:
            raw[name] = defn
            changed = True
    _custom_effects = raw
    if changed:
        _save_json_sync(CUSTOM_EFFECTS_FILE, _custom_effects)

    _publisher_task = asyncio.create_task(_mqtt_publisher())
    _listener_task  = asyncio.create_task(_mqtt_listener())
    await asyncio.sleep(1)   # give both connections time to establish
    yield

    _publisher_task.cancel()
    _listener_task.cancel()
    for t in (_publisher_task, _listener_task):
        try:
            await t
        except asyncio.CancelledError:
            pass

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="Zigbee Lights API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", include_in_schema=False)
async def index():
    return FileResponse("static/index.html")

# ── Pydantic models ───────────────────────────────────────────────────────────

class ColorHS(BaseModel):
    hue:        int = Field(..., ge=0, le=360)
    saturation: int = Field(..., ge=0, le=100)

class LightState(BaseModel):
    state:      Optional[str] = Field(None, pattern="^(ON|OFF|TOGGLE)$")
    brightness: Optional[int] = Field(None, ge=0,   le=254)
    color_temp: Optional[int] = Field(None, ge=150, le=500)
    color:      Optional[ColorHS] = None

class ProfileBody(BaseModel):
    name:       str
    brightness: int   = Field(..., ge=0,   le=254)
    color_temp: int   = Field(..., ge=150, le=500)
    transition: float = Field(1.0, ge=0)

class SceneBody(BaseModel):
    name:       str
    state:      Optional[str]     = "ON"
    brightness: Optional[int]     = Field(None, ge=0,   le=254)
    color_temp: Optional[int]     = Field(None, ge=150, le=500)
    color:      Optional[ColorHS] = None
    effect:     Optional[str]     = None
    transition: Optional[float]   = Field(None, ge=0)

class CaptureSceneBody(BaseModel):
    name: str

class EffectStep(BaseModel):
    type:       Literal["brightness", "color", "wait", "on", "off"]
    brightness: Optional[int]   = Field(None, ge=1,  le=254)
    hue:        Optional[int]   = Field(None, ge=0,  le=360)
    saturation: int             = Field(90,   ge=0,  le=100)
    duration:   float           = Field(1.0,  gt=0)

    @model_validator(mode="after")
    def _check_required_fields(self) -> "EffectStep":
        if self.type == "brightness" and self.brightness is None:
            raise ValueError("brightness steps must include a 'brightness' value")
        if self.type == "color" and self.hue is None:
            raise ValueError("color steps must include a 'hue' value")
        return self

class CustomEffectBody(BaseModel):
    name:          str
    steps:         list[EffectStep]
    loop:          bool = True
    ignore_master: bool = False   # when True, brightness steps bypass the master cap

# ── Lights ────────────────────────────────────────────────────────────────────

@app.get("/lights")
async def list_lights():
    return [_device_response(n, d) for n, d in _devices.items()]

@app.get("/lights/{name}")
async def get_light(name: str):
    if name not in _devices:
        raise HTTPException(404, "Device not found")
    return _device_response(name, _devices[name])

@app.put("/lights/{name}")
async def set_light(name: str, body: LightState):
    await _stop_all_effects()
    payload = body.model_dump(exclude_none=True)
    if not payload:
        raise HTTPException(400, "No fields provided")
    await _pub_lamp(name, payload)
    return {"ok": True}

@app.put("/lights/{name}/toggle")
async def toggle_light(name: str):
    await _stop_all_effects()
    await _pub_lamp(name, {"state": "TOGGLE"})
    return {"ok": True}

# NOTE: /lights/all/brightness/{value} must be registered BEFORE
#       /lights/{name}/brightness/{value} so "all" is not captured as a name.
@app.put("/lights/all/brightness/{value}")
async def set_brightness_all(value: int):
    global _master_brightness
    if not 0 <= value <= 254:
        raise HTTPException(400, "brightness must be 0–254")
    _master_brightness = value
    await _stop_all_effects()
    await _publish_many(_lamp_names(), {"brightness": value})
    return {"ok": True}

@app.put("/lights/{name}/brightness/{value}")
async def set_brightness(name: str, value: int):
    if not 0 <= value <= 254:
        raise HTTPException(400, "brightness must be 0–254")
    await _stop_all_effects()
    await _pub_lamp(name, {"brightness": value})
    return {"ok": True}

# ── Groups ────────────────────────────────────────────────────────────────────

@app.get("/groups")
async def list_groups():
    return GROUPS

@app.put("/groups/{group}/on")
async def group_on(group: str):
    if group not in GROUPS:
        raise HTTPException(404, "Unknown group")
    await _stop_all_effects()
    await _publish_many(GROUPS[group], {"state": "ON"})
    return {"ok": True}

@app.put("/groups/{group}/off")
async def group_off(group: str):
    if group not in GROUPS:
        raise HTTPException(404, "Unknown group")
    await _stop_all_effects()
    await _publish_many(GROUPS[group], {"state": "OFF"})
    return {"ok": True}

# ── Profiles ──────────────────────────────────────────────────────────────────

@app.get("/profiles")
async def list_profiles():
    return _profiles

@app.post("/profiles")
async def create_profile(body: ProfileBody):
    name = _validate_name(body.name, "Profile name")
    _profiles[name] = {
        "state":      "ON",
        "brightness": body.brightness,
        "color_temp": body.color_temp,
        "transition": body.transition,
    }
    await _save_json(PROFILES_FILE, _profiles)
    return {"ok": True, "name": name}

@app.delete("/profiles/{name}")
async def delete_profile(name: str):
    if name not in _profiles:
        raise HTTPException(404, "Profile not found")
    del _profiles[name]
    await _save_json(PROFILES_FILE, _profiles)
    return {"ok": True}

@app.put("/profiles/{name}/apply")
async def apply_profile(name: str):
    if name not in _profiles:
        raise HTTPException(404, "Profile not found")
    await _stop_all_effects()
    await _publish_many(_lamp_names(), _profiles[name])
    return {"ok": True}

@app.put("/profiles/{name}/groups/{group}")
async def apply_profile_group(name: str, group: str):
    if name not in _profiles:
        raise HTTPException(404, "Profile not found")
    if group not in GROUPS:
        raise HTTPException(404, "Group not found")
    await _stop_all_effects()
    await _publish_many(GROUPS[group], _profiles[name])
    return {"ok": True}

# ── Scenes ────────────────────────────────────────────────────────────────────

@app.get("/scenes")
async def list_scenes():
    return _scenes

@app.post("/scenes")
async def create_scene(body: SceneBody):
    name = _validate_name(body.name, "Scene name")
    payload: dict[str, Any] = {"state": body.state or "ON"}
    if body.brightness is not None: payload["brightness"] = body.brightness
    if body.color_temp is not None: payload["color_temp"] = body.color_temp
    if body.color      is not None: payload["color"]      = body.color.model_dump()
    if body.effect     is not None: payload["effect"]     = body.effect
    if body.transition is not None: payload["transition"] = body.transition
    _scenes[name] = payload
    await _save_json(SCENES_FILE, _scenes)
    return {"ok": True, "name": name}

@app.post("/scenes/capture")
async def capture_scene(body: CaptureSceneBody):
    name = _validate_name(body.name, "Scene name")
    lamps = _lamp_names()
    if not lamps:
        raise HTTPException(503, "No lamps seen yet — MQTT may still be connecting")
    per_lamp: dict[str, Any] = {}
    for lamp in lamps:
        d = _devices.get(lamp, {})
        entry: dict[str, Any] = {"state": d.get("state", "ON")}
        if d.get("brightness") is not None:
            entry["brightness"] = d["brightness"]
        if d.get("color_temp") is not None:
            entry["color_temp"] = d["color_temp"]
        if d.get("color") is not None:
            entry["color"] = {k: v for k, v in d["color"].items() if k in ("hue", "saturation")}
        per_lamp[lamp] = entry
    _scenes[name] = {"lights": per_lamp}
    await _save_json(SCENES_FILE, _scenes)
    return {"ok": True, "name": name}

@app.delete("/scenes/{name}")
async def delete_scene(name: str):
    if name not in _scenes:
        raise HTTPException(404, "Scene not found")
    del _scenes[name]
    await _save_json(SCENES_FILE, _scenes)
    return {"ok": True}

@app.put("/scenes/{name}/apply")
async def apply_scene(name: str):
    if name not in _scenes:
        raise HTTPException(404, "Scene not found")
    await _stop_all_effects()
    scene = _scenes[name]
    if "lights" in scene:
        # Per-lamp capture format: send each lamp its own saved state.
        for lamp, payload in scene["lights"].items():
            await _pub_lamp(lamp, payload)
    else:
        # Flat format: apply same payload to all lamps.
        await _publish_many(_lamp_names(), scene)
    return {"ok": True}

# ── Colors ────────────────────────────────────────────────────────────────────

@app.get("/colors")
async def list_colors():
    return COLORS

@app.put("/colors/{name}/apply")
async def apply_color(name: str):
    if name not in COLORS:
        raise HTTPException(404, "Unknown color")
    await _stop_all_effects()
    await _publish_many(_lamp_names(), COLORS[name])
    return {"ok": True}

# ── Effects ───────────────────────────────────────────────────────────────────

@app.get("/effects")
async def list_effects():
    return {
        "firmware":     list(FIRMWARE_EFFECTS.keys()),
        "user_defined": {n: d for n, d in _custom_effects.items()},
    }

@app.post("/effects/custom")
async def create_custom_effect(body: CustomEffectBody):
    name = _validate_name(body.name, "Effect name")
    _custom_effects[name] = {
        "steps":          [s.model_dump(exclude_none=True) for s in body.steps],
        "loop":           body.loop,
        "ignore_master":  body.ignore_master,
    }
    await _save_json(CUSTOM_EFFECTS_FILE, _custom_effects)
    return {"ok": True, "name": name}

@app.delete("/effects/custom/{name}")
async def delete_custom_effect(name: str):
    if name not in _custom_effects:
        raise HTTPException(404, "Custom effect not found")
    del _custom_effects[name]
    await _save_json(CUSTOM_EFFECTS_FILE, _custom_effects)
    return {"ok": True}

@app.put("/effects/stop")
async def stop_effects():
    await _stop_all_effects()
    return {"ok": True}

@app.put("/effects/{effect}/all")
async def apply_effect_all(effect: str):
    resolved = _resolve_effect(effect)
    if resolved is None:
        raise HTTPException(404, "Unknown effect")
    await _stop_all_effects()
    await _apply_effect(_lamp_names(), resolved)
    return {"ok": True, "effect": effect}

@app.put("/effects/{effect}/groups/{group}")
async def apply_effect_group(effect: str, group: str):
    if group not in GROUPS:
        raise HTTPException(404, "Group not found")
    resolved = _resolve_effect(effect)
    if resolved is None:
        raise HTTPException(404, "Unknown effect")
    await _stop_all_effects()
    await _apply_effect(GROUPS[group], resolved)
    return {"ok": True, "effect": effect, "group": group}

# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status":        "ok",
        "devices_seen":  len(_devices),
        "lamps_active":  len(_lamp_names()),
        "effects_running": len(_effect_tasks),
        "mqtt_connected": _mqtt_client is not None,
    }
