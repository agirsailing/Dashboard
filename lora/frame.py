"""Compact 29-byte binary frame for LoRa transport.

Fits the minimum LoRa payload at SF12 (51 bytes), covering GPS, wind, and
control surfaces in a single transmission.

Frame layout (big-endian):
  Offset  Size  Type  Field
       0     1     B  msg_type        always 0x01
       1     2     H  sequence        rolling uint16
       3     4     I  timestamp       unix seconds UTC (uint32)
       7     4     i  lat_e6          latitude  × 1 000 000 (int32, microdegrees)
      11     4     i  lon_e6          longitude × 1 000 000 (int32, microdegrees)
      15     2     H  speed_e2        speed_kn      × 100 (uint16)
      17     2     H  heading_e1      heading_deg   × 10  (uint16, 0–3600)
      19     2     H  wind_speed_e2   wind_speed_kn × 100 (uint16)
      21     2     H  wind_dir_e1     wind_dir_deg  × 10  (uint16, 0–3600)
      23     2     h  ride_height_e3  ride_height_m × 1000 (int16)
      25     2     h  flap_e1         flap_angle_deg × 10  (int16)
      27     2     h  rudder_e1       rudder_deg     × 10  (int16)
  Total: 29 bytes
"""

import struct
from datetime import UTC, datetime
from typing import Any

MSG_TYPE_TELEMETRY: int = 0x01

_STRUCT = struct.Struct(">BHIiiHHHHhhh")
FRAME_SIZE: int = _STRUCT.size

assert FRAME_SIZE == 29, f"frame size mismatch: expected 29, got {FRAME_SIZE}"


def pack(row: dict[str, Any], sequence: int = 0) -> bytes:
    """Pack a combined GPS+Wind+Ctrl row dict into a 29-byte LoRa frame.

    row must contain: timestamp_utc (datetime or ISO-8601 str), lat, lon,
    speed_kn, heading_deg, wind_speed_kn, wind_dir_deg, ride_height_m,
    flap_angle_deg, rudder_deg.
    """
    ts = row["timestamp_utc"]
    if isinstance(ts, str):
        ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        ts_unix = int(ts_dt.timestamp())
    else:
        ts_unix = int(ts.timestamp())

    return _STRUCT.pack(
        MSG_TYPE_TELEMETRY,
        sequence & 0xFFFF,
        ts_unix,
        round(float(row["lat"]) * 1_000_000),
        round(float(row["lon"]) * 1_000_000),
        round(float(row["speed_kn"]) * 100),
        round(float(row["heading_deg"]) * 10),
        round(float(row["wind_speed_kn"]) * 100),
        round(float(row["wind_dir_deg"]) * 10),
        round(float(row["ride_height_m"]) * 1000),
        round(float(row["flap_angle_deg"]) * 10),
        round(float(row["rudder_deg"]) * 10),
    )


def unpack(data: bytes) -> dict[str, Any]:
    """Unpack a 29-byte LoRa frame into a row dict.

    Returns a dict with JSON-serialisable values; timestamp_utc is an ISO-8601
    string. Raises ValueError for wrong size or unknown msg_type.
    The caller must add 'device_id' before forwarding to MQTT.
    """
    if len(data) != FRAME_SIZE:
        raise ValueError(f"expected {FRAME_SIZE} bytes, got {len(data)}")

    (
        msg_type,
        sequence,
        ts_unix,
        lat_e6,
        lon_e6,
        speed_e2,
        heading_e1,
        wind_speed_e2,
        wind_dir_e1,
        ride_height_e3,
        flap_e1,
        rudder_e1,
    ) = _STRUCT.unpack(data)

    if msg_type != MSG_TYPE_TELEMETRY:
        raise ValueError(f"unknown msg_type: 0x{msg_type:02x}")

    ts = datetime.fromtimestamp(ts_unix, tz=UTC)

    return {
        "sequence": sequence,
        "timestamp_utc": ts.isoformat(),
        "lat": lat_e6 / 1_000_000,
        "lon": lon_e6 / 1_000_000,
        "speed_kn": speed_e2 / 100.0,
        "heading_deg": heading_e1 / 10.0,
        "wind_speed_kn": wind_speed_e2 / 100.0,
        "wind_dir_deg": wind_dir_e1 / 10.0,
        "ride_height_m": ride_height_e3 / 1000.0,
        "flap_angle_deg": flap_e1 / 10.0,
        "rudder_deg": rudder_e1 / 10.0,
    }


def split_to_sensor_rows(
    row: dict[str, Any], device_id: str
) -> dict[str, dict[str, Any]]:
    """Split a combined frame row into per-sensor dicts for MQTT publishing.

    Returns a dict keyed by sensor type ('gps', 'wind', 'ctrl'), each value
    ready to JSON-encode and publish to agir/telemetry/<sensor_type>.
    """
    base = {"timestamp_utc": row["timestamp_utc"], "device_id": device_id}
    return {
        "gps": {
            **base,
            "lat": row["lat"],
            "lon": row["lon"],
            "speed_kn": row["speed_kn"],
            "heading_deg": row["heading_deg"],
        },
        "wind": {
            **base,
            "wind_speed_kn": row["wind_speed_kn"],
            "wind_dir_deg": row["wind_dir_deg"],
        },
        "ctrl": {
            **base,
            "ride_height_m": row["ride_height_m"],
            "flap_angle_deg": row["flap_angle_deg"],
            "rudder_deg": row["rudder_deg"],
        },
    }
