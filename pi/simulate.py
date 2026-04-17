#!/usr/bin/env python3
"""Sailing telemetry simulator — publishes synthetic live data to MQTT.

Sails a circular track so Grafana's map and all panels update in real time.
All four sensor types (gps, imu, wind, ctrl) are published each tick.

Requires: paho-mqtt>=2.0  (pip install paho-mqtt)

Usage:
    python pi/simulate.py
    python pi/simulate.py --radius 0.003 --period 60
    python pi/simulate.py --lat 57.7 --lon 11.97 --broker 192.168.1.10
"""

import argparse
import json
import math
import time
from datetime import UTC, datetime
from typing import Any

import paho.mqtt.client as mqtt


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _gps(theta: float, args: argparse.Namespace) -> dict[str, Any]:
    lat = args.lat + args.radius * math.sin(theta)
    # correct for longitude compression at this latitude
    lon = args.lon + args.radius * math.cos(theta) / math.cos(math.radians(args.lat))
    heading = (math.degrees(theta) + 90) % 360
    speed = args.speed + 0.8 * math.sin(theta * 2.3)
    return {
        "timestamp_utc": _now(),
        "device_id": args.device_id,
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "speed_kn": round(max(0.1, speed), 2),
        "heading_deg": round(heading, 1),
        "alt_m": round(0.4 + 0.2 * math.sin(theta), 2),
        "sats_used": 12,
        "hdop": round(0.8 + 0.1 * math.sin(theta * 1.7), 2),
        "fix_quality": 1,
    }


def _wind(theta: float, args: argparse.Namespace) -> dict[str, Any]:
    return {
        "timestamp_utc": _now(),
        "device_id": args.device_id,
        "wind_speed_kn": round(max(0.5, args.wind_speed + 2.0 * math.sin(theta * 1.3)), 1),
        "wind_dir_deg": round((args.wind_dir + 8 * math.sin(theta * 0.7)) % 360, 1),
    }


def _imu(theta: float, args: argparse.Namespace) -> dict[str, Any]:
    heading = (math.degrees(theta) + 90) % 360
    return {
        "timestamp_utc": _now(),
        "device_id": args.device_id,
        "yaw_deg": round(heading, 1),
        "pitch_deg": round(1.5 * math.sin(theta * 0.5), 2),
        "roll_deg": round(4.0 * math.cos(theta), 2),
        "heading_mag_deg": round((heading + 2.5) % 360, 1),
    }


def _ctrl(theta: float, args: argparse.Namespace) -> dict[str, Any]:
    speed = args.speed + 0.8 * math.sin(theta * 2.3)
    return {
        "timestamp_utc": _now(),
        "device_id": args.device_id,
        "ride_height_m": round(max(0.0, 0.15 + 0.25 * (speed / args.speed)), 3),
        "flap_angle_deg": round(-6 + 5 * math.sin(theta), 1),
        "rudder_deg": round(5 * math.cos(theta * 2), 1),
    }


def main() -> None:
    p = argparse.ArgumentParser(
        description="Publish synthetic sailing telemetry to MQTT",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--lat", type=float, default=57.70887, help="Track centre latitude")
    p.add_argument("--lon", type=float, default=11.97456, help="Track centre longitude")
    p.add_argument("--radius", type=float, default=0.004, help="Track radius in degrees (~400m)")
    p.add_argument("--period", type=float, default=120.0, help="Seconds per lap")
    p.add_argument("--speed", type=float, default=8.0, help="Base speed (knots)")
    p.add_argument("--wind-dir", type=float, default=220.0, dest="wind_dir", help="Base wind direction (°)")
    p.add_argument("--wind-speed", type=float, default=14.0, dest="wind_speed", help="Base wind speed (kn)")
    p.add_argument("--rate", type=float, default=1.0, help="Updates per second")
    p.add_argument("--broker", default="localhost", help="MQTT broker host")
    p.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    p.add_argument("--topic-prefix", default="agir/telemetry", dest="topic_prefix")
    p.add_argument("--device-id", default="agir_sim", dest="device_id")
    args = p.parse_args()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(args.broker, args.port, keepalive=60)
    client.loop_start()

    print(f"Simulator connected to {args.broker}:{args.port}")
    print(f"Sailing circles: centre ({args.lat}, {args.lon})  radius={args.radius}°  period={args.period}s")
    print("Ctrl-C to stop\n")

    interval = 1.0 / args.rate
    start = time.monotonic()
    count = 0

    try:
        while True:
            t = time.monotonic() - start
            theta = (2 * math.pi * t) / args.period

            for sensor, row in [
                ("gps",  _gps(theta, args)),
                ("imu",  _imu(theta, args)),
                ("wind", _wind(theta, args)),
                ("ctrl", _ctrl(theta, args)),
            ]:
                client.publish(f"{args.topic_prefix}/{sensor}", json.dumps(row), qos=1)

            count += 1
            lap_pct = int(((t % args.period) / args.period) * 100)
            gps = _gps(theta, args)
            print(
                f"\r  t={t:6.1f}s  lap={lap_pct:3d}%"
                f"  pos=({gps['lat']:.5f}, {gps['lon']:.5f})"
                f"  hdg={gps['heading_deg']:5.1f}°"
                f"  spd={gps['speed_kn']:.1f}kn"
                f"  [{count} ticks]",
                end="",
                flush=True,
            )

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\nStopped after {count} ticks ({t:.0f}s).")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
