#!/usr/bin/env python3
"""LoRa → MQTT bridge.

Reads 29-byte telemetry frames from a LoRa module over serial/UART, splits
each combined frame into per-sensor JSON messages, and publishes them to an
MQTT broker on the standard agir/telemetry/<sensor> topics.

Requires: pyserial>=3.5  paho-mqtt>=2.0
  pip install pyserial paho-mqtt

Hardware setup
--------------
Configure your LoRa module to deliver raw payload bytes over UART (not
AT-command framing). The bridge reads FRAME_SIZE (29) bytes at a time.
For AT-command modules (RAK, etc.) you will need to strip the response
prefix and decode the hex payload before passing bytes to frame.unpack().

Usage
-----
    python -m lora.bridge --serial /dev/ttyUSB0 --baud 115200 \\
        --broker localhost --device-id agir_pi_01
"""

import json
import logging
import signal
import sys
import time
from typing import Any

import paho.mqtt.client as mqtt

from .frame import FRAME_SIZE, split_to_sensor_rows, unpack

log = logging.getLogger(__name__)
_running = True


def _handle_signal(signum: int, frame: Any) -> None:
    global _running
    log.info("Signal %s received, shutting down…", signum)
    _running = False


def _read_loop(
    ser: Any,  # serial.Serial — typed as Any to avoid import at module level
    device_id: str,
    topic_prefix: str,
    client: mqtt.Client,
) -> None:
    """Read frames from serial port and publish to MQTT until _running is False."""
    buf = bytearray()
    while _running:
        waiting = ser.in_waiting
        if waiting:
            buf += ser.read(waiting)

        while len(buf) >= FRAME_SIZE:
            frame_bytes = bytes(buf[:FRAME_SIZE])
            buf = buf[FRAME_SIZE:]
            try:
                row = unpack(frame_bytes)
            except ValueError as exc:
                log.warning("Bad frame (skipping %d bytes): %s", FRAME_SIZE, exc)
                continue

            for sensor_type, sensor_row in split_to_sensor_rows(row, device_id).items():
                topic = f"{topic_prefix}/{sensor_type}"
                client.publish(topic, json.dumps(sensor_row), qos=1)

            log.debug(
                "seq=%d ts=%s → published gps/wind/ctrl",
                row["sequence"],
                row["timestamp_utc"],
            )

        if not buf:
            time.sleep(0.01)


def main() -> None:
    import argparse

    p = argparse.ArgumentParser(
        description="LoRa → MQTT bridge",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--serial", required=True, metavar="PORT", help="Serial port (e.g. /dev/ttyUSB0)")
    p.add_argument("--baud", type=int, default=115200, help="Baud rate")
    p.add_argument("--broker", default="localhost", help="MQTT broker host")
    p.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    p.add_argument(
        "--topic-prefix", default="agir/telemetry", help="MQTT topic prefix"
    )
    p.add_argument(
        "--device-id", default="agir_lora", help="device_id tag added to unpacked rows"
    )
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        import serial
    except ImportError:
        print("ERROR: pyserial is required — pip install pyserial", file=sys.stderr)
        sys.exit(1)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(args.broker, args.port, keepalive=60)
    client.loop_start()
    log.info("Connected to MQTT broker %s:%d", args.broker, args.port)

    with serial.Serial(args.serial, args.baud, timeout=0.1) as ser:
        log.info(
            "Reading LoRa frames from %s @ %d baud (frame_size=%d bytes)",
            args.serial,
            args.baud,
            FRAME_SIZE,
        )
        _read_loop(ser, args.device_id, args.topic_prefix, client)

    client.loop_stop()
    client.disconnect()
    log.info("Bridge stopped.")


if __name__ == "__main__":
    main()
