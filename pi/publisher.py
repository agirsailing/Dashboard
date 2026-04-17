#!/usr/bin/env python3
"""Publish CSV telemetry rows to MQTT.

Requires: paho-mqtt>=2.0  (pip install paho-mqtt)

Two modes:
  --file PATH   Read entire CSV and publish all rows (test / replay mode).
  --watch PATH  Tail a growing CSV file, publishing new rows as they appear (live mode).

The sensor type is inferred from the filename prefix (gps_, imu_, wind_, ctrl_).
"""

import argparse
import csv
import json
import sys
import time
from pathlib import Path

import paho.mqtt.client as mqtt

_KNOWN_SENSORS = {"gps", "imu", "wind", "ctrl"}


def _topic_for(filename: str, prefix: str) -> str | None:
    sensor_type = filename.split("_")[0]
    if sensor_type not in _KNOWN_SENSORS:
        return None
    return f"{prefix}/{sensor_type}"


def _publish_file(client: mqtt.Client, path: Path, topic: str, delay: float) -> None:
    """Read entire CSV and publish each row, with optional delay between rows."""
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            client.publish(topic, json.dumps(row), qos=1)
            if delay > 0:
                time.sleep(delay)
    print(f"Published all rows from {path.name} → {topic}")


def _watch_file(client: mqtt.Client, path: Path, topic: str) -> None:
    """Tail a growing CSV file and publish each new row as it appears."""
    print(f"Watching {path} → {topic}  (Ctrl-C to stop)")
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames  # consume header
        # seek to end so we only see rows appended from now on
        f.seek(0, 2)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.05)
                continue
            line = line.strip()
            if not line:
                continue
            values = next(csv.reader([line]))
            row = dict(zip(fieldnames or [], values))
            client.publish(topic, json.dumps(row), qos=1)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Publish CSV telemetry to MQTT",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", metavar="PATH", help="Publish entire CSV (test mode)")
    group.add_argument("--watch", metavar="PATH", help="Tail CSV for new rows (live mode)")
    p.add_argument("--broker", default="localhost", help="MQTT broker host")
    p.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    p.add_argument("--topic-prefix", default="agir/telemetry", help="MQTT topic prefix")
    p.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Seconds between rows in --file mode (0 = as fast as possible)",
    )
    args = p.parse_args()

    path = Path(args.file or args.watch)
    topic = _topic_for(path.name, args.topic_prefix)
    if topic is None:
        print(
            f"ERROR: cannot infer sensor type from filename {path.name!r}. "
            f"Expected prefix: {', '.join(sorted(_KNOWN_SENSORS))}_",
            file=sys.stderr,
        )
        sys.exit(1)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(args.broker, args.port, keepalive=60)
    client.loop_start()

    try:
        if args.file:
            _publish_file(client, path, topic, args.delay)
        else:
            _watch_file(client, path, topic)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
