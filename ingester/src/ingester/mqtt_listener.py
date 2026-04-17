"""MQTT subscriber that writes incoming telemetry to InfluxDB."""

import json
import logging
import queue
import threading
from typing import Any

import paho.mqtt.client as mqtt

from .parser import _parse_timestamp
from .schema import SCHEMAS, SensorSchema
from .writer import write_batch

log = logging.getLogger(__name__)

_TOPIC_PREFIX = "agir/telemetry/"


def _validate_row(
    payload: dict[str, Any], schema: SensorSchema
) -> tuple[dict[str, Any] | None, str | None]:
    """Validate a JSON message dict against a sensor schema.

    Returns (valid_row, None) on success or (None, reason) on failure.
    """
    ts_raw = str(payload.get("timestamp_utc", "")).strip()
    if not ts_raw:
        return None, "empty timestamp_utc"
    try:
        ts = _parse_timestamp(ts_raw)
    except (ValueError, TypeError) as exc:
        return None, f"invalid timestamp_utc: {exc}"

    device_id = str(payload.get("device_id", "")).strip()
    if not device_id:
        return None, "empty device_id"

    fields: dict[str, Any] = {}
    for col in schema.required_floats:
        val = payload.get(col)
        if val is None:
            return None, f"missing required field: {col}"
        try:
            fields[col] = float(val)
        except (TypeError, ValueError):
            return None, f"non-numeric value for {col}: {val!r}"

    for col in schema.required_ints:
        val = payload.get(col)
        if val is None:
            return None, f"missing required field: {col}"
        try:
            fields[col] = int(val)
        except (TypeError, ValueError):
            return None, f"non-integer value for {col}: {val!r}"

    for col in schema.optional_floats:
        val = payload.get(col)
        if val is not None:
            try:
                fields[col] = float(val)
            except (TypeError, ValueError):
                return None, f"non-numeric value for optional field {col}: {val!r}"

    for col in schema.optional_ints:
        val = payload.get(col)
        if val is not None:
            try:
                fields[col] = int(val)
            except (TypeError, ValueError):
                return None, f"non-integer value for optional field {col}: {val!r}"

    source = str(payload.get("source", "")).strip() or None

    return {
        "timestamp_utc": ts,
        "device_id": device_id,
        "source": source,
        **fields,
    }, None


class MQTTListener:
    """Subscribes to agir/telemetry/+ and writes validated rows to InfluxDB."""

    def __init__(self, cfg: dict[str, Any], influx_client: Any) -> None:
        self._cfg = cfg
        self._influx = influx_client
        self._q: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        if reason_code == 0:
            client.subscribe("agir/telemetry/+", qos=1)
            log.info("MQTT listener connected and subscribed to agir/telemetry/+")
        else:
            log.error("MQTT connection failed: reason_code=%s", reason_code)

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        if reason_code != 0:
            log.warning(
                "MQTT disconnected unexpectedly (reason=%s), paho will reconnect", reason_code
            )

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        sensor_type = msg.topic.removeprefix(_TOPIC_PREFIX)
        if sensor_type not in SCHEMAS:
            log.warning("Unknown sensor type in topic: %s", msg.topic)
            return
        try:
            payload: dict[str, Any] = json.loads(msg.payload.decode())
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            log.warning("Failed to decode MQTT message on %s: %s", msg.topic, exc)
            return
        self._q.put((sensor_type, payload))

    def start(self) -> None:
        broker = self._cfg["mqtt_broker"]
        port = self._cfg["mqtt_port"]
        self._client.connect(broker, port, keepalive=60)
        self._client.loop_start()
        self._thread = threading.Thread(target=self._drain_loop, daemon=True, name="mqtt-drain")
        self._thread.start()
        log.info("MQTT listener ready (broker=%s:%d)", broker, port)

    def stop(self) -> None:
        self._stop.set()
        self._client.loop_stop()
        self._client.disconnect()
        if self._thread:
            self._thread.join(timeout=5)

    def _drain_loop(self) -> None:
        batch_size: int = self._cfg["batch_size"]
        bucket: str = self._cfg["influx_bucket"]
        org: str = self._cfg["influx_org"]
        vessel: str = self._cfg["vessel"]

        while not self._stop.is_set():
            # Block up to 1 s for the first message
            try:
                sensor_type, payload = self._q.get(timeout=1.0)
            except queue.Empty:
                continue

            batches: dict[str, list[dict[str, Any]]] = {}

            def _accumulate(st: str, p: dict[str, Any]) -> None:
                schema = SCHEMAS[st]
                row, err = _validate_row(p, schema)
                if err:
                    log.warning("Invalid MQTT message (sensor=%s): %s", st, err)
                elif row:
                    batches.setdefault(st, []).append(row)

            _accumulate(sensor_type, payload)

            # Drain remaining without blocking, up to batch_size total
            while sum(len(v) for v in batches.values()) < batch_size:
                try:
                    sensor_type, payload = self._q.get_nowait()
                except queue.Empty:
                    break
                _accumulate(sensor_type, payload)

            for st, rows in batches.items():
                schema = SCHEMAS[st]
                ok = write_batch(self._influx, bucket, org, vessel, rows, schema.measurement)
                if ok:
                    log.info("MQTT: wrote %d row(s) → %s", len(rows), schema.measurement)
                else:
                    log.error("MQTT: failed to write %d row(s) for %s", len(rows), st)
