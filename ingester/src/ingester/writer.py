"""InfluxDB batch writer with exponential backoff."""

import logging
import time
from typing import Any

from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

logger = logging.getLogger(__name__)

_SKIP_FIELDS = {"timestamp_utc", "device_id", "source"}
_MAX_ATTEMPTS = 3


def _row_to_point(row: dict[str, Any], vessel: str, measurement: str) -> Point:
    p = (
        Point(measurement)
        .tag("vessel", vessel)
        .tag("device_id", row["device_id"])
        .time(row["timestamp_utc"])
    )
    if row.get("source"):
        p = p.tag("source", row["source"])
    for key, value in row.items():
        if key in _SKIP_FIELDS:
            continue
        p = p.field(key, value)
    return p  # type: ignore[no-any-return]


def write_batch(
    client: Any,
    bucket: str,
    org: str,
    vessel: str,
    rows: list[dict[str, Any]],
    measurement: str,
) -> bool:
    """Write a batch of validated rows to InfluxDB.

    Retries up to _MAX_ATTEMPTS times with exponential backoff (1s, 2s between attempts).
    Returns True on success, False if all attempts fail.
    """
    if not rows:
        return True

    points = [_row_to_point(row, vessel, measurement) for row in rows]
    write_api = client.write_api(write_options=SYNCHRONOUS)

    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            write_api.write(bucket=bucket, org=org, record=points)
            return True
        except Exception as exc:
            logger.warning("InfluxDB write attempt %d/%d failed: %s", attempt, _MAX_ATTEMPTS, exc)
            if attempt < _MAX_ATTEMPTS:
                time.sleep(2 ** (attempt - 1))  # 1s, 2s

    logger.error("InfluxDB write failed after %d attempts", _MAX_ATTEMPTS)
    return False
