"""InfluxDB batch writer with exponential backoff."""

import logging
import time

from influxdb_client import Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

logger = logging.getLogger(__name__)

MEASUREMENT = "telemetry_gps"
TAG_FIELDS = {"vessel", "device_id", "source"}
INT_FIELDS = {"sats_used", "fix_quality"}


def _row_to_point(row: dict, vessel: str) -> Point:
    p = (
        Point(MEASUREMENT)
        .tag("vessel", vessel)
        .tag("device_id", row["device_id"])
        .field("lat", float(row["lat"]))
        .field("lon", float(row["lon"]))
        .field("speed_kn", float(row["speed_kn"]))
        .field("heading_deg", float(row["heading_deg"]))
        .time(row["timestamp_utc"])
    )
    if row.get("source"):
        p = p.tag("source", row["source"])
    for col in ("alt_m", "hdop"):
        if col in row:
            p = p.field(col, float(row[col]))
    for col in ("sats_used", "fix_quality"):
        if col in row:
            p = p.field(col, int(row[col]))
    return p


def write_batch(client, bucket: str, org: str, vessel: str, rows: list[dict]) -> bool:
    """Write a batch of validated rows to InfluxDB.

    Retries with exponential backoff: 3 attempts, delays 1s / 2s / 4s.
    Returns True on success, False if all attempts fail.
    """
    if not rows:
        return True

    points = [_row_to_point(row, vessel) for row in rows]
    write_api = client.write_api(write_options=SYNCHRONOUS)

    delays = [1, 2, 4]
    for attempt, delay in enumerate(delays, start=1):
        try:
            write_api.write(bucket=bucket, org=org, record=points)
            return True
        except Exception as exc:
            logger.warning("InfluxDB write attempt %d/%d failed: %s", attempt, len(delays), exc)
            if attempt < len(delays):
                time.sleep(delay)

    logger.error("InfluxDB write failed after %d attempts", len(delays))
    return False
