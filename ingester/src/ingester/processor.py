"""File-level processing: parse → write → quarantine."""

import logging
from pathlib import Path
from typing import Any

from . import checkpoint, parser, writer
from .schema import schema_for_file

log = logging.getLogger(__name__)


def process_file(
    csv_path: Path,
    cfg: dict[str, Any],
    state: dict[str, Any],
    influx_client: Any,
) -> dict[str, Any]:
    """Parse and ingest one CSV file; return the updated state entry.

    The byte_offset in the returned entry only advances past rows that were
    successfully written.  If a batch write fails, the offset is held at its
    previous value so the rows are retried on the next cycle.
    """
    filename = csv_path.name
    entry: dict[str, Any] = state.get(filename, {"byte_offset": 0, "last_timestamp": None})

    schema = schema_for_file(filename)
    if schema is None:
        log.warning("Skipping %s: unknown sensor prefix", filename)
        return entry

    byte_offset: int = entry["byte_offset"]
    valid_rows, bad_rows, new_offset = parser.parse_file(csv_path, byte_offset, schema)

    if not valid_rows and not bad_rows:
        return entry  # no new data

    batch_size: int = cfg["batch_size"]
    written = 0
    all_ok = True
    last_ts: str | None = entry["last_timestamp"]

    for i in range(0, len(valid_rows), batch_size):
        batch = valid_rows[i : i + batch_size]
        success = writer.write_batch(
            influx_client,
            bucket=cfg["influx_bucket"],
            org=cfg["influx_org"],
            vessel=cfg["vessel"],
            rows=batch,
            measurement=schema.measurement,
        )
        if success:
            written += len(batch)
            if batch:
                last_ts = batch[-1]["timestamp_utc"].isoformat()
        else:
            log.error(
                "Batch write failed for %s; stopping further batches for this cycle", filename
            )
            all_ok = False
            break

    for row, reason in bad_rows:
        checkpoint.quarantine_row(cfg["quarantine_dir"], filename, row, reason)

    log.info(
        "file=%s processed=%d quarantined=%d last_ts=%s",
        filename,
        written,
        len(bad_rows),
        last_ts,
    )

    return {
        # Only advance past bytes we successfully wrote; hold offset on partial failure
        # so rows are retried next cycle (InfluxDB deduplicates by timestamp).
        "byte_offset": new_offset if all_ok else byte_offset,
        "last_timestamp": last_ts,
    }
