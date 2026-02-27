"""Ägir Dashboard ingester — CLI entry point and polling loop."""

import logging
import os
import signal
import sys
import time
from pathlib import Path

from influxdb_client import InfluxDBClient

from . import checkpoint, parser, writer

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------

def _env(key: str, default: str | None = None) -> str:
    val = os.environ.get(key, default)
    if val is None:
        print(f"ERROR: required environment variable {key} is not set", file=sys.stderr)
        sys.exit(1)
    return val


def _load_config() -> dict:
    return {
        "influx_url": _env("INFLUX_URL", "http://localhost:8086"),
        "influx_token": _env("INFLUX_TOKEN"),
        "influx_org": _env("INFLUX_ORG", "agir"),
        "influx_bucket": _env("INFLUX_BUCKET", "agir"),
        "vessel": _env("VESSEL", "agir"),
        "incoming_dir": Path(_env("INCOMING_DIR", "data/incoming")),
        "quarantine_dir": Path(_env("QUARANTINE_DIR", "data/quarantine")),
        "state_file": Path(_env("STATE_FILE", "state/ingest-checkpoints.json")),
        "log_file": Path(_env("LOG_FILE", "logs/ingester.log")),
        "poll_interval": int(_env("POLL_INTERVAL", "10")),
        "batch_size": int(_env("BATCH_SIZE", "500")),
    }


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file),
        ],
    )


# ---------------------------------------------------------------------------
# Polling loop
# ---------------------------------------------------------------------------

log = logging.getLogger(__name__)
_running = True


def _handle_signal(signum, frame):
    global _running
    log.info("Received signal %s, shutting down gracefully…", signum)
    _running = False


def _process_file(
    csv_path: Path,
    cfg: dict,
    state: dict,
    influx_client,
) -> dict:
    """Parse and ingest one CSV file; return updated state entry."""
    filename = csv_path.name
    entry = state.get(filename, {"byte_offset": 0, "last_timestamp": None})
    byte_offset = entry["byte_offset"]

    valid_rows, bad_rows, new_offset = parser.parse_file(csv_path, byte_offset)

    if not valid_rows and not bad_rows:
        return entry  # no new data

    # Write valid rows in batches
    batch_size = cfg["batch_size"]
    written = 0
    last_ts = entry["last_timestamp"]
    for i in range(0, len(valid_rows), batch_size):
        batch = valid_rows[i : i + batch_size]
        success = writer.write_batch(
            influx_client,
            bucket=cfg["influx_bucket"],
            org=cfg["influx_org"],
            vessel=cfg["vessel"],
            rows=batch,
        )
        if success:
            written += len(batch)
            if batch:
                last_ts = batch[-1]["timestamp_utc"].isoformat()
        else:
            log.error("Batch write failed for %s; stopping further batches for this cycle", filename)
            break

    # Quarantine bad rows
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
        "byte_offset": new_offset,
        "last_timestamp": last_ts,
    }


def _poll(cfg: dict, influx_client) -> None:
    incoming_dir: Path = cfg["incoming_dir"]
    state_file: Path = cfg["state_file"]

    state = checkpoint.load(state_file)

    csv_files = sorted(incoming_dir.glob("*.csv"))
    if not csv_files:
        return

    for csv_path in csv_files:
        if not _running:
            break
        try:
            updated = _process_file(csv_path, cfg, state, influx_client)
            state[csv_path.name] = updated
        except Exception:
            log.exception("Unexpected error processing %s", csv_path.name)

    checkpoint.save(state_file, state)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    # Show help if requested
    if "--help" in sys.argv or "-h" in sys.argv:
        print(
            "Ägir ingester v1\n\n"
            "Configuration via environment variables:\n"
            "  INFLUX_URL       InfluxDB URL          (default: http://localhost:8086)\n"
            "  INFLUX_TOKEN     InfluxDB API token     (required)\n"
            "  INFLUX_ORG       InfluxDB org           (default: agir)\n"
            "  INFLUX_BUCKET    InfluxDB bucket        (default: agir)\n"
            "  VESSEL           Vessel tag value       (default: agir)\n"
            "  INCOMING_DIR     Directory to watch     (default: data/incoming)\n"
            "  QUARANTINE_DIR   Quarantine directory   (default: data/quarantine)\n"
            "  STATE_FILE       Checkpoint JSON file   (default: state/ingest-checkpoints.json)\n"
            "  LOG_FILE         Log file path          (default: logs/ingester.log)\n"
            "  POLL_INTERVAL    Seconds between polls  (default: 10)\n"
            "  BATCH_SIZE       Rows per InfluxDB call (default: 500)\n"
        )
        sys.exit(0)

    cfg = _load_config()

    # Create runtime directories
    for d in (cfg["incoming_dir"], cfg["quarantine_dir"], cfg["state_file"].parent):
        d.mkdir(parents=True, exist_ok=True)

    _setup_logging(cfg["log_file"])
    log.info("Ägir ingester starting — vessel=%s incoming=%s", cfg["vessel"], cfg["incoming_dir"])

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    with InfluxDBClient(
        url=cfg["influx_url"],
        token=cfg["influx_token"],
        org=cfg["influx_org"],
    ) as influx_client:
        while _running:
            try:
                _poll(cfg, influx_client)
            except Exception:
                log.exception("Unhandled error in poll cycle")
            if _running:
                time.sleep(cfg["poll_interval"])

    log.info("Ingester stopped.")


if __name__ == "__main__":
    main()
