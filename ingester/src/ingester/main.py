"""Ägir Dashboard ingester — CLI entry point and file-watching loop."""

import logging
import os
import queue
import signal
import sys
from pathlib import Path
from typing import Any

from influxdb_client import InfluxDBClient
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from . import checkpoint, processor

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------


def _env(key: str, default: str | None = None) -> str:
    val = os.environ.get(key, default)
    if val is None:
        print(f"ERROR: required environment variable {key} is not set", file=sys.stderr)
        sys.exit(1)
    return val


def _load_config() -> dict[str, Any]:
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
# Watchdog event handler
# ---------------------------------------------------------------------------

log = logging.getLogger(__name__)
_running = True


def _handle_signal(signum: int, frame: Any) -> None:
    global _running
    log.info("Received signal %s, shutting down gracefully…", signum)
    _running = False


class _CSVHandler(FileSystemEventHandler):
    """Push created/modified CSV paths onto a queue for the main loop."""

    def __init__(self, q: "queue.Queue[Path]") -> None:
        self._q = q

    def _enqueue(self, event: FileSystemEvent) -> None:
        if not event.is_directory and str(event.src_path).endswith(".csv"):
            self._q.put(Path(str(event.src_path)))

    def on_created(self, event: FileSystemEvent) -> None:
        self._enqueue(event)

    def on_modified(self, event: FileSystemEvent) -> None:
        self._enqueue(event)


# ---------------------------------------------------------------------------
# Main loop helpers
# ---------------------------------------------------------------------------


def _scan_all(
    cfg: dict[str, Any],
    state: dict[str, Any],
    influx_client: Any,
) -> None:
    """Process every CSV in incoming_dir and persist the checkpoint."""
    state_file: Path = cfg["state_file"]
    for csv_path in sorted(cfg["incoming_dir"].glob("*.csv")):
        if not _running:
            break
        try:
            state[csv_path.name] = processor.process_file(csv_path, cfg, state, influx_client)
        except Exception:
            log.exception("Unexpected error processing %s", csv_path.name)
    checkpoint.save(state_file, state)


def _drain_queue(q: "queue.Queue[Path]") -> set[Path]:
    """Return all paths currently in the queue without blocking."""
    paths: set[Path] = set()
    while True:
        try:
            paths.add(q.get_nowait())
        except queue.Empty:
            break
    return paths


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
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

    for d in (cfg["incoming_dir"], cfg["quarantine_dir"], cfg["state_file"].parent):
        d.mkdir(parents=True, exist_ok=True)

    _setup_logging(cfg["log_file"])
    log.info("Ägir ingester starting — vessel=%s incoming=%s", cfg["vessel"], cfg["incoming_dir"])

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    event_q: queue.Queue[Path] = queue.Queue()
    obs = Observer()
    obs.schedule(_CSVHandler(event_q), str(cfg["incoming_dir"]), recursive=False)
    obs.start()
    log.info(
        "Watching %s for CSV files (poll_interval=%ds)",
        cfg["incoming_dir"],
        cfg["poll_interval"],
    )

    state = checkpoint.load(cfg["state_file"])

    with InfluxDBClient(
        url=cfg["influx_url"],
        token=cfg["influx_token"],
        org=cfg["influx_org"],
    ) as influx_client:
        # Process any files that arrived before the observer started
        _scan_all(cfg, state, influx_client)

        while _running:
            try:
                # Block until a file event or poll_interval elapses
                first = event_q.get(timeout=cfg["poll_interval"])
                paths = {first} | _drain_queue(event_q)
            except queue.Empty:
                # Timeout: periodic fallback scan catches anything the observer missed
                _scan_all(cfg, state, influx_client)
                continue

            for csv_path in sorted(paths):
                if not _running:
                    break
                try:
                    state[csv_path.name] = processor.process_file(
                        csv_path, cfg, state, influx_client
                    )
                except Exception:
                    log.exception("Unexpected error processing %s", csv_path.name)
            checkpoint.save(cfg["state_file"], state)

    obs.stop()
    obs.join()
    log.info("Ingester stopped.")


if __name__ == "__main__":
    main()
