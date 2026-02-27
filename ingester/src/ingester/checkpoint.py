"""Checkpoint persistence and quarantine writer."""

import json
from pathlib import Path
from typing import Any


def load(state_file: str | Path) -> dict[str, Any]:
    """Load checkpoint state from JSON file.

    Returns dict of {filename: {byte_offset, last_timestamp}}.
    Returns empty dict if file does not exist.
    """
    path = Path(state_file)
    if not path.exists():
        return {}
    with path.open() as f:
        return json.load(f)  # type: ignore[no-any-return]


def save(state_file: str | Path, state: dict[str, Any]) -> None:
    """Atomically write checkpoint state to JSON file."""
    path = Path(state_file)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.rename(path)


def quarantine_row(
    quarantine_dir: str | Path,
    filename: str,
    row_dict: dict[str, Any],
    reason: str,
) -> None:
    """Append a bad row plus metadata to quarantine/quarantine.jsonl (one JSON object per line).

    Using JSONL avoids CSV header/schema mismatch when rows from different sensors
    are quarantined to the same file.
    """
    q_dir = Path(quarantine_dir)
    q_dir.mkdir(parents=True, exist_ok=True)
    q_file = q_dir / "quarantine.jsonl"

    record: dict[str, Any] = dict(row_dict)
    record["_source_file"] = filename
    record["_reason"] = reason

    with q_file.open("a") as f:
        f.write(json.dumps(record, default=str) + "\n")
