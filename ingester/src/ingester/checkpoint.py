"""Checkpoint persistence and quarantine writer."""

import csv
import json
import os
from pathlib import Path


def load(state_file: str | Path) -> dict:
    """Load checkpoint state from JSON file.

    Returns dict of {filename: {byte_offset, last_timestamp}}.
    Returns empty dict if file does not exist.
    """
    path = Path(state_file)
    if not path.exists():
        return {}
    with path.open() as f:
        return json.load(f)


def save(state_file: str | Path, state: dict) -> None:
    """Atomically write checkpoint state to JSON file."""
    path = Path(state_file)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.rename(path)


def quarantine_row(
    quarantine_dir: str | Path,
    filename: str,
    row_dict: dict,
    reason: str,
) -> None:
    """Append a bad row plus metadata to data/quarantine/quarantine.csv."""
    q_dir = Path(quarantine_dir)
    q_dir.mkdir(parents=True, exist_ok=True)
    q_file = q_dir / "quarantine.csv"

    row = dict(row_dict)
    row["_source_file"] = filename
    row["_reason"] = reason

    write_header = not q_file.exists()
    with q_file.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()), extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)
