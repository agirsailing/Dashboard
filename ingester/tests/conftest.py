"""Shared fixtures and helpers for the ingester test suite."""

import csv
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> Path:
    """Write a list of dicts to a CSV file, deriving fieldnames if not given."""
    if not rows:
        path.write_text("")
        return path
    if fieldnames is None:
        fieldnames = list(dict.fromkeys(k for r in rows for k in r))
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


@pytest.fixture
def mock_influx_client():
    client = MagicMock()
    write_api = MagicMock()
    client.write_api.return_value = write_api
    return client, write_api
