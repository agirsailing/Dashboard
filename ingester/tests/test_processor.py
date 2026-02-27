"""Tests for ingester.processor."""

import json
from pathlib import Path
from unittest.mock import MagicMock

from conftest import write_csv

from ingester.processor import process_file

GPS_FIELDS = ["timestamp_utc", "device_id", "lat", "lon", "speed_kn", "heading_deg"]

GPS_ROW = {
    "timestamp_utc": "2024-01-15T10:30:00Z",
    "device_id": "gps1",
    "lat": "37.7749",
    "lon": "-122.4194",
    "speed_kn": "5.2",
    "heading_deg": "180.0",
}


def make_cfg(tmp_path: Path) -> dict:
    return {
        "influx_bucket": "bucket",
        "influx_org": "org",
        "vessel": "boat1",
        "quarantine_dir": tmp_path / "quarantine",
        "batch_size": 500,
    }


def make_client(success: bool = True) -> MagicMock:
    client = MagicMock()
    write_api = MagicMock()
    if not success:
        write_api.write.side_effect = Exception("influx down")
    client.write_api.return_value = write_api
    return client


def make_gps_csv(tmp_path: Path, rows: list | None = None) -> Path:
    if rows is None:
        rows = [GPS_ROW]
    p = tmp_path / "gps_test.csv"
    return write_csv(p, rows, fieldnames=GPS_FIELDS)


# ---------------------------------------------------------------------------
# Unknown prefix
# ---------------------------------------------------------------------------


def test_unknown_prefix_returns_entry_unchanged(tmp_path):
    p = tmp_path / "unknown_sensor.csv"
    p.write_text("timestamp_utc,device_id\n2024-01-01T00:00:00Z,dev1\n")
    cfg = make_cfg(tmp_path)
    entry = {"byte_offset": 0, "last_timestamp": None}
    state = {"unknown_sensor.csv": entry}

    result = process_file(p, cfg, state, make_client())
    assert result == entry


def test_unknown_prefix_no_influx_call(tmp_path):
    p = tmp_path / "unknown_sensor.csv"
    p.write_text("ts,id\nval,val\n")
    cfg = make_cfg(tmp_path)
    client = make_client()

    process_file(p, cfg, {}, client)
    client.write_api.assert_not_called()


# ---------------------------------------------------------------------------
# No new data
# ---------------------------------------------------------------------------


def test_no_new_data_returns_entry_unchanged(tmp_path):
    p = make_gps_csv(tmp_path)
    cfg = make_cfg(tmp_path)
    eof = p.stat().st_size
    state = {"gps_test.csv": {"byte_offset": eof, "last_timestamp": "2024-01-15T10:30:00+00:00"}}

    result = process_file(p, cfg, state, make_client())
    assert result["byte_offset"] == eof


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_valid_rows_offset_advances(tmp_path):
    p = make_gps_csv(tmp_path)
    cfg = make_cfg(tmp_path)
    eof = p.stat().st_size

    result = process_file(p, cfg, {}, make_client())
    assert result["byte_offset"] == eof


def test_valid_rows_last_timestamp_updated(tmp_path):
    p = make_gps_csv(tmp_path)
    cfg = make_cfg(tmp_path)

    result = process_file(p, cfg, {}, make_client())
    assert result["last_timestamp"] is not None
    assert "2024" in result["last_timestamp"]


def test_valid_rows_write_called(tmp_path):
    p = make_gps_csv(tmp_path)
    cfg = make_cfg(tmp_path)
    client = make_client()

    process_file(p, cfg, {}, client)
    client.write_api.return_value.write.assert_called_once()


# ---------------------------------------------------------------------------
# Write failure — offset must NOT advance (bug fix #3)
# ---------------------------------------------------------------------------


def test_write_failure_offset_does_not_advance(tmp_path):
    p = make_gps_csv(tmp_path)
    cfg = make_cfg(tmp_path)
    initial_offset = 0
    state = {"gps_test.csv": {"byte_offset": initial_offset, "last_timestamp": None}}

    result = process_file(p, cfg, state, make_client(success=False))
    assert result["byte_offset"] == initial_offset


def test_write_failure_returns_false(tmp_path):
    p = make_gps_csv(tmp_path)
    cfg = make_cfg(tmp_path)

    result = process_file(p, cfg, {}, make_client(success=False))
    # offset held at 0 (initial) — rows will be retried next cycle
    assert result["byte_offset"] == 0


# ---------------------------------------------------------------------------
# Bad rows → quarantine
# ---------------------------------------------------------------------------


def test_bad_rows_quarantine_file_created(tmp_path):
    p = make_gps_csv(tmp_path, [{**GPS_ROW, "lat": "bad"}])
    cfg = make_cfg(tmp_path)

    process_file(p, cfg, {}, make_client())
    assert (tmp_path / "quarantine" / "quarantine.jsonl").exists()


def test_bad_rows_quarantine_contains_reason(tmp_path):
    p = make_gps_csv(tmp_path, [{**GPS_ROW, "lat": "bad"}])
    cfg = make_cfg(tmp_path)

    process_file(p, cfg, {}, make_client())
    lines = (tmp_path / "quarantine" / "quarantine.jsonl").read_text().splitlines()
    record = json.loads(lines[0])
    assert "_reason" in record
    assert "_source_file" in record


# ---------------------------------------------------------------------------
# Mixed good and bad rows
# ---------------------------------------------------------------------------


def test_mixed_rows_writes_good_quarantines_bad(tmp_path):
    rows = [GPS_ROW, {**GPS_ROW, "lat": "bad"}, GPS_ROW]
    p = make_gps_csv(tmp_path, rows)
    cfg = make_cfg(tmp_path)
    client = make_client()

    process_file(p, cfg, {}, client)

    client.write_api.return_value.write.assert_called_once()
    _, kwargs = client.write_api.return_value.write.call_args
    assert len(kwargs["record"]) == 2  # two good rows
    assert (tmp_path / "quarantine" / "quarantine.jsonl").exists()


# ---------------------------------------------------------------------------
# Batching
# ---------------------------------------------------------------------------


def test_batching_calls_write_multiple_times(tmp_path):
    rows = [GPS_ROW] * 5
    p = make_gps_csv(tmp_path, rows)
    cfg = {**make_cfg(tmp_path), "batch_size": 2}
    client = make_client()

    process_file(p, cfg, {}, client)
    # 5 rows, batch_size=2 → 3 write calls
    assert client.write_api.return_value.write.call_count == 3
