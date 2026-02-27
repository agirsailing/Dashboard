"""Tests for ingester.parser."""

import csv
from datetime import UTC, datetime
from pathlib import Path

import pytest
from conftest import write_csv

from ingester.parser import _parse_timestamp, parse_file
from ingester.schema import SCHEMAS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

GPS = SCHEMAS["gps"]

GPS_FIELDS = ["timestamp_utc", "device_id", "lat", "lon", "speed_kn", "heading_deg"]

GPS_ROW: dict = {
    "timestamp_utc": "2024-01-15T10:30:00Z",
    "device_id": "gps1",
    "lat": "37.7749",
    "lon": "-122.4194",
    "speed_kn": "5.2",
    "heading_deg": "180.0",
}


def make_gps_csv(tmp_path: Path, rows: list[dict] | None = None) -> Path:
    if rows is None:
        rows = [GPS_ROW]
    p = tmp_path / "gps_test.csv"
    return write_csv(p, rows, fieldnames=GPS_FIELDS)


# ---------------------------------------------------------------------------
# _parse_timestamp
# ---------------------------------------------------------------------------


def test_parse_timestamp_z_suffix():
    dt = _parse_timestamp("2024-01-15T10:30:00Z")
    assert dt.tzinfo == UTC
    assert dt.year == 2024 and dt.month == 1 and dt.day == 15


def test_parse_timestamp_plus_offset():
    dt = _parse_timestamp("2024-01-15T10:30:00+00:00")
    assert dt.tzinfo is not None
    assert dt.hour == 10


def test_parse_timestamp_naive():
    dt = _parse_timestamp("2024-01-15T10:30:00")
    assert dt.tzinfo == UTC


def test_parse_timestamp_strips_whitespace():
    dt = _parse_timestamp("  2024-01-15T10:30:00Z  ")
    assert isinstance(dt, datetime)
    assert dt.year == 2024


def test_parse_timestamp_raises_on_garbage():
    with pytest.raises((ValueError, TypeError)):
        _parse_timestamp("not-a-date")


def test_parse_timestamp_raises_on_empty():
    with pytest.raises((ValueError, TypeError)):
        _parse_timestamp("")


def test_parse_timestamp_returns_datetime():
    result = _parse_timestamp("2024-01-15T10:30:00Z")
    assert isinstance(result, datetime)


# ---------------------------------------------------------------------------
# parse_file — happy path
# ---------------------------------------------------------------------------


def test_parse_single_row(tmp_path):
    p = make_gps_csv(tmp_path)
    valid, bad, _ = parse_file(p, 0, GPS)
    assert len(valid) == 1
    assert bad == []


def test_parse_correct_field_types(tmp_path):
    p = make_gps_csv(tmp_path)
    valid, _, _ = parse_file(p, 0, GPS)
    row = valid[0]
    assert isinstance(row["timestamp_utc"], datetime)
    assert isinstance(row["lat"], float)
    assert isinstance(row["lon"], float)
    assert isinstance(row["speed_kn"], float)
    assert isinstance(row["device_id"], str)


def test_parse_multiple_rows(tmp_path):
    rows = [{**GPS_ROW, "lat": "10.0"}, {**GPS_ROW, "lat": "20.0"}]
    p = make_gps_csv(tmp_path, rows)
    valid, bad, _ = parse_file(p, 0, GPS)
    assert len(valid) == 2
    assert bad == []


def test_parse_offset_advances_to_eof(tmp_path):
    p = make_gps_csv(tmp_path)
    _, _, offset = parse_file(p, 0, GPS)
    assert offset == p.stat().st_size


def test_parse_empty_file(tmp_path):
    p = tmp_path / "gps_empty.csv"
    p.write_text("")
    valid, bad, _ = parse_file(p, 0, GPS)
    assert valid == []
    assert bad == []


def test_parse_optional_floats_present(tmp_path):
    row = {**GPS_ROW, "alt_m": "100.5", "hdop": "1.2"}
    p = tmp_path / "gps_test.csv"
    write_csv(p, [row], fieldnames=GPS_FIELDS + ["alt_m", "hdop"])
    valid, _, _ = parse_file(p, 0, GPS)
    assert valid[0]["alt_m"] == pytest.approx(100.5)
    assert valid[0]["hdop"] == pytest.approx(1.2)


def test_parse_absent_optionals_not_in_output(tmp_path):
    p = make_gps_csv(tmp_path)
    valid, _, _ = parse_file(p, 0, GPS)
    assert "alt_m" not in valid[0]
    assert "hdop" not in valid[0]
    assert "sats_used" not in valid[0]


def test_parse_source_field_present(tmp_path):
    row = {**GPS_ROW, "source": "ais"}
    p = tmp_path / "gps_test.csv"
    write_csv(p, [row], fieldnames=GPS_FIELDS + ["source"])
    valid, _, _ = parse_file(p, 0, GPS)
    assert valid[0]["source"] == "ais"


def test_parse_source_field_absent(tmp_path):
    p = make_gps_csv(tmp_path)
    valid, _, _ = parse_file(p, 0, GPS)
    assert valid[0]["source"] is None


def test_parse_all_sensors_imu(tmp_path):
    schema = SCHEMAS["imu"]
    row = {
        "timestamp_utc": "2024-01-15T10:30:00Z",
        "device_id": "imu1",
        "yaw_deg": "1.0",
        "pitch_deg": "2.0",
        "roll_deg": "3.0",
    }
    p = tmp_path / "imu_test.csv"
    write_csv(p, [row])
    valid, bad, _ = parse_file(p, 0, schema)
    assert len(valid) == 1 and bad == []


def test_parse_all_sensors_wind(tmp_path):
    schema = SCHEMAS["wind"]
    row = {
        "timestamp_utc": "2024-01-15T10:30:00Z",
        "device_id": "wind1",
        "wind_speed_kn": "10.0",
        "wind_dir_deg": "90.0",
    }
    p = tmp_path / "wind_test.csv"
    write_csv(p, [row])
    valid, bad, _ = parse_file(p, 0, schema)
    assert len(valid) == 1 and bad == []


def test_parse_all_sensors_ctrl(tmp_path):
    schema = SCHEMAS["ctrl"]
    row = {
        "timestamp_utc": "2024-01-15T10:30:00Z",
        "device_id": "ctrl1",
        "ride_height_m": "0.5",
        "flap_angle_deg": "10.0",
        "rudder_deg": "5.0",
    }
    p = tmp_path / "ctrl_test.csv"
    write_csv(p, [row])
    valid, bad, _ = parse_file(p, 0, schema)
    assert len(valid) == 1 and bad == []


# ---------------------------------------------------------------------------
# Byte offset
# ---------------------------------------------------------------------------


def test_nonzero_offset_sees_only_new_rows(tmp_path):
    p = make_gps_csv(tmp_path, [GPS_ROW])
    _, _, offset1 = parse_file(p, 0, GPS)

    # Append one more data row (no header)
    with p.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=GPS_FIELDS, extrasaction="ignore")
        writer.writerow({**GPS_ROW, "lat": "55.0"})

    valid, bad, _ = parse_file(p, offset1, GPS)
    assert len(valid) == 1
    assert valid[0]["lat"] == pytest.approx(55.0)
    assert bad == []


def test_offset_at_eof_returns_empty(tmp_path):
    p = make_gps_csv(tmp_path)
    eof = p.stat().st_size
    valid, bad, offset = parse_file(p, eof, GPS)
    assert valid == []
    assert bad == []
    assert offset == eof


# ---------------------------------------------------------------------------
# Bad rows
# ---------------------------------------------------------------------------


def test_bad_empty_timestamp(tmp_path):
    p = make_gps_csv(tmp_path, [{**GPS_ROW, "timestamp_utc": ""}])
    valid, bad, _ = parse_file(p, 0, GPS)
    assert valid == []
    assert len(bad) == 1
    assert "empty" in bad[0][1]


def test_bad_invalid_timestamp(tmp_path):
    p = make_gps_csv(tmp_path, [{**GPS_ROW, "timestamp_utc": "not-a-date"}])
    valid, bad, _ = parse_file(p, 0, GPS)
    assert valid == []
    assert len(bad) == 1
    assert "invalid timestamp" in bad[0][1]


def test_bad_empty_device_id(tmp_path):
    p = make_gps_csv(tmp_path, [{**GPS_ROW, "device_id": ""}])
    valid, bad, _ = parse_file(p, 0, GPS)
    assert valid == []
    assert len(bad) == 1
    assert "device_id" in bad[0][1]


def test_bad_missing_required_float(tmp_path):
    p = make_gps_csv(tmp_path, [{**GPS_ROW, "lat": ""}])
    valid, bad, _ = parse_file(p, 0, GPS)
    assert valid == []
    assert len(bad) == 1
    assert "missing required field" in bad[0][1]


def test_bad_non_numeric_required_float(tmp_path):
    p = make_gps_csv(tmp_path, [{**GPS_ROW, "lat": "abc"}])
    valid, bad, _ = parse_file(p, 0, GPS)
    assert valid == []
    assert len(bad) == 1
    assert "non-numeric" in bad[0][1]


def test_bad_non_numeric_optional_float(tmp_path):
    row = {**GPS_ROW, "alt_m": "xyz"}
    p = tmp_path / "gps_test.csv"
    write_csv(p, [row], fieldnames=GPS_FIELDS + ["alt_m"])
    valid, bad, _ = parse_file(p, 0, GPS)
    assert valid == []
    assert len(bad) == 1
    assert "non-numeric" in bad[0][1]


def test_bad_missing_required_column_quarantines_all(tmp_path):
    # CSV missing the 'lat' column — all rows quarantined
    fields = ["timestamp_utc", "device_id", "lon", "speed_kn", "heading_deg"]
    row = {
        "timestamp_utc": "2024-01-15T10:30:00Z",
        "device_id": "gps1",
        "lon": "-122.4194",
        "speed_kn": "5.2",
        "heading_deg": "180.0",
    }
    p = tmp_path / "gps_test.csv"
    write_csv(p, [row, row], fieldnames=fields)
    valid, bad, _ = parse_file(p, 0, GPS)
    assert valid == []
    assert len(bad) == 2
    assert "missing required columns" in bad[0][1]


def test_mixed_good_and_bad_rows(tmp_path):
    rows = [
        GPS_ROW,
        {**GPS_ROW, "lat": "abc"},  # bad
        GPS_ROW,
    ]
    p = make_gps_csv(tmp_path, rows)
    valid, bad, _ = parse_file(p, 0, GPS)
    assert len(valid) == 2
    assert len(bad) == 1


def test_bad_whitespace_only_timestamp(tmp_path):
    p = make_gps_csv(tmp_path, [{**GPS_ROW, "timestamp_utc": "   "}])
    valid, bad, _ = parse_file(p, 0, GPS)
    assert valid == []
    assert len(bad) == 1
    assert "empty" in bad[0][1]


# ---------------------------------------------------------------------------
# Timestamp format parametrize
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ts",
    [
        "2024-01-15T10:30:00Z",
        "2024-01-15T10:30:00+00:00",
        "2024-01-15T10:30:00",
        "2024-01-15T10:30:00.123456Z",
    ],
)
def test_timestamp_formats_accepted(tmp_path, ts):
    p = make_gps_csv(tmp_path, [{**GPS_ROW, "timestamp_utc": ts}])
    valid, bad, _ = parse_file(p, 0, GPS)
    assert len(valid) == 1
    assert bad == []
    assert isinstance(valid[0]["timestamp_utc"], datetime)
