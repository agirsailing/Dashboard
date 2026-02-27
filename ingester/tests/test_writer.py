"""Tests for ingester.writer."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from ingester.writer import _row_to_point, write_batch

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

TS = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)

BASE_ROW: dict = {
    "timestamp_utc": TS,
    "device_id": "gps1",
    "source": None,
    "lat": 37.7749,
    "lon": -122.4194,
    "speed_kn": 5.2,
    "heading_deg": 180.0,
}

MEASUREMENT = "telemetry_gps"
VESSEL = "boat1"


def make_client():
    client = MagicMock()
    write_api = MagicMock()
    client.write_api.return_value = write_api
    return client, write_api


# ---------------------------------------------------------------------------
# _row_to_point
# ---------------------------------------------------------------------------


def test_row_to_point_measurement_name():
    p = _row_to_point(BASE_ROW, VESSEL, MEASUREMENT)
    assert p._name == MEASUREMENT


def test_row_to_point_vessel_tag():
    p = _row_to_point(BASE_ROW, VESSEL, MEASUREMENT)
    assert p._tags["vessel"] == VESSEL


def test_row_to_point_device_id_tag():
    p = _row_to_point(BASE_ROW, VESSEL, MEASUREMENT)
    assert p._tags["device_id"] == "gps1"


def test_row_to_point_timestamp():
    p = _row_to_point(BASE_ROW, VESSEL, MEASUREMENT)
    assert p._time == TS


def test_row_to_point_skip_timestamp_utc():
    p = _row_to_point(BASE_ROW, VESSEL, MEASUREMENT)
    assert "timestamp_utc" not in p._fields


def test_row_to_point_skip_device_id_from_fields():
    p = _row_to_point(BASE_ROW, VESSEL, MEASUREMENT)
    assert "device_id" not in p._fields


def test_row_to_point_skip_source_from_fields():
    p = _row_to_point(BASE_ROW, VESSEL, MEASUREMENT)
    assert "source" not in p._fields


def test_row_to_point_numeric_fields():
    p = _row_to_point(BASE_ROW, VESSEL, MEASUREMENT)
    assert p._fields["lat"] == pytest.approx(37.7749)
    assert p._fields["lon"] == pytest.approx(-122.4194)
    assert p._fields["speed_kn"] == pytest.approx(5.2)


def test_row_to_point_source_tag_present():
    row = {**BASE_ROW, "source": "ais"}
    p = _row_to_point(row, VESSEL, MEASUREMENT)
    assert p._tags.get("source") == "ais"


def test_row_to_point_source_tag_absent_when_none():
    p = _row_to_point(BASE_ROW, VESSEL, MEASUREMENT)
    assert "source" not in p._tags


def test_row_to_point_returns_point_instance():
    p = _row_to_point(BASE_ROW, VESSEL, MEASUREMENT)
    assert isinstance(p, Point)


# ---------------------------------------------------------------------------
# write_batch — success paths
# ---------------------------------------------------------------------------


def test_write_batch_empty_rows_returns_true():
    client, write_api = make_client()
    result = write_batch(client, "bucket", "org", VESSEL, [], MEASUREMENT)
    assert result is True


def test_write_batch_empty_rows_no_influx_call():
    client, write_api = make_client()
    write_batch(client, "bucket", "org", VESSEL, [], MEASUREMENT)
    write_api.write.assert_not_called()


def test_write_batch_single_row_returns_true():
    client, write_api = make_client()
    result = write_batch(client, "bucket", "org", VESSEL, [BASE_ROW], MEASUREMENT)
    assert result is True


def test_write_batch_write_called_once():
    client, write_api = make_client()
    write_batch(client, "bucket", "org", VESSEL, [BASE_ROW], MEASUREMENT)
    write_api.write.assert_called_once()


def test_write_batch_correct_bucket_and_org():
    client, write_api = make_client()
    write_batch(client, "mybucket", "myorg", VESSEL, [BASE_ROW], MEASUREMENT)
    kwargs = write_api.write.call_args.kwargs
    assert kwargs["bucket"] == "mybucket"
    assert kwargs["org"] == "myorg"


def test_write_batch_multiple_rows_sends_n_points():
    client, write_api = make_client()
    rows = [BASE_ROW, BASE_ROW, BASE_ROW]
    write_batch(client, "bucket", "org", VESSEL, rows, MEASUREMENT)
    kwargs = write_api.write.call_args.kwargs
    assert len(kwargs["record"]) == 3


def test_write_batch_write_api_created_with_synchronous():
    client, write_api = make_client()
    write_batch(client, "bucket", "org", VESSEL, [BASE_ROW], MEASUREMENT)
    client.write_api.assert_called_once_with(write_options=SYNCHRONOUS)


# ---------------------------------------------------------------------------
# write_batch — retry behaviour
# ---------------------------------------------------------------------------


@patch("ingester.writer.time.sleep")
def test_write_batch_retries_on_transient_failure(mock_sleep):
    client, write_api = make_client()
    write_api.write.side_effect = [Exception("transient"), None]
    result = write_batch(client, "bucket", "org", VESSEL, [BASE_ROW], MEASUREMENT)
    assert result is True
    assert write_api.write.call_count == 2


@patch("ingester.writer.time.sleep")
def test_write_batch_sleeps_between_retries(mock_sleep):
    client, write_api = make_client()
    write_api.write.side_effect = [Exception("fail"), None]
    write_batch(client, "bucket", "org", VESSEL, [BASE_ROW], MEASUREMENT)
    assert mock_sleep.called


@patch("ingester.writer.time.sleep")
def test_write_batch_all_attempts_fail_returns_false(mock_sleep):
    client, write_api = make_client()
    write_api.write.side_effect = Exception("always fails")
    result = write_batch(client, "bucket", "org", VESSEL, [BASE_ROW], MEASUREMENT)
    assert result is False


@patch("ingester.writer.time.sleep")
def test_write_batch_exactly_3_attempts(mock_sleep):
    client, write_api = make_client()
    write_api.write.side_effect = Exception("always fails")
    write_batch(client, "bucket", "org", VESSEL, [BASE_ROW], MEASUREMENT)
    assert write_api.write.call_count == 3


@patch("ingester.writer.time.sleep")
def test_write_batch_no_sleep_on_first_success(mock_sleep):
    client, write_api = make_client()
    write_batch(client, "bucket", "org", VESSEL, [BASE_ROW], MEASUREMENT)
    mock_sleep.assert_not_called()


@patch("ingester.writer.time.sleep")
def test_write_batch_sleep_delay_values(mock_sleep):
    client, write_api = make_client()
    # Two failures → sleeps with [1, 2]; third attempt succeeds
    write_api.write.side_effect = [Exception("f1"), Exception("f2"), None]
    write_batch(client, "bucket", "org", VESSEL, [BASE_ROW], MEASUREMENT)
    sleep_args = [c.args[0] for c in mock_sleep.call_args_list]
    assert sleep_args == [1, 2]
