"""Tests for ingester.schema."""

import dataclasses

import pytest

from ingester.schema import SCHEMAS, schema_for_file

# ---------------------------------------------------------------------------
# schema_for_file
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename",
    [
        "gps_2024_01_01.csv",
        "imu_2024_01_01.csv",
        "wind_2024_01_01.csv",
        "ctrl_2024_01_01.csv",
    ],
)
def test_schema_for_file_known_prefixes(filename):
    assert schema_for_file(filename) is not None


def test_schema_for_file_unknown_prefix():
    assert schema_for_file("depth_2024.csv") is None


def test_schema_for_file_no_underscore():
    # "foo" → prefix "foo" → not in SCHEMAS
    assert schema_for_file("foo") is None


def test_schema_for_file_empty():
    # "" → "".split("_")[0] = "" → not in SCHEMAS
    assert schema_for_file("") is None


def test_schema_for_file_case_sensitive():
    # "GPS_..." → prefix "GPS" → not in SCHEMAS (keys are lowercase)
    assert schema_for_file("GPS_2024.csv") is None
    assert schema_for_file("IMU_data.csv") is None


def test_schema_for_file_multiple_underscores():
    # "gps_sensor_board_2024.csv" → prefix "gps" → GPS schema
    result = schema_for_file("gps_sensor_board_2024.csv")
    assert result is not None
    assert result.measurement == "telemetry_gps"


# ---------------------------------------------------------------------------
# SensorSchema.required_columns
# ---------------------------------------------------------------------------


def test_required_columns_gps():
    schema = SCHEMAS["gps"]
    rc = schema.required_columns
    assert "timestamp_utc" in rc
    assert "device_id" in rc
    assert "lat" in rc
    assert "lon" in rc
    assert "speed_kn" in rc
    assert "heading_deg" in rc


def test_required_columns_excludes_optionals():
    schema = SCHEMAS["gps"]
    rc = schema.required_columns
    # optional_floats of GPS: alt_m, hdop; optional_ints: sats_used, fix_quality
    assert "alt_m" not in rc
    assert "hdop" not in rc
    assert "sats_used" not in rc
    assert "fix_quality" not in rc


def test_required_columns_imu():
    schema = SCHEMAS["imu"]
    rc = schema.required_columns
    assert {"timestamp_utc", "device_id", "yaw_deg", "pitch_deg", "roll_deg"} <= rc
    assert "heading_mag_deg" not in rc


def test_required_columns_wind():
    schema = SCHEMAS["wind"]
    rc = schema.required_columns
    assert {"timestamp_utc", "device_id", "wind_speed_kn", "wind_dir_deg"} <= rc


def test_required_columns_ctrl():
    schema = SCHEMAS["ctrl"]
    rc = schema.required_columns
    assert {"timestamp_utc", "device_id", "ride_height_m", "flap_angle_deg", "rudder_deg"} <= rc


def test_required_columns_is_frozenset():
    assert isinstance(SCHEMAS["gps"].required_columns, frozenset)


# ---------------------------------------------------------------------------
# Immutability (frozen=True)
# ---------------------------------------------------------------------------


def test_frozen_raises_on_mutation():
    schema = SCHEMAS["gps"]
    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        schema.measurement = "hacked"


def test_frozen_raises_on_tuple_mutation():
    schema = SCHEMAS["gps"]
    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        schema.required_floats = ("x",)
