"""Sensor schema definitions for all supported CSV prefixes."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SensorSchema:
    measurement: str
    required_floats: tuple[str, ...]
    required_ints: tuple[str, ...] = ()
    optional_floats: tuple[str, ...] = ()
    optional_ints: tuple[str, ...] = ()

    @property
    def required_columns(self) -> frozenset[str]:
        return frozenset(
            {"timestamp_utc", "device_id"} | set(self.required_floats) | set(self.required_ints)
        )


SCHEMAS: dict[str, SensorSchema] = {
    "gps": SensorSchema(
        measurement="telemetry_gps",
        required_floats=("lat", "lon", "speed_kn", "heading_deg"),
        optional_floats=("alt_m", "hdop"),
        optional_ints=("sats_used", "fix_quality"),
    ),
    "imu": SensorSchema(
        measurement="telemetry_imu",
        required_floats=("yaw_deg", "pitch_deg", "roll_deg"),
        optional_floats=("heading_mag_deg",),
    ),
    "wind": SensorSchema(
        measurement="telemetry_wind",
        required_floats=("wind_speed_kn", "wind_dir_deg"),
    ),
    "ctrl": SensorSchema(
        measurement="telemetry_ctrl",
        required_floats=("ride_height_m", "flap_angle_deg", "rudder_deg"),
    ),
    "ctrl_ultrasonic_left": SensorSchema(
        measurement="telemetry_ultrasonic_left",
        required_floats=("distance_m",),
    ),
    "ctrl_ultrasonic_right": SensorSchema(
        measurement="telemetry_ultrasonic_right",
        required_floats=("distance_m",),
    ),
    "ctrl_mean": SensorSchema(
        measurement="telemetry_ultrasonic_mean",
        required_floats=("distance_m",),
    ),
}


def schema_for_file(filename: str) -> SensorSchema | None:
    """Return the SensorSchema for a filename, or None if prefix is unknown."""
    for prefix in sorted(SCHEMAS, key=len, reverse=True):
        if filename.startswith(prefix + "_"):
            return SCHEMAS[prefix]
    return None
