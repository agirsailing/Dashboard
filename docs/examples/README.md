# CSV Examples Guide

These files are draft examples of the expected ingestion format for v1.
The ingester identifies sensor type from the filename prefix.

## GPS (`gps_`)

- `gps_v1_minimal.csv` — Required columns only.
- `gps_v1_extended.csv` — Required + optional GPS quality fields.
- `gps_v1_with_extra_columns.csv` — Extra columns to demonstrate forward compatibility.
- `gps_v1_with_nmea_direction_columns.csv` — Optional NMEA-style direction fields.

### Required columns
`timestamp_utc`, `device_id`, `lat`, `lon`, `speed_kn`, `heading_deg`

### Optional columns
`alt_m`, `hdop`, `sats_used`, `fix_quality`

---

## IMU (`imu_`)

- `imu_v1_example.csv`

### Required columns
`timestamp_utc`, `device_id`, `yaw_deg`, `pitch_deg`, `roll_deg`

### Optional columns
`heading_mag_deg`

---

## Wind (`wind_`)

- `wind_v1_example.csv`

### Required columns
`timestamp_utc`, `device_id`, `wind_speed_kn`, `wind_dir_deg`

---

## Control Surfaces (`ctrl_`)

- `ctrl_v1_example.csv`

### Required columns
`timestamp_utc`, `device_id`, `ride_height_m`, `flap_angle_deg`, `rudder_deg`

---

## Shared Rules

### Value Format

- `timestamp_utc`: ISO-8601 UTC (example: `2026-02-27T10:25:00Z`)
- `lat` / `lon`: signed decimal degrees
  - `lat > 0` = North, `lat < 0` = South
  - `lon > 0` = East, `lon < 0` = West
- All numeric fields: plain decimal values

### Unknown Prefix

Files whose filename does not match a known prefix (`gps_`, `imu_`, `wind_`, `ctrl_`) are
skipped with a warning — no rows are quarantined.

### Error Handling

- Missing required columns: all rows quarantined.
- Invalid numeric values in required/optional columns: row is quarantined.
- Invalid rows do not stop ingestion of healthy rows.
