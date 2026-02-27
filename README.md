# Ägir Dashboard

## Overview

Ägir Dashboard is a lightweight local Grafana-based dashboard for Ägir sailing telemetry.
Its v1 data flow is:

`Raspberry Pi CSV files -> Computer ingester -> InfluxDB -> Grafana dashboards`

The Raspberry Pi records telemetry as CSV, and a computer-side ingester imports that data
into InfluxDB. Grafana reads from InfluxDB for visualization.

## Usage (v1 architecture)

The program runs as three components:
1. InfluxDB (time-series store) [Docker]
2. Grafana (frontend) [Docker]
3. Ingester (data ingestion) [Local Python]

The ingester runs independently as a local Python script/service and is responsible for:
- detecting new/updated CSV files from the Raspberry Pi
- parsing and validating telemetry rows
- batching writes into InfluxDB
- quarantining malformed rows
- resuming from checkpoints after restart

For architecture details, see `docs/architecture-v1.md`.
For operational steps and troubleshooting, see `docs/operations.md`.
For sample CSV formats, see `docs/examples/README.md`.

## Supported Sensors (v1)

The ingester identifies sensor type from the CSV filename prefix:

| Prefix  | Measurement      | Required fields                          | Optional fields              |
|---------|------------------|------------------------------------------|------------------------------|
| `gps_`  | `telemetry_gps`  | lat, lon, speed_kn, heading_deg          | alt_m, hdop, sats_used, fix_quality |
| `imu_`  | `telemetry_imu`  | yaw_deg, pitch_deg, roll_deg             | heading_mag_deg              |
| `wind_` | `telemetry_wind` | wind_speed_kn, wind_dir_deg              | —                            |
| `ctrl_` | `telemetry_ctrl` | ride_height_m, flap_angle_deg, rudder_deg | —                           |

All sensors share `timestamp_utc` (ISO-8601 UTC) and `device_id` as required base columns.

Files with an unrecognised prefix are logged as a warning and skipped.

## InfluxDB Schema (v1)

Tags (low cardinality): `vessel`, `device_id`, `source`

| Measurement      | Fields                                                          |
|------------------|-----------------------------------------------------------------|
| `telemetry_gps`  | lat, lon, speed_kn, heading_deg, alt_m, hdop, sats_used, fix_quality |
| `telemetry_imu`  | yaw_deg, pitch_deg, roll_deg, heading_mag_deg                   |
| `telemetry_wind` | wind_speed_kn, wind_dir_deg                                     |
| `telemetry_ctrl` | ride_height_m, flap_angle_deg, rudder_deg                       |

## Ingestion Guarantees (v1)

- At-least-once ingestion with checkpoint resume (`filename + byte_offset + last_timestamp`).
- Batch writes to InfluxDB with retries and exponential backoff.
- Quarantine of malformed rows so healthy rows continue processing.
- Ingest logging includes processed rows, failed rows, and last ingested timestamp.

## Grafana Panels (v1)

1. **Sailing Track** — GPS track map
2. **Speed (kn)** — GPS speed over ground
3. **Heading (°)** — GPS true heading
4. **GPS Diagnostics** — satellites used, altitude, HDOP
5. **Wind** — wind speed (kn) and direction (°)
6. **IMU** — yaw, pitch, roll, magnetic heading (°)
7. **Control Surfaces** — ride height (m), flap angle (°), rudder (°)

## Validation Rules

- `timestamp_utc` must be ISO-8601 UTC.
- Coordinates (`lat`, `lon`) must be signed decimal degrees.
  - `lat > 0` North, `lat < 0` South
  - `lon > 0` East, `lon < 0` West
- Numeric parse errors are quarantined per row and do not fail the whole file.
- Optional source direction columns such as `lat_dir`/`lon_dir` (`N/S`, `E/W`) are allowed.
