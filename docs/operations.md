# Operations Runbook (v1)

## Purpose

This runbook defines how to run and troubleshoot the v1 telemetry pipeline:

`Raspberry Pi CSV files -> Computer ingester -> InfluxDB -> Grafana`

## Expected Runtime Layout

Example local layout on the ingest computer:

- `data/incoming/` - CSV files copied/synced from Raspberry Pi
- `data/quarantine/` - malformed rows and parse errors
- `state/ingest-checkpoints.json` - resume state (`filename + byte_offset + last_timestamp`)
- `logs/ingester.log` - runtime logs and counters

## Startup Checklist

1. Start InfluxDB and ensure bucket exists.
2. Start Grafana and verify datasource points to InfluxDB.
3. Verify CSV input folder is reachable and receiving files from Raspberry Pi.
4. Start ingester service/script on computer.
5. Confirm ingester can read state file and write to logs/quarantine folders.

## Smoke Test

1. Place a small valid CSV file into `data/incoming/`.
2. Check ingester log for:
   - file detected
   - rows parsed
   - batch write success
   - checkpoint updated
3. Confirm data appears in the expected InfluxDB measurement (e.g. `telemetry_gps` for a GPS file).
4. Confirm Grafana panel renders recent points.

## CSV Validation Rules (v1)

All sensors require `timestamp_utc` (ISO-8601 UTC) and `device_id`. Sensor type is determined by filename prefix.

| Prefix   | Required fields                              | Optional fields                       |
|----------|----------------------------------------------|---------------------------------------|
| `gps_`   | lat, lon, speed_kn, heading_deg              | alt_m, hdop, sats_used, fix_quality   |
| `imu_`   | yaw_deg, pitch_deg, roll_deg                 | heading_mag_deg                       |
| `wind_`  | wind_speed_kn, wind_dir_deg                  | —                                     |
| `ctrl_`  | ride_height_m, flap_angle_deg, rudder_deg    | —                                     |

Row-level rules:

- `timestamp_utc` is ISO-8601 UTC.
- `lat`/`lon` are signed decimal degrees.
  - `lat > 0` North, `lat < 0` South
  - `lon > 0` East, `lon < 0` West
- Invalid numeric values quarantine the row instead of stopping full-file ingest.
- Files with an unrecognised prefix are skipped with a warning; no rows are quarantined.

## Ingest Behavior and Guarantees

- At-least-once ingestion.
- Checkpoint resume after restart.
- Batch writes to InfluxDB.
- Retry with exponential backoff on write failures.
- Quarantine malformed rows while continuing healthy rows.

## Common Failure Scenarios

### 1) InfluxDB Unavailable

Symptoms:

- write failures in ingester log
- growing backlog of unprocessed rows/files

Actions:

1. Verify InfluxDB process/container is running.
2. Verify URL/token/bucket/org configuration.
3. Restart ingester only after InfluxDB is healthy.
4. Confirm checkpoint resumes where expected.

### 2) CSV Format Drift

Symptoms:

- sudden increase in quarantine entries
- parse errors on required columns

Actions:

1. Compare incoming CSV headers to v1 contract.
2. Fix upstream producer format on Raspberry Pi.
3. Reprocess affected files if needed after format correction.

### 3) Duplicate-looking Data

Symptoms:

- repeated points around service restarts

Actions:

1. Inspect checkpoint state and recent restart timeline.
2. Confirm ingester uses `filename + byte_offset + last_timestamp`.
3. Accept possible replay window under at-least-once semantics.

### 4) No Data in Grafana

Symptoms:

- Influx writes succeed, but dashboards are empty

Actions:

1. Verify Grafana datasource points to correct Influx instance/bucket.
2. Verify dashboard query time range and measurement (`telemetry_gps`, `telemetry_imu`, `telemetry_wind`, `telemetry_ctrl`).
3. Query Influx directly to confirm recent points exist.

## Maintenance

- Rotate/archive old CSV files after successful ingest confirmation.
- Back up checkpoint and logs periodically.
- Review quarantine files after each sailing session.
- Tune batch size and retry/backoff settings based on observed load.
