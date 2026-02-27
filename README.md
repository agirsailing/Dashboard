# Ägir Dashboard

## Overview

Ägir Dashboard is a lightweight local Grafana-based dashboard for Ägir sailing telemetry.
Its v1 data flow is:

`Raspberry Pi CSV files -> Computer ingester -> InfluxDB -> Grafana dashboards`

The Raspberry Pi records telemetry as CSV, and a computer-side ingester imports that data
into InfluxDB. Grafana reads from InfluxDB for visualization.

## Usage (v1 architecture)

The program runs as multiple components:
1. Grafana (frontend) [Docker]
2. FastAPI server (backend) [Docker]
3. Ingester (data ingestion) [Local]
4. InfluxDB (time-series store) [Docker/Local]

The ingester runs independently as a local Python script/service and is responsible for:
- detecting new/updated CSV files from the Raspberry Pi
- parsing and validating telemetry rows
- batching writes into InfluxDB
- quarantining malformed rows
- resuming from checkpoints after restart

For architecture details, see `docs/architecture-v1.md`.
For operational steps and troubleshooting, see `docs/operations.md`.

## CSV Data Contract (v1)

Required columns:
- `timestamp_utc`
- `device_id`
- `lat`
- `lon`
- `speed_kn`
- `heading_deg`

Optional columns:
- `alt_m`
- `sats_used`
- `hdop`
- `fix_quality`

Validation rules:
- `timestamp_utc` must be ISO-8601 UTC.
- Coordinates (`lat`, `lon`) must be decimal degrees.
- Numeric parse errors are quarantined per row and do not fail the whole file.

## Ingestion Guarantees (v1)

- At-least-once ingestion with checkpoint resume (`filename + byte_offset + last_timestamp`).
- Batch writes to InfluxDB with retries and exponential backoff.
- Quarantine of malformed rows so healthy rows continue processing.
- Ingest logging includes processed rows, failed rows, and last ingested timestamp.

## InfluxDB Schema (v1)

- Measurement: `telemetry_gps`
- Tags (low cardinality): `vessel`, `device_id`, `source`
- Fields: `lat`, `lon`, `speed_kn`, `heading_deg`, `alt_m`, `sats_used`
- Timestamp: source record time in UTC

## Displayed data

- GPS coordinates
- GPS speed
- GPS heading
- GPS diagnostics (satellites used, altitude, HDOP/fix quality)
- Wind speed and direction [TODO]
- Magnetic heading
- Yaw, pitch, roll
- Div. magnetic sensors
  - Ride height
  - Flap angle
  - Rudder inclination

## Interfaces

- Map
- Head-up display
- Text output

