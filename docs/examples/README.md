# CSV Examples Guide

These files are draft examples of the expected ingestion format for v1.

## Files

- `gps_v1_minimal.csv`
  - Smallest valid format (required columns only).
- `gps_v1_extended.csv`
  - Required columns plus optional GPS quality fields.
- `gps_v1_with_extra_columns.csv`
  - Includes extra columns to demonstrate forward compatibility.

## Column Rules

### Required (must exist)

- `timestamp_utc`
- `device_id`
- `lat`
- `lon`
- `speed_kn`
- `heading_deg`

### Optional (recognized if present)

- `alt_m`
- `sats_used`
- `hdop`
- `fix_quality`

### Extra / unknown (allowed)

Any additional columns may be included (for example `roll_deg`, `pitch_deg`,
`source`, `schema_version`). The ingester should ignore unknown fields unless
explicitly mapped in a later version.

## Value Format

- `timestamp_utc`: ISO-8601 UTC (example: `2026-02-27T10:25:00Z`)
- `lat` / `lon`: decimal degrees
- numeric fields: plain decimal values

## Error Handling Expectation

- Missing required columns: row is invalid.
- Invalid numeric values in required/recognized numeric columns: row is quarantined.
- Invalid rows should not stop ingestion of healthy rows.
