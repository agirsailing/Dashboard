# CSV Examples Guide

These files are draft examples of the expected ingestion format for v1.

## Files

- `gps_v1_minimal.csv`
  - Smallest valid format (required columns only).
- `gps_v1_extended.csv`
  - Required columns plus optional GPS quality fields.
- `gps_v1_with_extra_columns.csv`
  - Includes extra columns to demonstrate forward compatibility.
- `gps_v1_with_nmea_direction_columns.csv`
  - Shows optional NMEA-style direction fields (`N/S`, `E/W`) plus normalized coordinates.

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
- `lat` / `lon`: signed decimal degrees (recommended canonical format)
  - `lat > 0` = North, `lat < 0` = South
  - `lon > 0` = East, `lon < 0` = West
- numeric fields: plain decimal values

Optional direction-style columns may be included when source devices output NMEA-like values:
- `lat_dir`: `N` or `S`
- `lon_dir`: `E` or `W`
- `lat_raw`, `lon_raw`: source raw coordinates (if needed for debugging/audit)

In v1, the ingester should treat signed `lat`/`lon` as authoritative for storage.

## Error Handling Expectation

- Missing required columns: row is invalid.
- Invalid numeric values in required/recognized numeric columns: row is quarantined.
- Invalid rows should not stop ingestion of healthy rows.
