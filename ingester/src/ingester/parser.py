"""CSV parsing and v1 contract validation."""

import csv
from datetime import datetime, timezone
from pathlib import Path

REQUIRED_COLUMNS = {"timestamp_utc", "device_id", "lat", "lon", "speed_kn", "heading_deg"}

OPTIONAL_FLOAT = {"alt_m", "hdop"}
OPTIONAL_INT = {"sats_used", "fix_quality"}
IGNORED_COLUMNS = {"lat_dir", "lon_dir"}


def _parse_timestamp(value: str) -> datetime:
    """Parse ISO-8601 UTC timestamp; raise ValueError on failure."""
    # Accept trailing Z or +00:00
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_file(path: str | Path, byte_offset: int) -> tuple[list[dict], list[tuple[dict, str]], int]:
    """Parse a CSV file starting from byte_offset.

    Returns:
        valid_rows  – list of validated row dicts ready for writer
        bad_rows    – list of (raw_row_dict, reason) tuples
        new_offset  – file position after last byte read
    """
    valid_rows: list[dict] = []
    bad_rows: list[tuple[dict, str]] = []

    path = Path(path)
    with path.open("rb") as raw:
        raw.seek(byte_offset)
        remaining = raw.read()
        new_offset = raw.tell()

    if not remaining:
        return valid_rows, bad_rows, new_offset

    # Decode and parse — handle partial last line (no trailing newline)
    text = remaining.decode("utf-8", errors="replace")

    # If we're at the start of the file we include the header row.
    # If we're mid-file (resuming) the first chunk won't have a header,
    # so we need to reconstruct one by reading just the header from position 0.
    if byte_offset == 0:
        lines = text.splitlines()
        if not lines:
            return valid_rows, bad_rows, new_offset
        reader = csv.DictReader(lines)
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    else:
        # Read header from start of file
        with path.open("r", newline="", encoding="utf-8", errors="replace") as f:
            header_reader = csv.reader(f)
            try:
                fieldnames = next(header_reader)
            except StopIteration:
                return valid_rows, bad_rows, new_offset
        lines = text.splitlines()
        rows = [dict(zip(fieldnames, row)) for row in csv.reader(lines)]

    missing_required = REQUIRED_COLUMNS - set(fieldnames)
    if missing_required:
        # Quarantine everything — we can't validate without required columns
        for row in rows:
            bad_rows.append((row, f"missing required columns: {missing_required}"))
        return valid_rows, bad_rows, new_offset

    for row in rows:
        if not row:
            continue

        # --- timestamp ---
        ts_raw = row.get("timestamp_utc", "").strip()
        if not ts_raw:
            bad_rows.append((row, "empty timestamp_utc"))
            continue
        try:
            ts = _parse_timestamp(ts_raw)
        except (ValueError, TypeError) as exc:
            bad_rows.append((row, f"invalid timestamp_utc: {exc}"))
            continue

        # --- required float fields ---
        required_floats = {}
        ok = True
        for col in ("lat", "lon", "speed_kn", "heading_deg"):
            raw_val = row.get(col, "").strip()
            if raw_val == "":
                bad_rows.append((row, f"missing required field: {col}"))
                ok = False
                break
            try:
                required_floats[col] = float(raw_val)
            except ValueError:
                bad_rows.append((row, f"non-numeric value for {col}: {raw_val!r}"))
                ok = False
                break
        if not ok:
            continue

        # --- device_id ---
        device_id = row.get("device_id", "").strip()
        if not device_id:
            bad_rows.append((row, "empty device_id"))
            continue

        # --- optional fields ---
        optional: dict = {}
        for col in OPTIONAL_FLOAT:
            raw_val = row.get(col, "").strip()
            if raw_val:
                try:
                    optional[col] = float(raw_val)
                except ValueError:
                    bad_rows.append((row, f"non-numeric value for optional field {col}: {raw_val!r}"))
                    ok = False
                    break
        if not ok:
            continue

        for col in OPTIONAL_INT:
            raw_val = row.get(col, "").strip()
            if raw_val:
                try:
                    optional[col] = int(raw_val)
                except ValueError:
                    bad_rows.append((row, f"non-integer value for optional field {col}: {raw_val!r}"))
                    ok = False
                    break
        if not ok:
            continue

        # --- source (optional tag) ---
        source = row.get("source", "").strip() or None

        valid_rows.append(
            {
                "timestamp_utc": ts,
                "device_id": device_id,
                "source": source,
                **required_floats,
                **optional,
            }
        )

    return valid_rows, bad_rows, new_offset
