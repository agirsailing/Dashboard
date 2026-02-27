"""CSV parsing and v1 contract validation."""

import csv
from datetime import datetime, timezone
from pathlib import Path

from .schema import SensorSchema


def _parse_timestamp(value: str) -> datetime:
    """Parse ISO-8601 UTC timestamp; raise ValueError on failure."""
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_file(
    path: str | Path, byte_offset: int, schema: SensorSchema
) -> tuple[list[dict], list[tuple[dict, str]], int]:
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

    text = remaining.decode("utf-8", errors="replace")

    if byte_offset == 0:
        lines = text.splitlines()
        if not lines:
            return valid_rows, bad_rows, new_offset
        reader = csv.DictReader(lines)
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    else:
        with path.open("r", newline="", encoding="utf-8", errors="replace") as f:
            header_reader = csv.reader(f)
            try:
                fieldnames = next(header_reader)
            except StopIteration:
                return valid_rows, bad_rows, new_offset
        lines = text.splitlines()
        rows = [dict(zip(fieldnames, row)) for row in csv.reader(lines)]

    missing_required = schema.required_columns - set(fieldnames)
    if missing_required:
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
        for col in schema.required_floats:
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

        # --- required int fields ---
        required_ints = {}
        for col in schema.required_ints:
            raw_val = row.get(col, "").strip()
            if raw_val == "":
                bad_rows.append((row, f"missing required field: {col}"))
                ok = False
                break
            try:
                required_ints[col] = int(raw_val)
            except ValueError:
                bad_rows.append((row, f"non-integer value for {col}: {raw_val!r}"))
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
        for col in schema.optional_floats:
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

        for col in schema.optional_ints:
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
                **required_ints,
                **optional,
            }
        )

    return valid_rows, bad_rows, new_offset
