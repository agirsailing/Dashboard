"""Tests for ingester.checkpoint."""

import json
from pathlib import Path

from ingester.checkpoint import load, quarantine_row, save

# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------


def test_load_missing_file_returns_empty(tmp_path):
    assert load(tmp_path / "nonexistent.json") == {}


def test_load_round_trip(tmp_path):
    state_file = tmp_path / "state.json"
    data = {"file.csv": {"byte_offset": 100, "last_timestamp": "2024-01-01T00:00:00Z"}}
    state_file.write_text(json.dumps(data))
    assert load(state_file) == data


def test_load_valid_json(tmp_path):
    state_file = tmp_path / "state.json"
    state_file.write_text('{"key": "value"}')
    assert load(state_file) == {"key": "value"}


def test_load_empty_object(tmp_path):
    state_file = tmp_path / "state.json"
    state_file.write_text("{}")
    assert load(state_file) == {}


# ---------------------------------------------------------------------------
# save
# ---------------------------------------------------------------------------


def test_save_creates_file(tmp_path):
    state_file = tmp_path / "state.json"
    save(state_file, {})
    assert state_file.exists()


def test_save_round_trip(tmp_path):
    state_file = tmp_path / "state.json"
    data = {"foo.csv": {"byte_offset": 42}}
    save(state_file, data)
    assert json.loads(state_file.read_text()) == data


def test_save_no_tmp_left_behind(tmp_path):
    state_file = tmp_path / "state.json"
    save(state_file, {})
    assert not (tmp_path / "state.tmp").exists()


def test_save_overwrites(tmp_path):
    state_file = tmp_path / "state.json"
    save(state_file, {"a": 1})
    save(state_file, {"b": 2})
    assert load(state_file) == {"b": 2}


def test_save_valid_json_output(tmp_path):
    state_file = tmp_path / "state.json"
    save(state_file, {"list": [1, 2, 3], "nested": {"x": True}})
    assert json.loads(state_file.read_text()) == {"list": [1, 2, 3], "nested": {"x": True}}


# ---------------------------------------------------------------------------
# quarantine_row  (JSONL format — one JSON object per line)
# ---------------------------------------------------------------------------


def _read_quarantine(q_dir: Path) -> list[dict]:
    return [json.loads(line) for line in (q_dir / "quarantine.jsonl").read_text().splitlines()]


def test_quarantine_row_creates_dir(tmp_path):
    q_dir = tmp_path / "quarantine"
    quarantine_row(q_dir, "test.csv", {"ts": "bad"}, "invalid ts")
    assert q_dir.is_dir()


def test_quarantine_row_creates_jsonl(tmp_path):
    q_dir = tmp_path / "quarantine"
    quarantine_row(q_dir, "test.csv", {"ts": "bad"}, "invalid ts")
    assert (q_dir / "quarantine.jsonl").exists()


def test_quarantine_row_metadata_source_file(tmp_path):
    q_dir = tmp_path / "quarantine"
    quarantine_row(q_dir, "myfile.csv", {"col": "val"}, "reason text")
    assert _read_quarantine(q_dir)[0]["_source_file"] == "myfile.csv"


def test_quarantine_row_metadata_reason(tmp_path):
    q_dir = tmp_path / "quarantine"
    quarantine_row(q_dir, "myfile.csv", {"col": "val"}, "reason text")
    assert _read_quarantine(q_dir)[0]["_reason"] == "reason text"


def test_quarantine_row_appends_multiple(tmp_path):
    q_dir = tmp_path / "quarantine"
    quarantine_row(q_dir, "a.csv", {"x": "1"}, "r1")
    quarantine_row(q_dir, "b.csv", {"x": "2"}, "r2")
    assert len(_read_quarantine(q_dir)) == 2


def test_quarantine_row_each_line_valid_json(tmp_path):
    """JSONL: every line must be independently parseable."""
    q_dir = tmp_path / "quarantine"
    quarantine_row(q_dir, "a.csv", {"x": "1"}, "r")
    quarantine_row(q_dir, "b.csv", {"y": "2"}, "r")  # different schema — no mismatch issue
    lines = (q_dir / "quarantine.jsonl").read_text().splitlines()
    assert len(lines) == 2
    for line in lines:
        json.loads(line)  # must not raise


def test_quarantine_row_mixed_schemas_no_data_loss(tmp_path):
    """Rows with different columns are all preserved (JSONL has no fixed schema)."""
    q_dir = tmp_path / "quarantine"
    quarantine_row(q_dir, "gps.csv", {"lat": "bad"}, "non-numeric")
    quarantine_row(q_dir, "imu.csv", {"yaw_deg": "bad"}, "non-numeric")
    rows = _read_quarantine(q_dir)
    assert "lat" in rows[0]
    assert "yaw_deg" in rows[1]


def test_quarantine_row_does_not_mutate_original(tmp_path):
    q_dir = tmp_path / "quarantine"
    original = {"col": "val"}
    snapshot = dict(original)
    quarantine_row(q_dir, "test.csv", original, "reason")
    assert original == snapshot


def test_quarantine_row_data_preserved(tmp_path):
    q_dir = tmp_path / "quarantine"
    quarantine_row(q_dir, "test.csv", {"myfield": "myvalue"}, "bad row")
    assert _read_quarantine(q_dir)[0]["myfield"] == "myvalue"
