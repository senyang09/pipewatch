"""Tests for pipewatch.exporter."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from pipewatch.exporter import ExportResult, export_csv, export_json, export_snapshots
from pipewatch.history import MetricSnapshot
from pipewatch.metrics import PipelineMetric


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _snap(pipeline_id: str = "pipe-1", success: int = 90, total: int = 100) -> MetricSnapshot:
    metric = PipelineMetric(
        pipeline_id=pipeline_id,
        records_processed=total,
        records_failed=total - success,
        processing_time_seconds=10.0,
    )
    return MetricSnapshot(
        pipeline_id=pipeline_id,
        timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        metric=metric,
    )


# ---------------------------------------------------------------------------
# export_json
# ---------------------------------------------------------------------------

def test_export_json_returns_valid_json():
    result = export_json([_snap()])
    assert result.format == "json"
    data = json.loads(result.content)
    assert isinstance(data, list)
    assert len(data) == 1


def test_export_json_contains_expected_fields():
    result = export_json([_snap("pipe-x")])
    data = json.loads(result.content)
    assert data[0]["pipeline_id"] == "pipe-x"


def test_export_json_multiple_snapshots():
    snaps = [_snap("a"), _snap("b")]
    result = export_json(snaps)
    data = json.loads(result.content)
    assert len(data) == 2


def test_export_json_empty():
    result = export_json([])
    data = json.loads(result.content)
    assert data == []


# ---------------------------------------------------------------------------
# export_csv
# ---------------------------------------------------------------------------

def test_export_csv_returns_csv_format():
    result = export_csv([_snap()])
    assert result.format == "csv"


def test_export_csv_has_header_row():
    result = export_csv([_snap()])
    lines = result.content.strip().splitlines()
    assert "pipeline_id" in lines[0]


def test_export_csv_row_count_matches_snapshots():
    snaps = [_snap("a"), _snap("b"), _snap("c")]
    result = export_csv(snaps)
    lines = result.content.strip().splitlines()
    # header + 3 data rows
    assert len(lines) == 4


def test_export_csv_empty_returns_empty_string():
    result = export_csv([])
    assert result.content == ""


# ---------------------------------------------------------------------------
# export_snapshots dispatcher
# ---------------------------------------------------------------------------

def test_export_snapshots_json():
    result = export_snapshots([_snap()], fmt="json")
    assert result.format == "json"
    json.loads(result.content)  # must be valid JSON


def test_export_snapshots_csv():
    result = export_snapshots([_snap()], fmt="csv")
    assert result.format == "csv"
    assert "pipeline_id" in result.content


def test_export_snapshots_case_insensitive():
    result = export_snapshots([_snap()], fmt="JSON")
    assert result.format == "json"


def test_export_snapshots_unknown_format_raises():
    with pytest.raises(ValueError, match="Unsupported export format"):
        export_snapshots([_snap()], fmt="xml")


# ---------------------------------------------------------------------------
# ExportResult.write_to_file
# ---------------------------------------------------------------------------

def test_write_to_file(tmp_path):
    path = tmp_path / "out.json"
    result = export_json([_snap()])
    result.write_to_file(str(path))
    assert path.exists()
    data = json.loads(path.read_text())
    assert isinstance(data, list)
