"""Tests for pipewatch.schedule_config."""

import textwrap
import os

import pytest

from pipewatch.schedule_config import (
    PipelineScheduleEntry,
    load_schedule,
    load_schedule_from_yaml,
)


@pytest.fixture
def _base_config():
    return {
        "schedules": [
            {"pipeline": "orders", "interval_seconds": 60},
        ]
    }


def test_load_single_entry(_base_config):
    entries = load_schedule(_base_config)
    assert len(entries) == 1
    assert entries[0].pipeline_name == "orders"
    assert entries[0].interval_seconds == 60


def test_load_multiple_entries():
    data = {
        "schedules": [
            {"pipeline": "orders", "interval_seconds": 30},
            {"pipeline": "invoices", "interval_seconds": 120},
        ]
    }
    entries = load_schedule(data)
    names = [e.pipeline_name for e in entries]
    assert "orders" in names
    assert "invoices" in names


def test_empty_schedules_returns_empty_list():
    entries = load_schedule({"schedules": []})
    assert entries == []


def test_missing_schedules_key_returns_empty_list():
    entries = load_schedule({})
    assert entries == []


def test_missing_pipeline_field_raises():
    with pytest.raises(ValueError, match="missing required 'pipeline'"):
        load_schedule({"schedules": [{"interval_seconds": 60}]})


def test_missing_interval_raises():
    with pytest.raises(ValueError, match="missing 'interval_seconds'"):
        load_schedule({"schedules": [{"pipeline": "orders"}]})


def test_non_positive_interval_raises():
    with pytest.raises(ValueError, match="must be positive"):
        load_schedule({"schedules": [{"pipeline": "orders", "interval_seconds": 0}]})


def test_load_from_yaml(tmp_path):
    yaml_content = textwrap.dedent("""\
        schedules:
          - pipeline: payments
            interval_seconds: 45
          - pipeline: refunds
            interval_seconds: 90
    """)
    config_file = tmp_path / "schedule.yaml"
    config_file.write_text(yaml_content)
    entries = load_schedule_from_yaml(str(config_file))
    assert len(entries) == 2
    assert entries[0].pipeline_name == "payments"
    assert entries[1].interval_seconds == 90
