"""Unit tests for pipewatch.metrics module."""

from datetime import datetime

import pytest

from pipewatch.metrics import PipelineMetric, collect_metric


@pytest.fixture
def healthy_metric():
    return PipelineMetric(
        pipeline_name="orders_etl",
        records_processed=980,
        records_failed=20,
        duration_seconds=10.0,
        stage="transform",
    )


@pytest.fixture
def failing_metric():
    return PipelineMetric(
        pipeline_name="inventory_etl",
        records_processed=500,
        records_failed=600,
        duration_seconds=5.0,
    )


def test_success_rate_healthy(healthy_metric):
    assert healthy_metric.success_rate == 98.0


def test_success_rate_failing(failing_metric):
    assert failing_metric.success_rate == pytest.approx(45.45, rel=1e-2)


def test_success_rate_no_records():
    metric = PipelineMetric("empty_pipeline", 0, 0, 1.0)
    assert metric.success_rate == 100.0


def test_throughput(healthy_metric):
    assert healthy_metric.throughput == 98.0


def test_throughput_zero_duration():
    metric = PipelineMetric("pipeline", 100, 0, 0.0)
    assert metric.throughput == 0.0


def test_is_healthy_default_threshold(healthy_metric):
    assert healthy_metric.is_healthy() is True


def test_is_healthy_failing(failing_metric):
    assert failing_metric.is_healthy() is False


def test_is_healthy_custom_threshold(healthy_metric):
    assert healthy_metric.is_healthy(min_success_rate=99.0) is False


def test_to_dict_keys(healthy_metric):
    result = healthy_metric.to_dict()
    expected_keys = {
        "pipeline_name", "stage", "records_processed", "records_failed",
        "duration_seconds", "success_rate", "throughput", "timestamp",
    }
    assert set(result.keys()) == expected_keys


def test_to_dict_values(healthy_metric):
    result = healthy_metric.to_dict()
    assert result["pipeline_name"] == "orders_etl"
    assert result["stage"] == "transform"
    assert result["success_rate"] == 98.0


def test_collect_metric_factory():
    metric = collect_metric("test_pipeline", 100, 5, 2.5, stage="load")
    assert isinstance(metric, PipelineMetric)
    assert metric.pipeline_name == "test_pipeline"
    assert metric.stage == "load"
    assert isinstance(metric.timestamp, datetime)
