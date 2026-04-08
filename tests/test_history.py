"""Tests for pipewatch.history module."""

import json
import pytest
from pathlib import Path

from pipewatch.metrics import PipelineMetric
from pipewatch.history import MetricHistory, MetricSnapshot


@pytest.fixture
def tmp_store(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


@pytest.fixture
def sample_metric() -> PipelineMetric:
    return PipelineMetric(
        total_records=100,
        failed_records=2,
        processing_time_seconds=5.0,
        pipeline_name="orders",
    )


def test_record_creates_snapshot(tmp_store, sample_metric):
    history = MetricHistory(store_path=tmp_store)
    snapshot = history.record("orders", sample_metric)
    assert isinstance(snapshot, MetricSnapshot)
    assert snapshot.pipeline == "orders"
    assert snapshot.metric is sample_metric


def test_record_persists_to_disk(tmp_store, sample_metric):
    history = MetricHistory(store_path=tmp_store)
    history.record("orders", sample_metric)
    assert tmp_store.exists()
    data = json.loads(tmp_store.read_text())
    assert len(data) == 1
    assert data[0]["pipeline"] == "orders"
    assert data[0]["metric"]["total_records"] == 100


def test_get_pipeline_history_returns_newest_first(tmp_store, sample_metric):
    history = MetricHistory(store_path=tmp_store)
    history.record("orders", sample_metric)
    second = PipelineMetric(
        total_records=200, failed_records=1, processing_time_seconds=3.0,
        pipeline_name="orders",
    )
    history.record("orders", second)
    results = history.get_pipeline_history("orders")
    assert len(results) == 2
    assert results[0].metric.total_records == 200


def test_get_pipeline_history_limit(tmp_store, sample_metric):
    history = MetricHistory(store_path=tmp_store)
    for _ in range(5):
        history.record("orders", sample_metric)
    results = history.get_pipeline_history("orders", limit=3)
    assert len(results) == 3


def test_get_pipeline_history_filters_by_pipeline(tmp_store, sample_metric):
    history = MetricHistory(store_path=tmp_store)
    history.record("orders", sample_metric)
    other = PipelineMetric(
        total_records=50, failed_records=0, processing_time_seconds=1.0,
        pipeline_name="users",
    )
    history.record("users", other)
    results = history.get_pipeline_history("orders")
    assert len(results) == 1
    assert results[0].pipeline == "orders"


def test_history_reloads_from_disk(tmp_store, sample_metric):
    history = MetricHistory(store_path=tmp_store)
    history.record("orders", sample_metric)
    reloaded = MetricHistory(store_path=tmp_store)
    results = reloaded.get_pipeline_history("orders")
    assert len(results) == 1
    assert results[0].metric.total_records == 100


def test_clear_removes_records_and_file(tmp_store, sample_metric):
    history = MetricHistory(store_path=tmp_store)
    history.record("orders", sample_metric)
    history.clear()
    assert not tmp_store.exists()
    assert history.get_pipeline_history("orders") == []


def test_snapshot_to_dict(sample_metric):
    snapshot = MetricSnapshot(
        timestamp="2024-01-01T00:00:00",
        pipeline="orders",
        metric=sample_metric,
    )
    d = snapshot.to_dict()
    assert d["pipeline"] == "orders"
    assert d["timestamp"] == "2024-01-01T00:00:00"
    assert d["metric"]["pipeline_name"] == "orders"
