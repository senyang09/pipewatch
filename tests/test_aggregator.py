"""Tests for pipewatch.aggregator."""
from datetime import datetime, timezone

import pytest

from pipewatch.aggregator import aggregate, AggregatedStats
from pipewatch.history import MetricSnapshot


def _snap(
    pipeline_id: str = "pipe-a",
    success_rate: float = 1.0,
    throughput: float = 100.0,
    records_processed: int = 200,
    records_failed: int = 0,
) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline_id=pipeline_id,
        timestamp=datetime.now(tz=timezone.utc),
        success_rate=success_rate,
        throughput=throughput,
        records_processed=records_processed,
        records_failed=records_failed,
    )


def test_aggregate_single_snapshot():
    snap = _snap(success_rate=0.95, throughput=50.0, records_processed=100, records_failed=5)
    stats = aggregate([snap])
    assert stats.sample_count == 1
    assert stats.avg_success_rate == pytest.approx(0.95)
    assert stats.min_success_rate == pytest.approx(0.95)
    assert stats.max_success_rate == pytest.approx(0.95)
    assert stats.avg_throughput == pytest.approx(50.0)
    assert stats.total_records_processed == 100
    assert stats.total_errors == 5


def test_aggregate_multiple_snapshots():
    snaps = [
        _snap(success_rate=1.0, throughput=100.0, records_processed=200, records_failed=0),
        _snap(success_rate=0.8, throughput=60.0, records_processed=150, records_failed=30),
        _snap(success_rate=0.9, throughput=80.0, records_processed=180, records_failed=18),
    ]
    stats = aggregate(snaps)
    assert stats.sample_count == 3
    assert stats.avg_success_rate == pytest.approx((1.0 + 0.8 + 0.9) / 3)
    assert stats.min_success_rate == pytest.approx(0.8)
    assert stats.max_success_rate == pytest.approx(1.0)
    assert stats.avg_throughput == pytest.approx((100.0 + 60.0 + 80.0) / 3)
    assert stats.total_records_processed == 530
    assert stats.total_errors == 48


def test_aggregate_empty_raises():
    with pytest.raises(ValueError, match="empty"):
        aggregate([])


def test_pipeline_id_taken_from_first_snapshot():
    snaps = [_snap(pipeline_id="alpha"), _snap(pipeline_id="beta")]
    stats = aggregate(snaps)
    assert stats.pipeline_id == "alpha"


def test_to_dict_keys():
    stats = aggregate([_snap()])
    d = stats.to_dict()
    expected_keys = {
        "pipeline_id", "sample_count", "avg_success_rate",
        "min_success_rate", "max_success_rate", "avg_throughput",
        "total_records_processed", "total_errors",
    }
    assert set(d.keys()) == expected_keys


def test_format_returns_string():
    stats = aggregate([_snap(success_rate=0.99, throughput=120.0)])
    output = stats.format()
    assert "pipe-a" in output
    assert "99.0%" in output
    assert "120.0" in output
