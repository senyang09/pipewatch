"""Tests for pipewatch.trend module."""

import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.history import MetricSnapshot
from pipewatch.trend import analyse_trend, TrendSummary


def _snap(total: int, failed: int, time: float = 10.0) -> MetricSnapshot:
    metric = PipelineMetric(
        total_records=total,
        failed_records=failed,
        processing_time_seconds=time,
        pipeline_name="orders",
    )
    return MetricSnapshot(timestamp="2024-01-01T00:00:00", pipeline="orders", metric=metric)


def test_analyse_trend_single_snapshot():
    snaps = [_snap(100, 5)]
    summary = analyse_trend(snaps)
    assert summary.sample_count == 1
    assert summary.avg_success_rate == pytest.approx(0.95)
    assert summary.degrading is False


def test_analyse_trend_stable():
    snaps = [_snap(100, 5), _snap(100, 5)]
    summary = analyse_trend(snaps)
    assert summary.degrading is False


def test_analyse_trend_degrading():
    # newest first: latest has more failures
    snaps = [_snap(100, 20), _snap(100, 5)]
    summary = analyse_trend(snaps)
    assert summary.degrading is True


def test_analyse_trend_improving():
    # newest first: latest has fewer failures
    snaps = [_snap(100, 2), _snap(100, 20)]
    summary = analyse_trend(snaps)
    assert summary.degrading is False


def test_analyse_trend_avg_success_rate():
    snaps = [_snap(100, 0), _snap(100, 100)]
    summary = analyse_trend(snaps)
    assert summary.avg_success_rate == pytest.approx(0.5)


def test_analyse_trend_min_max():
    snaps = [_snap(100, 10), _snap(100, 50), _snap(100, 0)]
    summary = analyse_trend(snaps)
    assert summary.min_success_rate == pytest.approx(0.5)
    assert summary.max_success_rate == pytest.approx(1.0)


def test_analyse_trend_avg_throughput():
    snaps = [_snap(100, 0, time=10.0), _snap(200, 0, time=10.0)]
    summary = analyse_trend(snaps)
    # throughputs: 10.0 and 20.0 -> avg 15.0
    assert summary.avg_throughput == pytest.approx(15.0)


def test_analyse_trend_empty_raises():
    with pytest.raises(ValueError, match="no snapshots"):
        analyse_trend([])


def test_trend_summary_format_contains_pipeline():
    snaps = [_snap(100, 5)]
    summary = analyse_trend(snaps)
    output = summary.format()
    assert "orders" in output
    assert "stable/improving" in output


def test_trend_summary_format_degrading_label():
    snaps = [_snap(100, 50), _snap(100, 5)]
    summary = analyse_trend(snaps)
    assert "degrading" in summary.format()
