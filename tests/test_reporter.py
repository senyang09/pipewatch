"""Tests for pipewatch.reporter module."""

from datetime import datetime

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.metrics import PipelineMetric
from pipewatch.reporter import PipelineReport, format_report, report_to_dict


@pytest.fixture
def healthy_metric():
    return PipelineMetric(
        total_records=100,
        failed_records=2,
        duration_seconds=30.0,
    )


@pytest.fixture
def failing_metric():
    return PipelineMetric(
        total_records=100,
        failed_records=60,
        duration_seconds=30.0,
    )


@pytest.fixture
def critical_alert():
    return Alert(
        rule_name="low_success_rate",
        severity=AlertSeverity.CRITICAL,
        message="Success rate 40.0% is below threshold 50.0%",
    )


@pytest.fixture
def warning_alert():
    return Alert(
        rule_name="slow_pipeline",
        severity=AlertSeverity.WARNING,
        message="Duration 120s exceeds threshold 60s",
    )


def test_report_healthy_no_alerts(healthy_metric):
    report = PipelineReport("my_pipeline", healthy_metric, [])
    assert report.healthy is True
    assert report.has_warnings is False


def test_report_unhealthy_on_critical_alert(healthy_metric, critical_alert):
    report = PipelineReport("my_pipeline", healthy_metric, [critical_alert])
    assert report.healthy is False


def test_report_has_warnings(healthy_metric, warning_alert):
    report = PipelineReport("my_pipeline", healthy_metric, [warning_alert])
    assert report.has_warnings is True
    assert report.healthy is True


def test_report_timestamp_set_automatically(healthy_metric):
    report = PipelineReport("my_pipeline", healthy_metric, [])
    assert isinstance(report.timestamp, datetime)


def test_format_report_contains_pipeline_name(healthy_metric):
    report = PipelineReport("etl_job", healthy_metric, [])
    output = format_report(report)
    assert "etl_job" in output


def test_format_report_shows_alerts(healthy_metric, critical_alert):
    report = PipelineReport("etl_job", healthy_metric, [critical_alert])
    output = format_report(report)
    assert "low_success_rate" in output
    assert "CRITICAL" in output


def test_format_report_no_alerts_message(healthy_metric):
    report = PipelineReport("etl_job", healthy_metric, [])
    output = format_report(report)
    assert "none" in output


def test_report_to_dict_structure(healthy_metric, warning_alert):
    report = PipelineReport("etl_job", healthy_metric, [warning_alert])
    d = report_to_dict(report)
    assert d["pipeline_name"] == "etl_job"
    assert "timestamp" in d
    assert "healthy" in d
    assert "metrics" in d
    assert len(d["alerts"]) == 1
