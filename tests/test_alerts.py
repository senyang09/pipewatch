"""Tests for pipewatch.alerts — alert rule evaluation."""

import pytest

from pipewatch.alerts import Alert, AlertRule, AlertSeverity, evaluate_rules
from pipewatch.metrics import PipelineMetric


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def healthy_metric():
    return PipelineMetric(
        pipeline_name="orders",
        total_records=1000,
        failed_records=10,
        duration_seconds=100.0,
    )


@pytest.fixture
def failing_metric():
    return PipelineMetric(
        pipeline_name="orders",
        total_records=1000,
        failed_records=400,
        duration_seconds=200.0,
    )


@pytest.fixture
def slow_metric():
    return PipelineMetric(
        pipeline_name="orders",
        total_records=100,
        failed_records=0,
        duration_seconds=500.0,
    )


# ---------------------------------------------------------------------------
# AlertRule.evaluate
# ---------------------------------------------------------------------------

def test_no_alert_when_healthy(healthy_metric):
    rule = AlertRule(name="low-success", pipeline="orders", min_success_rate=0.95)
    assert rule.evaluate(healthy_metric) is None


def test_alert_fires_on_low_success_rate(failing_metric):
    rule = AlertRule(name="low-success", pipeline="orders", min_success_rate=0.95)
    alert = rule.evaluate(failing_metric)
    assert alert is not None
    assert alert.severity == AlertSeverity.WARNING
    assert "success_rate" in alert.detail


def test_alert_fires_on_low_throughput(slow_metric):
    rule = AlertRule(name="slow-pipe", pipeline="orders", min_throughput=5.0,
                     severity=AlertSeverity.CRITICAL)
    alert = rule.evaluate(slow_metric)
    assert alert is not None
    assert alert.severity == AlertSeverity.CRITICAL
    assert "throughput" in alert.detail


def test_alert_wrong_pipeline_ignored(failing_metric):
    rule = AlertRule(name="low-success", pipeline="inventory", min_success_rate=0.95)
    assert rule.evaluate(failing_metric) is None


def test_alert_custom_message(failing_metric):
    rule = AlertRule(
        name="low-success", pipeline="orders",
        min_success_rate=0.99, message="Pipeline degraded!"
    )
    alert = rule.evaluate(failing_metric)
    assert alert.detail == "Pipeline degraded!"


def test_alert_to_dict(failing_metric):
    rule = AlertRule(name="low-success", pipeline="orders", min_success_rate=0.99)
    alert = rule.evaluate(failing_metric)
    d = alert.to_dict()
    assert d["pipeline"] == "orders"
    assert d["severity"] == "warning"
    assert "rule" in d


def test_alert_str(failing_metric):
    rule = AlertRule(name="low-success", pipeline="orders", min_success_rate=0.99)
    alert = rule.evaluate(failing_metric)
    text = str(alert)
    assert "WARNING" in text
    assert "orders" in text


# ---------------------------------------------------------------------------
# evaluate_rules
# ---------------------------------------------------------------------------

def test_evaluate_rules_multiple(failing_metric):
    rules = [
        AlertRule(name="low-success", pipeline="orders", min_success_rate=0.99),
        AlertRule(name="slow-pipe", pipeline="orders", min_throughput=50.0),
    ]
    alerts = evaluate_rules(failing_metric, rules)
    assert len(alerts) == 2


def test_evaluate_rules_empty(healthy_metric):
    alerts = evaluate_rules(healthy_metric, [])
    assert alerts == []
