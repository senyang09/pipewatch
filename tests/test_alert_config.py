"""Tests for pipewatch.alert_config — loading rules from config dicts."""

import pytest

from pipewatch.alert_config import load_rules
from pipewatch.alerts import AlertSeverity


def _base_rule(**overrides):
    rule = {"name": "test-rule", "pipeline": "orders", "min_success_rate": 0.95}
    rule.update(overrides)
    return rule


def test_load_basic_rule():
    config = {"rules": [_base_rule()]}
    rules = load_rules(config)
    assert len(rules) == 1
    assert rules[0].name == "test-rule"
    assert rules[0].pipeline == "orders"
    assert rules[0].min_success_rate == pytest.approx(0.95)


def test_load_multiple_rules():
    config = {
        "rules": [
            _base_rule(name="rule-a"),
            _base_rule(name="rule-b", pipeline="inventory", min_throughput=10.0),
        ]
    }
    rules = load_rules(config)
    assert len(rules) == 2
    assert rules[1].min_throughput == pytest.approx(10.0)


def test_load_severity_critical():
    config = {"rules": [_base_rule(severity="critical")]}
    rules = load_rules(config)
    assert rules[0].severity == AlertSeverity.CRITICAL


def test_load_default_severity():
    config = {"rules": [_base_rule()]}
    rules = load_rules(config)
    assert rules[0].severity == AlertSeverity.WARNING


def test_load_empty_rules():
    rules = load_rules({})
    assert rules == []


def test_missing_required_key_raises():
    with pytest.raises(ValueError, match="missing required keys"):
        load_rules({"rules": [{"pipeline": "orders"}]})


def test_unknown_key_raises():
    with pytest.raises(ValueError, match="unknown keys"):
        load_rules({"rules": [_base_rule(bogus_key="oops")]})


def test_invalid_severity_raises():
    with pytest.raises(ValueError, match="Invalid severity"):
        load_rules({"rules": [_base_rule(severity="extreme")]})


def test_success_rate_out_of_range_raises():
    with pytest.raises(ValueError, match="min_success_rate"):
        load_rules({"rules": [_base_rule(min_success_rate=1.5)]})


def test_rules_key_not_list_raises():
    with pytest.raises(ValueError, match="'rules' must be a list"):
        load_rules({"rules": "not-a-list"})
