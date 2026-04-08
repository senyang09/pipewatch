"""Alert definitions and evaluation for pipeline health monitoring."""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, is_healthy, success_rate, throughput


class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class AlertRule:
    """Defines a threshold-based alert rule for a pipeline metric."""

    name: str
    pipeline: str
    min_success_rate: Optional[float] = None  # 0.0 - 1.0
    min_throughput: Optional[float] = None    # records per second
    severity: AlertSeverity = AlertSeverity.WARNING
    message: str = ""

    def evaluate(self, metric: PipelineMetric) -> Optional["Alert"]:
        """Return an Alert if the metric violates this rule, else None."""
        if metric.pipeline_name != self.pipeline:
            return None

        violations: List[str] = []

        if self.min_success_rate is not None:
            rate = success_rate(metric)
            if rate < self.min_success_rate:
                violations.append(
                    f"success_rate {rate:.2%} < threshold {self.min_success_rate:.2%}"
                )

        if self.min_throughput is not None:
            tput = throughput(metric)
            if tput < self.min_throughput:
                violations.append(
                    f"throughput {tput:.2f} rec/s < threshold {self.min_throughput:.2f} rec/s"
                )

        if not violations:
            return None

        detail = self.message or "; ".join(violations)
        return Alert(
            rule_name=self.name,
            pipeline=self.pipeline,
            severity=self.severity,
            detail=detail,
            metric=metric,
        )


@dataclass
class Alert:
    """Represents a fired alert for a pipeline."""

    rule_name: str
    pipeline: str
    severity: AlertSeverity
    detail: str
    metric: PipelineMetric

    def to_dict(self) -> dict:
        return {
            "rule": self.rule_name,
            "pipeline": self.pipeline,
            "severity": self.severity.value,
            "detail": self.detail,
        }

    def __str__(self) -> str:
        return (
            f"[{self.severity.value.upper()}] {self.pipeline} — "
            f"{self.rule_name}: {self.detail}"
        )


def evaluate_rules(
    metric: PipelineMetric, rules: List[AlertRule]
) -> List[Alert]:
    """Evaluate all rules against a metric and return any fired alerts."""
    alerts = []
    for rule in rules:
        alert = rule.evaluate(metric)
        if alert:
            alerts.append(alert)
    return alerts
