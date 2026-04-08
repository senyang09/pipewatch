"""Reporter module for formatting and outputting pipeline health reports."""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.metrics import PipelineMetric, is_healthy, to_dict as metric_to_dict


@dataclass
class PipelineReport:
    """Encapsulates a full health report for a pipeline run."""

    pipeline_name: str
    metric: PipelineMetric
    alerts: List[Alert]
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()

    @property
    def healthy(self) -> bool:
        return is_healthy(self.metric) and not any(
            a.severity == AlertSeverity.CRITICAL for a in self.alerts
        )

    @property
    def has_warnings(self) -> bool:
        return any(a.severity == AlertSeverity.WARNING for a in self.alerts)


def format_report(report: PipelineReport) -> str:
    """Return a human-readable string representation of the report."""
    lines = [
        f"Pipeline Report: {report.pipeline_name}",
        f"Timestamp : {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"Status    : {'HEALTHY' if report.healthy else 'UNHEALTHY'}",
        "-" * 40,
        "Metrics:",
    ]
    for key, value in metric_to_dict(report.metric).items():
        lines.append(f"  {key:<20}: {value}")

    if report.alerts:
        lines.append("-" * 40)
        lines.append("Alerts:")
        for alert in report.alerts:
            lines.append(f"  [{alert.severity.value.upper()}] {alert.rule_name}: {alert.message}")
    else:
        lines.append("-" * 40)
        lines.append("Alerts: none")

    return "\n".join(lines)


def report_to_dict(report: PipelineReport) -> dict:
    """Serialize the report to a plain dictionary."""
    return {
        "pipeline_name": report.pipeline_name,
        "timestamp": report.timestamp.isoformat(),
        "healthy": report.healthy,
        "has_warnings": report.has_warnings,
        "metrics": metric_to_dict(report.metric),
        "alerts": [a.to_dict() for a in report.alerts],
    }
