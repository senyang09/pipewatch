"""Core metrics data structures and collection for ETL pipeline health."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class PipelineMetric:
    """Represents a single health metric snapshot for an ETL pipeline."""

    pipeline_name: str
    records_processed: int
    records_failed: int
    duration_seconds: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    stage: Optional[str] = None

    @property
    def success_rate(self) -> float:
        """Calculate the percentage of successfully processed records."""
        total = self.records_processed + self.records_failed
        if total == 0:
            return 100.0
        return round((self.records_processed / total) * 100, 2)

    @property
    def throughput(self) -> float:
        """Calculate records processed per second."""
        if self.duration_seconds <= 0:
            return 0.0
        return round(self.records_processed / self.duration_seconds, 2)

    def is_healthy(self, min_success_rate: float = 95.0) -> bool:
        """Determine if the pipeline metric meets the minimum success threshold."""
        return self.success_rate >= min_success_rate

    def to_dict(self) -> dict:
        """Serialize metric to a plain dictionary."""
        return {
            "pipeline_name": self.pipeline_name,
            "stage": self.stage,
            "records_processed": self.records_processed,
            "records_failed": self.records_failed,
            "duration_seconds": self.duration_seconds,
            "success_rate": self.success_rate,
            "throughput": self.throughput,
            "timestamp": self.timestamp.isoformat(),
        }


def collect_metric(
    pipeline_name: str,
    records_processed: int,
    records_failed: int,
    duration_seconds: float,
    stage: Optional[str] = None,
) -> PipelineMetric:
    """Factory function to create and return a new PipelineMetric."""
    return PipelineMetric(
        pipeline_name=pipeline_name,
        records_processed=records_processed,
        records_failed=records_failed,
        duration_seconds=duration_seconds,
        stage=stage,
    )
