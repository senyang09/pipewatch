"""Aggregates metric snapshots into summary statistics for a pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.history import MetricSnapshot


@dataclass
class AggregatedStats:
    pipeline_id: str
    sample_count: int
    avg_success_rate: float
    min_success_rate: float
    max_success_rate: float
    avg_throughput: float
    total_records_processed: int
    total_errors: int

    def format(self) -> str:
        lines = [
            f"Pipeline : {self.pipeline_id}",
            f"Samples  : {self.sample_count}",
            f"Success  : avg={self.avg_success_rate:.1%}  "
            f"min={self.min_success_rate:.1%}  max={self.max_success_rate:.1%}",
            f"Throughput (avg): {self.avg_throughput:.1f} rec/s",
            f"Total processed : {self.total_records_processed}",
            f"Total errors    : {self.total_errors}",
        ]
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "pipeline_id": self.pipeline_id,
            "sample_count": self.sample_count,
            "avg_success_rate": round(self.avg_success_rate, 6),
            "min_success_rate": round(self.min_success_rate, 6),
            "max_success_rate": round(self.max_success_rate, 6),
            "avg_throughput": round(self.avg_throughput, 4),
            "total_records_processed": self.total_records_processed,
            "total_errors": self.total_errors,
        }


def aggregate(snapshots: List[MetricSnapshot]) -> AggregatedStats:
    """Compute summary statistics from a list of MetricSnapshots.

    Raises ValueError if *snapshots* is empty.
    """
    if not snapshots:
        raise ValueError("Cannot aggregate an empty snapshot list.")

    pipeline_id = snapshots[0].pipeline_id
    success_rates = [s.success_rate for s in snapshots]
    throughputs = [s.throughput for s in snapshots]
    total_processed = sum(s.records_processed for s in snapshots)
    total_errors = sum(s.records_failed for s in snapshots)

    return AggregatedStats(
        pipeline_id=pipeline_id,
        sample_count=len(snapshots),
        avg_success_rate=sum(success_rates) / len(success_rates),
        min_success_rate=min(success_rates),
        max_success_rate=max(success_rates),
        avg_throughput=sum(throughputs) / len(throughputs),
        total_records_processed=total_processed,
        total_errors=total_errors,
    )
