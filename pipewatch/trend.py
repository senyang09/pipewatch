"""Trend analysis over historical metric snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.history import MetricSnapshot
from pipewatch.metrics import success_rate, throughput


@dataclass
class TrendSummary:
    """Summary of metric trends for a pipeline."""

    pipeline: str
    sample_count: int
    avg_success_rate: float
    avg_throughput: float
    min_success_rate: float
    max_success_rate: float
    degrading: bool  # True if latest success_rate < oldest success_rate

    def format(self) -> str:
        direction = "degrading" if self.degrading else "stable/improving"
        return (
            f"Pipeline : {self.pipeline}\n"
            f"Samples  : {self.sample_count}\n"
            f"Avg success rate : {self.avg_success_rate:.1%}\n"
            f"Avg throughput   : {self.avg_throughput:.2f} rec/s\n"
            f"Success range    : {self.min_success_rate:.1%} – {self.max_success_rate:.1%}\n"
            f"Trend            : {direction}"
        )


def analyse_trend(snapshots: List[MetricSnapshot]) -> TrendSummary:
    """Compute trend statistics from a list of snapshots (newest first).

    Raises ValueError if snapshots is empty.
    """
    if not snapshots:
        raise ValueError("Cannot analyse trend with no snapshots.")

    pipeline = snapshots[0].pipeline
    rates = [success_rate(s.metric) for s in snapshots]
    throughputs = [throughput(s.metric) for s in snapshots]

    avg_sr = sum(rates) / len(rates)
    avg_tp = sum(throughputs) / len(throughputs)
    min_sr = min(rates)
    max_sr = max(rates)

    # snapshots are newest-first; compare latest vs oldest
    degrading = rates[0] < rates[-1] if len(rates) > 1 else False

    return TrendSummary(
        pipeline=pipeline,
        sample_count=len(snapshots),
        avg_success_rate=avg_sr,
        avg_throughput=avg_tp,
        min_success_rate=min_sr,
        max_success_rate=max_sr,
        degrading=degrading,
    )
