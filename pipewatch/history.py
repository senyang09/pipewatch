"""Metric history tracking for pipeline health trends."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, to_dict


@dataclass
class MetricSnapshot:
    """A timestamped snapshot of a pipeline metric."""

    timestamp: str
    pipeline: str
    metric: PipelineMetric

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "pipeline": self.pipeline,
            "metric": to_dict(self.metric),
        }


@dataclass
class MetricHistory:
    """Stores and retrieves historical metric snapshots."""

    store_path: Path
    _records: List[MetricSnapshot] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self.store_path = Path(self.store_path)
        if self.store_path.exists():
            self._load()

    def _load(self) -> None:
        with self.store_path.open() as fh:
            raw = json.load(fh)
        for entry in raw:
            m = entry["metric"]
            metric = PipelineMetric(
                total_records=m["total_records"],
                failed_records=m["failed_records"],
                processing_time_seconds=m["processing_time_seconds"],
                pipeline_name=m["pipeline_name"],
            )
            self._records.append(
                MetricSnapshot(
                    timestamp=entry["timestamp"],
                    pipeline=entry["pipeline"],
                    metric=metric,
                )
            )

    def record(self, pipeline: str, metric: PipelineMetric) -> MetricSnapshot:
        """Append a new snapshot and persist to disk."""
        snapshot = MetricSnapshot(
            timestamp=datetime.utcnow().isoformat(),
            pipeline=pipeline,
            metric=metric,
        )
        self._records.append(snapshot)
        self._persist()
        return snapshot

    def _persist(self) -> None:
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        with self.store_path.open("w") as fh:
            json.dump([r.to_dict() for r in self._records], fh, indent=2)

    def get_pipeline_history(
        self, pipeline: str, limit: Optional[int] = None
    ) -> List[MetricSnapshot]:
        """Return snapshots for a specific pipeline, newest first."""
        results = [r for r in self._records if r.pipeline == pipeline]
        results = list(reversed(results))
        return results[:limit] if limit is not None else results

    def clear(self) -> None:
        """Remove all records and delete the store file."""
        self._records = []
        if self.store_path.exists():
            self.store_path.unlink()
