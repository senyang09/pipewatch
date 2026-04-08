"""Load scheduler configuration from YAML."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore


@dataclass
class PipelineScheduleEntry:
    """Configuration entry for a single pipeline schedule."""

    pipeline_name: str
    interval_seconds: int

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError(
                f"interval_seconds must be positive, got {self.interval_seconds}"
            )


def _parse_entry(raw: dict) -> PipelineScheduleEntry:
    """Parse a single schedule entry dict."""
    name = raw.get("pipeline")
    if not name:
        raise ValueError("Schedule entry missing required 'pipeline' field.")
    interval = raw.get("interval_seconds")
    if interval is None:
        raise ValueError(f"Schedule entry for '{name}' missing 'interval_seconds'.")
    return PipelineScheduleEntry(
        pipeline_name=str(name),
        interval_seconds=int(interval),
    )


def load_schedule(data: dict) -> List[PipelineScheduleEntry]:
    """Load schedule entries from a parsed config dict."""
    schedules = data.get("schedules", [])
    if not isinstance(schedules, list):
        raise ValueError("'schedules' must be a list.")
    return [_parse_entry(entry) for entry in schedules]


def load_schedule_from_yaml(path: str) -> List[PipelineScheduleEntry]:
    """Load schedule entries from a YAML file."""
    if yaml is None:  # pragma: no cover
        raise RuntimeError("PyYAML is required to load schedule config from YAML.")
    with open(path, "r") as fh:
        data = yaml.safe_load(fh) or {}
    return load_schedule(data)
