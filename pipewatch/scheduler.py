"""Periodic check scheduler for pipewatch pipelines."""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional


@dataclass
class ScheduledJob:
    """Represents a single scheduled pipeline check."""

    pipeline_name: str
    interval_seconds: int
    callback: Callable[[str], None]
    _timer: Optional[threading.Timer] = field(default=None, init=False, repr=False)
    _running: bool = field(default=False, init=False, repr=False)

    def start(self) -> None:
        """Start the periodic job."""
        self._running = True
        self._schedule_next()

    def stop(self) -> None:
        """Stop the periodic job."""
        self._running = False
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _schedule_next(self) -> None:
        if not self._running:
            return
        self._timer = threading.Timer(self.interval_seconds, self._run)
        self._timer.daemon = True
        self._timer.start()

    def _run(self) -> None:
        try:
            self.callback(self.pipeline_name)
        finally:
            self._schedule_next()


class Scheduler:
    """Manages multiple scheduled pipeline check jobs."""

    def __init__(self) -> None:
        self._jobs: Dict[str, ScheduledJob] = {}

    def register(self, pipeline_name: str, interval_seconds: int,
                 callback: Callable[[str], None]) -> None:
        """Register a pipeline for periodic checks."""
        if pipeline_name in self._jobs:
            raise ValueError(f"Pipeline '{pipeline_name}' is already registered.")
        job = ScheduledJob(
            pipeline_name=pipeline_name,
            interval_seconds=interval_seconds,
            callback=callback,
        )
        self._jobs[pipeline_name] = job

    def start_all(self) -> None:
        """Start all registered jobs."""
        for job in self._jobs.values():
            job.start()

    def stop_all(self) -> None:
        """Stop all registered jobs."""
        for job in self._jobs.values():
            job.stop()

    def stop(self, pipeline_name: str) -> None:
        """Stop a specific pipeline job."""
        job = self._jobs.get(pipeline_name)
        if job is None:
            raise KeyError(f"No scheduled job found for pipeline '{pipeline_name}'.")
        job.stop()

    @property
    def registered_pipelines(self) -> list[str]:
        return list(self._jobs.keys())
