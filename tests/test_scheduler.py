"""Tests for pipewatch.scheduler."""

import threading
import time

import pytest

from pipewatch.scheduler import Scheduler, ScheduledJob


def test_register_and_list_pipelines():
    sched = Scheduler()
    sched.register("pipe_a", 60, lambda name: None)
    sched.register("pipe_b", 120, lambda name: None)
    assert set(sched.registered_pipelines) == {"pipe_a", "pipe_b"}


def test_register_duplicate_raises():
    sched = Scheduler()
    sched.register("pipe_a", 60, lambda name: None)
    with pytest.raises(ValueError, match="already registered"):
        sched.register("pipe_a", 30, lambda name: None)


def test_stop_unknown_pipeline_raises():
    sched = Scheduler()
    with pytest.raises(KeyError):
        sched.stop("nonexistent")


def test_callback_is_invoked(monkeypatch):
    """Callback fires after the interval elapses."""
    results: list[str] = []
    event = threading.Event()

    def callback(name: str) -> None:
        results.append(name)
        event.set()

    sched = Scheduler()
    sched.register("fast_pipe", 1, callback)
    sched.start_all()
    fired = event.wait(timeout=3)
    sched.stop_all()
    assert fired, "Callback was not invoked within timeout"
    assert results[0] == "fast_pipe"


def test_stop_all_prevents_further_calls():
    counter: list[int] = [0]

    def callback(name: str) -> None:
        counter[0] += 1

    sched = Scheduler()
    sched.register("pipe", 60, callback)
    sched.start_all()
    sched.stop_all()
    before = counter[0]
    time.sleep(0.1)
    assert counter[0] == before


def test_scheduled_job_stop_before_start():
    """Stopping a job that was never started should not raise."""
    job = ScheduledJob("pipe", 60, lambda name: None)
    job.stop()  # should be a no-op


def test_stop_individual_pipeline():
    sched = Scheduler()
    sched.register("pipe_a", 60, lambda name: None)
    sched.register("pipe_b", 60, lambda name: None)
    sched.start_all()
    sched.stop("pipe_a")
    # pipe_b still registered
    assert "pipe_b" in sched.registered_pipelines
    sched.stop_all()
