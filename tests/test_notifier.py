"""Tests for pipewatch.notifier."""
from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.notifier import LogChannel, EmailChannel, dispatch


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def critical_alert() -> Alert:
    return Alert(
        rule_name="low_success_rate",
        message="Success rate 0.50 below threshold 0.90",
        severity=AlertSeverity.CRITICAL,
        metric_value=0.50,
    )


@pytest.fixture()
def warning_alert() -> Alert:
    return Alert(
        rule_name="low_throughput",
        message="Throughput 5 below threshold 10",
        severity=AlertSeverity.WARNING,
        metric_value=5,
    )


# ---------------------------------------------------------------------------
# LogChannel
# ---------------------------------------------------------------------------

class TestLogChannel:
    def test_logs_critical_alert(self, critical_alert: Alert, caplog):
        channel = LogChannel()
        with caplog.at_level(logging.CRITICAL, logger="pipewatch.notifier"):
            channel.send([critical_alert])
        assert "low_success_rate" in caplog.text

    def test_logs_warning_alert(self, warning_alert: Alert, caplog):
        channel = LogChannel()
        with caplog.at_level(logging.WARNING, logger="pipewatch.notifier"):
            channel.send([warning_alert])
        assert "low_throughput" in caplog.text

    def test_no_output_for_empty_list(self, caplog):
        channel = LogChannel()
        with caplog.at_level(logging.DEBUG, logger="pipewatch.notifier"):
            channel.send([])
        assert caplog.text == ""


# ---------------------------------------------------------------------------
# EmailChannel
# ---------------------------------------------------------------------------

class TestEmailChannel:
    def _make_channel(self) -> EmailChannel:
        return EmailChannel(
            smtp_host="localhost",
            smtp_port=1025,
            sender="pipewatch@example.com",
            recipients=["ops@example.com"],
        )

    def test_send_calls_smtp(self, critical_alert: Alert):
        channel = self._make_channel()
        with patch("pipewatch.notifier.smtplib.SMTP") as mock_smtp_cls:
            mock_server = MagicMock()
            mock_smtp_cls.return_value.__enter__.return_value = mock_server
            channel.send([critical_alert])
        mock_server.send_message.assert_called_once()

    def test_no_smtp_call_for_empty_list(self):
        channel = self._make_channel()
        with patch("pipewatch.notifier.smtplib.SMTP") as mock_smtp_cls:
            channel.send([])
        mock_smtp_cls.assert_not_called()

    def test_smtp_error_is_caught(self, critical_alert: Alert, caplog):
        channel = self._make_channel()
        with patch("pipewatch.notifier.smtplib.SMTP", side_effect=OSError("refused")):
            with caplog.at_level(logging.ERROR, logger="pipewatch.notifier"):
                channel.send([critical_alert])  # must not raise
        assert "Failed to send" in caplog.text


# ---------------------------------------------------------------------------
# dispatch helper
# ---------------------------------------------------------------------------

def test_dispatch_calls_all_channels(critical_alert: Alert, warning_alert: Alert):
    ch1, ch2 = MagicMock(), MagicMock()
    dispatch([critical_alert, warning_alert], [ch1, ch2])
    ch1.send.assert_called_once_with([critical_alert, warning_alert])
    ch2.send.assert_called_once_with([critical_alert, warning_alert])


def test_dispatch_skips_channels_for_empty_alerts():
    ch = MagicMock()
    dispatch([], [ch])
    ch.send.assert_not_called()
