"""Notification dispatch for pipewatch alerts."""
from __future__ import annotations

import smtplib
import logging
from dataclasses import dataclass, field
from email.message import EmailMessage
from typing import List, Protocol

from pipewatch.alerts import Alert, AlertSeverity

logger = logging.getLogger(__name__)


class NotificationChannel(Protocol):
    """Protocol that every notification channel must satisfy."""

    def send(self, alerts: List[Alert]) -> None:  # pragma: no cover
        ...


@dataclass
class LogChannel:
    """Writes alerts to the Python logging system (always available)."""

    level_map: dict = field(default_factory=lambda: {
        AlertSeverity.CRITICAL: logging.CRITICAL,
        AlertSeverity.WARNING: logging.WARNING,
        AlertSeverity.INFO: logging.INFO,
    })

    def send(self, alerts: List[Alert]) -> None:
        for alert in alerts:
            lvl = self.level_map.get(alert.severity, logging.WARNING)
            logger.log(lvl, "[pipewatch] %s — %s", alert.rule_name, alert.message)


@dataclass
class EmailChannel:
    """Sends a plain-text e-mail summary via SMTP."""

    smtp_host: str
    smtp_port: int
    sender: str
    recipients: List[str]
    subject_prefix: str = "[pipewatch]"

    def send(self, alerts: List[Alert]) -> None:
        if not alerts:
            return

        body_lines = [f"pipewatch detected {len(alerts)} alert(s):\n"]
        for alert in alerts:
            body_lines.append(
                f"  [{alert.severity.value.upper()}] {alert.rule_name}: {alert.message}"
            )
        body = "\n".join(body_lines)

        severities = {a.severity for a in alerts}
        tag = "CRITICAL" if AlertSeverity.CRITICAL in severities else "WARNING"
        subject = f"{self.subject_prefix} {tag} — {len(alerts)} alert(s) fired"

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)
        msg.set_content(body)

        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.send_message(msg)
            logger.debug("Email notification sent to %s", self.recipients)
        except OSError as exc:
            logger.error("Failed to send email notification: %s", exc)


def dispatch(alerts: List[Alert], channels: List[NotificationChannel]) -> None:
    """Send *alerts* through every registered *channel*."""
    if not alerts:
        return
    for channel in channels:
        channel.send(alerts)
