"""Export pipeline metrics and reports to various output formats."""
from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from typing import List, Sequence

from pipewatch.history import MetricSnapshot


@dataclass
class ExportResult:
    format: str
    content: str

    def write_to_file(self, path: str) -> None:
        """Write exported content to a file at *path*."""
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(self.content)


def export_json(snapshots: Sequence[MetricSnapshot]) -> ExportResult:
    """Serialise *snapshots* to a JSON string."""
    records = [snap.to_dict() for snap in snapshots]
    content = json.dumps(records, indent=2, default=str)
    return ExportResult(format="json", content=content)


def export_csv(snapshots: Sequence[MetricSnapshot]) -> ExportResult:
    """Serialise *snapshots* to CSV.

    Columns are derived from the keys of the first snapshot's dict
    representation so the schema stays in sync with MetricSnapshot.
    """
    if not snapshots:
        return ExportResult(format="csv", content="")

    rows = [snap.to_dict() for snap in snapshots]
    fieldnames: List[str] = list(rows[0].keys())

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return ExportResult(format="csv", content=buf.getvalue())


def export_snapshots(
    snapshots: Sequence[MetricSnapshot],
    fmt: str = "json",
) -> ExportResult:
    """Dispatch to the correct exporter based on *fmt*.

    Supported formats: ``json``, ``csv``.
    Raises :class:`ValueError` for unknown formats.
    """
    fmt = fmt.lower()
    if fmt == "json":
        return export_json(snapshots)
    if fmt == "csv":
        return export_csv(snapshots)
    raise ValueError(f"Unsupported export format: {fmt!r}. Choose 'json' or 'csv'.")
