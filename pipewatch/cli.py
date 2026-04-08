"""CLI entry point for pipewatch."""

import json
import sys
from typing import Optional

import click

from pipewatch.alert_config import load_rules_from_yaml
from pipewatch.alerts import evaluate
from pipewatch.metrics import PipelineMetric
from pipewatch.reporter import PipelineReport, format_report, report_to_dict


@click.group()
def cli():
    """pipewatch — monitor and alert on ETL pipeline health metrics."""


@cli.command()
@click.option("--pipeline", required=True, help="Name of the pipeline.")
@click.option("--total", required=True, type=int, help="Total records processed.")
@click.option("--failed", required=True, type=int, help="Failed records count.")
@click.option("--duration", required=True, type=float, help="Run duration in seconds.")
@click.option("--rules", "rules_file", default=None, help="Path to YAML alert rules file.")
@click.option(
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format.",
)
def check(
    pipeline: str,
    total: int,
    failed: int,
    duration: float,
    rules_file: Optional[str],
    output: str,
):
    """Check pipeline health and emit a report."""
    metric = PipelineMetric(
        total_records=total,
        failed_records=failed,
        duration_seconds=duration,
    )

    rules = []
    if rules_file:
        try:
            rules = load_rules_from_yaml(rules_file)
        except (FileNotFoundError, ValueError) as exc:
            click.echo(f"Error loading rules: {exc}", err=True)
            sys.exit(2)

    alerts = [evaluate(rule, metric) for rule in rules]
    alerts = [a for a in alerts if a is not None]

    report = PipelineReport(pipeline_name=pipeline, metric=metric, alerts=alerts)

    if output == "json":
        click.echo(json.dumps(report_to_dict(report), indent=2))
    else:
        click.echo(format_report(report))

    if not report.healthy:
        sys.exit(1)


def main():
    cli()


if __name__ == "__main__":
    main()
