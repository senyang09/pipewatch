"""pipewatch — A lightweight CLI for monitoring and alerting on ETL pipeline health metrics."""

__version__ = "0.1.0"
__author__ = "pipewatch contributors"

from pipewatch.metrics import PipelineMetric, collect_metric

__all__ = ["PipelineMetric", "collect_metric"]
