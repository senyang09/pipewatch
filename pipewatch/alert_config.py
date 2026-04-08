"""Load and validate alert rules from a YAML or dict configuration."""

from typing import Any, Dict, List

from pipewatch.alerts import AlertRule, AlertSeverity


_REQUIRED_KEYS = {"name", "pipeline"}
_OPTIONAL_KEYS = {"min_success_rate", "min_throughput", "severity", "message"}


def _parse_rule(raw: Dict[str, Any]) -> AlertRule:
    """Parse a single rule dict into an AlertRule."""
    missing = _REQUIRED_KEYS - raw.keys()
    if missing:
        raise ValueError(f"Alert rule missing required keys: {missing}")

    unknown = raw.keys() - (_REQUIRED_KEYS | _OPTIONAL_KEYS)
    if unknown:
        raise ValueError(f"Alert rule has unknown keys: {unknown}")

    severity_raw = raw.get("severity", AlertSeverity.WARNING.value)
    try:
        severity = AlertSeverity(severity_raw)
    except ValueError:
        valid = [s.value for s in AlertSeverity]
        raise ValueError(
            f"Invalid severity '{severity_raw}'. Must be one of {valid}."
        )

    min_sr = raw.get("min_success_rate")
    if min_sr is not None and not (0.0 <= float(min_sr) <= 1.0):
        raise ValueError("min_success_rate must be between 0.0 and 1.0")

    return AlertRule(
        name=raw["name"],
        pipeline=raw["pipeline"],
        min_success_rate=float(min_sr) if min_sr is not None else None,
        min_throughput=float(raw["min_throughput"]) if "min_throughput" in raw else None,
        severity=severity,
        message=raw.get("message", ""),
    )


def load_rules(config: Dict[str, Any]) -> List[AlertRule]:
    """Load alert rules from a configuration dictionary.

    Expected format::

        {"rules": [{"name": ..., "pipeline": ..., ...}, ...]}
    """
    raw_rules = config.get("rules", [])
    if not isinstance(raw_rules, list):
        raise ValueError("'rules' must be a list")
    return [_parse_rule(r) for r in raw_rules]


def load_rules_from_yaml(path: str) -> List[AlertRule]:
    """Load alert rules from a YAML file."""
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "PyYAML is required to load rules from YAML. "
            "Install it with: pip install pyyaml"
        ) from exc

    with open(path, "r") as fh:
        config = yaml.safe_load(fh)
    return load_rules(config or {})
