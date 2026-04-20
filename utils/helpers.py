"""
utils/helpers.py
Shared utility functions: config loading, logging setup, path helpers.
"""

import os
import logging
import yaml
from pathlib import Path
from datetime import datetime, timezone


# ──────────────────────────────────────────────
# Config Loader
# ──────────────────────────────────────────────

def load_config(config_path: str = None) -> dict:
    """Load pipeline_config.yaml, resolving env-var placeholders."""
    if config_path is None:
        config_path = Path(__file__).resolve().parent.parent / "config" / "pipeline_config.yaml"

    with open(config_path, "r") as f:
        raw = f.read()

    # Substitute ${ENV_VAR} placeholders
    import re
    def _replace(match):
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))

    resolved = re.sub(r"\$\{(\w+)\}", _replace, raw)
    return yaml.safe_load(resolved)


# ──────────────────────────────────────────────
# Logger Setup
# ──────────────────────────────────────────────

def get_logger(name: str, config: dict = None) -> logging.Logger:
    """Return a consistently formatted logger."""
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # Already configured

    level = logging.INFO
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

    if config:
        log_cfg = config.get("logging", {})
        level = getattr(logging, log_cfg.get("level", "INFO"))
        fmt = log_cfg.get("format", fmt)
        log_file = log_cfg.get("log_file")
        if log_file:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(log_file)
            fh.setFormatter(logging.Formatter(fmt))
            logger.addHandler(fh)

    logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(fmt))
    logger.addHandler(ch)
    return logger


# ──────────────────────────────────────────────
# Path Helpers
# ──────────────────────────────────────────────

def get_partition_path(base_path: str, ts: datetime = None) -> str:
    """
    Returns a Hive-style partitioned path.
    e.g. /bronze/year=2024/month=06/day=15/hour=10
    """
    ts = ts or datetime.now(timezone.utc)
    return os.path.join(
        base_path,
        f"year={ts.year}",
        f"month={ts.month:02d}",
        f"day={ts.day:02d}",
        f"hour={ts.hour:02d}",
    )


def ensure_dir(path: str) -> str:
    """Create directory if it doesn't exist, return path."""
    Path(path).mkdir(parents=True, exist_ok=True)
    return path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
