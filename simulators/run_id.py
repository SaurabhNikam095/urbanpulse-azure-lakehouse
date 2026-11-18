"""Run-scoped ID helpers so two simulator runs never reuse trip or payment IDs."""

import os
import re
from datetime import UTC, datetime

_RUN_RE = re.compile(r"[A-Za-z0-9]{4,16}")


def get_run_id(explicit: str | None = None) -> str:
    """Priority: explicit arg, then SIM_RUN_ID env var, then a UTC timestamp."""
    rid = explicit or os.environ.get("SIM_RUN_ID") or datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    if not _RUN_RE.fullmatch(rid):
        raise ValueError(f"run_id must be 4-16 letters/digits, got {rid!r}")
    return rid


def make_id(prefix: str, run_id: str, n: int) -> str:
    """make_id('TRP', '20261120T091500', 1) -> 'TRP-20261120T091500-00000001'"""
    return f"{prefix}-{run_id}-{n:08d}"
