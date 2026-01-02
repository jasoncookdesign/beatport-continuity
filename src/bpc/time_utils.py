"""Time utilities for weekly bucketing."""
from __future__ import annotations

from datetime import date, timedelta


def week_bucket(d: date) -> date:
    """Return the Monday of the week containing the given date (local naive)."""

    weekday = d.weekday()  # Monday == 0
    return d - timedelta(days=weekday)


def today_bucket() -> date:
    """Return today's week bucket (Monday)."""

    return week_bucket(date.today())
