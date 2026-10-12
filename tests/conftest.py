"""Shared fixtures for building small in-memory chart histories."""
from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from typing import Dict, Iterable, Optional

import pytest

from bpc.db import init_db


def monday(iso: str) -> str:
    d = date.fromisoformat(iso)
    assert d.weekday() == 0, f"{iso} is not a Monday"
    return iso


def mondays(start: str, count: int) -> list[str]:
    d = date.fromisoformat(monday(start))
    return [(d + timedelta(weeks=i)).isoformat() for i in range(count)]


class History:
    """Writes snapshots and entries the way ingest does, without the network."""

    def __init__(self, conn: sqlite3.Connection, chart_id: str = "test-chart"):
        self.conn = conn
        self.chart_id = chart_id
        conn.execute(
            "INSERT OR IGNORE INTO charts (id, chart_type, genre_slug, name) VALUES (?, 'top100', 'test', ?)",
            (chart_id, chart_id),
        )
        conn.commit()

    def ok(self, week: str, ranks: Dict[str, int]) -> None:
        snap_id = f"{self.chart_id}:{week}"
        self.conn.execute(
            "INSERT INTO chart_snapshots (id, chart_id, snapshot_date, fetched_at, status) VALUES (?, ?, ?, ?, 'ok')",
            (snap_id, self.chart_id, week, week),
        )
        for track_id, rank in ranks.items():
            self.conn.execute("INSERT OR IGNORE INTO tracks (id, title) VALUES (?, ?)", (track_id, track_id))
            self.conn.execute(
                "INSERT INTO chart_entries (snapshot_id, track_id, rank) VALUES (?, ?, ?)",
                (snap_id, track_id, rank),
            )
        self.conn.commit()

    def failed(self, week: str, error: str = "HTTPError: 503") -> None:
        self.conn.execute(
            "INSERT INTO chart_snapshots (id, chart_id, snapshot_date, fetched_at, status, error) VALUES (?, ?, ?, ?, 'failed', ?)",
            (f"{self.chart_id}:{week}", self.chart_id, week, week, error),
        )
        self.conn.commit()

    def metrics(self, track_id: str, as_of: Optional[str] = None) -> Optional[sqlite3.Row]:
        if as_of is None:
            return self.conn.execute(
                "SELECT * FROM durability_metrics WHERE chart_id = ? AND track_id = ?",
                (self.chart_id, track_id),
            ).fetchone()
        return self.conn.execute(
            "SELECT * FROM durability_metrics WHERE chart_id = ? AND track_id = ? AND as_of_week = ?",
            (self.chart_id, track_id, as_of),
        ).fetchone()


@pytest.fixture
def conn() -> Iterable[sqlite3.Connection]:
    c = sqlite3.connect(":memory:")
    c.execute("PRAGMA foreign_keys = ON")
    c.row_factory = sqlite3.Row
    init_db(c)
    yield c
    c.close()
