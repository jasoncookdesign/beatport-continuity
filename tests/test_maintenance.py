"""Tests for database maintenance: chart_entries pruning and VACUUM.

durability_metrics retention is covered in test_retention.py.

TDD: these tests were written before the implementation in db.py.
"""
from __future__ import annotations

import importlib
import sqlite3
from datetime import date, timedelta

import pytest

from bpc.db import get_conn, init_db

# Import maintenance functions via getattr so that a missing function causes a
# clear test failure at call time (RED: feature missing) rather than a
# module-level ImportError that blocks test collection.
_db_mod = importlib.import_module("bpc.db")


def _get_fn(name: str):
    fn = getattr(_db_mod, name, None)
    if fn is None:
        pytest.fail(f"bpc.db.{name} is not yet implemented (RED: feature missing)")
    return fn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mem_db() -> sqlite3.Connection:
    """Return an in-memory SQLite connection with the full schema initialised."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def _monday(offset_weeks: int = 0) -> str:
    """Return an ISO date string for a Monday offset_weeks from 2026-06-09."""
    base = date(2026, 6, 9)
    return (base + timedelta(weeks=offset_weeks)).isoformat()


def _insert_chart(conn: sqlite3.Connection, chart_id: str = "test-chart") -> None:
    conn.execute(
        "INSERT OR IGNORE INTO charts (id, chart_type, genre_slug, name) VALUES (?, 'top', 'all', ?)",
        (chart_id, chart_id),
    )
    conn.commit()


def _insert_track(conn: sqlite3.Connection, track_id: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO tracks (id, title) VALUES (?, ?)",
        (track_id, track_id),
    )
    conn.commit()


def _insert_snapshot(
    conn: sqlite3.Connection, chart_id: str, snapshot_date: str
) -> str:
    snap_id = f"{chart_id}:{snapshot_date}"
    conn.execute(
        """INSERT OR IGNORE INTO chart_snapshots (id, chart_id, snapshot_date, fetched_at)
           VALUES (?, ?, ?, ?)""",
        (snap_id, chart_id, snapshot_date, snapshot_date),
    )
    conn.commit()
    return snap_id


def _insert_entry(
    conn: sqlite3.Connection, snapshot_id: str, track_id: str, rank: int = 1
) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO chart_entries (snapshot_id, track_id, rank) VALUES (?, ?, ?)",
        (snapshot_id, track_id, rank),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests: prune_chart_entries
# ---------------------------------------------------------------------------

class TestPruneChartEntries:
    def test_noop_when_all_snapshots_within_window(self):
        """No deletions when every snapshot is within the retention window."""
        prune_chart_entries = _get_fn("prune_chart_entries")
        conn = _make_mem_db()
        _insert_chart(conn)
        _insert_track(conn, "t1")

        # All within last 52 weeks (from 2026-06-09, go back 30 weeks)
        for i in range(30):
            snap_id = _insert_snapshot(conn, "test-chart", _monday(-i))
            _insert_entry(conn, snap_id, "t1", rank=1)

        entries_deleted, snaps_deleted = prune_chart_entries(conn, keep_weeks=52)

        assert entries_deleted == 0
        assert snaps_deleted == 0

    def test_deletes_old_entries_and_snapshots(self):
        """Snapshots older than keep_weeks are removed along with their entries."""
        prune_chart_entries = _get_fn("prune_chart_entries")
        conn = _make_mem_db()
        _insert_chart(conn)
        _insert_track(conn, "t1")

        # 60 weekly snapshots: most recent 52 should be kept, 8 should be pruned
        for i in range(60):
            snap_id = _insert_snapshot(conn, "test-chart", _monday(-i))
            _insert_entry(conn, snap_id, "t1", rank=1)

        entries_deleted, snaps_deleted = prune_chart_entries(conn, keep_weeks=52)

        assert snaps_deleted == 8
        assert entries_deleted == 8  # one entry per snapshot

        remaining_snaps = conn.execute(
            "SELECT COUNT(*) FROM chart_snapshots"
        ).fetchone()[0]
        assert remaining_snaps == 52

        remaining_entries = conn.execute(
            "SELECT COUNT(*) FROM chart_entries"
        ).fetchone()[0]
        assert remaining_entries == 52

    def test_noop_on_empty_tables(self):
        """Empty tables: should not raise and should return (0, 0)."""
        prune_chart_entries = _get_fn("prune_chart_entries")
        conn = _make_mem_db()
        result = prune_chart_entries(conn, keep_weeks=52)
        assert result == (0, 0)

    def test_keeps_most_recent_snapshots(self):
        """The retained snapshots are the most recent keep_weeks, not the oldest."""
        prune_chart_entries = _get_fn("prune_chart_entries")
        conn = _make_mem_db()
        _insert_chart(conn)
        _insert_track(conn, "t1")

        weeks = [_monday(-i) for i in range(60)]
        for w in weeks:
            snap_id = _insert_snapshot(conn, "test-chart", w)
            _insert_entry(conn, snap_id, "t1", rank=1)

        prune_chart_entries(conn, keep_weeks=52)

        kept_dates = {
            r[0]
            for r in conn.execute(
                "SELECT snapshot_date FROM chart_snapshots"
            ).fetchall()
        }
        expected = set(sorted(weeks)[-52:])
        assert kept_dates == expected


# ---------------------------------------------------------------------------
# Tests: vacuum_db
# ---------------------------------------------------------------------------

class TestVacuumDb:
    def test_vacuum_does_not_raise(self):
        """vacuum_db should complete without raising an exception."""
        vacuum_db = _get_fn("vacuum_db")
        conn = _make_mem_db()
        # Ensure no transaction is open
        conn.commit()
        vacuum_db(conn)  # Should not raise
