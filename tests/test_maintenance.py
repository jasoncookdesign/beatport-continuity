"""Tests for database maintenance: durability_metrics pruning, chart_entries pruning, and VACUUM.

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


def _insert_durability_row(
    conn: sqlite3.Connection, chart_id: str, track_id: str, as_of_week: str
) -> None:
    conn.execute(
        """INSERT OR IGNORE INTO durability_metrics (
               chart_id, track_id, as_of_week,
               weeks_on_chart, first_seen_week, last_seen_week, age_weeks, presence_ratio,
               current_streak_weeks, max_streak_weeks, reentry_count, segments_count,
               best_rank, best_rank_week, avg_rank, rank_stddev,
               top10_weeks, top25_weeks, durability_score
           ) VALUES (?, ?, ?, 1, ?, ?, 1, 1.0, 1, 1, 0, 1, 1, ?, 1.0, 0.0, 0, 0, 0.5)""",
        (chart_id, track_id, as_of_week, as_of_week, as_of_week, as_of_week),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests: prune_durability_metrics
# ---------------------------------------------------------------------------

class TestPruneDurabilityMetrics:
    def test_keeps_most_recent_weeks_exact(self):
        """When table has exactly keep_weeks distinct as_of_week values, nothing is deleted."""
        prune_durability_metrics = _get_fn("prune_durability_metrics")
        conn = _make_mem_db()
        _insert_chart(conn)
        _insert_track(conn, "t1")

        for i in range(12):
            _insert_durability_row(conn, "test-chart", "t1", _monday(-i))

        deleted = prune_durability_metrics(conn, keep_weeks=12)
        remaining = conn.execute(
            "SELECT COUNT(DISTINCT as_of_week) FROM durability_metrics"
        ).fetchone()[0]

        assert deleted == 0
        assert remaining == 12

    def test_prunes_older_weeks_when_over_limit(self):
        """With 15 distinct weeks, 3 should be removed keeping the 12 most recent."""
        prune_durability_metrics = _get_fn("prune_durability_metrics")
        conn = _make_mem_db()
        _insert_chart(conn)
        _insert_track(conn, "t1")

        for i in range(15):
            _insert_durability_row(conn, "test-chart", "t1", _monday(-i))

        deleted = prune_durability_metrics(conn, keep_weeks=12)
        remaining_weeks = conn.execute(
            "SELECT COUNT(DISTINCT as_of_week) FROM durability_metrics"
        ).fetchone()[0]

        assert deleted == 3
        assert remaining_weeks == 12

    def test_keeps_only_most_recent_when_many_weeks(self):
        """Retained weeks are the 12 most recent, not the 12 oldest."""
        prune_durability_metrics = _get_fn("prune_durability_metrics")
        conn = _make_mem_db()
        _insert_chart(conn)
        _insert_track(conn, "t1")

        weeks = [_monday(-i) for i in range(20)]
        for w in weeks:
            _insert_durability_row(conn, "test-chart", "t1", w)

        prune_durability_metrics(conn, keep_weeks=12)

        kept = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT as_of_week FROM durability_metrics"
            ).fetchall()
        }
        expected = set(sorted(weeks, reverse=True)[:12])
        assert kept == expected

    def test_noop_when_fewer_than_keep_weeks(self):
        """When fewer rows than keep_weeks, nothing is deleted."""
        prune_durability_metrics = _get_fn("prune_durability_metrics")
        conn = _make_mem_db()
        _insert_chart(conn)
        _insert_track(conn, "t1")

        for i in range(5):
            _insert_durability_row(conn, "test-chart", "t1", _monday(-i))

        deleted = prune_durability_metrics(conn, keep_weeks=12)

        assert deleted == 0
        assert (
            conn.execute(
                "SELECT COUNT(DISTINCT as_of_week) FROM durability_metrics"
            ).fetchone()[0]
            == 5
        )

    def test_noop_on_empty_table(self):
        """Empty table: should not raise and should return 0."""
        prune_durability_metrics = _get_fn("prune_durability_metrics")
        conn = _make_mem_db()
        deleted = prune_durability_metrics(conn, keep_weeks=12)
        assert deleted == 0

    def test_prunes_rows_across_multiple_charts(self):
        """Pruning applies per-week globally, not per-chart."""
        prune_durability_metrics = _get_fn("prune_durability_metrics")
        conn = _make_mem_db()
        for chart_id in ("chart-a", "chart-b"):
            _insert_chart(conn, chart_id)
        _insert_track(conn, "t1")

        # 15 weeks for each of 2 charts = 30 rows
        for i in range(15):
            _insert_durability_row(conn, "chart-a", "t1", _monday(-i))
            _insert_durability_row(conn, "chart-b", "t1", _monday(-i))

        deleted = prune_durability_metrics(conn, keep_weeks=12)

        # 3 old weeks x 2 charts x 1 track = 6 rows deleted
        assert deleted == 6
        remaining_weeks = conn.execute(
            "SELECT COUNT(DISTINCT as_of_week) FROM durability_metrics"
        ).fetchone()[0]
        assert remaining_weeks == 12


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
