"""Database size stays bounded: durability_metrics is a derived cache of the
latest computed slice per chart, and tracks no chart references are pruned."""
from __future__ import annotations

from datetime import date

from bpc import db
from bpc.compute import run_compute

from .conftest import History, mondays


def _slices(conn, chart_id):
    return [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT as_of_week FROM durability_metrics WHERE chart_id = ? ORDER BY 1",
            (chart_id,),
        )
    ]


def test_metrics_keep_one_slice_per_chart(conn):
    w = mondays("2026-03-02", 3)
    h = History(conn)
    for week in w:
        h.ok(week, {"a": 1, "b": 2})
        run_compute(conn, date.fromisoformat(week))

    assert _slices(conn, h.chart_id) == [w[-1]]
    # The surviving slice is complete, not a leftover.
    assert h.metrics("a", as_of=w[-1])["weeks_on_chart"] == 3


def test_stale_chart_slice_survives_other_charts_moving_on(conn):
    w = mondays("2026-03-02", 20)
    stale = History(conn, "stale-chart")
    live = History(conn, "live-chart")
    stale.ok(w[0], {"s": 1})
    for week in w:
        live.ok(week, {"l": 1})
    run_compute(conn, date.fromisoformat(w[-1]))

    assert _slices(conn, "stale-chart") == [w[0]]
    assert _slices(conn, "live-chart") == [w[-1]]


def test_prune_orphan_tracks_removes_only_unreferenced_tracks(conn):
    w = mondays("2025-01-06", 60)
    h = History(conn)
    h.ok(w[0], {"gone": 1, "kept": 2})
    for week in w[1:]:
        h.ok(week, {"kept": 1})
    conn.execute("INSERT INTO tracks (id, title) VALUES ('metric-only', 'm')")
    conn.execute(
        """INSERT INTO durability_metrics (
               chart_id, track_id, as_of_week, weeks_on_chart, first_seen_week, last_seen_week,
               age_weeks, presence_ratio, current_streak_weeks, max_streak_weeks, reentry_count,
               segments_count, best_rank, best_rank_week, avg_rank, rank_stddev, top10_weeks,
               top25_weeks, durability_score)
           VALUES (?, 'metric-only', ?, 1, ?, ?, 1, 1.0, 1, 1, 0, 1, 1, ?, 1.0, 0.0, 1, 1, 0.5)""",
        (h.chart_id, w[0], w[0], w[0], w[0]),
    )
    conn.commit()

    db.prune_chart_entries(conn, keep_weeks=52)
    deleted = db.prune_orphan_tracks(conn)

    remaining = {r[0] for r in conn.execute("SELECT id FROM tracks")}
    assert deleted == 1
    assert remaining == {"kept", "metric-only"}
