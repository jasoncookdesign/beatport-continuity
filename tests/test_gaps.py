"""Continuity metrics treat failed and missing weeks as unknown.

A week is *known* when the chart has a status='ok' snapshot for it. A week is
*unknown* when the fetch failed (status='failed') or nothing was recorded at
all (the run never happened or never got pushed). Unknown weeks must never read
as a drop-off (no re-entry, no presence penalty) and never as consecutive
(streaks, week-over-week deltas and momentum don't bridge them).
"""
from __future__ import annotations

from datetime import date

import pytest

from bpc import compute, report
from bpc.compute import run_compute

from .conftest import History, mondays


def _compute(conn, target: str) -> None:
    run_compute(conn, date.fromisoformat(target))


class TestJulyAugustGapShape:
    """Mirrors the real 2026 hole: ok through 07-06, nothing for the eight
    Mondays 07-13..08-31, ok again from 09-07."""

    @pytest.fixture
    def hist(self, conn):
        h = History(conn)
        h.ok("2026-06-22", {"steady": 10, "faded": 20})
        h.ok("2026-06-29", {"steady": 8, "faded": 21})
        h.ok("2026-07-06", {"steady": 6, "faded": 22, "straddle": 30})
        # 2026-07-13 .. 2026-08-31: no rows at all
        h.ok("2026-09-07", {"steady": 5, "straddle": 31})
        h.ok("2026-09-14", {"steady": 4})
        _compute(conn, "2026-09-14")
        return h

    def test_streak_does_not_run_across_the_gap(self, hist):
        m = hist.metrics("steady")
        assert m["as_of_week"] == "2026-09-14"
        assert m["weeks_on_chart"] == 5
        assert m["current_streak_weeks"] == 2
        assert m["max_streak_weeks"] == 3

    def test_gap_is_not_a_drop_off(self, hist):
        m = hist.metrics("steady")
        assert m["reentry_count"] == 0
        assert m["segments_count"] == 1
        # 5 known weeks since first seen; the 8 unknown weeks don't dilute presence
        assert m["age_weeks"] == 5
        assert m["presence_ratio"] == pytest.approx(1.0)

    def test_presence_on_both_sides_of_gap_is_one_segment(self, hist):
        m = hist.metrics("straddle")
        assert m["segments_count"] == 1
        assert m["reentry_count"] == 0
        assert m["max_streak_weeks"] == 1
        # It was observed absent on 2026-09-14, so it has dropped off.
        assert m["current_streak_weeks"] == 0

    def test_observed_absence_after_gap_is_still_a_drop_off(self, hist):
        m = hist.metrics("faded")
        assert m["last_seen_week"] == "2026-07-06"
        assert m["current_streak_weeks"] == 0
        assert m["max_streak_weeks"] == 3

    def test_motion_window_stays_inside_current_streak(self, hist):
        m = hist.metrics("steady")
        assert m["last_rank"] == 4
        assert m["prev_rank"] == 5
        assert m["wow_delta"] == 1
        # Only 09-07 and 09-14 are consecutive known weeks: (5 - 4) / 1
        assert m["momentum_4w"] == pytest.approx(1.0)

    def test_unknown_weeks_lists_the_hole(self, hist):
        weeks = compute.unknown_weeks(hist.conn, hist.chart_id, "2026-09-14")
        assert weeks == mondays("2026-07-13", 8)


class TestFailedWeek:
    def test_failed_week_is_not_a_drop_off_or_consecutive(self, conn):
        w = mondays("2026-03-02", 4)
        h = History(conn)
        h.ok(w[0], {"a": 3})
        h.ok(w[1], {"a": 2})
        h.failed(w[2])
        h.ok(w[3], {"a": 1})
        _compute(conn, w[3])

        m = h.metrics("a")
        assert m["reentry_count"] == 0
        assert m["segments_count"] == 1
        assert m["max_streak_weeks"] == 2
        assert m["current_streak_weeks"] == 1
        assert m["age_weeks"] == 3
        assert m["presence_ratio"] == pytest.approx(1.0)
        # The previous calendar week is unknown, so there is no week-over-week delta.
        assert m["prev_rank"] is None
        assert m["wow_delta"] is None

    def test_observed_absence_is_a_re_entry(self, conn):
        w = mondays("2026-03-02", 3)
        h = History(conn)
        h.ok(w[0], {"a": 3, "b": 1})
        h.ok(w[1], {"b": 1})
        h.ok(w[2], {"a": 2, "b": 1})
        _compute(conn, w[2])

        m = h.metrics("a")
        assert m["reentry_count"] == 1
        assert m["segments_count"] == 2
        assert m["current_streak_weeks"] == 1
        assert m["presence_ratio"] == pytest.approx(2 / 3)

    def test_failed_latest_week_keeps_last_known_metrics(self, conn):
        w = mondays("2026-03-02", 3)
        h = History(conn)
        h.ok(w[0], {"a": 3})
        h.ok(w[1], {"a": 2})
        h.failed(w[2])
        _compute(conn, w[2])

        assert h.metrics("a", as_of=w[2]) is None
        m = h.metrics("a", as_of=w[1])
        assert m is not None
        assert m["current_streak_weeks"] == 2
        assert m["last_rank"] == 2

    def test_unknown_weeks_includes_failed_weeks(self, conn):
        w = mondays("2026-03-02", 4)
        h = History(conn)
        h.ok(w[0], {"a": 1})
        h.failed(w[1])
        h.ok(w[3], {"a": 1})
        assert compute.unknown_weeks(conn, h.chart_id, w[3]) == [w[1], w[2]]


def test_two_snapshots_in_one_week_bucket_count_once(conn):
    # Early history has an off-cycle Friday run (2026-01-02) inside the
    # 2025-12-29 bucket.
    h = History(conn)
    h.ok("2025-12-29", {"a": 5})
    h.ok("2026-01-02", {"a": 4})
    h.ok("2026-01-05", {"a": 3})
    _compute(conn, "2026-01-05")

    m = h.metrics("a")
    assert m["weeks_on_chart"] == 2
    assert m["current_streak_weeks"] == 2
    assert m["prev_rank"] == 4


class TestReport:
    def test_weeks_observed_counts_ok_weeks_only(self, conn):
        w = mondays("2026-03-02", 3)
        h = History(conn)
        h.ok(w[0], {"a": 1})
        h.failed(w[1])
        h.ok(w[2], {"a": 1})
        assert report._fetch_weeks_observed(conn, h.chart_id, w[2]) == 2

    def test_chart_whose_latest_week_failed_stays_in_report(self, conn):
        w = mondays("2026-03-02", 2)
        good = History(conn, "good-chart")
        flaky = History(conn, "flaky-chart")
        good.ok(w[0], {"a": 1})
        good.ok(w[1], {"a": 1})
        flaky.ok(w[0], {"b": 1})
        flaky.failed(w[1])
        _compute(conn, w[1])

        targets = report._resolve_target_weeks(
            conn, date.fromisoformat(w[1]), ["good-chart", "flaky-chart"]
        )
        assert ("flaky-chart", "flaky-chart", w[0]) in targets
        assert ("good-chart", "good-chart", w[1]) in targets

    def test_untracked_chart_with_old_metrics_is_left_out(self, conn):
        w = mondays("2026-03-02", 2)
        tracked = History(conn, "tracked-chart")
        retired = History(conn, "retired-chart")
        retired.ok(w[0], {"r": 1})
        tracked.ok(w[0], {"t": 1})
        tracked.ok(w[1], {"t": 1})
        _compute(conn, w[1])

        targets = report._resolve_target_weeks(conn, date.fromisoformat(w[1]), ["tracked-chart"])
        assert targets == [("tracked-chart", "tracked-chart", w[1])]


def test_status_lists_unknown_weeks(conn, monkeypatch, capsys):
    from bpc import status

    h = History(conn)
    h.ok("2026-07-06", {"a": 1})
    h.ok("2026-09-07", {"a": 1})
    h.failed("2026-09-14")
    monkeypatch.setattr(
        status,
        "TRACKED_CHARTS",
        [{"id": h.chart_id, "name": "Test", "url": "https://example.invalid"}],
    )

    status.run_status(conn)

    out = capsys.readouterr().out
    assert "unknown:   9 weeks (2026-07-13, 2026-07-20" in out
    assert "2026-09-14)" in out
