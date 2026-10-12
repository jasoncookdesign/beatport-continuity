"""compute → report over real templates: a chart whose latest fetch failed
stays on the site at its last known week, and the cross-chart page only
combines charts from the same (latest) week."""
from __future__ import annotations

from dataclasses import replace
from datetime import date

from bpc import config, report
from bpc.compute import run_compute

from .conftest import History, mondays


def test_failed_latest_week_renders_as_of_last_known_week(conn, tmp_path, monkeypatch):
    w = mondays("2026-03-02", 2)
    live = History(conn, "live-chart")
    flaky = History(conn, "flaky-chart")
    live.ok(w[0], {"shared": 1, "solo": 2})
    live.ok(w[1], {"shared": 1, "solo": 2})
    flaky.ok(w[0], {"shared": 3})
    flaky.failed(w[1])
    third = History(conn, "third-chart")
    third.ok(w[0], {"solo": 5})
    third.ok(w[1], {"solo": 4})

    paths = replace(config.load_paths(), docs=tmp_path)
    monkeypatch.setattr(report, "load_paths", lambda: paths)
    monkeypatch.setattr(
        report,
        "TRACKED_CHARTS",
        [{"id": c} for c in ("live-chart", "flaky-chart", "third-chart")],
    )

    run_compute(conn, date.fromisoformat(w[1]))
    report.run_report(conn, date.fromisoformat(w[1]))

    assert f"As of week {w[0]}" in (tmp_path / "charts" / "flaky-chart.html").read_text()
    assert f"As of week {w[1]}" in (tmp_path / "charts" / "live-chart.html").read_text()
    index = (tmp_path / "index.html").read_text()
    assert f"As of week {w[0]}" in index and f"As of week {w[1]}" in index

    aggregate = (tmp_path / "all.html").read_text()
    assert f"Weeks covered: {w[1]} " in aggregate
    assert "2 charts" in aggregate
