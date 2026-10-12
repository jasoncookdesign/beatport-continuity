"""Every fetch attempt leaves a chart_snapshots row, so a failure is recorded
rather than looking like a week that never happened."""
from __future__ import annotations

from datetime import date

import pytest
import requests

from bpc import ingest

from .conftest import History

CHART = {
    "id": "test-chart",
    "chart_type": "top100",
    "genre_slug": "test",
    "name": "Test chart",
    "url": "https://example.invalid/top-100",
}
WEEK = "2026-03-02"


def _raise_http(url):
    raise requests.HTTPError("403 Client Error: Forbidden for url: " + url)


def _snapshot(conn):
    return conn.execute(
        "SELECT status, error, source_url FROM chart_snapshots WHERE chart_id = ? AND snapshot_date = ?",
        (CHART["id"], WEEK),
    ).fetchone()


def test_network_failure_is_recorded_as_failed_snapshot(conn, monkeypatch):
    monkeypatch.setattr(ingest, "fetch_chart_html_with_retry", _raise_http)

    ingest.run_ingestion(conn, [CHART], date.fromisoformat(WEEK))

    row = _snapshot(conn)
    assert row is not None
    assert row["status"] == "failed"
    assert "403" in row["error"]
    assert row["source_url"] == CHART["url"]


def test_failure_does_not_downgrade_an_ok_snapshot_for_the_same_week(conn, monkeypatch):
    History(conn, CHART["id"]).ok(WEEK, {"a": 1, "b": 2})
    monkeypatch.setattr(ingest, "fetch_chart_html_with_retry", _raise_http)

    ingest.run_ingestion(conn, [CHART], date.fromisoformat(WEEK))

    assert _snapshot(conn)["status"] == "ok"
    entries = conn.execute(
        "SELECT COUNT(*) FROM chart_entries WHERE snapshot_id = ?", (f"{CHART['id']}:{WEEK}",)
    ).fetchone()[0]
    assert entries == 2


def test_unexpected_error_in_one_chart_does_not_stop_the_run(conn, monkeypatch):
    other = {**CHART, "id": "other-chart", "url": "https://example.invalid/other"}

    def disk_full(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(ingest, "fetch_chart_html_with_retry", lambda url: "<html></html>")
    monkeypatch.setattr(ingest, "_save_debug_html", disk_full)

    ingest.run_ingestion(conn, [CHART, other], date.fromisoformat(WEEK))

    rows = dict(
        conn.execute(
            "SELECT chart_id, status FROM chart_snapshots WHERE snapshot_date = ?", (WEEK,)
        ).fetchall()
    )
    assert rows == {"test-chart": "failed", "other-chart": "failed"}
