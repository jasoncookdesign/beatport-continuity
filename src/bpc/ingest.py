"""Ingestion orchestration for Beatport charts."""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Optional

from .db import insert_entry, record_failed_snapshot, upsert_chart, upsert_snapshot, upsert_track
from .fetch import fetch_chart_html_with_retry, parse_chart
from .logging_utils import get_logger
from .config import load_paths
from .time_utils import week_bucket

LOG = get_logger(__name__)


def _save_debug_html(chart_id: str, snapshot_date: str, html: str) -> Path:
    """Save HTML to debug directory for troubleshooting."""
    debug_dir = load_paths().data / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    debug_path = debug_dir / f"{chart_id}_{snapshot_date}.html"
    # Limit HTML size to avoid huge files
    debug_path.write_text(html[:200_000], encoding="utf-8", errors="ignore")
    return debug_path


def _now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


def _chart_row(chart: Mapping[str, str]) -> dict:
    return {
        "id": chart["id"],
        "chart_type": chart["chart_type"],
        "genre_slug": chart["genre_slug"],
        "name": chart["name"],
    }


def _record_failure(
    conn,
    chart: Mapping[str, str],
    snap_str: str,
    fetched_at: str,
    error_msg: str,
    html_bytes: Optional[int] = None,
) -> None:
    """Write a status='failed' snapshot row so the week reads as unknown, not absent."""
    chart_id = chart["id"]
    try:
        conn.execute("BEGIN")
        upsert_chart(conn, _chart_row(chart))
        written = record_failed_snapshot(
            conn,
            chart_id=chart_id,
            snapshot_date=snap_str,
            source_url=chart["url"],
            fetched_at=fetched_at,
            error=error_msg,
            html_bytes=html_bytes,
        )
        conn.commit()
        if written:
            LOG.info("Recorded failed snapshot for chart %s: %s", chart_id, error_msg)
        else:
            LOG.warning("Kept existing ok snapshot for chart %s; this attempt failed: %s", chart_id, error_msg)
    except Exception as db_exc:
        conn.rollback()
        LOG.exception("Failed to record failed snapshot for chart %s: %s", chart_id, db_exc)


def _ingest_single_chart(
    conn,
    chart: Mapping[str, str],
    snapshot_date: date,
    snap_str: str,
) -> None:
    """Ingest a single chart for a given snapshot date.

    Every attempt leaves a chart_snapshots row: 'ok' with entries, or 'failed'
    with the error. Failures are recorded and logged; they never raise.
    """
    chart_id = chart["id"]
    url = chart["url"]

    LOG.info("Fetching chart %s (%s) for snapshot %s", chart_id, url, snap_str)
    try:
        html = fetch_chart_html_with_retry(url)
    except Exception as fetch_exc:
        error_msg = f"Fetch failed: {type(fetch_exc).__name__}: {str(fetch_exc)[:200]}"
        LOG.error("Fetch failure for chart %s (%s) snapshot %s: %s", chart_id, url, snap_str, error_msg)
        _record_failure(conn, chart, snap_str, _now(), error_msg)
        return

    html_bytes = len(html.encode("utf-8", errors="ignore"))
    fetched_at = _now()
    LOG.info("Fetched chart %s (%d bytes)", chart_id, html_bytes)

    try:
        entries = parse_chart(html)
    except Exception as parse_exc:
        debug_path = _save_debug_html(chart_id, snap_str, html)
        error_msg = f"{type(parse_exc).__name__}: {str(parse_exc)[:200]}"
        LOG.error(
            "Parse failure for chart %s (%s) snapshot %s: %s; saved HTML to %s",
            chart_id,
            url,
            snap_str,
            error_msg,
            debug_path,
        )
        _record_failure(conn, chart, snap_str, fetched_at, error_msg, html_bytes)
        return

    if len(entries) == 0:
        debug_path = _save_debug_html(chart_id, snap_str, html)
        error_msg = "Empty results (no chart entries parsed)."
        LOG.warning(
            "Empty results for chart %s (%s) snapshot %s; saved HTML to %s",
            chart_id,
            url,
            snap_str,
            debug_path,
        )
        _record_failure(conn, chart, snap_str, fetched_at, error_msg, html_bytes)
        return

    LOG.info("Parsed %d entries for chart %s", len(entries), chart_id)
    if len(entries) < 100:
        LOG.warning("Parsed %d entries for chart %s (expected up to 100)", len(entries), chart_id)

    try:
        conn.execute("BEGIN")
        upsert_chart(conn, _chart_row(chart))
        snapshot_id = upsert_snapshot(
            conn,
            chart_id=chart_id,
            snapshot_date=snap_str,
            source_url=url,
            fetched_at=fetched_at,
            status="ok",
            error=None,
            html_bytes=html_bytes,
        )
        conn.execute("DELETE FROM chart_entries WHERE snapshot_id = ?", (snapshot_id,))
        for entry in entries:
            upsert_track(
                conn,
                {
                    "id": entry["track_id"],
                    "title": entry["title"],
                    "url": entry["url"],
                    "mix_name": entry.get("mix_name"),
                    "artists": ", ".join(entry.get("artists", [])) or None,
                    "remixers": ", ".join(entry.get("remixers", [])) or None,
                },
            )
            insert_entry(conn, snapshot_id, entry["track_id"], int(entry["rank"]))
        conn.commit()
        LOG.info("Wrote snapshot %s with %d entries", snapshot_id, len(entries))
    except Exception as db_exc:
        conn.rollback()
        LOG.exception("Database error writing snapshot for chart %s: %s", chart_id, db_exc)
        _record_failure(conn, chart, snap_str, fetched_at, f"Database error: {db_exc}"[:200], html_bytes)


def run_ingestion(conn, tracked_charts: Iterable[Mapping[str, str]], snapshot_date: date) -> None:
    """Ingest all tracked charts for a given snapshot date.

    Each chart is handled independently; failures are logged and recorded
    but do not stop processing of remaining charts.
    """
    if snapshot_date.weekday() != 0:
        LOG.warning("Non-Monday snapshot_date received; bucketing applied")
    snapshot_date = week_bucket(snapshot_date)
    snap_str = snapshot_date.isoformat()

    for chart in tracked_charts:
        try:
            _ingest_single_chart(conn, chart, snapshot_date, snap_str)
        except Exception as exc:
            # Last-resort net: an unexpected error (e.g. writing debug HTML)
            # must not stop the remaining charts or the run.
            LOG.exception("Unexpected error ingesting chart %s: %s", chart["id"], exc)
            _record_failure(conn, chart, snap_str, _now(), f"Unexpected error: {type(exc).__name__}: {str(exc)[:200]}")
