"""Ingestion orchestration for Beatport charts."""
from __future__ import annotations

from datetime import datetime, date
from pathlib import Path
from typing import Iterable, Mapping

from .db import insert_entry, upsert_chart, upsert_snapshot, upsert_track
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


def _ingest_single_chart(
    conn,
    chart: Mapping[str, str],
    snapshot_date: date,
    snap_str: str,
) -> None:
    """Ingest a single chart for a given snapshot date.
    
    Handles failures gracefully by recording them in the database
    and continuing execution instead of raising.
    """
    chart_id = chart["id"]
    url = chart["url"]
    
    try:
        LOG.info("Fetching chart %s (%s) for snapshot %s", chart_id, url, snap_str)
        html = fetch_chart_html_with_retry(url)
        html_bytes = len(html.encode("utf-8", errors="ignore"))
        fetched_at = datetime.utcnow().isoformat(timespec="seconds")
        
        LOG.info("Fetched chart %s (%d bytes)", chart_id, html_bytes)
        
        try:
            entries = parse_chart(html)
        except Exception as parse_exc:
            # Save debug HTML for later troubleshooting
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
            
            # Record failed snapshot in DB
            conn.execute("BEGIN")
            try:
                upsert_chart(
                    conn,
                    {
                        "id": chart_id,
                        "chart_type": chart["chart_type"],
                        "genre_slug": chart["genre_slug"],
                        "name": chart["name"],
                    },
                )
                upsert_snapshot(
                    conn,
                    chart_id=chart_id,
                    snapshot_date=snap_str,
                    source_url=url,
                    fetched_at=fetched_at,
                    status="failed",
                    error=error_msg,
                    html_bytes=html_bytes,
                )
                conn.commit()
                LOG.info("Recorded failed snapshot for chart %s", chart_id)
            except Exception as db_exc:
                conn.rollback()
                LOG.exception("Failed to record failed snapshot for chart %s: %s", chart_id, db_exc)
            
            return  # Continue to next chart

        # Handle empty results (valid but no data)
        if len(entries) == 0:
            # Save debug HTML
            debug_path = _save_debug_html(chart_id, snap_str, html)
            error_msg = "Empty results (count=0). Beatport may have disabled hype for this genre or served a landing page."
            
            LOG.warning(
                "Empty results for chart %s (%s) snapshot %s; saved HTML to %s",
                chart_id,
                url,
                snap_str,
                debug_path,
            )
            
            # Record as failed snapshot
            conn.execute("BEGIN")
            try:
                upsert_chart(
                    conn,
                    {
                        "id": chart_id,
                        "chart_type": chart["chart_type"],
                        "genre_slug": chart["genre_slug"],
                        "name": chart["name"],
                    },
                )
                upsert_snapshot(
                    conn,
                    chart_id=chart_id,
                    snapshot_date=snap_str,
                    source_url=url,
                    fetched_at=fetched_at,
                    status="failed",
                    error=error_msg,
                    html_bytes=html_bytes,
                )
                conn.commit()
                LOG.info("Recorded empty results as failed snapshot for chart %s", chart_id)
            except Exception as db_exc:
                conn.rollback()
                LOG.exception("Failed to record empty snapshot for chart %s: %s", chart_id, db_exc)
            
            return  # Continue to next chart

        LOG.info("Parsed %d entries for chart %s", len(entries), chart_id)
        if len(entries) < 100:
            LOG.warning("Parsed %d entries for chart %s (expected up to 100)", len(entries), chart_id)

        # Write successful snapshot with entries
        conn.execute("BEGIN")
        try:
            upsert_chart(
                conn,
                {
                    "id": chart_id,
                    "chart_type": chart["chart_type"],
                    "genre_slug": chart["genre_slug"],
                    "name": chart["name"],
                },
            )

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
            return  # Continue to next chart
            
    except Exception as exc:
        # Catch-all for unexpected errors (network failures, etc.)
        LOG.exception("Unexpected error ingesting chart %s: %s", chart_id, exc)
        # Don't try to write to DB here since we may not have html/fetched_at
        return  # Continue to next chart


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
        _ingest_single_chart(conn, chart, snapshot_date, snap_str)
