"""Ingestion orchestration for Beatport charts."""
from __future__ import annotations

from datetime import datetime, date
from typing import Iterable, Mapping

from .db import insert_entry, upsert_chart, upsert_snapshot, upsert_track
from .fetch import fetch_chart_html_with_retry, parse_chart
from .logging_utils import get_logger

LOG = get_logger(__name__)


def run_ingestion(conn, tracked_charts: Iterable[Mapping[str, str]], snapshot_date: date) -> None:
    """Ingest all tracked charts for a given snapshot date.

    Each chart is handled in its own transaction; failures roll back per chart.
    """

    snap_str = snapshot_date.isoformat()

    for chart in tracked_charts:
        chart_id = chart["id"]
        url = chart["url"]
        try:
            LOG.info("Fetching chart %s (%s) for snapshot %s", chart_id, url, snap_str)
            html = fetch_chart_html_with_retry(url)

            LOG.info("Fetched chart %s", chart_id)
            entries = parse_chart(html)
            LOG.info("Parsed %d entries for chart %s", len(entries), chart_id)
            if len(entries) < 100:
                LOG.warning("Parsed %d entries for chart %s (expected up to 100)", len(entries), chart_id)

            conn.execute("BEGIN")

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
                fetched_at=datetime.utcnow().isoformat(timespec="seconds"),
            )

            conn.execute("DELETE FROM chart_entries WHERE snapshot_id = ?", (snapshot_id,))

            for entry in entries:
                upsert_track(
                    conn,
                    {
                        "id": entry["track_id"],
                        "title": entry["title"],
                        "url": entry["url"],
                    },
                )
                insert_entry(conn, snapshot_id, entry["track_id"], int(entry["rank"]))

            conn.commit()
            LOG.info("Wrote snapshot %s with %d entries", snapshot_id, len(entries))
        except Exception:
            try:
                conn.rollback()
            except Exception:
                LOG.exception("Rollback failed for chart %s", chart_id)
            LOG.exception("Failed to ingest chart %s; rolled back transaction", chart_id)
            continue
