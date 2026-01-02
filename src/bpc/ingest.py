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


def run_ingestion(conn, tracked_charts: Iterable[Mapping[str, str]], snapshot_date: date) -> None:
    """Ingest all tracked charts for a given snapshot date.

    Each chart is handled in its own transaction; failures roll back per chart.
    """
    if snapshot_date.weekday() != 0:
        LOG.warning("Non-Monday snapshot_date received; bucketing applied")
    snapshot_date = week_bucket(snapshot_date)
    snap_str = snapshot_date.isoformat()

    for chart in tracked_charts:
        chart_id = chart["id"]
        url = chart["url"]
        try:
            LOG.info("Fetching chart %s (%s) for snapshot %s", chart_id, url, snap_str)
            html = fetch_chart_html_with_retry(url)

            LOG.info("Fetched chart %s", chart_id)
            try:
                entries = parse_chart(html)
            except Exception as parse_exc:
                debug_dir = load_paths().data / "debug"
                debug_dir.mkdir(parents=True, exist_ok=True)
                debug_path = debug_dir / f"{chart_id}_{snap_str}.html"
                debug_path.write_text(html[:200_000], encoding="utf-8", errors="ignore")
                LOG.error(
                    "Parse failure for chart %s (%s) snapshot %s; saved HTML to %s",
                    chart_id,
                    url,
                    snap_str,
                    debug_path,
                )
                raise parse_exc

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
                        "mix_name": entry.get("mix_name"),
                        "artists": ", ".join(entry.get("artists", [])) or None,
                        "remixers": ", ".join(entry.get("remixers", [])) or None,
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
