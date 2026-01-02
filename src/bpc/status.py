"""Pipeline status reporter."""
from __future__ import annotations

from typing import Optional

from .config import TRACKED_CHARTS, load_paths
from .logging_utils import get_logger

LOG = get_logger(__name__)


def _latest_snapshot_info(conn, chart_id: str) -> tuple[Optional[str], Optional[int]]:
    row = conn.execute(
        "SELECT MAX(snapshot_date) FROM chart_snapshots WHERE chart_id = ?",
        (chart_id,),
    ).fetchone()
    latest_week = row[0] if row and row[0] else None
    if not latest_week:
        return None, None
    entry_row = conn.execute(
        """
        SELECT COUNT(*)
        FROM chart_entries ce
        JOIN chart_snapshots cs ON cs.id = ce.snapshot_id
        WHERE cs.chart_id = ? AND cs.snapshot_date = ?
        """,
        (chart_id, latest_week),
    ).fetchone()
    entry_count = entry_row[0] if entry_row else 0
    return latest_week, entry_count


def _latest_metrics_info(conn, chart_id: str) -> tuple[Optional[str], Optional[int]]:
    row = conn.execute(
        "SELECT MAX(as_of_week) FROM durability_metrics WHERE chart_id = ?",
        (chart_id,),
    ).fetchone()
    latest_week = row[0] if row and row[0] else None
    if not latest_week:
        return None, None
    count_row = conn.execute(
        "SELECT COUNT(*) FROM durability_metrics WHERE chart_id = ? AND as_of_week = ?",
        (chart_id, latest_week),
    ).fetchone()
    metrics_count = count_row[0] if count_row else 0
    return latest_week, metrics_count


def run_status(conn) -> None:
    paths = load_paths()
    db_path = paths.db
    report_path = paths.docs / "index.html"

    print("Beatport Continuity status\n")
    print(f"DB:      {db_path}")
    print(f"Report:  {report_path}")
    print("\nTracked charts:")
    for chart in TRACKED_CHARTS:
        print(f"- {chart['id']}: {chart['name']} ({chart['url']})")

    print("\nChart state:")
    for chart in TRACKED_CHARTS:
        chart_id = chart["id"]
        latest_snap, entry_count = _latest_snapshot_info(conn, chart_id)
        latest_metrics, metrics_count = _latest_metrics_info(conn, chart_id)

        snap_str = latest_snap if latest_snap else "MISSING"
        entry_str = entry_count if entry_count is not None else "-"
        metrics_week_str = latest_metrics if latest_metrics else "MISSING"
        metrics_count_str = metrics_count if metrics_count is not None else "-"

        print(f"* {chart_id}")
        print(f"  snapshots: {snap_str} (entries: {entry_str})")
        print(f"  metrics:   {metrics_week_str} (rows: {metrics_count_str})")
