"""HTML report rendering for Beatport Continuity."""
from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .config import load_paths
from .logging_utils import get_logger

LOG = get_logger(__name__)


def _resolve_target_weeks(conn, snapshot_date: Optional[date]):
    target_iso = snapshot_date.isoformat() if snapshot_date else None

    rows = conn.execute(
        """
        SELECT c.id, c.name, MAX(dm.as_of_week) as latest_week
        FROM charts c
        JOIN durability_metrics dm ON dm.chart_id = c.id
        GROUP BY c.id, c.name
        ORDER BY c.name
        """
    ).fetchall()

    chart_weeks = []
    for chart_id, chart_name, latest_week in rows:
        if target_iso:
            as_of_week = target_iso
            exists = conn.execute(
                "SELECT 1 FROM durability_metrics WHERE chart_id = ? AND as_of_week = ? LIMIT 1",
                (chart_id, as_of_week),
            ).fetchone()
            if not exists:
                LOG.warning(
                    "No durability metrics for chart %s at %s; skipping", chart_id, as_of_week
                )
                continue
        else:
            as_of_week = latest_week
        chart_weeks.append((chart_id, chart_name, as_of_week))

    return chart_weeks


def _fetch_weeks_observed(conn, chart_id: str, as_of_week: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT snapshot_date)
        FROM chart_snapshots
        WHERE chart_id = ? AND snapshot_date <= ?
        """,
        (chart_id, as_of_week),
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _fetch_rows_for_chart(conn, chart_id: str, as_of_week: str) -> List[Dict[str, object]]:
    rows = conn.execute(
        """
        SELECT
          dm.track_id,
          t.title,
          t.url,
          dm.durability_score,
          dm.weeks_on_chart,
          dm.current_streak_weeks,
          dm.max_streak_weeks,
          dm.reentry_count,
          dm.best_rank,
          dm.avg_rank,
          dm.last_rank,
          dm.wow_delta,
          dm.top10_weeks,
          dm.top25_weeks
        FROM durability_metrics dm
        JOIN tracks t ON t.id = dm.track_id
        WHERE dm.chart_id = ? AND dm.as_of_week = ?
        ORDER BY dm.durability_score DESC
        """,
        (chart_id, as_of_week),
    ).fetchall()

    out: List[Dict[str, object]] = []
    for r in rows:
        out.append(
            {
                "track_id": r[0],
                "title": r[1],
                "url": r[2],
                "durability_score": float(r[3]),
                "weeks_on_chart": int(r[4]),
                "current_streak_weeks": int(r[5]),
                "max_streak_weeks": int(r[6]),
                "reentry_count": int(r[7]),
                "best_rank": int(r[8]),
                "avg_rank": float(r[9]),
                "last_rank": r[10] if r[10] is not None else None,
                "wow_delta": r[11] if r[11] is not None else None,
                "top10_weeks": int(r[12]),
                "top25_weeks": int(r[13]),
            }
        )
    return out


def run_report(conn, snapshot_date: Optional[date] = None) -> str:
    paths = load_paths()
    paths.docs.mkdir(parents=True, exist_ok=True)

    chart_weeks = _resolve_target_weeks(conn, snapshot_date)
    if not chart_weeks:
        LOG.warning("No durability metrics available; skipping report")
        return str(paths.docs / "index.html")

    charts: List[Dict[str, object]] = []
    for chart_id, chart_name, as_of_week in chart_weeks:
        rows = _fetch_rows_for_chart(conn, chart_id, as_of_week)
        if not rows:
            continue
        charts.append(
            {
                "chart_id": chart_id,
                "chart_name": chart_name,
                "as_of_week": as_of_week,
                "weeks_observed_count": _fetch_weeks_observed(conn, chart_id, as_of_week),
                "rows": rows,
            }
        )

    env = Environment(
        loader=FileSystemLoader(str(paths.templates)),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    tmpl = env.get_template("report.html.j2")
    rendered = tmpl.render(
        generated_at=datetime.utcnow().isoformat(timespec="seconds"),
        charts=charts,
    )

    output_path = paths.docs / "index.html"
    output_path.write_text(rendered, encoding="utf-8")
    LOG.info("Report written to %s", output_path)
    return str(output_path)
