"""HTML report rendering for Beatport Continuity."""
from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .config import load_paths
from .logging_utils import get_logger

LOG = get_logger(__name__)

# Bucketing thresholds
DUR_HIGH = 0.70
STREAK_LONG = 4
AGE_SHORT = 3
AGE_LONG = 8
VOL_LOW = 1.0
VOL_HIGH = 4.0
STDDEV_HIGH = 20.0
PEAK_STRONG = 15
MOM_POS = 2.0
WOW_FALL = -3


def bucket_row(row: Dict[str, object]) -> str:
    """Derive a qualitative bucket for a durability row."""

    durability_score = float(row["durability_score"])
    max_streak_weeks = int(row["max_streak_weeks"])
    volatility_4w = row.get("volatility_4w")
    rank_stddev = row.get("rank_stddev")
    age_weeks = row.get("age_weeks")
    momentum_4w = row.get("momentum_4w")
    wow_delta = row.get("wow_delta")
    last_rank = row.get("last_rank")
    weeks_on_chart = int(row["weeks_on_chart"])
    best_rank = int(row["best_rank"])

    # Anchor
    if (
        durability_score >= DUR_HIGH
        and max_streak_weeks >= STREAK_LONG
        and (
            (volatility_4w is not None and volatility_4w <= VOL_LOW)
            or (rank_stddev is not None and rank_stddev <= 10)
        )
    ):
        return "Anchor"

    # Spike
    if (
        weeks_on_chart <= 2
        and best_rank <= PEAK_STRONG
        and (
            (volatility_4w is not None and volatility_4w >= VOL_HIGH)
            or (rank_stddev is not None and rank_stddev >= STDDEV_HIGH)
        )
    ):
        return "Spike"

    # Climber
    if (
        age_weeks is not None
        and age_weeks <= AGE_SHORT
        and (
            (momentum_4w is not None and momentum_4w >= MOM_POS)
            or (wow_delta is not None and wow_delta >= 3)
        )
        and last_rank is not None
    ):
        return "Climber"

    # Fader
    if (
        weeks_on_chart >= AGE_LONG
        and (
            (wow_delta is not None and wow_delta <= WOW_FALL)
            or (momentum_4w is not None and momentum_4w < 0)
        )
    ):
        return "Fader"

    return "—"


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
                        dm.rank_stddev,
                        dm.age_weeks,
                        dm.momentum_4w,
                        dm.volatility_4w,
                        dm.presence_ratio,
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
                "rank_stddev": float(r[12]) if r[12] is not None else None,
                "age_weeks": int(r[13]) if r[13] is not None else None,
                "momentum_4w": float(r[14]) if r[14] is not None else None,
                "volatility_4w": float(r[15]) if r[15] is not None else None,
                "presence_ratio": float(r[16]) if r[16] is not None else None,
                "top10_weeks": int(r[17]),
                "top25_weeks": int(r[18]),
            }
        )

    for row in out:
        row["bucket"] = bucket_row(row)
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
