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

AGGREGATE_STEP = 0.15
AGGREGATE_MAX_BOOST = 0.60


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
            t.mix_name,
            t.artists,
            t.remixers,
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
                "mix_name": r[2],
                "artists": r[3] or "",
                "remixers": r[4] or "",
                "url": r[5],
                "durability_score": float(r[6]),
                "weeks_on_chart": int(r[7]),
                "current_streak_weeks": int(r[8]),
                "max_streak_weeks": int(r[9]),
                "reentry_count": int(r[10]),
                "best_rank": int(r[11]),
                "avg_rank": float(r[12]),
                "last_rank": r[13] if r[13] is not None else None,
                "wow_delta": r[14] if r[14] is not None else None,
                "rank_stddev": float(r[15]) if r[15] is not None else None,
                "age_weeks": int(r[16]) if r[16] is not None else None,
                "momentum_4w": float(r[17]) if r[17] is not None else None,
                "volatility_4w": float(r[18]) if r[18] is not None else None,
                "presence_ratio": float(r[19]) if r[19] is not None else None,
                "top10_weeks": int(r[20]),
                "top25_weeks": int(r[21]),
            }
        )

    for row in out:
        row["bucket"] = bucket_row(row)
    return out


def _fetch_cross_chart_rows(conn, chart_weeks: List[tuple]) -> List[Dict[str, object]]:
    """Fetch durability rows for the supplied (chart_id, chart_name, as_of_week) tuples."""

    if not chart_weeks:
        return []

    values_clause = ", ".join(["(?, ?)"] * len(chart_weeks))
    params: List[str] = []
    for chart_id, _chart_name, as_of_week in chart_weeks:
        params.extend([chart_id, as_of_week])

    query = f"""
        WITH target_weeks(chart_id, as_of_week) AS (
            VALUES {values_clause}
        )
        SELECT
            dm.chart_id,
            dm.track_id,
            dm.as_of_week,
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
            t.title,
            t.mix_name,
            t.artists,
            t.remixers,
            t.url,
            c.name AS chart_name,
            c.chart_type
        FROM durability_metrics dm
        JOIN target_weeks tw ON dm.chart_id = tw.chart_id AND dm.as_of_week = tw.as_of_week
        JOIN tracks t ON t.id = dm.track_id
        JOIN charts c ON c.id = dm.chart_id
    """

    rows = conn.execute(query, params).fetchall()

    out: List[Dict[str, object]] = []
    for r in rows:
        out.append(
            {
                "chart_id": r["chart_id"],
                "chart_type": r["chart_type"],
                "chart_name": r["chart_name"],
                "as_of_week": r["as_of_week"],
                "track_id": r["track_id"],
                "title": r["title"],
                "mix_name": r["mix_name"],
                "artists": r["artists"] or "",
                "remixers": r["remixers"] or "",
                "url": r["url"],
                "durability_score": float(r["durability_score"]),
                "weeks_on_chart": int(r["weeks_on_chart"]),
                "current_streak_weeks": int(r["current_streak_weeks"]),
                "max_streak_weeks": int(r["max_streak_weeks"]),
                "reentry_count": int(r["reentry_count"]),
                "best_rank": int(r["best_rank"]),
                "avg_rank": float(r["avg_rank"]),
                "last_rank": r["last_rank"] if r["last_rank"] is not None else None,
                "wow_delta": r["wow_delta"] if r["wow_delta"] is not None else None,
                "rank_stddev": float(r["rank_stddev"]) if r["rank_stddev"] is not None else None,
                "age_weeks": int(r["age_weeks"]) if r["age_weeks"] is not None else None,
                "momentum_4w": float(r["momentum_4w"]) if r["momentum_4w"] is not None else None,
                "volatility_4w": float(r["volatility_4w"]) if r["volatility_4w"] is not None else None,
            }
        )

    return out


def _aggregate_across_charts(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """Collapse per-chart durability rows into a cross-chart aggregate."""

    tracks: Dict[str, Dict[str, object]] = {}

    for row in rows:
        chart_entry = {
            "chart_id": row["chart_id"],
            "chart_name": row["chart_name"],
            "chart_type": row["chart_type"],
            "as_of_week": row["as_of_week"],
            "durability_score": row["durability_score"],
            "last_rank": row["last_rank"],
            "best_rank": row["best_rank"],
        }

        # Reuse the existing qualitative bucketer using per-chart stats.
        bucket_input = {
            "durability_score": row["durability_score"],
            "max_streak_weeks": row["max_streak_weeks"],
            "volatility_4w": row["volatility_4w"],
            "rank_stddev": row["rank_stddev"],
            "age_weeks": row["age_weeks"],
            "momentum_4w": row["momentum_4w"],
            "wow_delta": row["wow_delta"],
            "last_rank": row["last_rank"],
            "weeks_on_chart": row["weeks_on_chart"],
            "best_rank": row["best_rank"],
            "reentry_count": row["reentry_count"],
        }
        chart_entry["bucket"] = bucket_row(bucket_input)

        track = tracks.setdefault(
            row["track_id"],
            {
                "track_id": row["track_id"],
                "title": row["title"],
                "mix_name": row["mix_name"],
                "artists": row["artists"],
                "remixers": row["remixers"],
                "url": row["url"],
                "charts": [],
            },
        )
        track["charts"].append(chart_entry)

    for track in tracks.values():
        track["charts"].sort(key=lambda c: c["durability_score"], reverse=True)

        chart_count = len(track["charts"])
        durabilities = [c["durability_score"] for c in track["charts"]]
        top_chart_count = sum(1 for c in track["charts"] if c["chart_type"] == "top100")
        hype_chart_count = sum(1 for c in track["charts"] if c["chart_type"] == "hype")

        avg_durability = sum(durabilities) / chart_count if chart_count else 0.0
        max_durability = max(durabilities) if durabilities else 0.0

        multiplier = 1 + AGGREGATE_STEP * (chart_count - 1)
        multiplier = min(multiplier, 1 + AGGREGATE_MAX_BOOST)  # cap at +60% boost

        track["chart_count"] = chart_count
        track["top_chart_count"] = top_chart_count
        track["hype_chart_count"] = hype_chart_count
        track["avg_durability"] = avg_durability
        track["max_durability"] = max_durability
        track["aggregate_multiplier"] = multiplier
        track["aggregate_score"] = avg_durability * multiplier

        # Use the bucket from the highest-durability chart as the primary label.
        track["bucket"] = sorted(
            track["charts"], key=lambda c: c["durability_score"], reverse=True
        )[0]["bucket"]

    aggregate_tracks = [track for track in tracks.values() if track["chart_count"] >= 2]
    aggregate_tracks.sort(key=lambda t: t["aggregate_score"], reverse=True)
    return aggregate_tracks


def run_report(conn, snapshot_date: Optional[date] = None) -> str:
    paths = load_paths()
    paths.docs.mkdir(parents=True, exist_ok=True)
    charts_dir = paths.docs / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

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

    generated_at = datetime.utcnow().isoformat(timespec="seconds")
    env = Environment(
        loader=FileSystemLoader(str(paths.templates)),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    index_tmpl = env.get_template("index.html.j2")
    chart_tmpl = env.get_template("chart.html.j2")
    aggregate_tmpl = env.get_template("all.html.j2")

    charts_for_index: List[Dict[str, object]] = []
    for chart in charts:
        page_href = f"charts/{chart['chart_id']}.html"
        chart_for_index = {**chart, "top_rows": chart["rows"][:5], "page_href": page_href}
        charts_for_index.append(chart_for_index)

        rendered_chart = chart_tmpl.render(
            generated_at=generated_at,
            chart=chart,
        )
        chart_path = charts_dir / f"{chart['chart_id']}.html"
        chart_path.write_text(rendered_chart, encoding="utf-8")
        LOG.info("Chart page written to %s", chart_path)

    chart_targets = [(chart["chart_id"], chart["chart_name"], chart["as_of_week"]) for chart in charts]

    aggregate_rows = _fetch_cross_chart_rows(conn, chart_targets)
    aggregate_tracks = _aggregate_across_charts(aggregate_rows)
    unique_weeks = sorted({c[2] for c in chart_targets})

    rendered_aggregate = aggregate_tmpl.render(
        generated_at=generated_at,
        tracks=aggregate_tracks,
        chart_weeks=chart_targets,
        unique_weeks=unique_weeks,
        aggregate_formula={
            "base_step": AGGREGATE_STEP,
            "max_boost": AGGREGATE_MAX_BOOST,
        },
    )

    rendered_index = index_tmpl.render(
        generated_at=generated_at,
        charts=charts_for_index,
    )

    aggregate_path = paths.docs / "all.html"
    aggregate_path.write_text(rendered_aggregate, encoding="utf-8")
    LOG.info("Aggregate page written to %s", aggregate_path)

    output_path = paths.docs / "index.html"
    output_path.write_text(rendered_index, encoding="utf-8")
    LOG.info("Report written to %s", output_path)
    return str(output_path)
