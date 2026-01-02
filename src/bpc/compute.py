"""Durability metric computation per chart."""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Sequence, Tuple

from .logging_utils import get_logger

LOG = get_logger(__name__)


@dataclass
class TrackWeek:
    idx: int
    week: str
    rank: int


def _pop_stddev(values: Sequence[float]) -> float:
    n = len(values)
    if n <= 1:
        return 0.0
    mean = sum(values) / n
    return math.sqrt(sum((v - mean) ** 2 for v in values) / n)


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _resolve_as_of_week(conn, provided: date | None) -> str | None:
    if provided:
        return provided.isoformat()
    row = conn.execute("SELECT MAX(snapshot_date) FROM chart_snapshots").fetchone()
    return row[0] if row and row[0] else None


def _fetch_chart_weeks(conn, chart_id: str, as_of_week: str) -> List[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT snapshot_date
        FROM chart_snapshots
        WHERE chart_id = ? AND snapshot_date <= ?
        ORDER BY snapshot_date
        """,
        (chart_id, as_of_week),
    ).fetchall()
    return [r[0] for r in rows]


def _fetch_chart_entries(conn, chart_id: str, as_of_week: str) -> List[Tuple[str, str, int]]:
    rows = conn.execute(
        """
        SELECT cs.snapshot_date, ce.track_id, ce.rank
        FROM chart_entries ce
        JOIN chart_snapshots cs ON cs.id = ce.snapshot_id
        WHERE cs.chart_id = ? AND cs.snapshot_date <= ?
        ORDER BY cs.snapshot_date
        """,
        (chart_id, as_of_week),
    ).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def _compute_segments(idxs: List[int]) -> Tuple[int, int, int]:
    """Return (segments_count, max_streak_weeks, last_streak_weeks)."""
    if not idxs:
        return 0, 0, 0

    segments = 1
    current = 1
    max_streak = 1
    last_streak = 1

    for prev, curr in zip(idxs, idxs[1:]):
        if curr == prev + 1:
            current += 1
        else:
            segments += 1
            current = 1
        max_streak = max(max_streak, current)
        last_streak = current

    return segments, max_streak, last_streak


def _compute_motion_window(entries: List[TrackWeek], as_of_week: str) -> Tuple[int | None, int | None, int | None, float | None, float | None]:
    if not entries:
        return None, None, None, None, None
    if entries[-1].week != as_of_week:
        return None, None, None, None, None

    last_rank = entries[-1].rank
    prev_rank = entries[-2].rank if len(entries) >= 2 else None
    wow_delta = prev_rank - last_rank if prev_rank is not None else None

    window = entries[-4:]
    ranks = [e.rank for e in window]
    momentum = None
    volatility = None

    if len(ranks) >= 2:
        momentum = (ranks[0] - ranks[-1]) / (len(ranks) - 1)
    if len(ranks) >= 3:
        deltas = [window[i - 1].rank - window[i].rank for i in range(1, len(window))]
        volatility = _pop_stddev(deltas) if len(deltas) >= 2 else 0.0

    return last_rank, prev_rank, wow_delta, momentum, volatility


def _build_metric_row(
    track_id: str,
    entries: List[TrackWeek],
    as_of_week: str,
    as_of_idx: int,
    max_weeks_observed: int,
) -> Dict[str, object]:
    entries_sorted = sorted(entries, key=lambda e: e.idx)
    idxs = [e.idx for e in entries_sorted]
    weeks_on_chart = len(entries_sorted)
    first_seen_week = entries_sorted[0].week
    last_seen_week = entries_sorted[-1].week

    first_idx = idxs[0]
    age_weeks = (as_of_idx - first_idx) + 1
    presence_ratio = weeks_on_chart / age_weeks if age_weeks > 0 else 0.0

    segments_count, max_streak_weeks, last_streak_weeks = _compute_segments(idxs)
    reentry_count = max(segments_count - 1, 0)
    current_streak_weeks = last_streak_weeks if last_seen_week == as_of_week else 0

    ranks = [e.rank for e in entries_sorted]
    best_rank = min(ranks)
    best_rank_week = next(e.week for e in entries_sorted if e.rank == best_rank)
    avg_rank = sum(ranks) / weeks_on_chart
    rank_stddev = _pop_stddev([float(r) for r in ranks])

    top10_weeks = sum(1 for r in ranks if r <= 10)
    top25_weeks = sum(1 for r in ranks if r <= 25)

    last_rank, prev_rank, wow_delta, momentum_4w, volatility_4w = _compute_motion_window(entries_sorted, as_of_week)

    rank_quality_norm = _clamp01(1 - ((avg_rank - 1) / 99))
    longevity_norm = math.log1p(weeks_on_chart) / math.log1p(max_weeks_observed) if max_weeks_observed > 0 else 0.0
    streak_norm = max_streak_weeks / max_weeks_observed if max_weeks_observed > 0 else 0.0
    presence_norm = presence_ratio
    churn_norm = 1 / (1 + reentry_count)

    durability_score = (
        0.35 * rank_quality_norm
        + 0.25 * longevity_norm
        + 0.20 * streak_norm
        + 0.10 * presence_norm
        + 0.10 * churn_norm
    )

    return {
        "chart_id": None,  # filled by caller
        "track_id": track_id,
        "as_of_week": as_of_week,
        "weeks_on_chart": weeks_on_chart,
        "first_seen_week": first_seen_week,
        "last_seen_week": last_seen_week,
        "age_weeks": age_weeks,
        "presence_ratio": presence_ratio,
        "current_streak_weeks": current_streak_weeks,
        "max_streak_weeks": max_streak_weeks,
        "reentry_count": reentry_count,
        "segments_count": segments_count,
        "best_rank": best_rank,
        "best_rank_week": best_rank_week,
        "avg_rank": avg_rank,
        "rank_stddev": rank_stddev,
        "top10_weeks": top10_weeks,
        "top25_weeks": top25_weeks,
        "last_rank": last_rank,
        "prev_rank": prev_rank,
        "wow_delta": wow_delta,
        "momentum_4w": momentum_4w,
        "volatility_4w": volatility_4w,
        "durability_score": durability_score,
    }


def run_compute(conn, as_of_week: date | None = None) -> None:
    target_week = _resolve_as_of_week(conn, as_of_week)
    if not target_week:
        LOG.warning("No snapshots found; skipping compute")
        return

    chart_rows = conn.execute(
        """
        SELECT DISTINCT chart_id
        FROM chart_snapshots
        WHERE snapshot_date <= ?
        ORDER BY chart_id
        """,
        (target_week,),
    ).fetchall()

    for (chart_id,) in chart_rows:
        try:
            weeks = _fetch_chart_weeks(conn, chart_id, target_week)
            if not weeks:
                LOG.info("No weeks for chart %s up to %s; skipping", chart_id, target_week)
                continue

            chart_as_of_week = weeks[-1]
            week_index = {w: i for i, w in enumerate(weeks)}
            max_weeks_observed = len(weeks)
            as_of_idx = week_index[chart_as_of_week]

            raw_entries = _fetch_chart_entries(conn, chart_id, chart_as_of_week)
            tracks: Dict[str, List[TrackWeek]] = {}
            for week, track_id, rank in raw_entries:
                if week not in week_index:
                    continue
                tracks.setdefault(track_id, []).append(
                    TrackWeek(idx=week_index[week], week=week, rank=int(rank))
                )

            if not tracks:
                LOG.info("No entries for chart %s; skipping", chart_id)
                continue

            conn.execute("BEGIN")
            conn.execute(
                "DELETE FROM durability_metrics WHERE chart_id = ? AND as_of_week = ?",
                (chart_id, chart_as_of_week),
            )

            rows_to_insert: List[Dict[str, object]] = []
            for track_id, tws in tracks.items():
                row = _build_metric_row(
                    track_id=track_id,
                    entries=tws,
                    as_of_week=chart_as_of_week,
                    as_of_idx=as_of_idx,
                    max_weeks_observed=max_weeks_observed,
                )
                row["chart_id"] = chart_id
                rows_to_insert.append(row)

            insert_sql = """
                INSERT INTO durability_metrics (
                    chart_id, track_id, as_of_week,
                    weeks_on_chart, first_seen_week, last_seen_week, age_weeks, presence_ratio,
                    current_streak_weeks, max_streak_weeks, reentry_count, segments_count,
                    best_rank, best_rank_week, avg_rank, rank_stddev,
                    top10_weeks, top25_weeks,
                    last_rank, prev_rank, wow_delta,
                    momentum_4w, volatility_4w,
                    durability_score
                ) VALUES (
                    :chart_id, :track_id, :as_of_week,
                    :weeks_on_chart, :first_seen_week, :last_seen_week, :age_weeks, :presence_ratio,
                    :current_streak_weeks, :max_streak_weeks, :reentry_count, :segments_count,
                    :best_rank, :best_rank_week, :avg_rank, :rank_stddev,
                    :top10_weeks, :top25_weeks,
                    :last_rank, :prev_rank, :wow_delta,
                    :momentum_4w, :volatility_4w,
                    :durability_score
                )
            """
            conn.executemany(insert_sql, rows_to_insert)
            conn.commit()
            LOG.info(
                "Computed durability for chart %s as of %s (%d tracks)",
                chart_id,
                chart_as_of_week,
                len(rows_to_insert),
            )
        except Exception:
            try:
                conn.rollback()
            except Exception:
                LOG.exception("Rollback failed for chart %s", chart_id)
            LOG.exception("Failed to compute durability for chart %s", chart_id)
            continue
