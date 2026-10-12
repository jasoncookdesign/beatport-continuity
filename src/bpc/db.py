"""SQLite helpers and schema definitions for Beatport continuity."""
from __future__ import annotations

import sqlite3
from typing import Mapping, Optional


def get_conn(db_path: str) -> sqlite3.Connection:
    """Return a SQLite connection with sensible defaults."""

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables and indexes if they do not already exist."""

    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS charts (
            id TEXT PRIMARY KEY,
            chart_type TEXT NOT NULL,
            genre_slug TEXT NOT NULL,
            name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chart_snapshots (
            id TEXT PRIMARY KEY,
            chart_id TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            source_url TEXT,
            UNIQUE(chart_id, snapshot_date),
            FOREIGN KEY(chart_id) REFERENCES charts(id)
        );

        CREATE TABLE IF NOT EXISTS tracks (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            url TEXT
        );

        CREATE TABLE IF NOT EXISTS chart_entries (
            snapshot_id TEXT NOT NULL,
            track_id TEXT NOT NULL,
            rank INTEGER NOT NULL,
            PRIMARY KEY(snapshot_id, track_id),
            UNIQUE(snapshot_id, rank),
            FOREIGN KEY(snapshot_id) REFERENCES chart_snapshots(id),
            FOREIGN KEY(track_id) REFERENCES tracks(id)
        );

        CREATE INDEX IF NOT EXISTS idx_chart_snapshots_chart_date
            ON chart_snapshots(chart_id, snapshot_date);

        CREATE INDEX IF NOT EXISTS idx_chart_entries_track
            ON chart_entries(track_id);

        CREATE TABLE IF NOT EXISTS durability_metrics (
            chart_id TEXT NOT NULL,
            track_id TEXT NOT NULL,
            as_of_week TEXT NOT NULL,

            weeks_on_chart INTEGER NOT NULL,
            first_seen_week TEXT NOT NULL,
            last_seen_week TEXT NOT NULL,
            age_weeks INTEGER NOT NULL,
            presence_ratio REAL NOT NULL,

            current_streak_weeks INTEGER NOT NULL,
            max_streak_weeks INTEGER NOT NULL,
            reentry_count INTEGER NOT NULL,
            segments_count INTEGER NOT NULL,

            best_rank INTEGER NOT NULL,
            best_rank_week TEXT NOT NULL,
            avg_rank REAL NOT NULL,
            rank_stddev REAL NOT NULL,

            top10_weeks INTEGER NOT NULL,
            top25_weeks INTEGER NOT NULL,

            last_rank INTEGER,
            prev_rank INTEGER,
            wow_delta INTEGER,

            momentum_4w REAL,
            volatility_4w REAL,

            durability_score REAL NOT NULL,

            PRIMARY KEY (chart_id, track_id, as_of_week),
            FOREIGN KEY(chart_id) REFERENCES charts(id),
            FOREIGN KEY(track_id) REFERENCES tracks(id)
        );

        CREATE INDEX IF NOT EXISTS idx_durability_chart_week
            ON durability_metrics(chart_id, as_of_week);

        CREATE INDEX IF NOT EXISTS idx_durability_chart_week_score
            ON durability_metrics(chart_id, as_of_week, durability_score DESC);
        """
    )
    conn.commit()

    # Lightweight migrations for tracks extra columns
    _ensure_columns(
        conn,
        "tracks",
        {
            "artists": "TEXT",
            "remixers": "TEXT",
            "mix_name": "TEXT",
        },
    )
    
    # Lightweight migrations for chart_snapshots failure tracking
    _ensure_columns(
        conn,
        "chart_snapshots",
        {
            "status": "TEXT NOT NULL DEFAULT 'ok'",
            "error": "TEXT",
            "html_bytes": "INTEGER",
        },
    )


def upsert_chart(conn: sqlite3.Connection, chart: Mapping[str, str]) -> None:
    """Insert or update a chart row.

    Expected keys: id, chart_type, genre_slug, name.
    """

    conn.execute(
        """
        INSERT INTO charts (id, chart_type, genre_slug, name)
        VALUES (:id, :chart_type, :genre_slug, :name)
        ON CONFLICT(id) DO UPDATE SET
            chart_type = excluded.chart_type,
            genre_slug = excluded.genre_slug,
            name = excluded.name
        """,
        chart,
    )


def upsert_track(conn: sqlite3.Connection, track: Mapping[str, Optional[str]]) -> None:
    """Insert or update a track row.

    Expected keys: id, title, url, artists (optional), remixers (optional), mix_name (optional).
    """

    conn.execute(
        """
        INSERT INTO tracks (id, title, url, artists, remixers, mix_name)
        VALUES (:id, :title, :url, :artists, :remixers, :mix_name)
        ON CONFLICT(id) DO UPDATE SET
            title = excluded.title,
            url = excluded.url,
            artists = excluded.artists,
            remixers = excluded.remixers,
            mix_name = excluded.mix_name
        """,
        track,
    )


def _build_snapshot_id(chart_id: str, snapshot_date: str) -> str:
    return f"{chart_id}:{snapshot_date}"


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: Mapping[str, str]) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    for col, ddl_type in columns.items():
        if col in existing:
            continue
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl_type}")
    conn.commit()


def upsert_snapshot(
    conn: sqlite3.Connection,
    chart_id: str,
    snapshot_date: str,
    source_url: Optional[str] = None,
    fetched_at: Optional[str] = None,
    status: str = "ok",
    error: Optional[str] = None,
    html_bytes: Optional[int] = None,
) -> str:
    """Insert or update a snapshot row; return snapshot_id.

    Caller should pass ISO-8601 strings for snapshot_date and fetched_at.
    status should be 'ok' or 'failed'.
    """

    snapshot_id = _build_snapshot_id(chart_id, snapshot_date)
    payload = {
        "id": snapshot_id,
        "chart_id": chart_id,
        "snapshot_date": snapshot_date,
        "fetched_at": fetched_at or snapshot_date,
        "source_url": source_url,
        "status": status,
        "error": error,
        "html_bytes": html_bytes,
    }
    conn.execute(
        """
        INSERT INTO chart_snapshots (id, chart_id, snapshot_date, fetched_at, source_url, status, error, html_bytes)
        VALUES (:id, :chart_id, :snapshot_date, :fetched_at, :source_url, :status, :error, :html_bytes)
        ON CONFLICT(chart_id, snapshot_date) DO UPDATE SET
            fetched_at = excluded.fetched_at,
            source_url = excluded.source_url,
            status = excluded.status,
            error = excluded.error,
            html_bytes = excluded.html_bytes
        """,
        payload,
    )
    return snapshot_id


def record_failed_snapshot(
    conn: sqlite3.Connection,
    chart_id: str,
    snapshot_date: str,
    source_url: Optional[str],
    fetched_at: str,
    error: str,
    html_bytes: Optional[int] = None,
) -> bool:
    """Record a failed fetch/parse for chart_id in the week snapshot_date.

    A failure never downgrades a snapshot that already succeeded for the same
    week (for example when a manual re-run fails after the scheduled run
    worked): the existing 'ok' row and its entries are left untouched.
    Returns True if a row was written, False if an 'ok' row was kept.
    """

    cursor = conn.execute(
        """
        INSERT INTO chart_snapshots (id, chart_id, snapshot_date, fetched_at, source_url, status, error, html_bytes)
        VALUES (?, ?, ?, ?, ?, 'failed', ?, ?)
        ON CONFLICT(chart_id, snapshot_date) DO UPDATE SET
            fetched_at = excluded.fetched_at,
            source_url = excluded.source_url,
            status = 'failed',
            error = excluded.error,
            html_bytes = excluded.html_bytes
        WHERE chart_snapshots.status != 'ok'
        """,
        (
            _build_snapshot_id(chart_id, snapshot_date),
            chart_id,
            snapshot_date,
            fetched_at,
            source_url,
            error,
            html_bytes,
        ),
    )
    return cursor.rowcount > 0


def insert_entry(
    conn: sqlite3.Connection,
    snapshot_id: str,
    track_id: str,
    rank: int,
) -> None:
    """Insert a chart entry for a snapshot with basic validation."""

    if rank <= 0:
        raise ValueError("rank must be > 0")

    conn.execute(
        """
        INSERT INTO chart_entries (snapshot_id, track_id, rank)
        VALUES (?, ?, ?)
        ON CONFLICT(snapshot_id, track_id) DO UPDATE SET
            rank = excluded.rank
        """,
        (snapshot_id, track_id, rank),
    )


# ---------------------------------------------------------------------------
# Database maintenance helpers
# ---------------------------------------------------------------------------


def prune_chart_entries(conn: sqlite3.Connection, keep_weeks: int = 52) -> tuple[int, int]:
    """Delete chart_entries and chart_snapshots older than keep_weeks.

    The cutoff date is computed as keep_weeks * 7 days before the most recent
    snapshot_date in chart_snapshots.  Snapshots at or after the cutoff are kept.

    chart_entries must be deleted before chart_snapshots because of the FK constraint
    (chart_entries.snapshot_id -> chart_snapshots.id).

    RESIDUAL LIMITATION: after entries older than keep_weeks are pruned, subsequent
    compute runs will see a shorter history for any track that first appeared before
    the retention window.  Metrics such as first_seen_week, weeks_on_chart, and
    age_weeks will reflect only the retained window, because durability_metrics is
    recomputed from the retained entries every run.  This trade-off is intentional:
    it keeps the committed database bounded.  Pruned weeks are not lost, since every
    earlier commit of data/beatport.sqlite still contains them.

    Returns (entries_deleted, snapshots_deleted).
    """
    row = conn.execute("SELECT MAX(snapshot_date) FROM chart_snapshots").fetchone()
    if not row or not row[0]:
        return 0, 0

    # The oldest snapshot to KEEP is (keep_weeks - 1) weeks before the most recent.
    # Using (keep_weeks - 1) * 7 days as the offset makes weekly bucket 0 through
    # bucket -(keep_weeks-1) inclusive the retained window, which is exactly
    # keep_weeks snapshots when snapshots land on weekly boundaries.
    cutoff_row = conn.execute(
        "SELECT date(?, ? || ' days')",
        (row[0], -((keep_weeks - 1) * 7)),
    ).fetchone()
    cutoff = cutoff_row[0]

    # Delete entries first to satisfy the FK constraint.
    entries_cursor = conn.execute(
        """
        DELETE FROM chart_entries
        WHERE snapshot_id IN (
            SELECT id FROM chart_snapshots WHERE snapshot_date < ?
        )
        """,
        (cutoff,),
    )
    entries_deleted = entries_cursor.rowcount

    snaps_cursor = conn.execute(
        "DELETE FROM chart_snapshots WHERE snapshot_date < ?",
        (cutoff,),
    )
    snaps_deleted = snaps_cursor.rowcount

    conn.commit()
    return entries_deleted, snaps_deleted


def prune_orphan_tracks(conn: sqlite3.Connection) -> int:
    """Delete tracks that no chart entry or durability row references.

    Run after prune_chart_entries: once a track's last retained entry ages out,
    its metadata has nothing left to describe. Returns the number deleted.
    """
    cursor = conn.execute(
        """
        DELETE FROM tracks
        WHERE id NOT IN (SELECT track_id FROM chart_entries)
          AND id NOT IN (SELECT track_id FROM durability_metrics)
        """
    )
    deleted = cursor.rowcount
    conn.commit()
    return deleted


def vacuum_db(conn: sqlite3.Connection) -> None:
    """Run VACUUM to reclaim freed pages and defragment the database file.

    VACUUM cannot run inside an open transaction.  Call this only after all
    pending transactions have been committed.  In Python's sqlite3, calling
    conn.commit() before this function ensures no implicit transaction is open.
    The isolation_level is briefly set to None (autocommit) to prevent Python's
    implicit transaction management from wrapping VACUUM in a BEGIN statement.
    """
    old_isolation = conn.isolation_level
    conn.isolation_level = None  # autocommit: prevents implicit BEGIN before VACUUM
    try:
        conn.execute("VACUUM")
    finally:
        conn.isolation_level = old_isolation
