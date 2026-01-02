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
        """
    )
    conn.commit()


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

    Expected keys: id, title, url.
    """

    conn.execute(
        """
        INSERT INTO tracks (id, title, url)
        VALUES (:id, :title, :url)
        ON CONFLICT(id) DO UPDATE SET
            title = excluded.title,
            url = excluded.url
        """,
        track,
    )


def _build_snapshot_id(chart_id: str, snapshot_date: str) -> str:
    return f"{chart_id}:{snapshot_date}"


def upsert_snapshot(
    conn: sqlite3.Connection,
    chart_id: str,
    snapshot_date: str,
    source_url: Optional[str] = None,
    fetched_at: Optional[str] = None,
) -> str:
    """Insert or update a snapshot row; return snapshot_id.

    Caller should pass ISO-8601 strings for snapshot_date and fetched_at.
    """

    snapshot_id = _build_snapshot_id(chart_id, snapshot_date)
    payload = {
        "id": snapshot_id,
        "chart_id": chart_id,
        "snapshot_date": snapshot_date,
        "fetched_at": fetched_at or snapshot_date,
        "source_url": source_url,
    }
    conn.execute(
        """
        INSERT INTO chart_snapshots (id, chart_id, snapshot_date, fetched_at, source_url)
        VALUES (:id, :chart_id, :snapshot_date, :fetched_at, :source_url)
        ON CONFLICT(chart_id, snapshot_date) DO UPDATE SET
            fetched_at = excluded.fetched_at,
            source_url = excluded.source_url
        """,
        payload,
    )
    return snapshot_id


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
