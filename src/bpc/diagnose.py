"""Diagnostic tool for fetching and parsing Beatport charts."""
from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Optional

from bs4 import BeautifulSoup

from .config import TRACKED_CHARTS, load_paths
from .fetch import fetch_chart_html_with_retry, parse_chart
from .logging_utils import get_logger

LOG = get_logger(__name__)


def _extract_next_data_json(html: str) -> Optional[str]:
    """Extract __NEXT_DATA__ script content if present."""
    soup = BeautifulSoup(html, "lxml")
    script = soup.find("script", id="__NEXT_DATA__")
    if script and script.string:
        return script.string
    return None


def run_diagnose(conn, snapshot_date: Optional[date] = None) -> int:
    """Fetch and parse all tracked charts, writing debug artifacts on failure.

    Returns 0 if all charts parse successfully (>= 50 entries), else 2.
    """
    snap_str = snapshot_date.isoformat() if snapshot_date else datetime.utcnow().date().isoformat()
    paths = load_paths()
    debug_base = paths.data / "debug"
    debug_base.mkdir(parents=True, exist_ok=True)

    all_ok = True
    results = []

    for chart in TRACKED_CHARTS:
        chart_id = chart["id"]
        url = chart["url"]

        try:
            LOG.info("Diagnosing chart %s (%s)", chart_id, url)
            html = fetch_chart_html_with_retry(url)
            html_len = len(html)
            has_next_data = "__NEXT_DATA__" in html

            parse_ok = False
            parsed_count = 0
            try:
                entries = parse_chart(html)
                parsed_count = len(entries)
                parse_ok = parsed_count >= 50
            except Exception as parse_exc:
                LOG.error("Parse error for chart %s: %s", chart_id, parse_exc)

            if not parse_ok:
                all_ok = False
                chart_debug_dir = debug_base / chart_id / snap_str
                chart_debug_dir.mkdir(parents=True, exist_ok=True)

                response_path = chart_debug_dir / "response.html"
                response_path.write_text(html[:200_000], encoding="utf-8", errors="ignore")

                if has_next_data:
                    next_data_json = _extract_next_data_json(html)
                    if next_data_json:
                        next_data_path = chart_debug_dir / "next_data.json"
                        next_data_path.write_text(
                            next_data_json[:500_000], encoding="utf-8", errors="ignore"
                        )

                LOG.error(
                    "Chart %s parse failure or low count: parsed=%d, debug saved to %s",
                    chart_id,
                    parsed_count,
                    chart_debug_dir,
                )
                results.append(
                    f"[FAIL] {chart_id} parsed={parsed_count} html={html_len} "
                    f"next_data={'yes' if has_next_data else 'no'} debug={chart_debug_dir}"
                )
            else:
                results.append(
                    f"[OK] {chart_id} parsed={parsed_count} html={html_len} "
                    f"next_data={'yes' if has_next_data else 'no'}"
                )

        except Exception:
            all_ok = False
            LOG.exception("Failed to fetch/diagnose chart %s", chart_id)
            results.append(f"[ERROR] {chart_id} fetch failed")
            continue

    print("\nDiagnostic Results:")
    for line in results:
        print(line)

    return 0 if all_ok else 2
