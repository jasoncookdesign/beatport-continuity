"""HTTP fetching and HTML parsing for Beatport charts."""
from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.beatport.com"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
DEFAULT_TIMEOUT = 15
DEFAULT_LIMIT = 100

# Canonical track ID extractor from Beatport track URLs:
# e.g. /track/edge-of-the-night/12345678
TRACK_ID_RE = re.compile(r"/track/[^/]+/(\d+)")


def fetch_chart_html(url: str) -> str:
    """Fetch a chart page with browser-like headers."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
    }
    resp = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def fetch_chart_html_with_retry(
    url: str,
    attempts: int = 3,
    base_delay: float = 1.0,
) -> str:
    """Fetch HTML with exponential backoff on network/HTTP errors."""

    last_err: Optional[Exception] = None
    for n in range(1, attempts + 1):
        try:
            return fetch_chart_html(url)
        except requests.RequestException as exc:
            last_err = exc
            if n == attempts:
                break
            sleep_for = base_delay * (2 ** (n - 1))
            time.sleep(sleep_for)

    assert last_err is not None
    raise last_err


def _build_url(href: str | None) -> str | None:
    if not href:
        return None
    return urljoin(BASE_URL, href)


def _extract_title_from_link(link) -> str:
    # Prefer visible text; fall back to helpful attributes if present.
    txt = link.get_text(strip=True) if link else ""
    if txt:
        return txt
    for k in ("title", "aria-label"):
        v = link.get(k) if link else None
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _parse_chart_from_analytics_dom(soup: BeautifulSoup) -> List[Dict[str, str | int]]:
    """DOM parser using analytics attributes (works on some Beatport variants)."""
    nodes = soup.select("[data-ec-position][data-ec-name]")
    entries: List[Dict[str, str | int]] = []

    for node in nodes:
        rank_text = (node.get("data-ec-position") or "").strip()
        title = (node.get("data-ec-name") or "").strip()

        link = node.select_one("a[href*='/track/']")
        if not link:
            continue

        href = link.get("href", "")
        m = TRACK_ID_RE.search(href)
        if not m:
            continue

        try:
            rank = int(rank_text)
        except ValueError:
            continue

        url = _build_url(href)
        if not url:
            continue

        track_id = m.group(1)
        link_title = _extract_title_from_link(link)

        entries.append(
            {
                "track_id": track_id,
                "title": title or link_title or f"track-{track_id}",
                "url": url,
                "rank": rank,
            }
        )

    if entries:
        entries.sort(key=lambda e: e["rank"])
    return entries


def _parse_chart_from_dom_order(soup: BeautifulSoup, limit: int = DEFAULT_LIMIT) -> List[Dict[str, Any]]:
    """Fallback DOM parser: derive rank from appearance order of track links.

    This works even when explicit rank fields are not embedded in HTML.
    We dedupe by track_id and take the first `limit` unique tracks.
    """
    # Try to anchor in <main> first to avoid nav/footer/sidebar links.
    anchors = soup.select("main a[href*='/track/']")
    if not anchors:
        anchors = soup.select("a[href*='/track/']")

    out: List[Dict[str, str | int]] = []
    seen: set[str] = set()

    for link in anchors:
        href = link.get("href", "")
        m = TRACK_ID_RE.search(href)
        if not m:
            continue

        track_id = m.group(1)
        if track_id in seen:
            continue
        seen.add(track_id)

        url = _build_url(href)
        if not url:
            continue

        title = _extract_title_from_link(link) or f"track-{track_id}"
        out.append(
            {
                "track_id": track_id,
                "title": title,
                "mix_name": None,
                "artists": [],
                "remixers": [],
                "url": url,
                "rank": len(out) + 1,
            }
        )

        if len(out) >= limit:
            break

    return out


def _find_next_data_json(soup: BeautifulSoup) -> Optional[dict]:
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return None
    try:
        return json.loads(script.string)
    except json.JSONDecodeError:
        return None


def _walk_lists(obj: Any) -> List[List[Any]]:
    lists: List[List[Any]] = []

    def rec(x: Any) -> None:
        if isinstance(x, list):
            lists.append(x)
            for v in x:
                rec(v)
        elif isinstance(x, dict):
            for v in x.values():
                rec(v)

    rec(obj)
    return lists


def _is_track_like_dict(d: dict) -> bool:
    """Heuristic: does this dict look like a Beatport track object?"""
    if "id" not in d:
        return False
    # Track objects usually have numeric-ish IDs and a name/title.
    if not isinstance(d.get("id"), (int, str)):
        return False
    name = d.get("name") or d.get("title")
    if not isinstance(name, str) or not name.strip():
        return False

    # Common supporting keys often present in the track payload
    # (don’t require all; just enough to reduce false positives)
    helpful_keys = 0
    for k in ("slug", "bpm", "genre", "image", "artists", "release_date", "encoded_date", "current_status"):
        if k in d:
            helpful_keys += 1
    return helpful_keys >= 2


def _extract_track_href_from_track_obj(d: dict) -> Optional[str]:
    """Try to find a href/path/url for a track-like object."""
    for k in ("url", "href", "path", "canonicalUrl", "canonical_url"):
        v = d.get(k)
        if isinstance(v, str) and "/track/" in v:
            return v

    # Sometimes nested: track dict may contain 'slug' + 'id' but no URL.
    slug = d.get("slug")
    tid = d.get("id")
    if isinstance(slug, str) and slug.strip() and isinstance(tid, (int, str)):
        return f"/track/{slug}/{tid}"

    return None


def _extract_people(objs: Any) -> List[str]:
    names: List[str] = []
    if isinstance(objs, list):
        for o in objs:
            if isinstance(o, dict):
                name = o.get("name") or o.get("title")
                if isinstance(name, str) and name.strip():
                    names.append(name.strip())
    return names


def _extract_mix_name(d: dict) -> Optional[str]:
    for key in ("mix", "mix_name", "mixName"):
        v = d.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _check_genre_supports_hype(next_data: dict) -> Optional[bool]:
    """Check if genre has is_included_in_hype=true in __NEXT_DATA__.
    
    Returns:
        True if genre supports hype (is_included_in_hype=true)
        False if genre explicitly doesn't support hype (is_included_in_hype=false)
        None if we can't determine (data not present)
    """
    try:
        props = next_data.get("props") or {}
        page_props = props.get("pageProps") or {}
        dehydrated = page_props.get("dehydratedState") or {}
        queries = dehydrated.get("queries") or []
        
        for query in queries:
            if not isinstance(query, dict):
                continue
            state = query.get("state") or {}
            data = state.get("data") or {}
            
            # Look for is_included_in_hype field in genre metadata
            if "is_included_in_hype" in data:
                return bool(data["is_included_in_hype"])
        
        return None
    except Exception:
        return None


def _parse_chart_from_next_data_order(html: str, limit: int = DEFAULT_LIMIT) -> List[Dict[str, Any]]:
    """Parse using Next.js __NEXT_DATA__ ordered lists (preferred path)."""
    soup = BeautifulSoup(html, "lxml")
    next_data = _find_next_data_json(soup)
    if not next_data:
        return []

    # Beatport often nests as props.pageProps.pageProps; we’ll search there first,
    # but also fall back to searching the entire payload if needed.
    props = next_data.get("props") or {}
    page_props = props.get("pageProps") or {}
    if isinstance(page_props, dict) and "pageProps" in page_props and isinstance(page_props["pageProps"], dict):
        page_props = page_props["pageProps"]

    search_roots = [page_props, next_data]

    best_list: List[Any] = []
    best_score = 0
    best_len = 0

    for root in search_roots:
        for lst in _walk_lists(root):
            if len(lst) < 20:
                continue
            # Score by how many items look like tracks
            trackish = 0
            for item in lst[: min(len(lst), 250)]:
                if isinstance(item, dict) and _is_track_like_dict(item):
                    trackish += 1

            # Need enough confidence this is actually a chart list
            if trackish < 20:
                continue
            density = trackish / max(len(lst), 1)
            if density < 0.50:
                continue

            if (trackish > best_score) or (trackish == best_score and len(lst) > best_len):
                best_list = lst
                best_score = trackish
                best_len = len(lst)

        if best_score >= 50:
            break  # good enough; don’t keep scanning

    if not best_list:
        return []

    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for item in best_list:
        if not isinstance(item, dict) or not _is_track_like_dict(item):
            continue

        href = _extract_track_href_from_track_obj(item)
        if not href:
            continue

        m = TRACK_ID_RE.search(href)
        if m:
            track_id = m.group(1)
        else:
            # if href built from slug/id, it may not match regex yet (but it should)
            tid = item.get("id")
            track_id = str(tid) if tid is not None else ""
        if not track_id or track_id in seen:
            continue
        seen.add(track_id)

        url = _build_url(href) or href
        title = (item.get("name") or item.get("title") or "").strip() or f"track-{track_id}"
        mix_name = _extract_mix_name(item)
        artists = _extract_people(item.get("artists"))
        remixers = _extract_people(item.get("remixers"))

        out.append(
            {
                "track_id": track_id,
                "title": title,
                "mix_name": mix_name,
                "artists": artists,
                "remixers": remixers,
                "url": str(url),
                "rank": len(out) + 1,
            }
        )
        if len(out) >= limit:
            break

    return out


def parse_chart(html: str) -> List[Dict[str, Any]]:
    """Parse a Beatport chart page into a list of track entries.

    Returns dictionaries with keys: track_id, title, mix_name, artists, remixers, url, rank.
    Returns empty list for unsupported hype charts (is_included_in_hype=false).
    Raises ValueError if no valid chart entries can be parsed from a supported chart.
    """
    soup = BeautifulSoup(html, "lxml")

    # Check early if this is an unsupported hype chart
    is_hype_request = "hype=true" in html
    if is_hype_request:
        next_data = _find_next_data_json(soup)
        if next_data:
            supports_hype = _check_genre_supports_hype(next_data)
            if supports_hype is False:
                # Genre explicitly doesn't support hype; return empty (not an error)
                return []

    # Prefer __NEXT_DATA__ for structured fields
    entries = _parse_chart_from_next_data_order(html, limit=DEFAULT_LIMIT)
    if entries:
        return entries

    # Fallback: explicit rank + title embedded in DOM (limited fields)
    entries = _parse_chart_from_analytics_dom(soup)
    if entries:
        for e in entries:
            e.setdefault("mix_name", None)
            e.setdefault("artists", [])
            e.setdefault("remixers", [])
        return entries

    # Fallback: derive rank from DOM order of track links (limited fields)
    entries = _parse_chart_from_dom_order(soup, limit=DEFAULT_LIMIT)
    if entries:
        return entries

    raise ValueError("No valid chart entries could be parsed from HTML")
