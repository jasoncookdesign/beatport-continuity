"""A Hype 100 page for a genre Beatport keeps out of its Hype program is
reported as exactly that, not as a generic empty or unparseable page."""
from __future__ import annotations

import json

import pytest

from bpc import fetch


def _page(count: int, included_in_hype: bool) -> str:
    next_data = {
        "page": "/genre/[description]/[id]/hype-100",
        "props": {
            "pageProps": {
                "dehydratedState": {
                    "queries": [
                        {"state": {"data": {"count": count, "results": []}}},
                        {"state": {"data": {"id": 1, "name": "Example", "is_included_in_hype": included_in_hype}}},
                    ]
                }
            }
        },
    }
    return (
        '<html><body><script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(next_data)
        + "</script></body></html>"
    )


def test_genre_outside_hype_program_raises_hype_not_offered():
    with pytest.raises(fetch.HypeNotOffered) as exc_info:
        fetch.parse_chart(_page(count=0, included_in_hype=False))
    assert "is_included_in_hype=false" in str(exc_info.value)


def test_empty_page_for_hype_genre_is_still_a_generic_empty_result():
    with pytest.raises(ValueError) as exc_info:
        fetch.parse_chart(_page(count=0, included_in_hype=True))
    assert not isinstance(exc_info.value, fetch.HypeNotOffered)
    assert "count=0" in str(exc_info.value)
