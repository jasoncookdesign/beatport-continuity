"""Project configuration and tracked chart defaults."""
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


# Canonical repository paths
REPO_ROOT: Path = Path(__file__).resolve().parents[2]
DATA_DIR: Path = REPO_ROOT / "data"
DB_PATH: Path = DATA_DIR / "beatport.sqlite"
DOCS_PATH: Path = REPO_ROOT / "docs" / "index.html"
TEMPLATES_DIR: Path = REPO_ROOT / "templates"


@dataclass
class Paths:
    """Canonical project paths used across the pipeline."""

    root: Path = REPO_ROOT
    data: Path = DATA_DIR
    docs: Path = DOCS_PATH.parent
    templates: Path = TEMPLATES_DIR
    db: Path = DB_PATH


def load_paths() -> Paths:
    """Return default project paths.

    Expand this helper once configuration sources (env, files, CLI) are defined.
    """

    return Paths()


# Tracked Beatport charts (Top 100 + Hype where applicable)
#
# Beatport only offers a Hype 100 for genres in its Hype program. These genres
# have no Hype chart: their genre metadata carries is_included_in_hype=false
# and the hype-100 URL returns a page with count=0 (checked every week from
# 2026-01-05 to 2026-10-05). They are tracked by Top 100 only:
#   bass-club, breaks, deep-dubstep-grime, jackin-house, techno-rdh,
#   trap-future-bass, uk-garage-bassline.
# If Beatport adds one of them to Hype, re-add its hype-100 entry; a page for a
# genre outside the program fails with fetch.HypeNotOffered.
TRACKED_CHARTS: List[Dict[str, str]] = [
    # Overall
    {
        "id": "overall-top-100",
        "chart_type": "top100",
        "genre_slug": "overall",
        "name": "Beatport Top 100",
        "url": "https://www.beatport.com/top-100",
    },
    {
        "id": "overall-hype-100",
        "chart_type": "hype",
        "genre_slug": "overall",
        "name": "Beatport Hype 100",
        "url": "https://www.beatport.com/hype-100",
    },

    # Deep Dubstep / Grime
    {
        "id": "deep-dubstep-grime-top-100",
        "chart_type": "top100",
        "genre_slug": "deep-dubstep-grime",
        "name": "Deep Dubstep / Grime Top 100",
        "url": "https://www.beatport.com/genre/140-deep-dubstep-grime/95/top-100",
    },

    # Bass Club
    {
        "id": "bass-club-top-100",
        "chart_type": "top100",
        "genre_slug": "bass-club",
        "name": "Bass Club Top 100",
        "url": "https://www.beatport.com/genre/bass-club/85/top-100",
    },

    # Breaks / Breakbeat / UK Bass
    {
        "id": "breaks-top-100",
        "chart_type": "top100",
        "genre_slug": "breaks",
        "name": "Breaks / Breakbeat / UK Bass Top 100",
        "url": "https://www.beatport.com/genre/breaks-breakbeat-uk-bass/9/top-100",
    },

    # Deep House
    {
        "id": "deep-house-top-100",
        "chart_type": "top100",
        "genre_slug": "deep-house",
        "name": "Deep House Top 100",
        "url": "https://www.beatport.com/genre/deep-house/12/top-100",
    },
    {
        "id": "deep-house-hype-100",
        "chart_type": "hype",
        "genre_slug": "deep-house",
        "name": "Deep House Hype 100",
        "url": "https://www.beatport.com/genre/deep-house/12/hype-100",
    },

    # House
    {
        "id": "house-top-100",
        "chart_type": "top100",
        "genre_slug": "house",
        "name": "House Top 100",
        "url": "https://www.beatport.com/genre/house/5/top-100",
    },
    {
        "id": "house-hype-100",
        "chart_type": "hype",
        "genre_slug": "house",
        "name": "House Hype 100",
        "url": "https://www.beatport.com/genre/house/5/hype-100",
    },

    # Jackin House
    {
        "id": "jackin-house-top-100",
        "chart_type": "top100",
        "genre_slug": "jackin-house",
        "name": "Jackin House Top 100",
        "url": "https://www.beatport.com/genre/jackin-house/97/top-100",
    },

    # Mainstage
    {
        "id": "mainstage-top-100",
        "chart_type": "top100",
        "genre_slug": "mainstage",
        "name": "Mainstage Top 100",
        "url": "https://www.beatport.com/genre/mainstage/96/top-100",
    },
    {
        "id": "mainstage-hype-100",
        "chart_type": "hype",
        "genre_slug": "mainstage",
        "name": "Mainstage Hype 100",
        "url": "https://www.beatport.com/genre/mainstage/96/hype-100",
    },

    # Melodic House & Techno
    {
        "id": "melodic-house-techno-top-100",
        "chart_type": "top100",
        "genre_slug": "melodic-house-techno",
        "name": "Melodic House & Techno Top 100",
        "url": "https://www.beatport.com/genre/melodic-house-techno/90/top-100",
    },
    {
        "id": "melodic-house-techno-hype-100",
        "chart_type": "hype",
        "genre_slug": "melodic-house-techno",
        "name": "Melodic House & Techno Hype 100",
        "url": "https://www.beatport.com/genre/melodic-house-techno/90/hype-100",
    },

    # Minimal / Deep Tech
    {
        "id": "minimal-deep-tech-top-100",
        "chart_type": "top100",
        "genre_slug": "minimal-deep-tech",
        "name": "Minimal / Deep Tech Top 100",
        "url": "https://www.beatport.com/genre/minimal-deep-tech/14/top-100",
    },
    {
        "id": "minimal-deep-tech-hype-100",
        "chart_type": "hype",
        "genre_slug": "minimal-deep-tech",
        "name": "Minimal / Deep Tech Hype 100",
        "url": "https://www.beatport.com/genre/minimal-deep-tech/14/hype-100",
    },

    # Organic House
    {
        "id": "organic-house-top-100",
        "chart_type": "top100",
        "genre_slug": "organic-house",
        "name": "Organic House Top 100",
        "url": "https://www.beatport.com/genre/organic-house/93/top-100",
    },
    {
        "id": "organic-house-hype-100",
        "chart_type": "hype",
        "genre_slug": "organic-house",
        "name": "Organic House Hype 100",
        "url": "https://www.beatport.com/genre/organic-house/93/hype-100",
    },

    # Progressive House
    {
        "id": "progressive-house-top-100",
        "chart_type": "top100",
        "genre_slug": "progressive-house",
        "name": "Progressive House Top 100",
        "url": "https://www.beatport.com/genre/progressive-house/15/top-100",
    },
    {
        "id": "progressive-house-hype-100",
        "chart_type": "hype",
        "genre_slug": "progressive-house",
        "name": "Progressive House Hype 100",
        "url": "https://www.beatport.com/genre/progressive-house/15/hype-100",
    },

    # Tech House
    {
        "id": "tech-house-top-100",
        "chart_type": "top100",
        "genre_slug": "tech-house",
        "name": "Tech House Top 100",
        "url": "https://www.beatport.com/genre/tech-house/11/top-100",
    },
    {
        "id": "tech-house-hype-100",
        "chart_type": "hype",
        "genre_slug": "tech-house",
        "name": "Tech House Hype 100",
        "url": "https://www.beatport.com/genre/tech-house/11/hype-100",
    },

    # Techno (Peak Time / Driving)
    {
        "id": "techno-ptd-top-100",
        "chart_type": "top100",
        "genre_slug": "techno-ptd",
        "name": "Techno (Peak Time / Driving) Top 100",
        "url": "https://www.beatport.com/genre/techno-peak-time-driving/6/top-100",
    },
    {
        "id": "techno-ptd-hype-100",
        "chart_type": "hype",
        "genre_slug": "techno-ptd",
        "name": "Techno (Peak Time / Driving) Hype 100",
        "url": "https://www.beatport.com/genre/techno-peak-time-driving/6/hype-100",
    },

    # Techno (Raw / Deep / Hypnotic)
    {
        "id": "techno-rdh-top-100",
        "chart_type": "top100",
        "genre_slug": "techno-rdh",
        "name": "Techno (Raw / Deep / Hypnotic) Top 100",
        "url": "https://www.beatport.com/genre/techno-raw-deep-hypnotic/92/top-100",
    },

    # Trap / Future Bass
    {
        "id": "trap-future-bass-top-100",
        "chart_type": "top100",
        "genre_slug": "trap-future-bass",
        "name": "Trap / Future Bass Top 100",
        "url": "https://www.beatport.com/genre/trap-future-bass/38/top-100",
    },

    # UK Garage / Bassline
    {
        "id": "uk-garage-bassline-top-100",
        "chart_type": "top100",
        "genre_slug": "uk-garage-bassline",
        "name": "UK Garage / Bassline Top 100",
        "url": "https://www.beatport.com/genre/uk-garage-bassline/86/top-100",
    },
]

