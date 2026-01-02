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


# Minimal set of tracked charts to seed ingestion.
TRACKED_CHARTS: List[Dict[str, str]] = [
    {
        "id": "overall-top-100",
        "chart_type": "top100",
        "genre_slug": "overall",
        "name": "Beatport Top 100",
        "url": "https://www.beatport.com/top-100",
    },
    {
        "id": "house-top-100",
        "chart_type": "top100",
        "genre_slug": "house",
        "name": "House Top 100",
        "url": "https://www.beatport.com/genre/house/5/top-100",
    },
    {
        "id": "techno-top-100",
        "chart_type": "top100",
        "genre_slug": "techno",
        "name": "Techno (Peak Time / Driving) Top 100",
        "url": "https://www.beatport.com/genre/techno-peak-time-driving/6/top-100",
    },
    {
        "id": "overall-hype-100",
        "chart_type": "hype",
        "genre_slug": "overall",
        "name": "Beatport Hype 100",
        "url": "https://www.beatport.com/hype",
    },
]
