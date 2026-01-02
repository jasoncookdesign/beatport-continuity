"""Project configuration placeholders."""
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Paths:
    """Canonical project paths used across the pipeline."""

    root: Path = Path(__file__).resolve().parent.parent
    data: Path = root / "data"
    docs: Path = root / "docs"
    templates: Path = root / "templates"


def load_paths() -> Paths:
    """Return default project paths.

    Expand this helper once configuration sources (env, files, CLI) are defined.
    """

    return Paths()
