# beatport-continuity

A personal continuity engine for Beatport that simulates what I would notice if I checked the charts every week. This scaffold sets up the Python package, CLI entry point, and placeholders for the weekly workflow and reporting template.

## Requirements

- Python 3.11
- macOS or Linux (tested on macOS)

## Setup

1. Create and activate a virtual environment:
	```bash
	python3 -m venv .venv
	source .venv/bin/activate
	```
2. Install the project in editable mode:
	```bash
	pip install -e .
	```

## CLI usage

After installation, the CLI is available via the `bpc` script or module mode:

```bash
python -m bpc.cli --help
bpc --help
```

Subcommands (all currently print TODO and exit 0):

- `init-db` — prepare storage scaffolding
- `ingest` — fetch weekly charts
- `compute` — generate continuity metrics
- `report` — render the static HTML
- `run-all` — run the above in order

Example:

```bash
bpc run-all
```

## Structure

- `src/bpc` — package source (CLI, config, logging utils)
- `templates/report.html.j2` — Jinja2 report template stub
- `data/` and `docs/` — placeholders to keep directories under version control
- `.github/workflows/weekly.yml` — scheduled workflow stub

## Next steps

- Implement Beatport ingestion (requests + BeautifulSoup4 + lxml)
- Define persistence model for weekly snapshots
- Compute durability metrics and render into the template
