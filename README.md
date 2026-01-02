# beatport-continuity

**beatport-continuity** is a personal “memory prosthetic” for Beatport charts.

It automatically captures weekly chart snapshots, computes longevity and movement metrics per chart, and renders a static HTML report that surfaces which tracks are *actually sticking around* versus spiking and disappearing.

The goal is not prediction, recommendation, or taste judgment. It’s to simulate what I would notice if I checked multiple Beatport charts every week, consistently, over time.

This project is intentionally:

* deterministic
* transparent
* explainable
* low-maintenance once running

It is built for personal insight, not as a commercial product.

---

## What it does

### Ingestion

* Fetches Beatport charts weekly (Top 100, genre Top 100s, Hype 100)
* Normalizes snapshot dates to weekly buckets
* Stores immutable snapshots in SQLite
* Includes local diagnostics to validate parsing and catch breakage early

### Metrics

Computed **per chart**, not globally:

* Weeks on chart
* First / last seen
* Streaks and re-entries
* Week-over-week movement
* Momentum and volatility windows
* Composite durability score
* Qualitative buckets (Anchor / Climber / Fader / Spike)

All metrics are persisted and inspectable—nothing is a black box.

### Reporting

* Renders a static HTML report (`docs/index.html`)
* Ranks tracks by durability score per chart
* Displays raw metrics alongside qualitative buckets
* Separates track title, artists, remixers, and mix name cleanly
* Designed to be served via GitHub Pages or opened locally

### Automation

* Weekly GitHub Actions workflow:

  * ingest → compute → report
  * commits updated database and report
* No external services required
* No runtime dependencies beyond Python + SQLite

---

## Requirements

* Python 3.11
* macOS or Linux (developed and tested on macOS)
* SQLite (bundled with Python)

---

## Setup

1. Create and activate a virtual environment:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. Install in editable mode:

   ```bash
   pip install -e .
   ```

---

## CLI usage

The CLI is available via module mode:

```bash
python -m bpc.cli --help
```

### Commands

* `init-db`
  Initialize or migrate the local SQLite database.

* `ingest [--snapshot-date YYYY-MM-DD]`
  Fetch weekly charts and store snapshots (date is bucketed to week start).

* `compute [--snapshot-date YYYY-MM-DD]`
  Compute durability metrics per chart up to the given week.

* `report [--snapshot-date YYYY-MM-DD]`
  Render the static HTML report to `docs/index.html`.

* `run-all [--snapshot-date YYYY-MM-DD]`
  Run `init-db → ingest → compute → report`.

* `status`
  Print a concise overview of:

  * tracked charts
  * latest snapshots
  * metric coverage
  * report location

* `diagnose`
  Locally test chart parsing and save debug artifacts if parsing fails.

Example:

```bash
python -m bpc.cli run-all
```

---

## Project structure

```
src/bpc/
  cli.py          # CLI entry point
  ingest.py       # Chart ingestion
  compute.py      # Metric computation
  report.py       # HTML report generation
  diagnose.py     # Local parsing diagnostics
  status.py       # Pipeline health summary
  fetch.py        # Beatport fetching + parsing
  db.py           # SQLite schema + helpers

templates/
  report.html.j2  # Jinja2 HTML template

data/
  beatport.sqlite # SQLite database (tracked)

docs/
  index.html      # Generated report (tracked)

.github/workflows/
  weekly.yml      # Scheduled weekly run
```

---

## What this is *not*

* Not a recommender system
* Not machine learning
* Not predictive
* Not trying to guess “good taste”

It does not hallucinate, infer intent, or smooth away uncertainty.

It simply:

> remembers consistently
> computes honestly
> and shows its work

---

## Status

**Phase 1 complete.**

The system is stable, automated, and already useful.
Future changes, if any, will be driven by observed behavior over time—not feature ambition.