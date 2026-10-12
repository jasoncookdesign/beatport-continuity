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

* Fetches Beatport charts weekly (Top 100, genre Top 100s, and Hype 100s)
* Normalizes snapshot dates to weekly buckets (most recent Monday)
* Stores weekly snapshots in SQLite (see retention below)
* **Resilient to partial failure**:
  * If a chart fails to fetch, fails to parse, or returns empty results, the run continues
  * Every attempt leaves one `chart_snapshots` row per chart per week: `status='ok'` with its entries, or `status='failed'` with the error. Network and HTTP errors are recorded too.
  * A failure never overwrites a week that already succeeded (for example a manual re-run)
  * Unrelated charts are never rolled back
* Includes local diagnostics to validate parsing and catch breakage early

This ensures continuity is preserved even when Beatport pages are inconsistent.

Beatport only publishes a Hype 100 for genres in its Hype program. Bass / Club, Breaks / Breakbeat / UK Bass, 140 / Deep Dubstep / Grime, Jackin House, Techno (Raw / Deep / Hypnotic), Trap / Future Bass, and UK Garage / Bassline aren't in it (their genre data has `is_included_in_hype: false`), so they're tracked by Top 100 only. A Hype page for a genre outside the program fails with `HypeNotOffered`.

---

### Metrics

Computed **per chart**, never globally:

* Weeks on chart
* First / last seen
* Current streak length
* Re-entry count
* Week-over-week rank movement
* Momentum and rank volatility windows
* Composite durability score
* Qualitative buckets:
  * **Anchor** — long-lived, stable tracks
  * **Climber** — steadily gaining adoption
  * **Fader** — decaying relevance
  * **Spike** — short-lived hype appearances

Each chart's latest metrics are persisted and inspectable—nothing is a black box.  
Buckets become more accurate over time as historical data accumulates.

#### Unknown weeks

Weeks are Monday buckets on the calendar. A week is **known** for a chart when it has a `status='ok'` snapshot. It is **unknown** when the fetch failed or when nothing was recorded at all (for example the weekly run never landed, as for the eight Mondays from 2026-07-13 to 2026-08-31, or most charts on 2026-05-11). Unknown weeks are never read as data:

* **Not a drop-off.** A re-entry needs an observed absence, a known week without the track. Weeks on chart, age, and presence count known weeks only.
* **Not consecutive.** A streak continues only into the next calendar week, so an unknown week ends it. Week-over-week movement compares with the previous calendar week only, and momentum and volatility use the current streak (at most its last four weeks).
* **Not the latest week.** A chart's metrics are as of its last known week. If this week's fetch failed, its page keeps last week's numbers and says "As of week …".

`status` lists each chart's unknown weeks.

---

### Reporting

* Renders a fully static HTML report:
  * `docs/index.html` — landing page
  * `docs/charts/*.html` — one page per chart
* Landing page shows:
  * Tracked charts
  * Top durable tracks per chart (summary view)
* Individual chart pages include:
  * Ranked tables by durability score
  * Clear separation of track title, mix, artists, and remixers
  * Qualitative bucket labels
  * Hover tooltips explaining every metric column
* Client-side interactivity (no backend):
  * Text search (track / artist / remixer)
  * Toggle filters (Anchors, Climbers, etc.)
  * Option to hide dim rows (tracks no longer on chart)

Designed to be opened locally or served via GitHub Pages.

---

### Aggregate views (explicit and cautious)

In addition to per-chart analysis, the report includes an **Across All Charts** view that:

* Surfaces tracks appearing in multiple charts
* Highlights durability across contexts
* Uses an explicitly documented aggregation method
* Combines only charts at the newest week; a chart whose latest fetch failed keeps its own page but is left out here

This page is deliberately conservative:
* No hidden weighting
* No “global taste” claims
* Purely a lens for cross-chart persistence

---

### Automation

* Weekly GitHub Actions workflow:
  * `ingest → compute → report`
  * Commits updated database and HTML output
* Safe by default:
  * Partial ingestion failures do not block the run
  * Status command surfaces missing or failed charts cleanly
* No external services required
* No runtime dependencies beyond Python + SQLite

### Storage and retention

`data/beatport.sqlite` is committed by the weekly run, so it must stay under GitHub's 100 MiB file limit (Git warns above 50 MiB). Each `compute` keeps it bounded:

* `chart_entries` and `chart_snapshots` keep the last 52 weeks. Older weeks remain in every earlier commit of the database, so nothing is unrecoverable.
* `tracks` that no retained entry or metric references are deleted.
* `durability_metrics` is a derived cache holding one slice per chart: the latest computed week. Any other week can be recomputed from `chart_entries` with `compute --snapshot-date`.
* `VACUUM` reclaims the freed pages.

At 27 charts that levels off at roughly 35 MiB once 52 weeks are retained.

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
````

2. Install in editable mode:

   ```bash
   pip install -e .
   ```

3. To run the tests:

   ```bash
   pip install -e ".[test]"
   python -m pytest
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
  Fetch weekly charts and store snapshots.
  If no date is provided, defaults to the most recent weekly bucket.

* `compute [--snapshot-date YYYY-MM-DD]`
  Compute durability metrics per chart up to the given week, as of each chart's last known week. Replaces that chart's previous metrics slice, then applies retention.

* `report [--snapshot-date YYYY-MM-DD]`
  Render static HTML pages to `docs/` for the charts in `TRACKED_CHARTS`, each as of its latest metrics on or before the given week.

* `run-all [--snapshot-date YYYY-MM-DD]`
  Run `init-db → ingest → compute → report`.

* `status`
  Print a concise overview of:

  * tracked charts
  * latest successful snapshots
  * missing or failed chart/weeks
  * unknown weeks per chart (failed, or no record)
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
  ingest.py       # Chart ingestion (resilient)
  compute.py      # Metric computation
  report.py       # HTML report generation
  diagnose.py     # Local parsing diagnostics
  status.py       # Pipeline health summary
  fetch.py        # Beatport fetching + parsing
  db.py           # SQLite schema + helpers

templates/
  report.html.j2
  chart.html.j2   # Per-chart pages
  aggregate.html.j2

data/
  beatport.sqlite # SQLite database (tracked)
  debug/          # Saved HTML on parse failure

docs/
  index.html
  charts/
  aggregate.html

.github/workflows/
  weekly.yml
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

The system is stable, resilient, automated, and already useful.
Future changes will be driven by observed behavior over time—not feature ambition.