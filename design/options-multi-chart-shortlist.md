# Multi-chart DJ shortlist — options analysis

Status: proposal for decision (work#22). Written 2026-10-11. All access checks below were run on 2026-10-11 unless dated otherwise.

The question is whether the Beatport / Spotify / Hypeddit shortlist should be built by extending this repo as V2 or as a new app. The short answer is a third shape: keep this repo as the Beatport source and build a small private shortlist app that reuses its compute code and reads its data. The data-source findings drive that more than the code does. Spotify chart data is mostly out of reach on its terms, Hypeddit needs written permission, and this repo publishes everything it stores to a public site.

## 1. What exists today

`beatport-continuity` is a weekly batch pipeline: `init-db → ingest → compute → report`, run by `.github/workflows/weekly.yml` on Mondays at 10:15 UTC on a GitHub-hosted runner, which commits `data/beatport.sqlite` and the rendered `docs/` back to `main`. GitHub Pages serves `docs/` publicly (legacy build from `main:/docs`). The repo is public.

Layers and where Beatport is baked in:

- **Config** (`src/bpc/config.py`) — `TRACKED_CHARTS` is a hardcoded Python list of 34 Beatport charts: 17 genres (counting "overall") × Top 100 and Hype 100. Each entry carries `id`, `chart_type` (`top100` / `hype`), `genre_slug`, `name`, `url`. Changing the chart set means a code change.
- **Fetch/parse** (`src/bpc/fetch.py`) — entirely Beatport-specific. Plain `requests` GET with a browser user agent, then three parsers in order: Next.js `__NEXT_DATA__` (track-like dict heuristics, Beatport track URL regex `/track/<slug>/<id>`), analytics DOM attributes, DOM link order. Also Beatport-specific hype detection (`is_included_in_hype`, `count=0`).
- **Ingest** (`src/bpc/ingest.py`) — per-chart isolation: each chart commits or fails on its own. Parse errors and empty results are recorded as `status='failed'` snapshot rows. Network and HTTP errors (a 403 after retries, for example) are logged and write no row at all. Mostly generic; only an error message names Beatport.
- **Storage** (`src/bpc/db.py`) — SQLite: `charts`, `chart_snapshots` (one per chart per Monday bucket), `tracks` (keyed by the Beatport track ID, with title, mix, artists, remixers as comma-joined text), `chart_entries` (rank per snapshot), `durability_metrics`. Retention keeps 12 weeks of metrics and 52 weeks of entries. Track identity is the source's own ID, so there is no notion of "the same track on two sources."
- **Compute** (`src/bpc/compute.py`) — source-agnostic. It only needs ranked lists per chart per week. Per chart and track it computes weeks on chart, streaks, re-entries, best/avg rank, rank stddev, week-over-week delta, 4-week momentum and volatility, and a composite `durability_score` (0.35 rank quality, 0.25 longevity, 0.20 streak, 0.10 presence, 0.10 churn).
- **Report** (`src/bpc/report.py`, `templates/`) — static HTML. Buckets (Anchor, Climber, Fader, Spike) are derived from metrics. The "Across all charts" page boosts a track's average durability by 15% per extra chart, capped at +60%, counts `top100` vs `hype` charts by `chart_type`, and lists only tracks on two or more charts.
- **Tests** — `tests/test_maintenance.py` covers retention and VACUUM only. Fetch, parse, compute, and report have no tests, and `pytest` isn't a declared dependency.

What the data shows from 2025-12-29 to 2026-10-05: 34 distinct snapshot dates, 31–33 per configured chart, across a 41-week calendar span. The eight Mondays from 2026-07-13 to 2026-08-31 are missing (see the size limit below), and 2026-05-11 is partial (4 charts). That adds up to 87,101 chart entries over 20,400 tracks and a 78 MiB SQLite file committed every week. Two stale chart IDs from early runs (`techno-top-100`, `top100:tech-house`) are still in the database. Seven of the 17 Hype 100 charts have failed every single week: bass-club, breaks, deep-dubstep-grime, jackin-house, techno-rdh, trap-future-bass, uk-garage-bassline. Those are close to the breaks and bass end of your range, so for those genres the Beatport stamina signal comes from the Top 100 alone.

### Already covered vs. missing, against your criteria

| Criterion (work#22) | Today | Gap |
|---|---|---|
| Stamina — enduring rankings | `durability_score`, streaks, Anchor bucket | none within a chart |
| Fast movers | `wow_delta`, `momentum_4w`, Climber and Spike buckets | weekly cadence only; no "new this week, high entry" flag |
| Cross-chart presence | aggregate multiplier on the all-charts page | counts Beatport Top 100 and Hype of the same genre as two charts; nothing cross-source; no shared track identity |
| Specific named charts | yes, but hardcoded | needs a config file |
| Chart set configurable by URL | URL is a field, source is assumed | needs source detection from the URL and a parser per source |

Three defects carry into any option.

- **Failed weeks read as absence.** `compute._fetch_chart_weeks` counts `status='failed'` snapshots as observed weeks, so a parse or empty-result failure reads as every track dropping off that week. That breaks streaks, adds a re-entry, and lowers presence. `report._fetch_weeks_observed` has the same missing filter, but it only affects the displayed week count. It hasn't mattered because the failing charts fail every week, but a flaky second source would hit it right away.
- **Missing weeks are bridged silently.** HTTP failures write no row, and compute indexes whatever weeks exist as consecutive. So a skipped week disappears and streaks run straight across it. The July–August hole is already bridged this way in the current metrics.
- **The database has already outgrown GitHub's file limit once, with Beatport alone.** Git warns above 50 MiB and GitHub rejects files above 100 MiB [6]. Every weekly run from 2026-07-20 to 2026-08-31 failed with "File data/beatport.sqlite is 105.55 MB; this exceeds GitHub's file size limit" (run 33382432781). The retention commit a7b17d2 (2026-09-02) brought it back under. It's 78 MiB now and still growing: `durability_metrics` holds about 62 MiB of it and adds roughly 600 rows per weekly slice, and `chart_entries` won't reach its 52-week cap until late December 2026. [Inference] At this rate the Beatport-only file approaches 100 MiB again around year-end. Adding more sources to this database would hit the limit sooner.

## 2. Data-source feasibility

Checked by direct request from this machine on 2026-10-11 plus each provider's own documentation. "Reachable" means the data came back. It does not mean using it is allowed.

| Source | Official API for chart data | What I could actually reach | Terms | Verdict |
|---|---|---|---|---|
| **Beatport** charts | v4 API at `api.beatport.com/v4/docs/` exists (docs page returns 200). Third-party reports say access is partner-only with no public client tier [10] [Unverified against Beatport]. | Chart HTML scraped weekly from GitHub-hosted runners, last success 2026-10-05 (Actions run 37297140497). From this machine the same URLs return **403** (Cloudflare). `observation-engine` disabled its Beatport adapter for the same reason. | `robots.txt` allows `/`. The terms page returned 403 to both this machine and a web fetcher, so the scraping clause is **[Unverified]**. | Works today, but only from GitHub's runners. Unofficial and fragile. |
| **Spotify Charts** (charts.spotify.com) | None. No public API is documented. | Unauthenticated `charts-spotify-com-service.spotify.com/public/v0/charts` returns **3 charts only**: Global weekly Top 50 tracks, albums, artists (dated 2026-10-08). Any specific chart (e.g. `/public/v0/charts/regional-global-weekly/latest`) returns 404, and the `/auth/v0/` path returns **401**. Every other chart needs a logged-in session token. | Developer Terms v10 (eff. 2025-05-15) §IV.2.d(i) bars "any robot, spider, site search/retrieval application, or other tool to retrieve" Spotify content [5]. | Only the global Top 50, which is pop-dominated and off-genre. Anything genre-relevant is behind login and against the terms. |
| **Spotify playlists** (editorial "charts" such as mint, or curator playlists) | Web API `GET /playlists/{id}`. | New use cases lost **"algorithmic and Spotify-owned editorial playlists"** on 2024-11-27 [1]. Since the Feb 2026 Development Mode changes (new apps from 2026-02-11, existing dev apps migrated 2026-03-09), playlist items are returned **"only for playlists the user owns or collaborates on"** [3]. Extended quota (which would lift that) has accepted **organizations only, with at least 250k MAU**, since 2025-05-15 [2]. The public embed page does carry the tracklist (75 tracks observed for mint), but `open.spotify.com/robots.txt` disallows `/embed/`. | §IV.2.d(i) as above. §IV.3.a(i): "you may not store, aggregate or create compilations or databases of Spotify Content" beyond what the app strictly needs, and "do not store Spotify Content indefinitely" [5]. §IV.2.a(i) bars ingesting Spotify Content into an ML or AI model [5]. | **Not reachable within the terms** for any playlist you don't own. One manual path stays inside them: copy a chart playlist into a playlist you own and read that through a Premium-owned dev-mode app. Even then, keeping week-over-week history is in tension with §IV.3.a(i). |
| **Spotify track lookup** (for identity matching) | `GET /tracks/{id}`, single track (the batch `GET /tracks` was removed Feb 2026) [3]. `external_ids` (ISRC) was listed as removed in Feb, then **reverted** in March 2026 [4]. | Not exercised: it needs a Premium-owned dev app. | Same storage limits. | Usable for ISRC lookups at low volume, one call per track. The dev-mode quota is shared per developer account (July 2026) [4b]. |
| **Hypeddit** charts | None. The terms don't mention an API. | Undocumented JSON: `POST https://hypeddit.com/top100smartlinks` with `page=1&type=<genre>` returns **100 entries** per genre (`techno` and all-genre both verified) with `title`, `username` (artist text), `no_of_clicks`, `type`, and for Spotify links the **Spotify track ID** (`trackid`). The genre page pattern is `hypeddit.com/charts/streams/genre/<slug>`; the Downloads chart uses `POST /top100tracks`. The genre selector lists `breaks`, `bass`, `bass-house`, `dubstep`, `drum-bass`, `house`, `tech-house`, `techno`, and `trap` among others. Only `techno` has been confirmed to return genre-filtered data, and genre pages return 200 even for made-up slugs, so each slug needs checking against the JSON. Reachable from this machine. Not tested from GitHub runners. | §2(6): no "robot, spider, other automatic device, or manual process to monitor or copy pages from the Website ... without the prior express written permission of Hypeddit LLC". §2(13): no mirroring on another server [7]. `robots.txt` is `Disallow:` (empty). | **Technically easy and legally gated.** It needs written permission from Hypeddit. |

Data-quality notes on Hypeddit: the ranking method isn't documented anywhere I could find. The payload suggests smart-link clicks (`no_of_clicks`). Those are promotional links that artists drive traffic to, so the chart measures promo reach more than listening, and it can be gamed. Titles are user-entered and noisy. The all-genre #1 on 2026-10-11 was titled "Play - SONG on SPOTIFY" by "DON'T FORGET CLICK (+) To SAVE IT Track". Promoted "instream" placements come back as a separate `instream` list (10 per page) beside the 100-entry `response`, and the page splices them into the display. A JSON parser that reads `response` never sees them, but it should still drop entries where `chart_eligible != 1` or `status != 0`, which the page also replaces with promos. One useful property: every entry checked was a Spotify link with a Spotify track ID, so Hypeddit is the cheapest legitimate-looking route to a Spotify-linked signal.

Identity across sources: Beatport tracks are keyed by Beatport ID and Hypeddit and Spotify entries by Spotify track ID. Whether Beatport chart payloads carry ISRC is **[Unverified]**. The only saved payloads are from failed charts, and Beatport blocks this machine. Plan on normalized artist + title + mix matching with a manual override table, and measure the match rate on real data before letting cross-source presence move the score. ISRC through Spotify `GET /tracks/{id}` is an upgrade, not the starting point.

## 3. Options

### A. Extend `beatport-continuity` as V2

**What changes.** Characterization tests come first, for parse (saved fixtures), compute, and the aggregate, since none exist. Then a source-adapter seam: `fetch.py` becomes `sources/beatport.py` behind a `fetch(url) -> list[Entry]` interface, and `sources/hypeddit.py` (and maybe `sources/spotify.py`) sit beside it. The source comes from the URL host. `TRACKED_CHARTS` moves to a `charts.yaml` of URLs plus display names, and `chart_type`/`genre_slug` become optional metadata. Add a `source` column on `charts`, a `track_links` table (source, source_track_id → canonical track id), a matching step, and a shortlist scorer that generalizes `_aggregate_across_charts` to count sources rather than charts. Fix the failed-week defect. Rename user-facing "Beatport Continuity" strings.

**Effort.** The largest of the three: several focused sessions. Roughly a third of it is refactor and test backfill before any new capability lands.

**Risks.** Publication is the big one. The repo is public and Pages publishes `docs/` publicly. Committing Hypeddit or Spotify chart data to it, or rendering it to Pages, mirrors those sites' content on another server, which runs against Hypeddit §2(13) and Spotify §IV.3. To avoid that, A has to keep non-Beatport data out of git and out of `docs/`. That breaks the repo's own model (state in a committed SQLite, output on Pages), or else the repo goes private and you lose the public Beatport report. Making it private doesn't fix Pages either: access-controlled Pages needs GitHub Enterprise Cloud [11]. Two more risks: database growth toward the 100 MiB limit, and run location. Beatport only works from GitHub runners and Hypeddit is untested there. If Hypeddit blocks datacenter IPs, the single pipeline has nowhere to run both.

**Second-order effects.** One codebase and one history, and the existing Beatport history stays in place. But a stable, finished tool ("Phase 1 complete") becomes a moving target again, and its public site inherits the shortlist's private-use constraints.

**URL config and scoring fit.** Natural: compute and buckets already express stamina and movers, so the work is the identity layer and a source-aware boost.

### B. New app from scratch

**What changes.** A new private repo with adapters for all three sources, its own schema with cross-source identity from day one, its own compute, scoring, and output. It either re-scrapes Beatport, which means duplicating `fetch.py` and its fragility, or imports the existing history once.

**Effort.** Medium to large. The Beatport scraper and the durability math get rebuilt or copied, and copied code drifts.

**Risks.** Two Beatport scrapers hitting the same Cloudflare edge. The new one is stuck on GitHub runners too, and when Beatport changes markup it breaks in two places. Starting from zero history means no stamina signal for weeks unless the import is done right.

**Second-order effects.** Clean boundaries and private by default. But you'd own two copies of the same mechanics with no reason to.

**URL config and scoring fit.** Clean, because it's designed for both up front, but it re-derives what `compute.py` already does.

### C. Composition: Beatport stays here, a private shortlist app reuses it (recommended)

**What changes.**

- *This repo* stays the public, Beatport-only source and gets a hardening PR. It fixes both week-handling defects, so failed weeks and missing weeks are each treated as unknown rather than absent or consecutive. It adds characterization tests for compute and declares `pytest`. It also takes the database off its path to the 100 MiB limit, either with tighter metrics retention or by moving the SQLite out of git into a release asset or Actions artifact. The size work is owed regardless of option. No new sources come in here.
- *A new private repo* (working name `chart-shortlist`) installs `beatport-continuity` as a pinned git dependency and reuses `bpc.db.init_db` and `bpc.compute.run_compute` unchanged on its own private SQLite. The schema already fits any ranked weekly list. It copies Beatport snapshots (`chart_snapshots`, `chart_entries`, `tracks`) for the configured charts from this repo's published database, read-only, and recomputes metrics locally with the same `run_compute`. One engine produces every source's metrics, and this repo's `durability_metrics` is never read. It also adds its own Hypeddit adapter. A Spotify adapter is added only if you settle on a path that fits the terms (see questions). The adapters write snapshots into that same `bpc` schema, so the per-chart metrics come from the same code for every source. The new code is a `charts.yaml` of URLs, the identity matcher with an override table, and the shortlist scorer. The scorer takes stamina from `durability_score` and the Anchor bucket (`bpc.report.bucket_row`, so `report.py` is part of the reused surface too), movers from `wow_delta`, `momentum_4w`, and new high entries, and boosts cross-source presence above same-source multi-chart presence. Output is a weekly `shortlist.json`/`.csv` committed to the private repo.

**Effort.** Medium. Less than A, because there's no refactor of working code, and less than B, because there's no second Beatport scraper or second metrics engine. The Hypeddit adapter is small given the JSON endpoint, so most of the work is identity matching and scoring.

**Risks.** A cross-repo contract: the shortlist depends on the `bpc` schema, `run_compute` (which always prunes to 12/52 weeks and VACUUMs, and that behavior is part of the contract), and `bucket_row`. Pin by commit and add one contract test in the new repo that runs the real `run_compute` over a fixture DB, so a schema change in this repo fails loudly there. Hypeddit permission is still required before the adapter runs on a schedule. Two Actions workflows instead of one, though private-repo minutes are a non-issue at one short weekly run.

**Second-order effects.** The public site stays exactly as it is, and only Beatport data is ever published. Data under third-party terms lives in a private repo and is never mirrored publicly. The shortlist repo becomes the integration surface for the Reel series and #21 without touching this tool. If Spotify access never materializes, nothing is lost: the shortlist runs on Beatport and Hypeddit, and Hypeddit's Spotify track IDs leave the Spotify join ready.

**URL config and scoring fit.** Beatport URLs in `charts.yaml` pick which of this repo's tracked charts count. A Beatport URL this repo doesn't track yet fails validation with a clear message and gets added here by PR, which suits "Beatport rarely changes." Hypeddit and Spotify URLs, the ones that change, live only in the shortlist config.

### Rejected: fold it into `observation-engine`

It looks like the obvious home, since it already has adapter config and feeds the vault. It doesn't fit. Its pipeline sends every item through an LLM processor, which collides with Spotify §IV.2.a(i) for Spotify content. It runs on this machine, where Beatport returns 403. And it's a daily new-signal notifier with URL dedup, not a weekly ranked-history engine. The right link is downstream: the shortlist's output becomes one of its inputs.

## 4. Integration point (Reel series, #21)

Keep the seam a file, not a call. The shortlist publishes a stable weekly artifact in the private repo (track, artists, source links, per-source rank and bucket, score, and why it qualified), and consumers pull it. Two consumers are plausible. One is a direct append of candidates to the Reel backlog. The other is a small `observation-engine` adapter that turns new shortlist entries into vault notes, so the #21 flag-for-reel path takes over from there. Which one depends on #21's open flag-format and trigger decisions, so the build here should stop at the artifact and its schema. If Spotify-sourced fields ever flow into the LLM-backed engine, strip them first (§IV.2.a(i)).

## 5. Recommendation

Build **C**. This repo stays the Beatport source and only gets a hardening PR. A new private shortlist app reuses `bpc`'s schema and compute, adds Hypeddit, and owns URL config, identity matching, scoring, and the Reel-facing artifact. Sequence: (1) hardening PR here, including the database-size fix; (2) ask Hypeddit for written permission while (3) building the shortlist on Beatport-only data to prove the identity and scoring layers; (4) add Hypeddit once permission lands; (5) decide Spotify last, on your answer below.

Choose **A** instead only if you're willing to take this repo private and give up the public Beatport report. Then most of A's publication problem goes away and one repo is simpler. **B** has no advantage over C.

## 6. Open questions for you

1. **Pick:** C as recommended, A (accepting a private repo and no public report), or B?
2. **Chart URLs:** which Beatport charts from the 34 tracked here count for the shortlist, and which Hypeddit genre charts (`hypeddit.com/charts/streams/genre/<slug>`), Streaming, Downloads, or both?
3. **What "Spotify charts" means to you.** charts.spotify.com has no genre charts I could reach (only the global Top 50 is public). If you mean editorial or curator playlists, the only route inside Spotify's terms is a playlist you own. Is a manual weekly copy acceptable, should Spotify drop out with Hypeddit's Spotify-linked chart as the stand-in, or is there a specific chart URL you have in mind?
4. **Hypeddit permission:** should I draft the request to Hypeddit for you to send from your account? Its terms require their written permission for automated access.
5. **Spotify developer app:** do you have a Premium-owned Spotify developer app that predates 2024-11-27? It wouldn't restore editorial playlist access, since Feb 2026 limits items to owned playlists either way, but it decides whether ISRC lookups are available for matching.

## Sources

Accessed 2026-10-11 unless noted.

1. Spotify for Developers blog, "Introducing some changes to our Web API", posted 2024-11-27. https://developer.spotify.com/blog/2024-11-27-changes-to-the-web-api
2. Spotify for Developers, "Quota modes" (undated page) and blog "Updating the Criteria for Web API Extended Access", 2025-04-15. https://developer.spotify.com/documentation/web-api/concepts/quota-modes, https://developer.spotify.com/blog/2025-04-15-updating-the-criteria-for-web-api-extended-access
3. Spotify for Developers, "February 2026 Web API Dev Mode Changes — Migration Guide" (effective 2026-02-11 for new apps, 2026-03-09 for existing). https://developer.spotify.com/documentation/web-api/tutorials/february-2026-migration-guide
4. Spotify Web API changelog, March 2026 (`external_ids` reverted). https://developer.spotify.com/documentation/web-api/references/changes/march-2026 — 4b. July 2026 (quota per developer account; 25 Client IDs). https://developer.spotify.com/documentation/web-api/references/changes/july-2026
5. Spotify Developer Terms, Version 10, effective 2025-05-15, §IV.2.a(i), §IV.2.d(i), §IV.3.a(i), §IV.3.b. https://developer.spotify.com/terms
6. GitHub Docs, "About large files on GitHub" (50 MiB warning, 100 MiB block). https://docs.github.com/en/repositories/working-with-files/managing-large-files/about-large-files-on-github
7. Hypeddit Terms, §2 items 6 and 13 (no date shown on page). https://hypeddit.com/terms
8. `open.spotify.com/robots.txt` (disallows `/embed/`), `hypeddit.com/robots.txt` (empty `Disallow:`), `www.beatport.com/robots.txt` (`Allow: /`). Fetched 2026-10-11.
9. Direct requests, 2026-10-11: Spotify Charts public and auth endpoints, the mint embed page, Hypeddit `/top100smartlinks` (genres `charts` and `techno`), Beatport chart URLs (403 from this machine), `api.beatport.com/v4/docs/` (200). GitHub Actions runs 37297140497 (Beatport ingest succeeded 2026-10-05) and 33382432781 (failed 2026-08-31 on the 100 MB file limit).
10. apis.io Beatport provider page (third-party, describes v4 as partner-only). https://apis.io/providers/beatport/ — [Unverified against Beatport].
11. GitHub Docs (Enterprise Cloud), "Changing the visibility of your GitHub Pages site": "To publish a GitHub Pages site privately, your organization must use GitHub Enterprise Cloud." https://docs.github.com/en/enterprise-cloud@latest/pages/getting-started-with-github-pages/changing-the-visibility-of-your-github-pages-site
