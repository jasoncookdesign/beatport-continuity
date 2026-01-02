# Ingestion Failure Handling Implementation

## Summary

Implemented robust error handling for chart ingestion that allows the pipeline to continue processing even when individual charts fail. Failures are now recorded in the database and visible in the status report.

## Changes Made

### 1. Database Schema (src/bpc/db.py)

Added three new columns to the `chart_snapshots` table via idempotent migration:
- `status TEXT NOT NULL DEFAULT 'ok'` - Tracks whether snapshot succeeded ('ok') or failed ('failed')
- `error TEXT` - Stores error message for failed snapshots
- `html_bytes INTEGER` - Records size of fetched HTML for debugging

Updated `upsert_snapshot()` function to accept and store these new fields.

### 2. Ingestion Logic (src/bpc/ingest.py)

**Before**: Single transaction for all charts. If one chart failed, the entire ingestion would roll back and stop.

**After**: Each chart is processed independently in `_ingest_single_chart()`:
- Failures are caught at multiple levels (parse errors, empty results, DB errors)
- Failed snapshots are recorded in the database with status='failed' and error message
- Debug HTML is still saved for troubleshooting
- Execution continues to the next chart instead of stopping

**Failure Handling**:
1. **Parse failures**: Catch exceptions from `parse_chart()`, save debug HTML, record failed snapshot
2. **Empty results**: Detect `count=0` responses, record as failed with clear message
3. **Database errors**: Catch and log DB write failures, continue processing
4. **Network errors**: Already handled by existing retry logic in `fetch.py`

### 3. Parse Detection (src/bpc/fetch.py)

Enhanced `_parse_chart_from_next_data_order()` to detect the "empty results" condition in `__NEXT_DATA__`:
- Checks for `count: 0` with `results: []`
- Raises `ValueError` with descriptive message: "Empty results in __NEXT_DATA__ (count=0, results=[]). Beatport may have disabled hype for this genre or served a landing page."
- This is caught by ingestion logic and recorded as a controlled failure

### 4. Status Reporting (src/bpc/status.py)

Updated `_latest_snapshot_info()` and status display to distinguish:
- **MISSING**: No snapshot rows exist for the chart (never attempted or no DB record)
- **FAILED**: Snapshot row exists with status='failed'
  - Shows date + "FAILED" marker
  - Displays truncated error message (first 60 chars)
  - Entry count shows as 0

**Example output**:
```
* bass-club-hype-100
  snapshots: 2026-01-06 FAILED (Empty results in __NEXT_DATA__ (count=0, result...) (entries: 0)
  metrics:   MISSING (rows: -)
```

## Acceptance Tests

### ✅ Database Migration
```bash
python -c "from src.bpc.db import get_conn, init_db; ..."
```
Verified new columns are created idempotently.

### ✅ Failure Tracking
```bash
python test_failure_tracking.py
```
Verified failed snapshots can be recorded with all fields.

### ✅ Status Display
```bash
python -m bpc.cli status
```
Verified status shows existing snapshots correctly (some as MISSING for hype charts that haven't been ingested).

## Benefits

1. **Resilience**: Ingestion completes even when some charts fail (e.g., Beatport disables hype for certain genres)
2. **Transparency**: Failed charts are visible in status, not silently missing
3. **Debugging**: HTML and error messages are preserved for troubleshooting
4. **Continuity**: Successful charts are processed and available for compute/report steps
5. **No Breaking Changes**: Existing functionality preserved; changes are additive

## Usage

### Run ingestion (will now complete even with failures):
```bash
python -m bpc.cli ingest --snapshot-date 2026-01-06
```

### Check status (shows FAILED vs MISSING):
```bash
python -m bpc.cli status
```

### Full pipeline (run-all continues to compute/report for successful snapshots):
```bash
python -m bpc.cli run-all --snapshot-date 2026-01-06
```

## Migration Notes

- Schema migration is **idempotent** - safe to run multiple times
- Existing snapshots will have `status='ok'`, `error=NULL`, `html_bytes=NULL` (defaults)
- No data loss or breaking changes
- First run of `init_db()` after upgrade will add the new columns automatically
