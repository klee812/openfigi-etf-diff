# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install (editable, with test deps)
pip install -e ".[dev]"

# Run tests
python3 -m pytest tests/

# Run a single test file or test
python3 -m pytest tests/test_scanner.py
python3 -m pytest tests/test_scanner.py::TestIncrementalDiff::test_detects_new_records

# Run CLI (must be from project root)
python3 -m src.main <command>
# Commands: scan, diff --format csv|json|both, exchanges, info
# Add -v for debug logging
```

Requires `OPENFIGI_API_KEY` environment variable for any command that hits the API.

## Architecture

The tool snapshots all ETF/ETP listings from OpenFIGI and diffs against previous snapshots to find new listings.

**Data flow:** `client.py` fetches from the API → `scanner.py` orchestrates full scans or incremental diffs → `storage.py` persists snapshots and exports → `main.py` wires CLI commands.

**Key design decisions:**
- **Dedup on `compositeFIGI`** (one per instrument per country) rather than raw `figi` (per-exchange-listing). This is the primary identity key throughout.
- **Incremental diff strategy:** For each exchange, fetch only the `total` count (1 API call). Only paginate exchanges where the total increased or the exchange is new. This reduces daily runs from ~5 hours to ~13 minutes.
- **Checkpointing:** `full_scan` saves the snapshot every 50 exchanges so interrupted multi-hour scans don't lose progress.
- **Atomic writes:** Snapshots are written to a temp file then `os.replace()`'d to prevent corruption from partial writes.
- **Rate limiting:** Sliding-window limiter (20 calls/60s) in `RateLimiter`, plus retry with backoff (10/30/60s) on HTTP 429 in `_post_filter()`.
- **Per-exchange error isolation:** If one exchange fails during scan/diff, it's logged and skipped — the rest continue.

**API partitioning:** The OpenFIGI filter endpoint caps results at 15K per query, so queries are partitioned by `exchCode` to get global coverage (~614K FIGIs across all exchanges).

## Models (`models.py`)

`FigiRecord.from_api(dict)` maps camelCase API fields to snake_case attributes via `_API_FIELD_MAP`. `to_dict()` reverses the mapping. All tests mock the API client — no real HTTP calls in the test suite.

## Data files (gitignored)

- `data/snapshot.json` — ~10-15 MB JSON with all exchange snapshots and a flat set of all composite FIGIs
- `data/output/new_etfs_*.csv|json` — timestamped diff exports
