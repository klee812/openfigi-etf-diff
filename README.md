# openfigi-etf-diff

Monitor global ETF/ETP listings using the [OpenFIGI API](https://www.openfigi.com/api). The tool takes a full snapshot of every exchange, then runs fast incremental diffs daily to detect new listings.

## Features

- **Full scan**: paginate all ~614K ETP FIGIs across every exchange (~5 hours, checkpointed every 50 exchanges)
- **Incremental diff**: check only totals per exchange, paginate only those that grew (~13 minutes)
- **Deduplication** on `compositeFIGI` — one record per instrument per country
- **Export** new listings to CSV and/or JSON with UTC timestamps
- **Rate limiting**: sliding-window (20 req/60 s) + exponential backoff on HTTP 429

## Requirements

- Python 3.12+
- An [OpenFIGI API key](https://www.openfigi.com/api#api-key)

## Installation

```bash
# Clone the repo
git clone <repo-url>
cd openfigi-etf-diff

# Install in editable mode (with test dependencies)
pip install -e ".[dev]"
```

## Configuration

Set your API key as an environment variable or in a `.env` file in the project root:

```bash
# Option 1: export directly
export OPENFIGI_API_KEY=your-key-here

# Option 2: .env file (loaded automatically)
echo "OPENFIGI_API_KEY=your-key-here" > .env
```

## Usage

All commands are run as a Python module from the project root:

```bash
python3 -m src.main <command> [options]
```

Add `-v` / `--verbose` to any command for DEBUG-level logging.

### Commands

| Command | Description |
|---|---|
| `exchanges` | List all valid exchange codes from the API |
| `scan` | Full scan — paginate every exchange and save a snapshot |
| `diff` | Incremental diff against the last snapshot |
| `info` | Print statistics about the current snapshot |

#### `exchanges`

```bash
python3 -m src.main exchanges
```

Prints all exchange codes available on OpenFIGI (e.g. `US`, `LN`, `HK`, ...).

#### `scan`

```bash
python3 -m src.main scan
```

Fetches all ETP listings across every exchange and saves them to `data/snapshot.json`. Run this once before using `diff`. A full scan takes several hours; progress is checkpointed every 50 exchanges.

#### `diff`

```bash
python3 -m src.main diff [--format csv|json|both]
```

Compares the current API state against the saved snapshot and reports new listings. Exports results to `data/output/new_etfs_<timestamp>.csv` and/or `.json`.

- `--format csv` — CSV only
- `--format json` — JSON only
- `--format both` — both (default)

Requires a snapshot from a previous `scan` run.

#### `info`

```bash
python3 -m src.main info
```

Prints the snapshot timestamp, total exchange and FIGI counts, and a table of the top 20 exchanges by listing count.

## Data files

All data files are gitignored.

| Path | Description |
|---|---|
| `data/snapshot.json` | Full snapshot (~10–15 MB JSON) |
| `data/output/new_etfs_*.csv` | Timestamped CSV exports of new listings |
| `data/output/new_etfs_*.json` | Timestamped JSON exports of new listings |

## Architecture

```
src/
  config.py    — API endpoints, rate-limit settings, file paths, env key loader
  models.py    — FigiRecord, ExchangeSnapshot, FullSnapshot, DiffResult dataclasses
  client.py    — OpenFigiClient (HTTP, rate limiting, pagination, retry)
  scanner.py   — full_scan() and incremental_diff() orchestration
  storage.py   — load/save snapshot (atomic write), CSV/JSON export
  main.py      — argparse CLI wiring
```

**Data flow:** `client.py` fetches from the API → `scanner.py` orchestrates scans/diffs → `storage.py` persists snapshots and exports → `main.py` wires CLI commands.

**Key design decisions:**

- **Dedup on `compositeFIGI`** (one per instrument per country) rather than raw `figi` (per-exchange-listing).
- **Incremental diff strategy:** fetch only the `total` count per exchange (1 call each). Only paginate exchanges where the total increased or the exchange is new.
- **Checkpointing:** `full_scan` saves every 50 exchanges so interrupted scans don't lose progress.
- **Atomic writes:** snapshots are written to a temp file then `os.replace()`'d to prevent corruption.
- **Rate limiting:** sliding-window limiter (20 calls/60 s) plus retry with backoff (10/30/60 s) on HTTP 429.
- **Per-exchange error isolation:** if one exchange fails, it is logged and skipped; the rest continue.

## Running tests

```bash
python3 -m pytest tests/
```

All tests mock the API client — no real HTTP calls are made.
