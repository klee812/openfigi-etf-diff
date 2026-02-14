import os
from pathlib import Path

API_BASE_URL = "https://api.openfigi.com"
FILTER_ENDPOINT = "/v3/filter"
MAPPING_VALUES_ENDPOINT = "/v3/mapping/values/exchCode"

ETP_FILTER_PARAMS = {
    "securityType": "ETP",
    "marketSecDes": "Equity",
}

# Authenticated rate limit: 20 requests per 60 seconds
RATE_LIMIT_CALLS = 20
RATE_LIMIT_PERIOD = 60  # seconds

# Retry backoff for 429 responses
RETRY_BACKOFFS = [10, 30, 60]
MAX_RETRIES = 3

# Pagination
PAGE_SIZE = 15000  # API max per request

# Checkpointing
CHECKPOINT_INTERVAL = 50  # Save every N exchanges during full scan

# Paths
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data"
SNAPSHOT_PATH = DATA_DIR / "snapshot.json"
OUTPUT_DIR = DATA_DIR / "output"


def get_api_key() -> str:
    key = os.environ.get("OPENFIGI_API_KEY")
    if not key:
        raise RuntimeError(
            "OPENFIGI_API_KEY environment variable is not set. "
            "Export it or add it to your .env file."
        )
    return key
