import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

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

# Retry backoff for 429 responses and connection errors
RETRY_BACKOFFS = [10, 30, 60]
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30  # seconds

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
    """Read the OpenFIGI API key from the environment.

    Looks for the ``OPENFIGI_API_KEY`` environment variable, which may be set
    directly or loaded from a ``.env`` file in the project root.

    Returns:
        The API key string.

    Raises:
        RuntimeError: If the environment variable is not set or is empty.
    """
    key = os.environ.get("OPENFIGI_API_KEY")
    if not key:
        raise RuntimeError(
            "OPENFIGI_API_KEY environment variable is not set. "
            "Export it or add it to your .env file."
        )
    return key
