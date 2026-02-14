from __future__ import annotations

import logging
import time
from collections import deque

import requests

from . import config
from .models import FigiRecord

logger = logging.getLogger(__name__)


class RateLimiter:
    """Sliding-window rate limiter: max `calls` within `period` seconds."""

    def __init__(self, calls: int = config.RATE_LIMIT_CALLS, period: float = config.RATE_LIMIT_PERIOD):
        self.calls = calls
        self.period = period
        self._timestamps: deque[float] = deque()

    def wait(self) -> None:
        now = time.monotonic()
        # Discard timestamps outside the window
        while self._timestamps and self._timestamps[0] <= now - self.period:
            self._timestamps.popleft()
        if len(self._timestamps) >= self.calls:
            sleep_until = self._timestamps[0] + self.period
            wait_time = sleep_until - now
            if wait_time > 0:
                logger.debug("Rate limit reached, sleeping %.1fs", wait_time)
                time.sleep(wait_time)
        self._timestamps.append(time.monotonic())


class OpenFigiClient:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or config.get_api_key()
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "X-OPENFIGI-APIKEY": self.api_key,
        })
        self.rate_limiter = RateLimiter()

    def get_exchange_codes(self) -> list[str]:
        """GET /v3/mapping/values/exchCode — returns list of exchange code strings."""
        url = config.API_BASE_URL + config.MAPPING_VALUES_ENDPOINT
        resp = self.session.get(url)
        resp.raise_for_status()
        data = resp.json()
        # API returns a list of objects with "values" key, or a flat list
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return [item.get("values", item.get("value", "")) for item in data]
        return [str(v) for v in data]

    def filter_etp_total(self, exch_code: str) -> int:
        """Single filter request, return only the total count for this exchange."""
        body = {**config.ETP_FILTER_PARAMS, "exchCode": exch_code, "start": "0"}
        data = self._post_filter(body)
        return data.get("total", 0)

    def paginate_exchange(self, exch_code: str) -> tuple[list[FigiRecord], int]:
        """Paginate through all ETP results for an exchange.

        Returns (records, total).
        """
        records: list[FigiRecord] = []
        start: str | None = "0"
        total = 0

        while start is not None:
            body = {**config.ETP_FILTER_PARAMS, "exchCode": exch_code, "start": start}
            data = self._post_filter(body)
            total = data.get("total", total)
            raw_data = data.get("data", [])
            for item in raw_data:
                records.append(FigiRecord.from_api(item))
            next_val = data.get("next")
            if next_val and raw_data:
                start = next_val
            else:
                start = None

        logger.debug("Exchange %s: fetched %d records (total=%d)", exch_code, len(records), total)
        return records, total

    def _post_filter(self, body: dict) -> dict:
        """Low-level POST to the filter endpoint with rate limiting and 429 retry."""
        url = config.API_BASE_URL + config.FILTER_ENDPOINT

        for attempt in range(config.MAX_RETRIES + 1):
            self.rate_limiter.wait()
            resp = self.session.post(url, json=body)

            if resp.status_code == 429:
                if attempt < config.MAX_RETRIES:
                    backoff = config.RETRY_BACKOFFS[min(attempt, len(config.RETRY_BACKOFFS) - 1)]
                    logger.warning("429 rate limited, retry %d/%d in %ds", attempt + 1, config.MAX_RETRIES, backoff)
                    time.sleep(backoff)
                    continue
                else:
                    resp.raise_for_status()

            resp.raise_for_status()
            return resp.json()

        return {}  # unreachable
