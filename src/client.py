from __future__ import annotations

import logging
import time
from collections import deque

import requests

from . import config
from .models import FigiRecord

logger = logging.getLogger(__name__)


class RateLimiter:
    """Sliding-window rate limiter: max ``calls`` within ``period`` seconds.

    Tracks the timestamps of recent API calls and blocks (sleeps) when the
    limit is reached until the oldest call falls outside the window.
    """

    def __init__(self, calls: int = config.RATE_LIMIT_CALLS, period: float = config.RATE_LIMIT_PERIOD):
        """Initialise the rate limiter.

        Args:
            calls: Maximum number of calls allowed within ``period`` seconds.
                Defaults to ``config.RATE_LIMIT_CALLS`` (20).
            period: Length of the sliding window in seconds.
                Defaults to ``config.RATE_LIMIT_PERIOD`` (60).
        """
        self.calls = calls
        self.period = period
        self._timestamps: deque[float] = deque()

    def wait(self) -> None:
        """Block until a new API call is permitted under the rate limit.

        Discards timestamps outside the current window, then sleeps if the
        call budget is exhausted. Records the current monotonic time once the
        call is allowed to proceed.
        """
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
    """HTTP client for the OpenFIGI v3 API.

    Handles authentication, rate limiting, pagination, and retry logic for
    429 responses. All requests go through ``_post_filter`` which enforces the
    sliding-window rate limiter and exponential-ish backoff on throttling.
    """

    def __init__(self, api_key: str | None = None):
        """Initialise the client with an API key and a shared requests Session.

        Args:
            api_key: OpenFIGI API key. If ``None``, reads from the
                ``OPENFIGI_API_KEY`` environment variable via
                ``config.get_api_key()``.
        """
        self.api_key = api_key or config.get_api_key()
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "X-OPENFIGI-APIKEY": self.api_key,
        })
        self.rate_limiter = RateLimiter()

    def get_exchange_codes(self) -> list[str]:
        """Fetch all valid exchange codes from the OpenFIGI mapping values endpoint.

        Calls ``GET /v3/mapping/values/exchCode`` and normalises the response,
        which may be a list of plain strings or a list of objects with a
        ``"values"`` or ``"value"`` key.

        Returns:
            A list of exchange code strings (e.g. ``["US", "LN", "HK", ...]``).

        Raises:
            requests.HTTPError: If the API returns a non-2xx status code.
        """
        url = config.API_BASE_URL + config.MAPPING_VALUES_ENDPOINT
        resp = self.session.get(url)
        resp.raise_for_status()
        data = resp.json()
        # API returns {"values": ["US", "LN", ...]}
        if isinstance(data, dict):
            return [str(v) for v in data.get("values", data.get("value", []))]
        # Fallback: flat list of strings
        return [str(v) for v in data]

    def filter_etp_total(self, exch_code: str) -> int:
        """Fetch only the total ETP count for a single exchange (no pagination).

        Makes a single POST to ``/v3/filter`` (no ``start`` cursor) and returns
        the ``total`` field from the response. Used by the incremental diff to
        cheaply check whether an exchange has grown.

        Args:
            exch_code: The OpenFIGI exchange code to query (e.g. ``"US"``).

        Returns:
            The total number of ETP instruments reported by the API for this
            exchange, or ``0`` if the field is absent.

        Raises:
            requests.HTTPError: If the API returns a non-2xx/non-429 status.
        """
        body = {**config.ETP_FILTER_PARAMS, "exchCode": exch_code}
        data = self._post_filter(body)
        return data.get("total", 0)

    def paginate_exchange(self, exch_code: str) -> tuple[list[FigiRecord], int]:
        """Paginate through all ETP results for a single exchange.

        Follows the ``next`` cursor returned by each page until the API
        signals there are no more results.

        Args:
            exch_code: The OpenFIGI exchange code to paginate (e.g. ``"LN"``).

        Returns:
            A 2-tuple of ``(records, total)`` where ``records`` is the full
            list of :class:`FigiRecord` objects fetched and ``total`` is the
            final total count reported by the API.

        Raises:
            requests.HTTPError: If any page request fails with a non-2xx status.
        """
        records: list[FigiRecord] = []
        start: str | None = None
        total = 0

        page = 0
        while True:
            page += 1
            body = {**config.ETP_FILTER_PARAMS, "exchCode": exch_code}
            if start is not None:
                body["start"] = start
            data = self._post_filter(body)
            total = data.get("total", total)
            raw_data = data.get("data", [])
            for item in raw_data:
                records.append(FigiRecord.from_api(item))
            logger.info(
                "  %s page %d: +%d records (%d/%d total)",
                exch_code, page, len(raw_data), len(records), total,
            )
            next_val = data.get("next")
            if next_val and raw_data:
                start = next_val
            else:
                break

        logger.debug("Exchange %s: fetched %d records (total=%d)", exch_code, len(records), total)
        return records, total

    def _post_filter(self, body: dict) -> dict:
        """Low-level POST to ``/v3/filter`` with rate limiting and 429 retry.

        Waits for the sliding-window rate limiter before each attempt, then
        retries on HTTP 429 using the backoff schedule in ``config.RETRY_BACKOFFS``.

        Args:
            body: The JSON request body to send to the filter endpoint.

        Returns:
            The parsed JSON response dictionary from the API.

        Raises:
            requests.HTTPError: If the request fails after all retries, or
                fails with a status code other than 429.
        """
        url = config.API_BASE_URL + config.FILTER_ENDPOINT

        for attempt in range(config.MAX_RETRIES + 1):
            self.rate_limiter.wait()
            try:
                resp = self.session.post(url, json=body, timeout=config.REQUEST_TIMEOUT)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                if attempt < config.MAX_RETRIES:
                    backoff = config.RETRY_BACKOFFS[min(attempt, len(config.RETRY_BACKOFFS) - 1)]
                    logger.warning(
                        "Connection error on attempt %d/%d, retrying in %ds: %s",
                        attempt + 1, config.MAX_RETRIES, backoff, exc,
                    )
                    time.sleep(backoff)
                    continue
                raise

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
