import json
from unittest.mock import MagicMock, patch

import pytest

from src.client import OpenFigiClient, RateLimiter


class TestRateLimiter:
    def test_no_wait_under_limit(self):
        limiter = RateLimiter(calls=5, period=60)
        # Should not sleep for first 5 calls
        with patch("src.client.time.sleep") as mock_sleep:
            for _ in range(5):
                limiter.wait()
            mock_sleep.assert_not_called()

    def test_waits_when_limit_reached(self):
        limiter = RateLimiter(calls=2, period=60)
        with patch("src.client.time.sleep") as mock_sleep, \
             patch("src.client.time.monotonic") as mock_time:
            mock_time.return_value = 100.0
            limiter.wait()  # call 1

            mock_time.return_value = 100.5
            limiter.wait()  # call 2

            mock_time.return_value = 101.0
            limiter.wait()  # call 3 — should wait
            mock_sleep.assert_called_once()


class TestOpenFigiClient:
    def _make_client(self):
        with patch.dict("os.environ", {"OPENFIGI_API_KEY": "test-key"}):
            client = OpenFigiClient(api_key="test-key")
        client.rate_limiter = RateLimiter(calls=100, period=1)  # effectively no limit
        return client

    def test_get_exchange_codes(self):
        client = self._make_client()
        mock_resp = MagicMock()
        mock_resp.json.return_value = ["US", "LN", "JP"]
        mock_resp.raise_for_status = MagicMock()
        client.session.get = MagicMock(return_value=mock_resp)

        codes = client.get_exchange_codes()
        assert codes == ["US", "LN", "JP"]

    def test_filter_etp_total(self):
        client = self._make_client()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"total": 5923, "data": []}
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status_code = 200
        client.session.post = MagicMock(return_value=mock_resp)

        total = client.filter_etp_total("US")
        assert total == 5923

    def test_paginate_single_page(self):
        client = self._make_client()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "total": 2,
            "data": [
                {"figi": "F1", "compositeFIGI": "C1", "name": "ETF 1", "exchCode": "US"},
                {"figi": "F2", "compositeFIGI": "C2", "name": "ETF 2", "exchCode": "US"},
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status_code = 200
        client.session.post = MagicMock(return_value=mock_resp)

        records, total = client.paginate_exchange("US")
        assert total == 2
        assert len(records) == 2
        assert records[0].figi == "F1"

    def test_paginate_multi_page(self):
        client = self._make_client()

        resp1 = MagicMock()
        resp1.json.return_value = {
            "total": 3,
            "next": "abc123",
            "data": [
                {"figi": "F1", "compositeFIGI": "C1"},
                {"figi": "F2", "compositeFIGI": "C2"},
            ],
        }
        resp1.raise_for_status = MagicMock()
        resp1.status_code = 200

        resp2 = MagicMock()
        resp2.json.return_value = {
            "total": 3,
            "data": [
                {"figi": "F3", "compositeFIGI": "C3"},
            ],
        }
        resp2.raise_for_status = MagicMock()
        resp2.status_code = 200

        client.session.post = MagicMock(side_effect=[resp1, resp2])

        records, total = client.paginate_exchange("US")
        assert total == 3
        assert len(records) == 3

    def test_retry_on_429(self):
        client = self._make_client()

        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_429.raise_for_status = MagicMock()

        resp_ok = MagicMock()
        resp_ok.status_code = 200
        resp_ok.json.return_value = {"total": 1, "data": []}
        resp_ok.raise_for_status = MagicMock()

        client.session.post = MagicMock(side_effect=[resp_429, resp_ok])

        with patch("src.client.time.sleep"):
            total = client.filter_etp_total("US")
        assert total == 1
        assert client.session.post.call_count == 2
