from unittest.mock import MagicMock, patch

import pytest

from src.models import ExchangeSnapshot, FigiRecord, FullSnapshot
from src.scanner import full_scan, incremental_diff


@pytest.fixture
def mock_client():
    client = MagicMock()
    return client


class TestFullScan:
    @patch("src.scanner.save_snapshot")
    def test_basic_scan(self, mock_save, mock_client):
        mock_client.get_exchange_codes.return_value = ["US", "LN"]
        mock_client.paginate_exchange.side_effect = [
            (
                [
                    FigiRecord(figi="F1", composite_figi="C1", exch_code="US"),
                    FigiRecord(figi="F2", composite_figi="C2", exch_code="US"),
                ],
                2,
            ),
            (
                [
                    FigiRecord(figi="F3", composite_figi="C3", exch_code="LN"),
                ],
                1,
            ),
        ]

        snapshot = full_scan(mock_client)
        assert len(snapshot.exchanges) == 2
        assert snapshot.all_composite_figis == {"C1", "C2", "C3"}
        assert snapshot.exchanges["US"].total == 2
        assert snapshot.exchanges["LN"].total == 1

    @patch("src.scanner.save_snapshot")
    def test_scan_skips_errored_exchanges(self, mock_save, mock_client):
        mock_client.get_exchange_codes.return_value = ["US", "BAD", "LN"]
        mock_client.paginate_exchange.side_effect = [
            ([FigiRecord(figi="F1", composite_figi="C1")], 1),
            Exception("API error"),
            ([FigiRecord(figi="F2", composite_figi="C2")], 1),
        ]

        snapshot = full_scan(mock_client)
        assert len(snapshot.exchanges) == 2
        assert "BAD" not in snapshot.exchanges

    @patch("src.scanner.save_snapshot")
    def test_scan_deduplicates(self, mock_save, mock_client):
        mock_client.get_exchange_codes.return_value = ["US"]
        mock_client.paginate_exchange.return_value = (
            [
                FigiRecord(figi="F1", composite_figi="C1"),
                FigiRecord(figi="F2", composite_figi="C1"),  # duplicate composite
            ],
            2,
        )

        snapshot = full_scan(mock_client)
        assert len(snapshot.exchanges["US"].figis) == 1
        assert snapshot.all_composite_figis == {"C1"}


class TestIncrementalDiff:
    @patch("src.scanner.save_snapshot")
    def test_no_changes(self, mock_save, mock_client):
        previous = FullSnapshot(
            timestamp="2024-01-01T00:00:00Z",
            exchanges={
                "US": ExchangeSnapshot(exch_code="US", total=100, figis=["C1", "C2"]),
            },
            all_composite_figis={"C1", "C2"},
        )
        mock_client.get_exchange_codes.return_value = ["US"]
        mock_client.filter_etp_total.return_value = 100  # unchanged

        diff = incremental_diff(mock_client, previous)
        assert diff.new_count == 0
        assert diff.exchanges_checked == 1
        assert diff.exchanges_changed == 0
        mock_client.paginate_exchange.assert_not_called()

    @patch("src.scanner.save_snapshot")
    def test_detects_new_records(self, mock_save, mock_client):
        previous = FullSnapshot(
            timestamp="2024-01-01T00:00:00Z",
            exchanges={
                "US": ExchangeSnapshot(exch_code="US", total=2, figis=["C1", "C2"]),
            },
            all_composite_figis={"C1", "C2"},
        )
        mock_client.get_exchange_codes.return_value = ["US"]
        mock_client.filter_etp_total.return_value = 3  # increased
        mock_client.paginate_exchange.return_value = (
            [
                FigiRecord(figi="F1", composite_figi="C1"),
                FigiRecord(figi="F2", composite_figi="C2"),
                FigiRecord(figi="F3", composite_figi="C3", name="New ETF"),
            ],
            3,
        )

        diff = incremental_diff(mock_client, previous)
        assert diff.new_count == 1
        assert diff.new_records[0].composite_figi == "C3"
        assert diff.exchanges_changed == 1

    @patch("src.scanner.save_snapshot")
    def test_new_exchange(self, mock_save, mock_client):
        previous = FullSnapshot(
            timestamp="2024-01-01T00:00:00Z",
            exchanges={
                "US": ExchangeSnapshot(exch_code="US", total=2, figis=["C1"]),
            },
            all_composite_figis={"C1"},
        )
        mock_client.get_exchange_codes.return_value = ["US", "LN"]
        mock_client.filter_etp_total.side_effect = [2, 1]  # US unchanged, LN is new
        mock_client.paginate_exchange.return_value = (
            [FigiRecord(figi="F2", composite_figi="C2", exch_code="LN")],
            1,
        )

        diff = incremental_diff(mock_client, previous)
        assert diff.new_count == 1
        assert diff.new_records[0].composite_figi == "C2"
        assert diff.exchanges_changed == 1
        assert "LN" in previous.exchanges
