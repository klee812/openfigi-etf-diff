from src.models import DiffResult, ExchangeSnapshot, FigiRecord, FullSnapshot


class TestFigiRecord:
    def test_from_api(self):
        api_data = {
            "figi": "BBG000BHTMY2",
            "compositeFIGI": "BBG000BHTMY3",
            "shareClassFIGI": "BBG000BHTMY4",
            "name": "SPDR S&P 500 ETF Trust",
            "ticker": "SPY",
            "exchCode": "US",
            "securityType": "ETP",
            "securityType2": "ETF",
            "marketSector": "Equity",
            "securityDescription": "SPY",
        }
        record = FigiRecord.from_api(api_data)
        assert record.figi == "BBG000BHTMY2"
        assert record.composite_figi == "BBG000BHTMY3"
        assert record.ticker == "SPY"
        assert record.exch_code == "US"
        assert record.security_type == "ETP"

    def test_from_api_missing_fields(self):
        record = FigiRecord.from_api({"figi": "BBG123"})
        assert record.figi == "BBG123"
        assert record.composite_figi == ""
        assert record.name == ""

    def test_to_dict_roundtrip(self):
        api_data = {
            "figi": "BBG000BHTMY2",
            "compositeFIGI": "BBG000BHTMY3",
            "name": "Test ETF",
            "ticker": "TEST",
            "exchCode": "US",
        }
        record = FigiRecord.from_api(api_data)
        d = record.to_dict()
        assert d["figi"] == "BBG000BHTMY2"
        assert d["compositeFIGI"] == "BBG000BHTMY3"
        assert d["name"] == "Test ETF"

    def test_to_csv_row(self):
        record = FigiRecord(figi="F1", composite_figi="C1", name="Test")
        row = record.to_csv_row()
        assert row["figi"] == "F1"
        assert row["composite_figi"] == "C1"
        assert row["name"] == "Test"
        assert "security_description" in row


class TestExchangeSnapshot:
    def test_roundtrip(self):
        snap = ExchangeSnapshot(
            exch_code="US", total=100, figis=["BBG1", "BBG2"], last_scanned="2024-01-01T00:00:00Z"
        )
        d = snap.to_dict()
        restored = ExchangeSnapshot.from_dict(d)
        assert restored.exch_code == "US"
        assert restored.total == 100
        assert restored.figis == ["BBG1", "BBG2"]


class TestFullSnapshot:
    def test_roundtrip(self):
        snap = FullSnapshot(
            timestamp="2024-01-01T00:00:00Z",
            exchanges={
                "US": ExchangeSnapshot(exch_code="US", total=100, figis=["BBG1", "BBG2"]),
            },
            all_composite_figis={"BBG1", "BBG2"},
        )
        d = snap.to_dict()
        restored = FullSnapshot.from_dict(d)
        assert restored.timestamp == "2024-01-01T00:00:00Z"
        assert "US" in restored.exchanges
        assert restored.exchanges["US"].total == 100
        assert restored.all_composite_figis == {"BBG1", "BBG2"}

    def test_now_timestamp(self):
        ts = FullSnapshot.now_timestamp()
        assert ts.endswith("Z")
        assert "T" in ts


class TestDiffResult:
    def test_empty_diff(self):
        diff = DiffResult()
        assert diff.new_count == 0
        assert "0" in diff.summary()

    def test_with_records(self):
        diff = DiffResult(
            new_records=[FigiRecord(figi="F1"), FigiRecord(figi="F2")],
            exchanges_checked=10,
            exchanges_changed=2,
            total_before=100,
            total_after=102,
        )
        assert diff.new_count == 2
        summary = diff.summary()
        assert "10" in summary
        assert "2" in summary
