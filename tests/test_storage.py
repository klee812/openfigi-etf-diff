import csv
import json
from pathlib import Path

import pytest

from src.models import DiffResult, ExchangeSnapshot, FigiRecord, FullSnapshot
from src.storage import (
    export_diff_csv,
    export_diff_json,
    load_snapshot,
    save_snapshot,
)


@pytest.fixture
def tmp_data_dir(tmp_path):
    return tmp_path


@pytest.fixture
def sample_snapshot():
    return FullSnapshot(
        timestamp="2024-01-01T00:00:00Z",
        exchanges={
            "US": ExchangeSnapshot(
                exch_code="US", total=100, figis=["BBG1", "BBG2"],
                last_scanned="2024-01-01T00:00:00Z",
            ),
        },
        all_composite_figis={"BBG1", "BBG2"},
    )


class TestSnapshotIO:
    def test_save_and_load(self, tmp_data_dir, sample_snapshot):
        path = tmp_data_dir / "snapshot.json"
        save_snapshot(sample_snapshot, path)
        assert path.exists()

        loaded = load_snapshot(path)
        assert loaded is not None
        assert loaded.timestamp == "2024-01-01T00:00:00Z"
        assert "US" in loaded.exchanges
        assert loaded.exchanges["US"].total == 100
        assert loaded.all_composite_figis == {"BBG1", "BBG2"}

    def test_load_nonexistent(self, tmp_data_dir):
        result = load_snapshot(tmp_data_dir / "nope.json")
        assert result is None

    def test_atomic_write(self, tmp_data_dir, sample_snapshot):
        path = tmp_data_dir / "snapshot.json"
        save_snapshot(sample_snapshot, path)

        # Verify it's valid JSON
        with open(path) as f:
            data = json.load(f)
        assert data["timestamp"] == "2024-01-01T00:00:00Z"


class TestExport:
    def test_export_csv(self, tmp_data_dir):
        diff = DiffResult(
            new_records=[
                FigiRecord(figi="F1", composite_figi="C1", name="ETF One", ticker="E1", exch_code="US"),
                FigiRecord(figi="F2", composite_figi="C2", name="ETF Two", ticker="E2", exch_code="LN"),
            ],
        )
        path = export_diff_csv(diff, output_dir=tmp_data_dir)
        assert path.exists()
        assert path.name.startswith("new_etfs_")
        assert path.suffix == ".csv"

        with open(path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 2
        assert rows[0]["figi"] == "F1"
        assert rows[1]["exch_code"] == "LN"

    def test_export_json(self, tmp_data_dir):
        diff = DiffResult(
            new_records=[
                FigiRecord(figi="F1", composite_figi="C1", name="ETF One"),
            ],
            exchanges_checked=5,
            exchanges_changed=1,
            total_before=100,
            total_after=101,
        )
        path = export_diff_json(diff, output_dir=tmp_data_dir)
        assert path.exists()

        with open(path) as f:
            data = json.load(f)
        assert data["summary"]["new_count"] == 1
        assert data["summary"]["exchanges_checked"] == 5
        assert len(data["new_records"]) == 1
        assert data["new_records"][0]["figi"] == "F1"

    def test_export_empty_diff(self, tmp_data_dir):
        diff = DiffResult()
        path = export_diff_csv(diff, output_dir=tmp_data_dir)
        with open(path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 0
