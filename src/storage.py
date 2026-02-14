from __future__ import annotations

import csv
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from . import config
from .models import DiffResult, FigiRecord, FullSnapshot

logger = logging.getLogger(__name__)


def load_snapshot(path: Path | None = None) -> FullSnapshot | None:
    path = path or config.SNAPSHOT_PATH
    if not path.exists():
        return None
    with open(path, "r") as f:
        data = json.load(f)
    logger.info("Loaded snapshot from %s (%d exchanges)", path, len(data.get("exchanges", {})))
    return FullSnapshot.from_dict(data)


def save_snapshot(snapshot: FullSnapshot, path: Path | None = None) -> None:
    path = path or config.SNAPSHOT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    data = snapshot.to_dict()
    # Atomic write: write to temp file then replace
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.replace(tmp_path, path)
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    logger.info("Saved snapshot to %s", path)


def export_diff_csv(diff: DiffResult, output_dir: Path | None = None) -> Path:
    output_dir = output_dir or config.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    path = output_dir / f"new_etfs_{ts}.csv"
    fieldnames = [
        "figi", "composite_figi", "share_class_figi", "name", "ticker",
        "exch_code", "security_type", "security_type2", "market_sector",
        "security_description",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in diff.new_records:
            writer.writerow(record.to_csv_row())
    logger.info("Exported %d records to %s", len(diff.new_records), path)
    return path


def export_diff_json(diff: DiffResult, output_dir: Path | None = None) -> Path:
    output_dir = output_dir or config.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    path = output_dir / f"new_etfs_{ts}.json"
    data = {
        "summary": {
            "exchanges_checked": diff.exchanges_checked,
            "exchanges_changed": diff.exchanges_changed,
            "new_count": diff.new_count,
            "total_before": diff.total_before,
            "total_after": diff.total_after,
        },
        "new_records": [r.to_dict() for r in diff.new_records],
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info("Exported %d records to %s", len(diff.new_records), path)
    return path
