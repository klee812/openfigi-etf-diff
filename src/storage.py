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
    """Load a persisted snapshot from disk.

    If the file does not exist, returns ``None`` rather than raising an error,
    so callers can detect a first-run scenario.

    Args:
        path: Path to the JSON snapshot file. Defaults to
            ``config.SNAPSHOT_PATH`` if ``None``.

    Returns:
        A :class:`~models.FullSnapshot` deserialized from the file, or
        ``None`` if the file does not exist.

    Raises:
        json.JSONDecodeError: If the file exists but contains invalid JSON.
        OSError: If the file cannot be read for reasons other than not existing.
    """
    path = path or config.SNAPSHOT_PATH
    if not path.exists():
        return None
    with open(path, "r") as f:
        data = json.load(f)
    logger.info("Loaded snapshot from %s (%d exchanges)", path, len(data.get("exchanges", {})))
    return FullSnapshot.from_dict(data)


def save_snapshot(snapshot: FullSnapshot, path: Path | None = None) -> None:
    """Atomically persist a snapshot to disk as JSON.

    Writes to a temporary file in the same directory, then calls
    ``os.replace()`` to atomically swap it into place. This prevents a partial
    write from corrupting the existing snapshot if the process is interrupted.
    The parent directory is created if it does not already exist.

    Args:
        snapshot: The :class:`~models.FullSnapshot` to serialize and save.
        path: Destination path for the JSON file. Defaults to
            ``config.SNAPSHOT_PATH`` if ``None``.

    Raises:
        OSError: If the temporary file cannot be created or the replace fails.
    """
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
    """Export new ETF records from a diff result to a timestamped CSV file.

    The output filename is of the form ``new_etfs_YYYY-MM-DD_HHMMSS.csv``.
    The output directory is created if it does not already exist.

    Args:
        diff: A :class:`~models.DiffResult` whose ``new_records`` will be
            written as rows.
        output_dir: Directory in which to create the CSV. Defaults to
            ``config.OUTPUT_DIR`` if ``None``.

    Returns:
        The :class:`~pathlib.Path` of the newly created CSV file.

    Raises:
        OSError: If the output file cannot be created or written.
    """
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
    """Export new ETF records from a diff result to a timestamped JSON file.

    The output file contains a ``summary`` object with run statistics and a
    ``new_records`` array with camelCase instrument objects. The output
    filename is of the form ``new_etfs_YYYY-MM-DD_HHMMSS.json``.

    Args:
        diff: A :class:`~models.DiffResult` whose ``new_records`` and summary
            statistics will be serialized.
        output_dir: Directory in which to create the JSON file. Defaults to
            ``config.OUTPUT_DIR`` if ``None``.

    Returns:
        The :class:`~pathlib.Path` of the newly created JSON file.

    Raises:
        OSError: If the output file cannot be created or written.
    """
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
