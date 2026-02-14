from __future__ import annotations

import logging

from . import config
from .client import OpenFigiClient
from .models import DiffResult, ExchangeSnapshot, FigiRecord, FullSnapshot
from .storage import save_snapshot

logger = logging.getLogger(__name__)


def full_scan(client: OpenFigiClient) -> FullSnapshot:
    """Enumerate all exchange codes, paginate each, build and save snapshot.

    Checkpoints every CHECKPOINT_INTERVAL exchanges so interrupted scans
    don't lose all progress.
    """
    exchange_codes = client.get_exchange_codes()
    logger.info("Found %d exchange codes", len(exchange_codes))

    snapshot = FullSnapshot(timestamp=FullSnapshot.now_timestamp())
    all_figis: set[str] = set()

    for i, code in enumerate(exchange_codes, 1):
        logger.info("[%d/%d] Scanning exchange %s", i, len(exchange_codes), code)
        try:
            records, total = client.paginate_exchange(code)
            composite_figis = [
                r.composite_figi for r in records if r.composite_figi
            ]
            # Deduplicate within this exchange
            unique_figis = list(dict.fromkeys(composite_figis))
            all_figis.update(unique_figis)

            snapshot.exchanges[code] = ExchangeSnapshot(
                exch_code=code,
                total=total,
                figis=unique_figis,
                last_scanned=FullSnapshot.now_timestamp(),
            )
            logger.info(
                "  %s: %d records, %d unique composite FIGIs (total=%d)",
                code, len(records), len(unique_figis), total,
            )
        except Exception:
            logger.exception("Error scanning exchange %s, skipping", code)

        # Checkpoint
        if i % config.CHECKPOINT_INTERVAL == 0:
            snapshot.all_composite_figis = all_figis.copy()
            save_snapshot(snapshot)
            logger.info("Checkpoint saved after %d exchanges", i)

    snapshot.all_composite_figis = all_figis
    snapshot.timestamp = FullSnapshot.now_timestamp()
    save_snapshot(snapshot)
    logger.info(
        "Full scan complete: %d exchanges, %d unique composite FIGIs",
        len(snapshot.exchanges), len(all_figis),
    )
    return snapshot


def incremental_diff(client: OpenFigiClient, previous: FullSnapshot) -> DiffResult:
    """For each exchange, fetch total. If increased or new, paginate and diff.

    Merges new data into the snapshot and saves. Returns DiffResult.
    """
    exchange_codes = client.get_exchange_codes()
    logger.info("Checking %d exchanges for changes", len(exchange_codes))

    diff = DiffResult(
        total_before=len(previous.all_composite_figis),
    )

    for i, code in enumerate(exchange_codes, 1):
        diff.exchanges_checked += 1
        prev_snap = previous.exchanges.get(code)
        prev_total = prev_snap.total if prev_snap else 0

        try:
            current_total = client.filter_etp_total(code)
        except Exception:
            logger.exception("Error checking total for %s, skipping", code)
            continue

        if current_total <= prev_total and prev_snap is not None:
            logger.debug("[%d/%d] %s: unchanged (%d)", i, len(exchange_codes), code, current_total)
            continue

        logger.info(
            "[%d/%d] %s: changed %d -> %d, paginating",
            i, len(exchange_codes), code, prev_total, current_total,
        )
        diff.exchanges_changed += 1

        try:
            records, total = client.paginate_exchange(code)
        except Exception:
            logger.exception("Error paginating exchange %s, skipping", code)
            continue

        composite_figis = [r.composite_figi for r in records if r.composite_figi]
        unique_figis = list(dict.fromkeys(composite_figis))

        # Find new composite FIGIs
        new_figis = set(unique_figis) - previous.all_composite_figis
        if new_figis:
            # Collect full records for new FIGIs (deduplicated by composite_figi)
            seen = set()
            for record in records:
                if record.composite_figi in new_figis and record.composite_figi not in seen:
                    diff.new_records.append(record)
                    seen.add(record.composite_figi)

        # Update snapshot
        previous.exchanges[code] = ExchangeSnapshot(
            exch_code=code,
            total=total,
            figis=unique_figis,
            last_scanned=FullSnapshot.now_timestamp(),
        )
        previous.all_composite_figis.update(unique_figis)

    previous.timestamp = FullSnapshot.now_timestamp()
    diff.total_after = len(previous.all_composite_figis)
    save_snapshot(previous)

    logger.info("Incremental diff complete:\n%s", diff.summary())
    return diff
