from __future__ import annotations

import logging

from . import config
from .client import OpenFigiClient
from .models import DiffResult, ExchangeSnapshot, FigiRecord, FullSnapshot
from .storage import save_snapshot

logger = logging.getLogger(__name__)


def full_scan(client: OpenFigiClient) -> FullSnapshot:
    """Enumerate all exchange codes, paginate each one, and persist the result.

    Fetches every exchange code from the API, then paginates through all ETP
    listings for each exchange. Results are checkpointed to disk every
    ``config.CHECKPOINT_INTERVAL`` exchanges so that a long-running scan
    (which can take ~5 hours) does not lose progress if interrupted. A final
    save is performed at completion.

    Exchanges that raise an exception during pagination are logged and skipped;
    the scan continues with the remaining exchanges.

    Args:
        client: An authenticated :class:`~client.OpenFigiClient` instance.

    Returns:
        A :class:`~models.FullSnapshot` containing per-exchange snapshots and
        the flat set of all composite FIGIs discovered.
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
    """Detect new ETP listings by comparing the current API state to a previous snapshot.

    For each exchange, fetches only the total count (1 API call). If the total
    has increased since the previous snapshot, or if the exchange is new, the
    exchange is fully paginated and its new composite FIGIs are collected.
    Exchanges with unchanged totals are skipped entirely, reducing a ~5-hour
    full scan to ~13 minutes for a daily run.

    The ``previous`` snapshot is updated in-place with new data and saved to
    disk before this function returns. Exchanges that raise exceptions are
    logged and skipped; the diff continues with the rest.

    Args:
        client: An authenticated :class:`~client.OpenFigiClient` instance.
        previous: The most recent :class:`~models.FullSnapshot` loaded from
            disk, used as the baseline for comparison. Modified in-place.

    Returns:
        A :class:`~models.DiffResult` containing full :class:`~models.FigiRecord`
        objects for each newly discovered composite FIGI, along with summary
        statistics about the run.
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
            seen: set[str] = set()
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
