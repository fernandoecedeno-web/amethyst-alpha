import copy
import csv
from pathlib import Path

from bot.utils import normalize_timestamp_utc


def _canonicalize_position_row(row):
    normalized = dict(row)
    raw_entry_ts = normalized.get("timestamp", "") or normalized.get("entry_ts", "")
    entry_ts = normalize_timestamp_utc(raw_entry_ts)
    if entry_ts is not None:
        normalized["timestamp"] = entry_ts.isoformat()
    close_ts = normalize_timestamp_utc(normalized.get("close_time", ""))
    if close_ts is not None:
        normalized["close_time"] = close_ts.isoformat()
    normalized.pop("entry_ts", None)
    return normalized


def load_open_positions(open_trades_log, offline_mode=False, offline_rows=None):
    if offline_mode:
        return [_canonicalize_position_row(row) for row in copy.deepcopy(offline_rows or [])]
    if not Path(open_trades_log).exists():
        return []
    with open(open_trades_log, newline="") as f:
        return [_canonicalize_position_row(row) for row in csv.DictReader(f)]


def save_open_positions(rows, open_trades_log, open_fields, offline_mode=False, offline_rows_ref=None):
    normalized_rows = [_canonicalize_position_row(row) for row in rows]
    if offline_mode:
        if offline_rows_ref is not None:
            offline_rows_ref.clear()
            offline_rows_ref.extend(copy.deepcopy(normalized_rows))
        return
    with open(open_trades_log, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=open_fields)
        writer.writeheader()
        writer.writerows(normalized_rows)


def reconcile_open_positions(rows, now, log=None):
    reconciled = []
    counts = {"open": 0, "expired_unresolved": 0, "corrupt": 0}
    for row in rows:
        ticker = row.get("ticker", "?")
        side = row.get("side", "")
        if not ticker or not side:
            if log is not None:
                log.info(f"[RECONCILE] ticker={ticker or '?'} status=corrupt")
            counts["corrupt"] += 1
            continue
        close_dt = normalize_timestamp_utc(row.get("close_time", ""))
        if close_dt is not None:
            if close_dt <= now:
                if log is not None:
                    log.info(f"[RECONCILE] ticker={ticker} status=expired_unresolved")
                counts["expired_unresolved"] += 1
            else:
                if log is not None:
                    log.info(f"[RECONCILE] ticker={ticker} status=open")
                counts["open"] += 1
            reconciled.append(row)
            continue
        entry_dt = normalize_timestamp_utc(row.get("timestamp", ""))
        if entry_dt is None:
            if log is not None:
                log.info(f"[RECONCILE] ticker={ticker} status=corrupt")
            counts["corrupt"] += 1
            continue
        if log is not None:
            log.info(f"[RECONCILE] ticker={ticker} status=open")
        counts["open"] += 1
        reconciled.append(row)
    return reconciled, counts


def purge_stale_positions(rows, now, log=None):
    reconciled, counts = reconcile_open_positions(rows, now, log=log)
    removed = 0
    return reconciled, removed
