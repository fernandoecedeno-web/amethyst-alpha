import copy
import csv
from datetime import datetime, timezone
from pathlib import Path

from bot.utils import normalize_timestamp_utc


OPEN_FIELDS = [
    "timestamp", "ticker", "side", "crowd_prob", "model_prob", "ev", "position_usd",
    "hours_to_close", "family", "strike", "spot_price", "close_time", "tier"
]
RESOLVED_FIELDS = OPEN_FIELDS + ["resolved_yes", "resolved_no", "won", "pnl_usd", "exit_type"]


def ensure_trade_logs(open_trades_log, resolved_trades_log):
    if not Path(open_trades_log).exists():
        with open(open_trades_log, "w", newline="") as f:
            csv.writer(f).writerow(OPEN_FIELDS)
    if not Path(resolved_trades_log).exists():
        with open(resolved_trades_log, "w", newline="") as f:
            csv.writer(f).writerow(RESOLVED_FIELDS)
        return
    with open(resolved_trades_log, newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        if "exit_type" in headers:
            return
        rows = list(reader)
    with open(resolved_trades_log, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESOLVED_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            row.setdefault("exit_type", "expiry")
            writer.writerow(row)


def load_open_rows(offline_mode, offline_rows, open_trades_log):
    if offline_mode:
        return copy.deepcopy(offline_rows)
    if not Path(open_trades_log).exists():
        return []
    with open(open_trades_log, newline="") as f:
        return list(csv.DictReader(f))


def save_positions(rows, offline_mode, open_trades_log, offline_rows_ref):
    if offline_mode:
        offline_rows_ref.clear()
        offline_rows_ref.extend(copy.deepcopy(rows))
        return
    with open(open_trades_log, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OPEN_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def cleanup_position_state(
    ticker,
    side,
    reason,
    expected_entry_ts,
    entry_metrics,
    trade_state,
    peak_pnl_by_position_id,
    entry_cooldown_by_ticker,
    log,
    offline_lifecycle_test,
):
    position_id = f"{ticker}|{side}"
    key = (ticker, side)
    current_meta = entry_metrics.get(key, {})
    current_entry_ts = str(current_meta.get("entry_ts") or "")
    expected_entry_ts = str(expected_entry_ts or "")
    should_clear_entry = True
    if expected_entry_ts and current_entry_ts and current_entry_ts != expected_entry_ts:
        should_clear_entry = False
    if should_clear_entry:
        if key in entry_metrics:
            msg = (
                f"[ENTRY_METRICS_CLEAR] ticker={ticker} side={side}"
                f" reason={reason} entry_ts={current_entry_ts or expected_entry_ts or '?'}"
            )
            if offline_lifecycle_test:
                log.info(msg)
            else:
                log.debug(msg)
        entry_metrics.pop(key, None)
    trade_state.pop((ticker, side), None)
    peak_pnl_by_position_id.pop(position_id, None)
    entry_cooldown_by_ticker.pop(ticker, None)


def record_open_trade(
    market,
    side,
    crowd,
    model,
    ev,
    size,
    hours,
    family,
    strike,
    spot_price,
    tier,
    replace,
    load_rows_fn,
    save_positions_fn,
    last_exit_meta_by_ticker,
    log,
):
    ticker = market.get("ticker", "")
    close_time = market.get("close_time") or market.get("expiration_time", "")
    existing_rows = load_rows_fn()
    existing_keys = {(row["ticker"], row["side"]) for row in existing_rows}
    original_ts = None
    if (ticker, side) in existing_keys:
        if not replace:
            return None
        original_ts = next((row.get("timestamp") for row in existing_rows if row["ticker"] == ticker and row["side"] == side), None)
        existing_rows = [row for row in existing_rows if not (row["ticker"] == ticker and row["side"] == side)]
    written_ts = normalize_timestamp_utc(original_ts or datetime.now(timezone.utc).isoformat()).isoformat()
    normalized_close_time = normalize_timestamp_utc(close_time)
    existing_rows.append({
        "timestamp": written_ts,
        "ticker": ticker,
        "side": side,
        "crowd_prob": f"{crowd:.4f}",
        "model_prob": f"{model:.4f}",
        "ev": f"{ev:.4f}",
        "position_usd": size,
        "hours_to_close": f"{hours:.2f}",
        "family": family,
        "strike": strike if strike is not None else "",
        "spot_price": f"{spot_price:.2f}" if spot_price is not None else "",
        "close_time": normalized_close_time.isoformat() if normalized_close_time is not None else close_time,
        "tier": tier,
    })
    save_positions_fn(existing_rows)
    reentered = (not replace) and (ticker in last_exit_meta_by_ticker)
    action = "REENTER" if (replace or reentered) else "OPEN"
    log.info(f"{action:<6} | {ticker:<28} | {side.upper():<3} | ${size:>5.2f} @ {crowd:>5.1%}")
    return written_ts
