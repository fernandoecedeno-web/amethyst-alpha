import json
from pathlib import Path

from bot.utils import normalize_timestamp_utc


def read_json_state(path, default=None):
    if default is None:
        default = {}
    file_path = Path(path)
    if not file_path.exists():
        return default
    try:
        with open(file_path) as f:
            return json.load(f)
    except Exception:
        return default


def write_json_state(path, payload):
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(payload, f)


def load_runtime_state(path):
    raw = read_json_state(path, default={})
    last_exit_meta_raw = raw.get("last_exit_meta_by_ticker", {}) or {}
    last_exit_meta = {}
    for ticker, meta in last_exit_meta_raw.items():
        if not isinstance(meta, dict):
            continue
        ts = normalize_timestamp_utc(meta.get("ts"))
        edge = meta.get("edge", 0.0)
        if ts is None:
            continue
        try:
            edge = float(edge)
        except Exception:
            edge = 0.0
        last_exit_meta[ticker] = {"ts": ts, "edge": edge}
    reinforce_raw = raw.get("reinforce_count_by_ticker", {}) or {}
    reinforce_count = {}
    for ticker, count in reinforce_raw.items():
        try:
            reinforce_count[ticker] = int(count)
        except Exception:
            continue
    try:
        realized_cash_pnl = float(raw.get("realized_cash_pnl", 0.0) or 0.0)
    except Exception:
        realized_cash_pnl = 0.0
    return {
        "realized_cash_pnl": realized_cash_pnl,
        "last_exit_meta_by_ticker": last_exit_meta,
        "reinforce_count_by_ticker": reinforce_count,
    }


def save_runtime_state(path, realized_cash_pnl, last_exit_meta_by_ticker, reinforce_count_by_ticker):
    payload = {
        "realized_cash_pnl": float(realized_cash_pnl),
        "last_exit_meta_by_ticker": {
            ticker: {
                "ts": meta["ts"].isoformat() if meta.get("ts") is not None else "",
                "edge": float(meta.get("edge", 0.0) or 0.0),
            }
            for ticker, meta in (last_exit_meta_by_ticker or {}).items()
            if isinstance(meta, dict)
        },
        "reinforce_count_by_ticker": {
            ticker: int(count)
            for ticker, count in (reinforce_count_by_ticker or {}).items()
        },
    }
    write_json_state(path, payload)
