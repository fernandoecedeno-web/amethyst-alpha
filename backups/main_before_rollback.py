
import asyncio
import aiohttp
import csv
import math
import math as _math
import copy
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

from bot.config import *  # noqa: F403
from bot.execution.trade_lifecycle import (
    OPEN_FIELDS,
    RESOLVED_FIELDS,
    cleanup_position_state as lifecycle_cleanup_position_state,
    ensure_trade_logs,
    load_open_rows as lifecycle_load_open_rows,
    record_open_trade as lifecycle_record_open_trade,
    save_positions as lifecycle_save_positions,
)
from bot.portfolio.open_positions import (
    load_open_positions,
    reconcile_open_positions,
    save_open_positions,
)
from bot.portfolio.resolved_positions import append_resolved_trade
from bot.reporting.console import (
    print_balance_summary,
    print_candidate_summary,
    print_cycle_header,
    print_exit_summary,
    print_exposure_summary,
    print_skip_reason_summary,
)
from bot.logger import (
    color_text,
    cyan,
    fmt_money,
    fmt_pnl,
    green,
    magenta,
    pnl_color,
    red,
    setup_logger,
    yellow,
)
from bot.state import load_runtime_state, read_json_state, save_runtime_state, write_json_state
import bot.analytics as _analytics
from bot.strategy.scoring import (
    classify_conviction_delta,
    classify_conviction_state,
    compute_killer_score_components,
)
from bot.utils import normalize_timestamp_utc
try:
    from kalshi_rich_dashboard import (
        BotDashboard,
        snapshot_from_bot_state,
        positions_from_open_trades,
        candidates_from_ranked,
    )
    _DASHBOARD_AVAILABLE = True
except ImportError:
    _DASHBOARD_AVAILABLE = False

log = setup_logger(__name__)

_OFFLINE_FIXTURE_CACHE = {}
_OFFLINE_OPEN_ROWS = []
_offline_lifecycle_cycle = 0

def _offline_iso(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _offline_market(ticker, title, close_dt, volume=25000, liquidity=8000, open_interest=1200):
    return {
        "ticker": ticker,
        "title": title,
        "market_type": "binary",
        "status": "active",
        "close_time": _offline_iso(close_dt),
        "volume": volume,
        "volume_24h": volume,
        "liquidity": liquidity,
        "open_interest": open_interest,
    }

def _offline_price(yes_bid, yes_ask, no_bid=None, no_ask=None, yes_touch=120, no_touch=120, pressure_score=0.62):
    if no_bid is None:
        no_bid = round(max(0.01, 1.0 - yes_ask), 4)
    if no_ask is None:
        no_ask = round(min(0.99, 1.0 - yes_bid), 4)
    no_pressure_score = round(max(0.0, min(1.0, 1.0 - pressure_score)), 4)
    return {
        "yes_bid": round(yes_bid, 4),
        "yes_ask": round(yes_ask, 4),
        "no_bid": round(no_bid, 4),
        "no_ask": round(no_ask, 4),
        "yes_pressure": round(pressure_score * 1000.0, 2),
        "no_pressure": round(no_pressure_score * 1000.0, 2),
        "pressure_score": round(pressure_score, 4),
        "no_pressure_score": no_pressure_score,
        "yes_touch_size": float(yes_touch),
        "no_touch_size": float(no_touch),
        "yes_depth_near": float(max(yes_touch * 3, yes_touch)),
        "no_depth_near": float(max(no_touch * 3, no_touch)),
    }

def _offline_open_row(now, ticker, side, crowd_prob, model_prob, ev, position_usd, hours_to_close, family, strike, spot_price, close_dt, tier=2, held_seconds=900):
    return {
        "timestamp": _offline_iso(now - timedelta(seconds=held_seconds)),
        "ticker": ticker,
        "side": side,
        "crowd_prob": f"{crowd_prob:.4f}",
        "model_prob": f"{model_prob:.4f}",
        "ev": f"{ev:.4f}",
        "position_usd": position_usd,
        "hours_to_close": f"{hours_to_close:.2f}",
        "family": family,
        "strike": strike if strike is not None else "",
        "spot_price": f"{spot_price:.2f}" if spot_price is not None else "",
        "close_time": _offline_iso(close_dt),
        "tier": tier,
    }

def _offline_entry_meta(entry_edge, quality_score, entry_spread, entry_pressure, entry_ts, series_key, regime="momentum_clean"):
    return {
        "elite_score": round(min(0.99, max(0.10, quality_score + 0.15)), 4),
        "pressure_score": entry_pressure,
        "crowd": 0.0,
        "velocity": 0.0,
        "entry_pressure": entry_pressure,
        "entry_spread": entry_spread,
        "entry_score": round(entry_edge + quality_score, 4),
        "regime": regime,
        "entry_edge": entry_edge,
        "liquidity_score": round(min(0.99, max(0.20, quality_score)), 4),
        "quality_score": quality_score,
        "series_key": series_key,
        "time_stop_secs": MAX_HOLD_SECONDS,
        "entry_ts": entry_ts,
    }

def _build_offline_fixture(scenario):
    now = utc_now()
    btc_spot = 71800.0
    eth_spot = 2225.0
    spots = {"KXBTC": btc_spot, "KXETH": eth_spot}
    markets = []
    prices = {}
    open_rows = []
    entry_metrics = {}
    market_results = {}

    def add_open(row, edge, quality, spread, pressure):
        open_rows.append(row)
        entry_metrics[(row["ticker"], row["side"])] = _offline_entry_meta(
            edge, quality, spread, pressure, row["timestamp"], row["family"]
        )
        prices[row["ticker"]] = prices.get(row["ticker"], {})

    def add_candidate(market, price):
        markets.append(market)
        prices[market["ticker"]] = price

    if scenario in {"rotation", "all"}:
        close_a = now + timedelta(hours=3.0)
        close_b = now + timedelta(hours=3.5)
        close_c = now + timedelta(hours=4.0)
        add_open(
            _offline_open_row(now, "KXBTC-12APR1100-B70400", "yes", 0.34, 0.46, 0.12, 2.0, 3.0, "KXBTC", 70400, btc_spot, close_a, held_seconds=30),
            0.12, 0.46, 0.030, 0.42,
        )
        prices["KXBTC-12APR1100-B70400"] = _offline_price(0.31, 0.33, yes_touch=60, no_touch=40, pressure_score=0.38)
        add_open(
            _offline_open_row(now, "KXBTC-12APR1130-B70700", "yes", 0.31, 0.52, 0.18, 2.0, 3.5, "KXBTC", 70700, btc_spot, close_b, held_seconds=45),
            0.18, 0.58, 0.022, 0.49,
        )
        prices["KXBTC-12APR1130-B70700"] = _offline_price(0.30, 0.32, yes_touch=80, no_touch=70, pressure_score=0.52)
        add_open(
            _offline_open_row(now, "KXETH-12APR1145-B2190", "yes", 0.29, 0.44, 0.09, 2.0, 4.0, "KXETH", 2190, eth_spot, close_c, held_seconds=60),
            0.09, 0.40, 0.028, 0.35,
        )
        prices["KXETH-12APR1145-B2190"] = _offline_price(0.28, 0.30, yes_touch=45, no_touch=35, pressure_score=0.30)

    if scenario in {"diversify", "all"}:
        open_rows = []
        entry_metrics = {}
        close_a = now + timedelta(hours=3.0)
        close_b = now + timedelta(hours=3.25)
        close_c = now + timedelta(hours=3.5)
        add_open(
            _offline_open_row(now, "KXBTC-12APR1100-B70400", "yes", 0.34, 0.46, 0.12, 2.0, 3.0, "KXBTC", 70400, btc_spot, close_a, held_seconds=30),
            0.12, 0.46, 0.030, 0.42,
        )
        prices["KXBTC-12APR1100-B70400"] = _offline_price(0.31, 0.33, yes_touch=60, no_touch=40, pressure_score=0.38)
        add_open(
            _offline_open_row(now, "KXBTC-12APR1130-B70700", "yes", 0.31, 0.52, 0.18, 2.0, 3.25, "KXBTC", 70700, btc_spot, close_b, held_seconds=45),
            0.18, 0.58, 0.022, 0.49,
        )
        prices["KXBTC-12APR1130-B70700"] = _offline_price(0.30, 0.32, yes_touch=80, no_touch=70, pressure_score=0.52)
        add_open(
            _offline_open_row(now, "KXBTC-12APR1200-B70950", "yes", 0.27, 0.41, 0.07, 2.0, 3.5, "KXBTC", 70950, btc_spot, close_c, held_seconds=60),
            0.07, 0.35, 0.030, 0.29,
        )
        prices["KXBTC-12APR1200-B70950"] = _offline_price(0.25, 0.28, yes_touch=35, no_touch=28, pressure_score=0.27)

    if scenario == "downgrade":
        open_rows = []
        entry_metrics = {}
        close_a = now + timedelta(hours=3.0)
        close_b = now + timedelta(hours=3.25)
        close_c = now + timedelta(hours=3.5)
        add_open(
            _offline_open_row(now, "KXBTC-12APR1100-B70400", "yes", 0.34, 0.46, 0.21, 2.0, 3.0, "KXBTC", 70400, btc_spot, close_a, held_seconds=30),
            0.21, 0.60, 0.020, 0.55,
        )
        prices["KXBTC-12APR1100-B70400"] = _offline_price(0.35, 0.37, yes_touch=120, no_touch=100, pressure_score=0.56)
        add_open(
            _offline_open_row(now, "KXBTC-12APR1130-B70700", "yes", 0.36, 0.48, 0.19, 2.0, 3.25, "KXBTC", 70700, btc_spot, close_b, held_seconds=45),
            0.19, 0.57, 0.022, 0.51,
        )
        prices["KXBTC-12APR1130-B70700"] = _offline_price(0.35, 0.37, yes_touch=110, no_touch=95, pressure_score=0.52)
        add_open(
            _offline_open_row(now, "KXBTC-12APR1200-B70950", "yes", 0.39, 0.52, 0.16, 2.0, 3.5, "KXBTC", 70950, btc_spot, close_c, held_seconds=60),
            0.16, 0.75, 0.024, 0.49,
        )
        prices["KXBTC-12APR1200-B70950"] = _offline_price(0.38, 0.40, yes_touch=140, no_touch=120, pressure_score=0.48)

    if scenario in {"elite", "all"}:
        elite_close = now + timedelta(hours=2.0)
        add_candidate(
            _offline_market("KXBTC-12APR1300-B70000", "BTC > 70000 by 1:00 PM UTC", elite_close),
            _offline_price(0.16, 0.18, no_bid=0.81, no_ask=0.84, yes_touch=5000, no_touch=4200, pressure_score=0.74),
        )

    if scenario in {"rotation", "all"}:
        rotation_close = now + timedelta(hours=2.5)
        add_candidate(
            _offline_market("KXETH-12APR1330-B2180", "ETH > 2180 by 1:30 PM UTC", rotation_close),
            _offline_price(0.21, 0.24, no_bid=0.74, no_ask=0.77, yes_touch=240, no_touch=220, pressure_score=0.68),
        )

    if scenario in {"diversify", "all"}:
        diversify_close = now + timedelta(hours=2.75)
        add_candidate(
            _offline_market("KXETH-12APR1345-B2215", "ETH > 2215 by 1:45 PM UTC", diversify_close),
            _offline_price(0.43, 0.44, no_bid=0.55, no_ask=0.57, yes_touch=260, no_touch=240, pressure_score=0.66),
        )

    if scenario in {"downgrade", "all"}:
        downgrade_close = now + timedelta(hours=2.2)
        add_candidate(
            _offline_market("KXBTC-12APR1315-B71750", "BTC > 71750 by 1:15 PM UTC", downgrade_close),
            _offline_price(0.37, 0.39, no_bid=0.60, no_ask=0.63, yes_touch=28, no_touch=26, pressure_score=0.41),
        )

    if scenario in {"edge_floor", "all"}:
        edge_floor_close = now + timedelta(hours=2.1)
        add_candidate(
            _offline_market("KXBTC-12APR1310-B71950", "BTC > 71950 by 1:10 PM UTC", edge_floor_close),
            _offline_price(0.398, 0.425, no_bid=0.55, no_ask=0.575, yes_touch=12, no_touch=10, pressure_score=0.32),
        )

    return {
        "scenario": scenario,
        "spots": spots,
        "markets": markets,
        "prices": prices,
        "open_positions": open_rows,
        "entry_metrics": entry_metrics,
        "market_results": market_results,
    }

def get_offline_fixture():
    scenario = OFFLINE_SCENARIO if OFFLINE_SCENARIO in {"elite", "rotation", "diversify", "downgrade", "edge_floor", "all"} else "elite"
    fixture = _OFFLINE_FIXTURE_CACHE.get(scenario)
    if fixture is None:
        fixture = _build_offline_fixture(scenario)
        _OFFLINE_FIXTURE_CACHE[scenario] = fixture
    return fixture

def _offline_lifecycle_ticker():
    return "KXBTC-12APR1305-B71000"

def load_offline_lifecycle_spots():
    return {"KXBTC": 71800.0, "KXETH": 2225.0}

def load_offline_lifecycle_markets():
    now = utc_now()
    ticker = _offline_lifecycle_ticker()
    close_dt = now + timedelta(hours=2.0)
    if _offline_lifecycle_cycle in {1, 4}:
        return [_offline_market(ticker, "BTC > 71000 lifecycle test", close_dt)]
    return []

def load_offline_lifecycle_prices():
    ticker = _offline_lifecycle_ticker()
    if _offline_lifecycle_cycle in {1, 4}:
        return {
            ticker: _offline_price(0.22, 0.24, no_bid=0.75, no_ask=0.78, yes_touch=240, no_touch=220, pressure_score=0.62)
        }
    if _offline_lifecycle_cycle == 2:
        return {
            ticker: _offline_price(0.23, 0.25, no_bid=0.74, no_ask=0.77, yes_touch=210, no_touch=180, pressure_score=0.58)
        }
    if _offline_lifecycle_cycle == 3:
        return {
            ticker: _offline_price(0.19, 0.21, no_bid=0.79, no_ask=0.82, yes_touch=80, no_touch=190, pressure_score=0.22)
        }
    if _offline_lifecycle_cycle == 5:
        return {
            ticker: _offline_price(0.23, 0.25, no_bid=0.74, no_ask=0.77, yes_touch=200, no_touch=170, pressure_score=0.57)
        }
    if _offline_lifecycle_cycle >= 6:
        return {
            ticker: _offline_price(0.24, 0.26, no_bid=0.73, no_ask=0.76, yes_touch=220, no_touch=180, pressure_score=0.60)
        }
    return {}

def advance_offline_lifecycle_state(cycle_num):
    global _OFFLINE_OPEN_ROWS
    if not OFFLINE_LIFECYCLE_TEST:
        return
    ticker = _offline_lifecycle_ticker()
    if cycle_num == 3:
        if ticker in _last_exit_meta_by_ticker:
            _last_exit_meta_by_ticker[ticker]["ts"] = utc_now() - timedelta(seconds=180)
        _post_exit_cooldown_by_series.pop("KXBTC", None)
        cooldowns = load_cooldowns()
        cooldowns.pop(f"{ticker}|yes", None)
        save_cooldowns(cooldowns)
        return
    if not _OFFLINE_OPEN_ROWS:
        return
    age_targets = {1: 100, 2: 140, 4: 90, 5: 105}
    target_age = age_targets.get(cycle_num)
    if target_age is None:
        return
    now = utc_now()
    updated = []
    for row in _OFFLINE_OPEN_ROWS:
        new_row = dict(row)
        new_row["timestamp"] = (now - timedelta(seconds=target_age)).isoformat()
        updated.append(new_row)
    _OFFLINE_OPEN_ROWS = updated

def load_offline_spots():
    if OFFLINE_LIFECYCLE_TEST:
        return load_offline_lifecycle_spots()
    return copy.deepcopy(get_offline_fixture()["spots"])

def load_offline_markets():
    if OFFLINE_LIFECYCLE_TEST:
        return load_offline_lifecycle_markets()
    return copy.deepcopy(get_offline_fixture()["markets"])

def load_offline_prices():
    if OFFLINE_LIFECYCLE_TEST:
        return load_offline_lifecycle_prices()
    return copy.deepcopy(get_offline_fixture()["prices"])

def load_offline_open_positions():
    if OFFLINE_LIFECYCLE_TEST:
        return []
    return copy.deepcopy(get_offline_fixture()["open_positions"])

def prepare_offline_debug_state():
    global _OFFLINE_OPEN_ROWS
    if OFFLINE_LIFECYCLE_TEST:
        _OFFLINE_OPEN_ROWS = []
        _entry_metrics.clear()
        _trade_state.clear()
        _rotation_book.clear()
        _peak_pnl_by_position_id.clear()
        _entry_cooldown_by_ticker.clear()
        _post_exit_cooldown_by_series.clear()
        _last_trade_ts_by_ticker.clear()
        _last_exit_price_by_ticker.clear()
        _last_exit_meta_by_ticker.clear()
        recent_losses.clear()
        _series_open_count.clear()
        return
    fixture = get_offline_fixture()
    _OFFLINE_OPEN_ROWS = copy.deepcopy(fixture["open_positions"])
    _entry_metrics.clear()
    _trade_state.clear()
    _rotation_book.clear()
    _peak_pnl_by_position_id.clear()
    _entry_cooldown_by_ticker.clear()
    _post_exit_cooldown_by_series.clear()
    _last_trade_ts_by_ticker.clear()
    _last_exit_price_by_ticker.clear()
    _last_exit_meta_by_ticker.clear()
    _reinforce_count_by_ticker.clear()
    recent_losses.clear()
    _series_open_count.clear()
    for key, meta in fixture["entry_metrics"].items():
        _entry_metrics[key] = copy.deepcopy(meta)
    for row in _OFFLINE_OPEN_ROWS:
        series = row.get("family", "")
        _series_open_count[series] = _series_open_count.get(series, 0) + 1

Path("data").mkdir(exist_ok=True)
SIGNAL_LOG = "data/signals.csv"
if not Path(SIGNAL_LOG).exists():
    with open(SIGNAL_LOG, "w", newline="") as f:
        csv.writer(f).writerow(["timestamp","ticker","title","side","crowd_prob","model_prob","ev","position_usd","hours_to_close","family","strike","yes_bid","yes_ask","no_bid","no_ask","spot_price"])

OPEN_TRADES_LOG    = "data/open_trades.csv"
RESOLVED_TRADES_LOG = "data/resolved_trades.csv"
ensure_trade_logs(OPEN_TRADES_LOG, RESOLVED_TRADES_LOG)

COOLDOWN_LOG  = "data/early_exit_cooldown.json"
RUNTIME_STATE_LOG = "data/state/runtime_state.json"
STARTUP_TIME  = datetime.now(timezone.utc)
FAMILY_RATES  = {"KXBTC":0.962,"KXETH":0.958,"KXGDP":0.975,"KXFED":0.978,"KXINFL":0.971,"KXSPX":0.960,"DEFAULT":0.965}
_last_crowd       = {}   # (ticker, side) -> last observed crowd float for stability check
_price_history    = {}   # (ticker, side) -> last 5 observed crowd floats
_pressure_history = {}   # ticker -> last 5 pressure scores
_spread_history   = {}   # ticker -> last 5 spreads
_entry_metrics    = {}   # (ticker, side) -> entry quality snapshot
_trade_state      = {}   # (ticker, side) -> mfe/mae/peak state
_last_spot_prices = {"KXBTC": None, "KXETH": None}
_prev_spot_prices = {"KXBTC": None, "KXETH": None}
_session_hold_secs = []  # hold durations (seconds) for early exits this session
_entry_cooldown_by_ticker = {}
_post_exit_cooldown_by_series = {}
_last_trade_ts_by_ticker = {}
_last_exit_price_by_ticker = {}
_last_exit_meta_by_ticker = {}
_rotation_book = {}
_rotation_perf = {"count": 0, "alpha_sum": 0.0, "wins": 0}
_peak_pnl_by_position_id = {}
_last_skip_reason_counts = {}
_series_open_count = {}
_recent_rejects_by_ticker = {}
_reinforce_count_by_ticker = {}
recent_losses = {}  # {ticker: timestamp}
MAX_OPEN_TRADES = MAX_OPEN_POSITIONS
EXEC_SCORE_THRESHOLD = 0.42   # mid-signal floor; skip if also low pressure (<0.55)
VERBOSE_LOGS = False           # True → full diagnostic output; False → clean runtime output

# Session-level counters — reset to zero on each startup, never persisted to disk
session_trades   = 0
session_wins     = 0
session_losses   = 0
session_pnl      = 0.0
_realized_cash_pnl = 0.0
_dashboard = None   # BotDashboard instance, started in main()

def reset_paper_state():
    global _realized_cash_pnl, _OFFLINE_OPEN_ROWS
    with open(OPEN_TRADES_LOG, "w", newline="") as f:
        csv.writer(f).writerow(OPEN_FIELDS)
    write_json_state(COOLDOWN_LOG, {})
    write_json_state(RUNTIME_STATE_LOG, {})
    _last_crowd.clear()
    _price_history.clear()
    _pressure_history.clear()
    _spread_history.clear()
    _entry_metrics.clear()
    _trade_state.clear()
    _session_hold_secs.clear()
    _entry_cooldown_by_ticker.clear()
    _post_exit_cooldown_by_series.clear()
    _last_trade_ts_by_ticker.clear()
    _last_exit_meta_by_ticker.clear()
    _reinforce_count_by_ticker.clear()
    _rotation_book.clear()
    _rotation_perf["count"] = 0
    _rotation_perf["alpha_sum"] = 0.0
    _rotation_perf["wins"] = 0
    _OFFLINE_OPEN_ROWS = []
    _peak_pnl_by_position_id.clear()
    _last_skip_reason_counts.clear()
    _series_open_count.clear()
    _recent_rejects_by_ticker.clear()
    recent_losses.clear()
    _realized_cash_pnl = 0.0
    log.info("PAPER STATE RESET COMPLETE")


def restore_runtime_state():
    global _realized_cash_pnl
    runtime_state = load_runtime_state(RUNTIME_STATE_LOG)
    _realized_cash_pnl = float(runtime_state.get("realized_cash_pnl", 0.0) or 0.0)
    _last_exit_meta_by_ticker.clear()
    _last_exit_meta_by_ticker.update(runtime_state.get("last_exit_meta_by_ticker", {}))
    _reinforce_count_by_ticker.clear()
    _reinforce_count_by_ticker.update(runtime_state.get("reinforce_count_by_ticker", {}))
    log.info(
        f"[STATE_RESTORE] realized_cash_pnl={_realized_cash_pnl:.2f}"
        f" last_exit_meta={len(_last_exit_meta_by_ticker)}"
        f" reinforce_count={len(_reinforce_count_by_ticker)}"
    )


def persist_runtime_state():
    save_runtime_state(
        RUNTIME_STATE_LOG,
        _realized_cash_pnl,
        _last_exit_meta_by_ticker,
        _reinforce_count_by_ticker,
    )
    log.info(
        f"[STATE_SAVE] realized_cash_pnl={_realized_cash_pnl:.2f}"
        f" last_exit_meta={len(_last_exit_meta_by_ticker)}"
        f" reinforce_count={len(_reinforce_count_by_ticker)}"
    )

def cleanup_position_state(ticker, side, reason="unknown", expected_entry_ts=None):
    lifecycle_cleanup_position_state(
        ticker=ticker,
        side=side,
        reason=reason,
        expected_entry_ts=expected_entry_ts,
        entry_metrics=_entry_metrics,
        trade_state=_trade_state,
        peak_pnl_by_position_id=_peak_pnl_by_position_id,
        entry_cooldown_by_ticker=_entry_cooldown_by_ticker,
        log=log,
        offline_lifecycle_test=OFFLINE_LIFECYCLE_TEST,
    )

def _capital_fields(open_exposure=0.0, cycle_cap_limit=0.0, cycle_cap_used=0.0, trade_cap_limit=0.0):
    cash_balance = current_cash_balance(open_exposure)
    cycle_cap_remaining = max(0.0, cycle_cap_limit - cycle_cap_used)
    return {
        "bankroll": float(BANKROLL),
        "realized_cash_pnl": float(_realized_cash_pnl),
        "cash_balance": float(cash_balance),
        "open_exposure": float(open_exposure),
        "cycle_cap_limit": float(cycle_cap_limit),
        "cycle_cap_used": float(cycle_cap_used),
        "cycle_cap_remaining": float(cycle_cap_remaining),
        "trade_cap_limit": float(trade_cap_limit),
    }

def log_capital_state(tag, open_exposure=0.0, cycle_cap_limit=0.0, cycle_cap_used=0.0, trade_cap_limit=0.0):
    fields = _capital_fields(open_exposure, cycle_cap_limit, cycle_cap_used, trade_cap_limit)
    prefix = "CAPITAL" if tag == "CAPITAL" else tag
    msg = (
        f"{prefix} | bankroll={fields['bankroll']:.2f} realized_cash_pnl={fields['realized_cash_pnl']:.2f}"
        f" cash_balance={fields['cash_balance']:.2f} open_exposure={fields['open_exposure']:.2f}"
        f" cycle_cap_limit={fields['cycle_cap_limit']:.2f} cycle_cap_used={fields['cycle_cap_used']:.2f}"
        f" cycle_cap_remaining={fields['cycle_cap_remaining']:.2f} trade_cap_limit={fields['trade_cap_limit']:.2f}"
    )
    log.info(cyan(msg))
    bad = []
    if fields["cash_balance"] < -0.01:
        bad.append("cash_balance")
    if fields["cycle_cap_remaining"] < -0.01:
        bad.append("cycle_cap_remaining")
    if fields["open_exposure"] < -0.01:
        bad.append("open_exposure")
    if fields["trade_cap_limit"] < 0:
        bad.append("trade_cap_limit")
    for name, value in fields.items():
        if not _math.isfinite(value):
            bad.append(name)
    if bad:
        log.error(
            f"CAPITAL_INVARIANT | failed={','.join(sorted(set(bad)))}"
            f" bankroll={fields['bankroll']:.4f} realized_cash_pnl={fields['realized_cash_pnl']:.4f}"
            f" cash_balance={fields['cash_balance']:.4f} open_exposure={fields['open_exposure']:.4f}"
            f" cycle_cap_limit={fields['cycle_cap_limit']:.4f} cycle_cap_used={fields['cycle_cap_used']:.4f}"
            f" cycle_cap_remaining={fields['cycle_cap_remaining']:.4f} trade_cap_limit={fields['trade_cap_limit']:.4f}"
        )

def load_open_rows():
    return load_open_positions(OPEN_TRADES_LOG, offline_mode=OFFLINE_MODE, offline_rows=_OFFLINE_OPEN_ROWS)

def save_positions(rows):
    save_open_positions(rows, OPEN_TRADES_LOG, OPEN_FIELDS, offline_mode=OFFLINE_MODE, offline_rows_ref=_OFFLINE_OPEN_ROWS)

def current_position_snapshot(row, prices):
    ticker = row.get("ticker", "")
    side = row.get("side", "")
    entry_price = safe_float(row.get("crowd_prob"), 0.0) or 0.0
    p = prices.get(ticker, {})
    if side == "no":
        exit_price = safe_float(p.get("no_bid"), safe_float(p.get("no_ask"), entry_price))
    else:
        exit_price = safe_float(p.get("yes_bid"), safe_float(p.get("yes_ask"), entry_price))
    if exit_price is None:
        exit_price = entry_price
    pnl_pct = exit_price - entry_price
    edge = safe_float(_entry_metrics.get((ticker, side), {}).get("entry_edge"), safe_float(row.get("ev"), 0.0) or 0.0) or 0.0
    return edge, pnl_pct, exit_price

def current_cash_balance(open_exposure=0.0):
    return max(0.0, BANKROLL + _realized_cash_pnl - open_exposure)

def parse_utc_timestamp(raw):
    return normalize_timestamp_utc(raw)

def position_held_seconds(row, now):
    entry_meta = _entry_metrics.get((row.get("ticker", ""), row.get("side", "")), {})
    ts = parse_utc_timestamp(row.get("timestamp")) or parse_utc_timestamp(entry_meta.get("entry_ts"))
    if not ts:
        return 0.0
    return max(0.0, (now - ts).total_seconds())

def position_entry_quality(row):
    return clamp01(
        safe_float(
            _entry_metrics.get((row.get("ticker", ""), row.get("side", "")), {}).get("quality_score"),
            0.0,
        ) or 0.0
    )

def position_expiry_progress(row, now):
    entry_meta = _entry_metrics.get((row.get("ticker", ""), row.get("side", "")), {})
    start_ts = parse_utc_timestamp(row.get("timestamp")) or parse_utc_timestamp(entry_meta.get("entry_ts"))
    close_ts = parse_utc_timestamp(row.get("close_time"))
    if not start_ts or not close_ts:
        return 0.0
    total_window = max(1.0, (close_ts - start_ts).total_seconds())
    elapsed = max(0.0, (now - start_ts).total_seconds())
    return clamp01(elapsed / total_window)

def position_cluster_key(row):
    close_time = row.get("close_time", "")
    if close_time:
        return f"{row.get('family', '')}|{close_time}"
    return row.get("family", "")

def portfolio_concentration_score(rows):
    if not rows:
        return 0
    family_counts = defaultdict(int)
    direction_counts = defaultdict(int)
    cluster_counts = defaultdict(int)
    for row in rows:
        family_counts[row.get("family", "")] += 1
        direction_counts[row.get("side", "")] += 1
        cluster_counts[position_cluster_key(row)] += 1
    return max(
        max(family_counts.values() or [0]),
        max(direction_counts.values() or [0]),
        max(cluster_counts.values() or [0]),
    )

def diversification_bonus(candidate, victim_row, open_rows):
    current_rows = list(open_rows)
    replacement_row = {
        "ticker": candidate.get("ticker", ""),
        "side": candidate.get("side", ""),
        "family": candidate.get("family", ""),
        "close_time": candidate.get("close_time", ""),
    }
    upgraded_rows = [
        replacement_row if (row.get("ticker") == victim_row.get("ticker") and row.get("side") == victim_row.get("side")) else row
        for row in current_rows
    ]
    before = portfolio_concentration_score(current_rows)
    after = portfolio_concentration_score(upgraded_rows)
    return 0.05 if after < before else 0.0

def same_strike_family(a, b):
    try:
        return a.split('-')[-1] == b.split('-')[-1]
    except:
        return False

def rotate_open_position(open_rows, victim_row, replacement, prices, now):
    old_ticker = victim_row.get("ticker", "")
    new_ticker = replacement.get("ticker", "")
    if new_ticker == old_ticker or same_strike_family(new_ticker, old_ticker):
        log.info(f"[ROTATION BLOCKED] redundant trade {new_ticker} vs {old_ticker}")
        return None
    old_side = victim_row.get("side", "")
    old_size = safe_float(victim_row.get("position_usd"), 0.0) or 0.0
    old_edge, pnl_pct, exit_price = current_position_snapshot(victim_row, prices)
    old_quality = position_entry_quality(victim_row)
    old_entry_ts = normalize_timestamp_utc(victim_row.get("timestamp", ""))
    old_held_secs = (now - old_entry_ts).total_seconds() if old_entry_ts else 0.0
    pnl = round(old_size * pnl_pct, 2)
    won = 1 if pnl > 0 else 0
    resolved_yes = 1 if old_side == "yes" else 0
    resolved_no = 1 - resolved_yes
    append_resolved_trade(
        RESOLVED_TRADES_LOG,
        OPEN_FIELDS,
        victim_row,
        resolved_yes,
        resolved_no,
        won,
        pnl,
        "rotation",
    )
    filtered_rows = [row for row in open_rows if not (row.get("ticker") == old_ticker and row.get("side") == old_side)]
    save_positions(filtered_rows)
    global session_trades, session_wins, session_losses, session_pnl, _realized_cash_pnl
    session_trades += 1
    session_pnl = round(session_pnl + pnl, 2)
    _realized_cash_pnl = round(_realized_cash_pnl + pnl, 2)
    if won:
        session_wins += 1
    else:
        session_losses += 1
    cleanup_position_state(
        old_ticker,
        old_side,
        reason="rotation",
        expected_entry_ts=victim_row.get("timestamp", ""),
    )
    _last_exit_price_by_ticker[old_ticker] = exit_price
    _last_trade_ts_by_ticker[old_ticker] = now
    _last_exit_meta_by_ticker[old_ticker] = {"ts": now, "edge": old_edge}
    log.info(
        f"[ROTATION_OUT] ticker={old_ticker} reason=rotation_out"
        f" pnl={pnl_pct:.4f} usd={fmt_money(pnl)} held={int(old_held_secs)}"
    )
    log.info(
        f"[ROTATION] replacing {old_ticker} -> {replacement['ticker']}"
        f" | old_edge={old_edge:.3f}"
        f" old_pnl={pnl_pct:.4f} new_edge={replacement['edge']:.3f}"
    )
    return filtered_rows, pnl, old_edge, pnl_pct, old_quality

def execution_priority_score(candidate):
    edge_score = clamp01((candidate.get("edge", 0.0) or 0.0) / 0.35)
    quality_score = clamp01(candidate.get("quality_score", 0.0) or 0.0)
    spread_score = clamp01(1.0 - (candidate.get("spread", 0.0) or 0.0) / max(HARD_SPREAD_CEIL, 0.08))
    touch = candidate.get("selected_touch", 0.0) or 0.0
    touch_score = clamp01(touch / 50.0)
    liq_score = clamp01(candidate.get("liquidity_score", 0.0) or 0.0)
    touch_liq_score = clamp01(touch_score * 0.55 + liq_score * 0.45)
    minutes = candidate.get("minutes_to_expiry", SELECTION_MAX_MINUTES) or SELECTION_MAX_MINUTES
    time_score = clamp01(1.0 - max(0.0, minutes - SELECTION_MIN_MINUTES) / max(SELECTION_MAX_MINUTES - SELECTION_MIN_MINUTES, 1))
    score = round(
        edge_score * 0.40 +
        quality_score * 0.25 +
        spread_score * 0.15 +
        touch_liq_score * 0.10 +
        time_score * 0.10,
        4
    )
    pressure = candidate.get("pressure_score", 0.0) or 0.0
    if pressure < 0.30:
        penalized = round(score * 0.92, 4)
        if VERBOSE_LOGS:
            log.info(f"[PRESSURE_PENALTY] ticker={candidate.get('ticker','')} pressure={pressure:.3f} old_score={score:.4f} new_score={penalized:.4f}")
        return penalized
    return score

def diversify_ranked_candidates(candidates, open_rows):
    if len(candidates) < 2:
        return candidates
    open_families = {row.get("family", "") for row in open_rows}
    ranked = list(candidates)
    for idx in range(len(ranked) - 1):
        cur = ranked[idx]
        nxt = ranked[idx + 1]
        if abs(cur["execution_priority"] - nxt["execution_priority"]) > 0.05:
            continue
        if cur.get("family") in open_families and nxt.get("family") not in open_families:
            ranked[idx], ranked[idx + 1] = ranked[idx + 1], ranked[idx]
            log.info(f"[DIVERSIFY] promoted {nxt['ticker']} over {cur['ticker']}")
    return ranked

def build_allocator_portfolio_state(open_rows, prices):
    family_usd = defaultdict(float)
    family_count = defaultdict(int)
    conviction_usd = {"elite": 0.0, "strong": 0.0, "neutral": 0.0, "weak": 0.0}
    total_open_usd = 0.0
    for row in open_rows:
        ticker = row.get("ticker", "")
        side = row.get("side", "")
        size = safe_float(row.get("position_usd"), 0.0) or 0.0
        total_open_usd += size
        family = row.get("family", "OTHER")
        family_usd[family] += size
        family_count[family] += 1
        entry_meta = _entry_metrics.get((ticker, side), {})
        intel = compute_position_intel(row, side, prices, entry_meta)
        conviction_usd[intel["conviction_state"]] += size
    return {
        "total_open_usd": total_open_usd,
        "family_usd": family_usd,
        "family_count": family_count,
        "conviction_usd": conviction_usd,
        "btc_exposure_pct": (family_usd.get("KXBTC", 0.0) / BANKROLL) if BANKROLL > 0 else 0.0,
        "eth_exposure_pct": (family_usd.get("KXETH", 0.0) / BANKROLL) if BANKROLL > 0 else 0.0,
        "weak_bucket_pct": (conviction_usd["weak"] / BANKROLL) if BANKROLL > 0 else 0.0,
    }


def compute_allocation_score(candidate, portfolio_state, ticker=""):
    edge = clamp01(candidate.get("edge", 0.0) or 0.0)
    killer = clamp01(candidate.get("killer_score", 0.0) or 0.0)
    quality = clamp01(candidate.get("quality_score", 0.0) or 0.0)
    spread = candidate.get("spread", 0.0) or 0.0
    family = candidate.get("family", "OTHER")
    conviction_state = classify_conviction_state(killer, edge)
    conviction_bonus = {
        "elite": 0.08,
        "strong": 0.04,
        "neutral": 0.00,
        "weak": -0.06,
    }[conviction_state]
    spread_penalty = min(0.20, spread * 2.0)
    concentration_penalty = 0.0
    if family == "KXBTC" and portfolio_state["btc_exposure_pct"] > 0.45:
        penalty = min(0.12, (portfolio_state["btc_exposure_pct"] - 0.45) * 0.60)
        concentration_penalty += penalty
        if VERBOSE_LOGS: log.info(f"[ALLOCATOR_PENALTY] ticker={ticker} reason=btc_concentration value={penalty:.4f}")
    if family == "KXETH" and portfolio_state["eth_exposure_pct"] > 0.45:
        penalty = min(0.12, (portfolio_state["eth_exposure_pct"] - 0.45) * 0.60)
        concentration_penalty += penalty
        if VERBOSE_LOGS: log.info(f"[ALLOCATOR_PENALTY] ticker={ticker} reason=eth_concentration value={penalty:.4f}")
    if portfolio_state["weak_bucket_pct"] > 0.20:
        penalty = min(0.10, (portfolio_state["weak_bucket_pct"] - 0.20) * 0.50)
        concentration_penalty += penalty
        if VERBOSE_LOGS: log.info(f"[ALLOCATOR_PENALTY] ticker={ticker} reason=weak_bucket value={penalty:.4f}")
    if portfolio_state["family_count"].get(family, 0) >= 2:
        penalty = min(0.08, 0.03 * (portfolio_state["family_count"].get(family, 0) - 1))
        concentration_penalty += penalty
        if VERBOSE_LOGS: log.info(f"[ALLOCATOR_PENALTY] ticker={ticker} reason=family_load value={penalty:.4f}")
    score = round(
        edge * 0.55 +
        killer * 0.20 +
        quality * 0.15 +
        conviction_bonus -
        spread_penalty -
        concentration_penalty,
        4
    )
    if candidate.get("rotation_override"):
        score = round(score + 0.05, 4)
    if portfolio_state["weak_bucket_pct"] > 0.25:
        score = round(score - 0.08, 4)
        if VERBOSE_LOGS: log.info(f"[ALLOCATOR_PENALTY] ticker={ticker} reason=weak_bucket value=0.0800")
    score = max(-0.25, min(1.0, score))
    if score <= 0:
        return score, conviction_state
    return score, conviction_state


def allocation_size_multiplier(score):
    if score >= 0.42:
        return 1.00
    if score >= 0.34:
        return 0.75
    if score >= 0.26:
        return 0.50
    if score >= 0.20:
        return 0.35
    return 0.0


def compute_kelly_lite_multiplier(candidate, conviction_state, ticker="", max_mult=1.15):
    edge = clamp01(candidate.get("edge", 0.0) or 0.0)
    quality = clamp01(candidate.get("quality_score", 0.0) or 0.0)
    spread = max(0.0, candidate.get("spread", 0.0) or 0.0)
    edge_strength = clamp01(edge / 0.45)
    conviction_factor = {
        "elite": 1.00,
        "strong": 0.90,
        "neutral": 0.78,
        "weak": 0.62,
    }.get(conviction_state, 0.62)
    book_factor = clamp01(quality * 0.70 + (1.0 - min(1.0, spread / 0.10)) * 0.30)
    spread_drag = min(0.25, spread * 1.5)
    raw_mult = 0.35 + edge_strength * conviction_factor * book_factor * 0.80 - spread_drag
    mult = max(0.35, min(max_mult, round(raw_mult, 4)))
    log.info(
        f"[KELLY_LITE] ticker={ticker} edge_strength={edge_strength:.3f}"
        f" conviction_factor={conviction_factor:.3f} book_factor={book_factor:.3f}"
        f" spread_drag={spread_drag:.3f} mult={mult:.3f}"
    )
    return mult


def compute_trade_size(candidate, trade_cap, cycle_cap_remaining, free_capital, portfolio_state, ticker="", kelly_max_mult=1.15):
    base_size = candidate.get("base_size", candidate.get("size", 1.0)) or 1.0
    allocation_score, conviction_state = compute_allocation_score(candidate, portfolio_state, ticker=ticker)
    size_mult = allocation_size_multiplier(allocation_score)
    kelly_lite_mult = compute_kelly_lite_multiplier(candidate, conviction_state, ticker=ticker, max_mult=kelly_max_mult)
    candidate["allocation_score"] = allocation_score
    candidate["allocation_conviction_state"] = conviction_state
    candidate["allocation_size_mult"] = size_mult
    candidate["kelly_lite_mult"] = kelly_lite_mult
    if VERBOSE_LOGS:
        log.info(
            f"[ALLOCATOR] ticker={ticker} score={allocation_score:.4f} size_mult={size_mult:.2f}"
        )
    if size_mult <= 0.0:
        return 0.0
    sized = base_size * size_mult * kelly_lite_mult
    worst_case_loss_pct = max(
        abs(safe_float(CATASTROPHIC_STOP_LOSS, 0.0) or 0.0),
        abs(safe_float(CATASTROPHIC_STOP, 0.0) or 0.0),
        abs(safe_float(STOP_PNL, 0.0) or 0.0),
        0.0001,
    )
    risk_cap = MAX_LOSS_PER_TRADE / worst_case_loss_pct
    cap_limit = min(MAX_TRADE_SIZE, trade_cap, cycle_cap_remaining, free_capital, risk_cap)
    final_size = min(sized, cap_limit)
    if final_size < 1.0:
        if VERBOSE_LOGS:
            log.info(f"[SIZE ADAPT] ticker={ticker} base={base_size} mult={size_mult}"
                     f" kelly={kelly_lite_mult:.3f}"
                     f" final={final_size:.2f} allocator={allocation_score:.3f} -> blocked (< 1.0)")
        return 0.0
    final_size = round(final_size * 2.0) / 2.0
    final_size = min(MAX_TRADE_SIZE, max(1.0, final_size))
    if VERBOSE_LOGS:
        log.info(f"[SIZE ADAPT] ticker={ticker} base={base_size} mult={size_mult}"
                 f" kelly={kelly_lite_mult:.3f}"
                 f" final={final_size:.2f} allocator={allocation_score:.3f} conviction={conviction_state}")
    return final_size

def open_position_retain_score(row, prices, now):
    ticker = row.get("ticker", "")
    side = row.get("side", "")
    edge, pnl_pct, _exit_price = current_position_snapshot(row, prices)
    p = prices.get(ticker, {})
    if side == "no":
        pressure = safe_float(p.get("no_pressure_score"), _entry_metrics.get((ticker, side), {}).get("entry_pressure", 0.0)) or 0.0
    else:
        pressure = safe_float(p.get("pressure_score"), _entry_metrics.get((ticker, side), {}).get("entry_pressure", 0.0)) or 0.0
    spread = 0.0
    if p.get("yes_bid") is not None and p.get("yes_ask") is not None:
        spread = float(p["yes_ask"]) - float(p["yes_bid"])
    else:
        spread = safe_float(_entry_metrics.get((ticker, side), {}).get("entry_spread"), 0.05) or 0.05
    held_secs = position_held_seconds(row, now)
    time_held_penalty = -0.10 * clamp01(held_secs / 1800.0)
    winner_protected = pnl_pct > 0.01 and pressure >= 0.45 and held_secs < 900
    winner_bonus = 0.12 if winner_protected else 0.0
    score = round(
        edge * 0.5 +
        pnl_pct * 0.2 +
        pressure * 0.2 +
        time_held_penalty +
        winner_bonus,
        4
    )
    return score, held_secs, pressure, spread, winner_protected

def select_rotation_candidate(open_rows, prices, now, incoming_family=None, cluster_counts=None, force_cross_family=False):
    if not open_rows:
        return None
    scored = []
    for row in open_rows:
        victim_score, held_secs, pressure, spread, winner_protected = open_position_retain_score(row, prices, now)
        edge, pnl_pct, exit_price = current_position_snapshot(row, prices)
        log.info(f"[VICTIM_SCORE] ticker={row.get('ticker','')} score={victim_score:.4f}")
        scored.append((row, victim_score, edge, pnl_pct, exit_price, held_secs, pressure, spread, winner_protected))

    # Family-aware victim selection: when incoming family is at or above the soft cluster
    # cap, prefer evicting from the same family so the rotation is net-neutral on
    # concentration.  Only fall back to global pool if no same-family victim exists or
    # if force_cross_family (diversify_force / hard_elite) explicitly allows it.
    same_family_pool = []
    if (
        incoming_family
        and cluster_counts is not None
        and cluster_counts.get(incoming_family, 0) >= MAX_POSITIONS_PER_CLUSTER
        and not force_cross_family
    ):
        same_family_pool = [s for s in scored if s[0].get("family") == incoming_family]

    if same_family_pool:
        pool = same_family_pool
        mode = "same_family"
    else:
        pool = scored
        mode = "global"

    same_count = len([s for s in scored if s[0].get("family") == incoming_family])
    log.info(
        f"[VICTIM_POOL] family={incoming_family} same_family_candidates={same_count}"
        f" global_candidates={len(scored)} mode={mode}"
    )
    best = min(pool, key=lambda item: (item[1], item[3], item[2]))
    log.info(f"[VICTIM_SELECTION] mode={mode} selected={best[0].get('ticker','')}")
    return best

def classify_rotation_reason(candidate, victim_row, victim_score, victim_edge, victim_quality, diversification_boost):
    if diversification_boost > 0:
        return "diversification_upgrade"
    if candidate.get("quality_score", 0.0) >= victim_quality + 0.08:
        return "quality_upgrade"
    victim_entry_spread = safe_float(_entry_metrics.get((victim_row.get("ticker", ""), victim_row.get("side", "")), {}).get("entry_spread"), 0.05) or 0.05
    if candidate.get("spread", 0.0) < victim_entry_spread or victim_score < 0.05:
        return "risk_reduction"
    return "priority_upgrade"

def killer_instinct_filter(candidate, prices, open_rows, now, adaptive_touch_req=10, adaptive_spread_cap=0.05):
    """Pre-entry sniper gate: rejects fake edge and requires confirmation before capital is committed."""
    edge     = candidate.get("edge", 0.0) or 0.0
    quality  = candidate.get("quality_score", 0.0) or 0.0
    touch    = candidate.get("selected_touch", 0.0) or 0.0
    spread   = candidate.get("spread", 0.0) or 0.0
    pressure = candidate.get("pressure_score", 0.0) or 0.0
    ticker   = candidate.get("ticker", "")
    elite_trade = edge >= 0.55

    # ── Fake-edge detection ──────────────────────────────────────────────────
    if edge >= 0.30 and quality < 0.45:
        return False, "fake_edge_low_quality", {"edge": edge, "quality": quality, "touch": touch, "spread": spread, "pressure": pressure}
    touch_floor = max(6.0, adaptive_touch_req - 2.0)
    if edge >= 0.40 and quality < 0.75 and touch < touch_floor:
        return False, "fake_edge_weak_touch",  {"edge": edge, "quality": quality, "touch": touch, "spread": spread, "pressure": pressure}
    if edge >= 0.40 and spread >= adaptive_spread_cap * 0.95:
        return False, "fake_edge_wide_spread",  {"edge": edge, "quality": quality, "touch": touch, "spread": spread, "pressure": pressure}
    if edge >= 0.45 and pressure < 0.45:
        if VERBOSE_LOGS:
            log.info(
                yellow(
                    f"[PIPELINE_PASS] stage=killer ticker={ticker}"
                    f" disabled_filter=pressure edge={edge:.3f}"
                    f" pressure={pressure:.3f}"
                )
            )

    # ── Momentum confirmation (non-elite only) ───────────────────────────────
    if not elite_trade:
        confirmed = (
            pressure >= MIN_PRESSURE_ENTRY
            or spread <= max(0.02, adaptive_spread_cap * 0.6)
            or quality >= 0.70
            or touch >= adaptive_touch_req * 2
        )
        if not confirmed:
            if VERBOSE_LOGS:
                log.info(
                    yellow(
                        f"[PIPELINE_PASS] stage=killer ticker={ticker}"
                        f" disabled_filter=momentum_confirmation edge={edge:.3f}"
                        f" pressure={pressure:.3f} spread={spread:.3f}"
                        f" quality={quality:.3f} touch={touch:.0f}"
                    )
                )

    # ── Killer score ─────────────────────────────────────────────────────────
    touch_norm  = min(1.0, touch / max(1.0, adaptive_touch_req * 2))
    spread_norm = max(0.0, 1.0 - (spread / max(0.001, adaptive_spread_cap)))
    killer_score = clamp01(
        edge     * 0.40 +
        quality  * 0.25 +
        touch_norm  * 0.15 +
        spread_norm * 0.10 +
        pressure * 0.10
    )
    if VERBOSE_LOGS:
        log.info(
            f"[KILLER] ticker={ticker} edge={edge:.3f} quality={quality:.3f}"
            f" touch={touch:.0f} spread={spread:.3f} pressure={pressure:.3f} score={killer_score:.3f}"
        )
    if touch < MIN_BOOK_SIZE:
        if killer_score >= 0.62 and quality >= 0.70 and spread <= adaptive_spread_cap:
            log.info(f"[LIQUIDITY OVERRIDE] {ticker}")
        else:
            return False, "bad_liquidity", {"edge": edge, "quality": quality, "touch": touch, "spread": spread, "pressure": pressure, "killer_score": killer_score, "threshold": 0.62}

    # ── Tiered killer score ──────────────────────────────────────────────────
    # Edge-tiered threshold: stronger edge lowers the required killer score.
    if edge >= 0.35:
        threshold = 0.50
    elif edge >= 0.25:
        threshold = 0.52
    else:
        threshold = 0.54
    HARD_LOWER_BUFFER = 0.03          # scores in [threshold-0.03, threshold) → NEAR_STALKER
    killer_cutoff = threshold + 0.06
    if killer_score >= killer_cutoff:
        killer_tier = "KILLER"
    elif killer_score >= threshold:
        killer_tier = "STALKER"
    elif killer_score >= threshold - HARD_LOWER_BUFFER:
        killer_tier = "NEAR_STALKER"  # passes at half-size instead of hard blocking
    else:
        return False, "killer_score", {"edge": edge, "quality": quality, "touch": touch, "spread": spread, "pressure": pressure, "killer_score": killer_score, "threshold": threshold}

    diag = {"edge": edge, "quality": quality, "touch": touch, "spread": spread, "pressure": pressure, "killer_score": killer_score, "killer_tier": killer_tier, "threshold": threshold}
    return True, "pass", diag

def compute_position_intel(trade, side, prices, entry_meta, adaptive_touch_req=10, adaptive_spread_cap=0.05):
    ticker = trade.get("ticker", "")
    p = prices.get(ticker, {}) or {}
    entry_price = safe_float(trade.get("crowd_prob"), 0.0) or 0.0
    model_prob = safe_float(trade.get("model_prob"), None)
    if model_prob is None:
        model_prob = clamp01(entry_price + (safe_float(entry_meta.get("entry_edge"), 0.0) or 0.0))
    quotes = normalize_quote_state(p)
    spread = quotes[f"{side}_spread"]
    if spread is None:
        spread = safe_float(entry_meta.get("entry_spread"), 0.05) or 0.05
    pressure = safe_float(p.get("no_pressure_score" if side == "no" else "pressure_score"), None)
    if pressure is None:
        pressure = clamp01(safe_float(entry_meta.get("entry_pressure"), entry_meta.get("pressure_score", 0.0)) or 0.0)
    touch = quotes[f"{side}_touch"]
    quality = market_quality_score(dict(p), side, spread, safe_float(trade.get("hours_to_close"), 1.0) * 60.0)
    if side == "no":
        ref_price = quotes["no_bid"] if quotes["no_bid"] is not None else quotes["no_ask"]
        if ref_price is None:
            ref_price = entry_price
        edge = max(0.0, (1.0 - model_prob) - ref_price)
        exit_price = ref_price
    else:
        ref_price = quotes["yes_bid"] if quotes["yes_bid"] is not None else quotes["yes_ask"]
        if ref_price is None:
            ref_price = entry_price
        edge = max(0.0, model_prob - ref_price)
        exit_price = ref_price
    killer_score, _, _ = compute_killer_score_components(edge, quality, touch, spread, pressure, adaptive_touch_req=adaptive_touch_req, adaptive_spread_cap=adaptive_spread_cap)
    conviction_state = classify_conviction_state(killer_score, edge)
    entry_killer = safe_float(entry_meta.get("entry_killer_score"), None)
    if entry_killer is None:
        entry_killer = safe_float(entry_meta.get("killer_score"), 0.0) or 0.0
    killer_delta = round(killer_score - entry_killer, 4)
    quality_delta = round(quality - clamp01(safe_float(entry_meta.get("quality_score"), 0.0) or 0.0), 4)
    conviction_delta_state = classify_conviction_delta(killer_delta)
    return {
        "current_killer_score": round(killer_score, 4),
        "current_quality_score": round(quality, 4),
        "current_pressure_score": round(pressure, 4),
        "current_spread": round(max(0.0, spread), 4),
        "current_touch": round(max(0.0, touch), 2),
        "current_edge": round(max(0.0, edge), 4),
        "conviction_state": conviction_state,
        "killer_delta": killer_delta,
        "quality_delta": quality_delta,
        "conviction_delta_state": conviction_delta_state,
        "current_exit_price": ref_price,
        "current_pnl_pct": round((ref_price - entry_price) if side == "yes" else (ref_price - entry_price), 4),
    }


def log_portfolio_intel_summary(open_rows, prices, now):
    conviction_usd = {"elite": 0.0, "strong": 0.0, "neutral": 0.0, "weak": 0.0}
    family_usd = defaultdict(float)
    winners = 0
    losers = 0
    pnl_sum = 0.0
    max_position_size = 0.0
    total_open_usd = 0.0
    for row in open_rows:
        ticker = row.get("ticker", "")
        side = row.get("side", "")
        size = safe_float(row.get("position_usd"), 0.0) or 0.0
        total_open_usd += size
        max_position_size = max(max_position_size, size)
        family_usd[row.get("family", "OTHER")] += size
        entry_meta = _entry_metrics.get((ticker, side), {})
        intel = compute_position_intel(row, side, prices, entry_meta)
        conviction_usd[intel["conviction_state"]] += size
        pnl_usd = round(size * intel["current_pnl_pct"], 2)
        pnl_sum += pnl_usd
        if pnl_usd > 0:
            winners += 1
        elif pnl_usd < 0:
            losers += 1
    avg_pnl = (pnl_sum / len(open_rows)) if open_rows else 0.0
    btc_exp = (family_usd.get("KXBTC", 0.0) / total_open_usd) if total_open_usd > 0 else 0.0
    eth_exp = (family_usd.get("KXETH", 0.0) / total_open_usd) if total_open_usd > 0 else 0.0
    if VERBOSE_LOGS:
        log.info(
            cyan(
                f"PORTFOLIO_STATE | total_open={len(open_rows)}"
                f" elite_usd={conviction_usd['elite']:.2f} strong_usd={conviction_usd['strong']:.2f}"
                f" neutral_usd={conviction_usd['neutral']:.2f} weak_usd={conviction_usd['weak']:.2f}"
            )
        )
        log.info(
            cyan(
                f"PORTFOLIO_RISK | btc_exposure={btc_exp:.1%} eth_exposure={eth_exp:.1%}"
                f" max_single_position={max_position_size:.2f}"
            )
        )
        log.info(
            cyan(
                f"PORTFOLIO_PERF | winners={winners} losers={losers} avg_pnl={fmt_money(avg_pnl)}"
            )
        )


def compute_rotation_score(candidate, victim, open_rows):
    victim_row, _victim_score, victim_edge, _victim_pnl_pct, _victim_exit_price, victim_held_secs, _victim_pressure, _victim_spread, _winner_protected = victim
    victim_quality = position_entry_quality(victim_row)
    new_ev = (candidate.get("edge", 0.0) or 0.0) * clamp01(candidate.get("quality_score", 0.0) or 0.0)
    old_ev = victim_edge * victim_quality
    time_decay_bonus = round(0.05 * clamp01(victim_held_secs / 1800.0), 4)
    spread_penalty = round(max(0.0, (candidate.get("spread", 0.0) or 0.0) - 0.02) * 0.8, 4)
    diversification_boost = diversification_bonus(candidate, victim_row, open_rows)
    rotation_score = round(new_ev - old_ev + time_decay_bonus - spread_penalty + diversification_boost, 4)
    return rotation_score, victim_quality, time_decay_bonus, spread_penalty, diversification_boost


def rotation_upgrade_allowed(open_pos, candidate):
    victim_row, _victim_score, victim_edge, victim_pnl_pct, _victim_exit_price, victim_held_secs, victim_pressure, victim_spread, _winner_protected = open_pos
    victim_quality = position_entry_quality(victim_row)
    victim_entry_price = safe_float(victim_row.get("crowd_prob"), 0.0) or 0.0
    victim_size = safe_float(victim_row.get("position_usd"), 0.0) or 0.0
    victim_side = victim_row.get("side", "")
    victim_entry_meta = _entry_metrics.get((victim_row.get("ticker", ""), victim_side), {})
    # Suppress rotation for long-horizon high-edge positions.
    # Same rationale as time_stop suppression: a YES at 10¢ with 8h+ to close
    # and genuine model edge cannot play out in the bot's short hold window —
    # rotating it out locks in the loss before any resolution is possible.
    # dead_micro / cheap_stale paths are exempt (those are genuinely dead).
    _victim_hours = safe_float(victim_entry_meta.get("hours_to_close"), 0.0) or 0.0
    _victim_edge  = safe_float(victim_entry_meta.get("entry_edge"), victim_edge) or victim_edge
    if _victim_hours > 1.0 and _victim_edge > 0.20:
        return False, "rotation_suppressed_long_horizon", 0.0
    victim_entry_spread = max(0.0, safe_float(victim_entry_meta.get("entry_spread"), 0.05) or 0.05)

    challenger_edge = safe_float(candidate.get("edge"), 0.0) or 0.0
    challenger_quality = clamp01(safe_float(candidate.get("quality_score"), 0.0) or 0.0)
    challenger_pressure = clamp01(safe_float(candidate.get("pressure_score"), 0.0) or 0.0)
    challenger_touch = max(0.0, safe_float(candidate.get("selected_touch"), 0.0) or 0.0)
    challenger_spread = max(0.0, safe_float(candidate.get("spread"), 0.05) or 0.05)

    challenger_score = round(
        0.40 * challenger_edge
        + 0.20 * challenger_quality
        + 0.20 * challenger_pressure
        + 0.10 * min(challenger_touch / 100.0, 1.0)
        + 0.10 * max(0.0, 0.05 - challenger_spread),
        4,
    )
    reinforce_eligible = (
        victim_pnl_pct >= 0.0
        and victim_held_secs >= REINFORCE_MIN_HOLD_SECS
        and victim_quality >= 0.65
        and victim_pressure >= 0.50
        and victim_spread <= max(0.05, victim_entry_spread * 1.5)
    )

    dead_micro = (
        victim_entry_price <= DEAD_PRICE_MAX
        and victim_size <= DEAD_SIZE_MAX
        and victim_pnl_pct <= DEAD_PNL
        and victim_held_secs >= DEAD_HOLD_SECS
    )
    cheap_stale = (
        victim_entry_price <= CHEAP_ENTRY_PRICE_MAX
        and victim_size <= CHEAP_POSITION_USD_MAX
        and victim_pnl_pct <= CHEAP_NEG_PNL_EXIT
        and victim_held_secs >= CHEAP_MAX_HOLD_SECS
    )
    stale_weak = (
        victim_held_secs >= 180
        and victim_pnl_pct <= 0.0
        and (
            victim_edge < 0.18
            or victim_quality < 0.55
            or victim_pressure < 0.50
            or victim_spread >= 0.04
        )
    )
    weakness_score = round(
        0.35 * max(0.0, -victim_pnl_pct)
        + 0.20 * min(victim_held_secs / 300.0, 1.0)
        + 0.15 * (0.0 if reinforce_eligible else 1.0)
        + 0.15 * (1.0 if victim_entry_price <= ROTATION_CHEAP_ENTRY_MAX else 0.0)
        + 0.15 * max(0.0, victim_spread - victim_entry_spread),
        4,
    )
    alpha = round(challenger_score - weakness_score, 4)

    if dead_micro and alpha >= ROTATION_MIN_ALPHA:
        return True, "dead_micro_recycle", alpha
    if cheap_stale and alpha >= ROTATION_MIN_ALPHA:
        return True, "cheap_stale_upgrade", alpha
    if (
        stale_weak
        and challenger_edge >= victim_edge + ROTATION_MIN_EDGE_ADVANTAGE
        and alpha >= ROTATION_MIN_ALPHA
    ):
        return True, "stale_weak_upgrade", alpha
    if (
        challenger_quality >= victim_quality + ROTATION_MIN_QUALITY_ADVANTAGE
        and alpha >= ROTATION_MIN_ALPHA
    ):
        return True, "material_upgrade", alpha
    return False, "no_material_upgrade", alpha

def rebuild_open_position_state():
    open_rows = load_open_rows()
    _series_open_count.clear()
    total_open_exposure = 0.0
    for row in open_rows:
        fam = row.get("family", "")
        _series_open_count[fam] = _series_open_count.get(fam, 0) + 1
        total_open_exposure += safe_float(row.get("position_usd"), 0.0) or 0.0
    log.info(f"[POST-EXIT STATE] active_positions={len(open_rows)}")
    return open_rows, total_open_exposure

def validate_cycle_state(open_rows, total_open_exposure, exited_tickers, exits_this_cycle=0, entries_this_cycle=0):
    file_rows = load_open_rows()
    delta = len(file_rows) - len(open_rows)  # positive = file grew, negative = file shrank
    if delta != 0:
        if delta > 0 and delta <= entries_this_cycle:
            log.warning(f"[STATE WARN] open_count_delta | rebuilt={len(open_rows)} file={len(file_rows)} explained_by_entries={entries_this_cycle}")
        elif delta < 0 and abs(delta) <= exits_this_cycle:
            log.warning(f"[STATE WARN] open_count_delta | rebuilt={len(open_rows)} file={len(file_rows)} explained_by_exits={exits_this_cycle}")
        else:
            log.error(f"[STATE ERROR] open_count_mismatch | rebuilt={len(open_rows)} file={len(file_rows)} exits={exits_this_cycle} entries={entries_this_cycle}")
    exposure_check = round(sum(safe_float(r.get('position_usd'), 0.0) or 0.0 for r in open_rows), 4)
    if round(total_open_exposure, 4) != exposure_check:
        log.error(f"[STATE ERROR] exposure_mismatch | tracked={total_open_exposure:.4f} rebuilt={exposure_check:.4f}")
    open_tickers = {r.get('ticker') for r in open_rows}
    for ticker in exited_tickers:
        if ticker in open_tickers:
            log.error(f"[STATE ERROR] exited_ticker_still_open | ticker={ticker}")

def clamp01(x):
    return max(0.0, min(1.0, x))

def safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default

def ticker_series(ticker):
    return (ticker or "").split("-")[0]

def utc_now():
    return datetime.now(timezone.utc)

def _touch_size(levels, side="bid"):
    if not levels:
        return 0.0
    best_price = None
    best_size = 0.0
    for level in levels:
        price = safe_float(level[0], None)
        size = safe_float(level[1], 0.0) or 0.0
        if price is None or size <= 0:
            continue
        if side == "bid":
            is_better = best_price is None or price > best_price
        else:
            is_better = best_price is None or price < best_price
        if is_better:
            best_price = price
            best_size = size
    return best_size

def _near_touch_size(levels, band=0.03, side="bid"):
    if not levels:
        return 0.0
    prices = [(safe_float(level[0], None), safe_float(level[1], 0.0) or 0.0) for level in levels]
    prices = [(price, size) for price, size in prices if price is not None and size > 0]
    if not prices:
        return 0.0
    best_price = max(price for price, _ in prices) if side == "bid" else min(price for price, _ in prices)
    if side == "bid":
        return sum(size for price, size in prices if price >= best_price - band)
    return sum(size for price, size in prices if price <= best_price + band)


def _extract_levels_from_book(container, candidate_keys):
    if not isinstance(container, dict):
        return []
    for key in candidate_keys:
        levels = container.get(key)
        if isinstance(levels, list):
            return levels
    return []


def _best_price_from_levels(levels, side="bid"):
    if not levels:
        return None
    prices = []
    for level in levels:
        price = None
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            price = safe_float(level[0], None)
            size = safe_float(level[1], 0.0) or 0.0
            if price is not None and size > 0:
                prices.append(price)
        elif isinstance(level, dict):
            price = safe_float(level.get("price", level.get("p")), None)
            size = safe_float(level.get("size", level.get("quantity", level.get("qty", level.get("q")))), 0.0) or 0.0
            if price is not None and size > 0:
                prices.append(price)
    if not prices:
        return None
    return max(prices) if side == "bid" else min(prices)


def best_bid(levels):
    best = None
    for level in levels or []:
        try:
            if isinstance(level, (list, tuple)) and len(level) >= 1:
                price = float(level[0])
            elif isinstance(level, dict):
                price = float(level.get("price") or level.get("p"))
            else:
                continue

            if best is None or price > best:
                best = price
        except Exception:
            continue
    return best


def _normalize_level_list(levels):
    normalized = []
    if not isinstance(levels, list):
        return normalized
    for level in levels:
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            price = safe_float(level[0], None)
            size = safe_float(level[1], 0.0) or 0.0
        elif isinstance(level, dict):
            price = safe_float(level.get("price", level.get("p")), None)
            size = safe_float(level.get("size", level.get("quantity", level.get("qty", level.get("q")))), 0.0) or 0.0
        else:
            continue
        if price is None or size <= 0:
            continue
        normalized.append([price, size])
    return normalized


def _extract_book_payload(data):
    if not isinstance(data, dict):
        return {}
    if isinstance(data.get("orderbook"), dict):
        return data.get("orderbook") or {}
    if isinstance(data.get("market"), dict) and isinstance((data.get("market") or {}).get("orderbook"), dict):
        return data["market"]["orderbook"] or {}
    return data


def parse_single_orderbook_payload(ticker, data):
    function_name = "parse_single_orderbook_payload"
    print("TRACE: HIT", function_name)
    payload = _extract_book_payload(data)
    fetched_orderbook = bool(payload)
    fp = payload.get("orderbook_fp", {}) if isinstance(payload, dict) else {}
    payload_keys = list(payload.keys()) if isinstance(payload, dict) else []

    yes_bid_levels = _normalize_level_list(
        _extract_levels_from_book(payload, ["yes_bids", "yes_bid_levels", "yes", "yes_orders", "yes_dollars"])
        or _extract_levels_from_book(fp, ["yes_bids", "yes_bid_levels", "yes", "yes_orders", "yes_dollars"])
    )
    yes_ask_levels = _normalize_level_list(
        _extract_levels_from_book(payload, ["yes_asks", "yes_ask_levels", "sell_yes", "sell_yes_orders", "yes_asks_dollars"])
        or _extract_levels_from_book(fp, ["yes_asks", "yes_ask_levels", "sell_yes", "sell_yes_orders", "yes_asks_dollars"])
    )
    no_bid_levels = _normalize_level_list(
        _extract_levels_from_book(payload, ["no_bids", "no_bid_levels", "no", "no_orders", "no_dollars"])
        or _extract_levels_from_book(fp, ["no_bids", "no_bid_levels", "no", "no_orders", "no_dollars"])
    )
    no_ask_levels = _normalize_level_list(
        _extract_levels_from_book(payload, ["no_asks", "no_ask_levels", "sell_no", "sell_no_orders", "no_asks_dollars"])
        or _extract_levels_from_book(fp, ["no_asks", "no_ask_levels", "sell_no", "sell_no_orders", "no_asks_dollars"])
    )

    yes_bid = (
        safe_float(payload.get("yes_bid", payload.get("best_yes_bid")), None)
        if isinstance(payload, dict) else None
    )
    yes_ask = (
        safe_float(payload.get("yes_ask", payload.get("best_yes_ask")), None)
        if isinstance(payload, dict) else None
    )
    no_bid = (
        safe_float(payload.get("no_bid", payload.get("best_no_bid")), None)
        if isinstance(payload, dict) else None
    )
    no_ask = (
        safe_float(payload.get("no_ask", payload.get("best_no_ask")), None)
        if isinstance(payload, dict) else None
    )

    if yes_bid is None:
        yes_bid = _best_price_from_levels(yes_bid_levels, side="bid")
    if yes_ask is None:
        yes_ask = _best_price_from_levels(yes_ask_levels, side="ask")
    if no_bid is None:
        no_bid = _best_price_from_levels(no_bid_levels, side="bid")
    if no_ask is None:
        no_ask = _best_price_from_levels(no_ask_levels, side="ask")
    if fetched_orderbook and all(v is None for v in [yes_bid, yes_ask, no_bid, no_ask]):
        log.warning(f"[BOOK_PARSE_EMPTY] ticker={ticker} raw_payload={payload}")
    print("OB_KEYS:", payload_keys)
    print("BIDS:", yes_bid, no_bid)

    yes_bid_touch = _touch_size(yes_bid_levels, side="bid")
    yes_ask_touch = _touch_size(yes_ask_levels, side="ask")
    no_bid_touch = _touch_size(no_bid_levels, side="bid")
    no_ask_touch = _touch_size(no_ask_levels, side="ask")
    yes_touch_size = yes_ask_touch if yes_ask_touch > 0 else (no_bid_touch if no_bid is not None else yes_bid_touch)
    no_touch_size = no_ask_touch if no_ask_touch > 0 else (yes_bid_touch if yes_bid is not None else no_bid_touch)
    yes_depth_near = _near_touch_size(yes_bid_levels, side="bid")
    no_depth_near = _near_touch_size(no_bid_levels, side="bid")
    yes_pressure = sum(level[1] for level in yes_bid_levels)
    no_pressure = sum(level[1] for level in no_bid_levels)
    total_pressure = yes_pressure + no_pressure
    pressure_score = (yes_pressure / total_pressure) if total_pressure > 0 else None
    no_pressure_score = (no_pressure / total_pressure) if total_pressure > 0 else None

    if VERBOSE_LOGS:
        log.info(f"[BOOK_PARSE] ticker={ticker} yes_bid={yes_bid} yes_ask={yes_ask} no_bid={no_bid} no_ask={no_ask} yes_touch={yes_touch_size} no_touch={no_touch_size}")
    return {
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "yes_pressure": yes_pressure,
        "no_pressure": no_pressure,
        "pressure_score": pressure_score,
        "no_pressure_score": no_pressure_score,
        "yes_touch_size": yes_touch_size,
        "no_touch_size": no_touch_size,
        "yes_depth_near": yes_depth_near,
        "no_depth_near": no_depth_near,
    }

def _normalize_edge(edge):
    return clamp01((edge or 0.0) / 0.05)

def _normalize_volume(volume):
    return clamp01(math.log10(1.0 + max(0.0, volume or 0.0)) / 5.0)

def pressure_accel(current, recent_history):
    if current is None or not recent_history:
        return 0.0
    if len(recent_history) >= 2:
        return float(current) - float(recent_history[-1]) + (float(recent_history[-1]) - float(recent_history[-2]))
    return float(current) - float(recent_history[-1])

def spread_is_expanding(ticker):
    hist = _spread_history.get(ticker, [])
    if len(hist) < 3:
        return False
    return hist[-1] > hist[-2] > hist[-3]

def book_depth_score(market):
    quotes = normalize_quote_state(market)
    yes_touch = quotes["yes_touch"]
    no_touch = quotes["no_touch"]
    near_touch = (safe_float(market.get("yes_depth_near"), 0.0) or 0.0) + (safe_float(market.get("no_depth_near"), 0.0) or 0.0)
    both_sides = 1.0 if yes_touch > 0 and no_touch > 0 else 0.0
    touch_component = clamp01(min(yes_touch, no_touch) / 20.0)
    near_component = clamp01(near_touch / 60.0)
    return clamp01(both_sides * 0.35 + touch_component * 0.35 + near_component * 0.30)

def liquidity_score(market):
    quotes = normalize_quote_state(market)
    best_spread = min([sp for sp in [quotes["yes_spread"], quotes["no_spread"]] if sp is not None], default=None)
    if best_spread is None:
        return 0.0
    spread_score = clamp01((MAX_HARD_SPREAD - max(0.0, best_spread)) / max(MAX_HARD_SPREAD, 0.0001))
    volume_score = _normalize_volume(safe_float(market.get("volume_24h"), 0.0) or safe_float(market.get("volume"), 0.0) or 0.0)
    touch_score = clamp01((quotes["yes_touch"] + quotes["no_touch"]) / 30.0)
    depth_score = clamp01(((safe_float(market.get("yes_depth_near"), 0.0) or 0.0) + (safe_float(market.get("no_depth_near"), 0.0) or 0.0)) / 80.0)
    thin_penalty = 0.20 if min(quotes["yes_touch"], quotes["no_touch"]) < 2.0 else 0.0
    return clamp01(spread_score * 0.35 + volume_score * 0.20 + touch_score * 0.25 + depth_score * 0.20 - thin_penalty)

def has_real_liquidity(book):
    if not book:
        return False
    quotes = normalize_quote_state(book)
    yes_bid_size = quotes["yes_touch"]
    no_bid_size  = quotes["no_touch"]
    best_spread = min([sp for sp in [quotes["yes_spread"], quotes["no_spread"]] if sp is not None], default=None)
    if best_spread is None:
        return False
    return (yes_bid_size >= 50 or no_bid_size >= 50) and best_spread <= 0.05

def is_real_book(row):
    return not is_dead_book(row)

def is_dead_book(row):
    quotes = normalize_quote_state(row)
    return not (quotes["yes_has_real_book"] or quotes["no_has_real_book"])

def compute_cycle_regime(markets):
    """Inspect current orderbook set and return adaptive thresholds for this cycle."""
    spreads = []
    count_real_book = count_touch_50 = count_touch_10 = 0
    count_spread_05 = count_spread_08 = 0
    for m in markets:
        quotes = normalize_quote_state(m)
        if not quotes["market_has_any_real_side"]:
            continue
        count_real_book += 1
        best_spread = min(
            [sp for sp in [quotes["yes_spread"], quotes["no_spread"]] if sp is not None],
            default=None,
        )
        if best_spread is not None:
            spreads.append(best_spread)
        yt = quotes["yes_touch"]
        nt = quotes["no_touch"]
        if max(yt, nt) >= 50: count_touch_50 += 1
        if max(yt, nt) >= 10: count_touch_10 += 1
        if best_spread is not None and best_spread <= 0.05: count_spread_05 += 1
        if best_spread is not None and best_spread <= 0.08: count_spread_08 += 1
    n = len(spreads)
    if n == 0:
        median_sp = p25_sp = HARD_SPREAD_FLOOR
        p75_sp = HARD_SPREAD_CEIL
    else:
        spreads.sort()
        median_sp = spreads[n // 2]
        p25_sp    = spreads[max(0, n // 4)]
        p75_sp    = spreads[min(3 * n // 4, n - 1)]
    adaptive_spread_cap = min(HARD_SPREAD_CEIL, max(HARD_SPREAD_FLOOR, max(0.05, median_sp)))
    if median_sp <= 0.05:
        adaptive_touch_req = TOUCH_REQ_TIGHT
    elif median_sp <= 0.07:
        adaptive_touch_req = TOUCH_REQ_MID
    else:
        adaptive_touch_req = TOUCH_REQ_WIDE
    bad_regime = (median_sp >= 0.08 and count_spread_05 == 0)
    log.info(cyan(
        f"REGIME | median_spread={median_sp:.4f} p25={p25_sp:.4f} p75={p75_sp:.4f}"
        f" real_book={count_real_book} touch50={count_touch_50} touch10={count_touch_10}"
        f" spread<=0.05={count_spread_05} spread<=0.08={count_spread_08}"
        f" bad_regime={bad_regime}"
    ))
    return {
        "median_spread":             median_sp,
        "p25_spread":                p25_sp,
        "p75_spread":                p75_sp,
        "count_with_real_book":      count_real_book,
        "count_with_touch_ge_50":    count_touch_50,
        "count_with_touch_ge_10":    count_touch_10,
        "count_with_spread_le_0_05": count_spread_05,
        "count_with_spread_le_0_08": count_spread_08,
        "adaptive_spread_cap":       adaptive_spread_cap,
        "adaptive_touch_req":        adaptive_touch_req,
        "bad_regime":                bad_regime,
    }


def compute_global_regime(cycle_regime, candidates, open_rows, prices):
    real_book_count = cycle_regime["count_with_real_book"]
    spread_ratio = cycle_regime["count_with_spread_le_0_05"] / max(1, real_book_count)
    touch_ratio = cycle_regime["count_with_touch_ge_10"] / max(1, real_book_count)
    deep_touch_ratio = cycle_regime["count_with_touch_ge_50"] / max(1, real_book_count)
    book_health = clamp01(spread_ratio * 0.45 + touch_ratio * 0.35 + deep_touch_ratio * 0.20)

    top_candidates = sorted(
        candidates,
        key=lambda c: (c.get("edge", 0.0), c.get("killer_score", 0.0), c.get("quality_score", 0.0)),
        reverse=True,
    )[:5]
    if top_candidates:
        avg_edge = sum(c.get("edge", 0.0) or 0.0 for c in top_candidates) / len(top_candidates)
        avg_killer = sum(c.get("killer_score", 0.0) or 0.0 for c in top_candidates) / len(top_candidates)
        avg_quality = sum(c.get("quality_score", 0.0) or 0.0 for c in top_candidates) / len(top_candidates)
        opportunity_quality = clamp01(avg_edge * 0.45 + avg_killer * 0.30 + avg_quality * 0.25)
    else:
        opportunity_quality = 0.0

    allocator_state = build_allocator_portfolio_state(open_rows, prices)
    open_exposure = sum(safe_float(r.get("position_usd"), 0.0) or 0.0 for r in open_rows)
    free_capital_pct = clamp01(1.0 - (open_exposure / max(1.0, BANKROLL)))
    family_load = max(allocator_state["family_count"].values(), default=0)
    family_drag = clamp01(family_load / max(1, MAX_OPEN_POSITIONS))
    portfolio_freedom = clamp01(
        free_capital_pct * 0.60 +
        (1.0 - allocator_state["weak_bucket_pct"]) * 0.25 +
        (1.0 - family_drag) * 0.15
    )

    execution_quality = clamp01(
        (1.0 - min(1.0, cycle_regime["median_spread"] / max(0.08, HARD_SPREAD_CEIL))) * 0.55 +
        touch_ratio * 0.25 +
        (cycle_regime["count_with_spread_le_0_08"] / max(1, real_book_count)) * 0.20
    )

    regime_score = round(
        book_health * 0.30 +
        opportunity_quality * 0.30 +
        portfolio_freedom * 0.20 +
        execution_quality * 0.20,
        4
    )
    if regime_score >= 0.68:
        regime = "ATTACK"
        action = {
            "allow_new_entries": True,
            "allow_reinforce": True,
            "allow_rotation": True,
            "cycle_cap_mult": 1.10,
            "kelly_max_mult": 1.15,
            "min_alloc_score": max(0.18, MIN_ALLOC_SCORE - 0.02),
        }
    elif regime_score >= 0.50:
        regime = "NORMAL"
        action = {
            "allow_new_entries": True,
            "allow_reinforce": True,
            "allow_rotation": True,
            "cycle_cap_mult": 1.00,
            "kelly_max_mult": 1.05,
            "min_alloc_score": MIN_ALLOC_SCORE,
        }
    elif regime_score >= 0.35:
        regime = "DEFENSIVE"
        action = {
            "allow_new_entries": True,
            "allow_reinforce": True,
            "allow_rotation": True,
            "cycle_cap_mult": 0.65,
            "kelly_max_mult": 0.90,
            "min_alloc_score": max(MIN_ALLOC_SCORE, 0.26),
        }
    else:
        regime = "NO_TRADE"
        action = {
            "allow_new_entries": False,
            "allow_reinforce": False,
            "allow_rotation": False,
            "cycle_cap_mult": 0.0,
            "kelly_max_mult": 0.60,
            "min_alloc_score": 1.01,
        }
    if VERBOSE_LOGS:
        log.info(cyan(
            f"[REGIME_FACTORS] book_health={book_health:.3f} opportunity_quality={opportunity_quality:.3f}"
            f" portfolio_freedom={portfolio_freedom:.3f} execution_quality={execution_quality:.3f}"
        ))
        log.info(cyan(f"[REGIME_SCORE] score={regime_score:.3f} regime={regime}"))
        log.info(cyan(
            f"[REGIME_ACTION] allow_new_entries={1 if action['allow_new_entries'] else 0}"
            f" allow_reinforce={1 if action['allow_reinforce'] else 0}"
            f" allow_rotation={1 if action['allow_rotation'] else 0}"
            f" cycle_cap_mult={action['cycle_cap_mult']:.2f}"
            f" kelly_max_mult={action['kelly_max_mult']:.2f}"
            f" min_alloc_score={action['min_alloc_score']:.2f}"
        ))
    return {
        "book_health": book_health,
        "opportunity_quality": opportunity_quality,
        "portfolio_freedom": portfolio_freedom,
        "execution_quality": execution_quality,
        "regime_score": regime_score,
        "regime": regime,
        "action": action,
    }

def market_quality_score(m, selected_side, spread, minutes_to_expiry):
    """Score 0–1 rating this market/side combination on book quality."""
    quotes = normalize_quote_state(m)
    spread_score = clamp01(1.0 - spread / 0.10)
    touch = float(quotes.get(f"{selected_side}_entry_touch", 0.0) or 0.0)
    touch_score = clamp01(touch / 100.0)
    yt = float(quotes.get("yes_entry_touch", 0.0) or 0.0)
    nt = float(quotes.get("no_entry_touch", 0.0) or 0.0)
    total_touch = yt + nt
    balance = (1.0 - abs(yt - nt) / max(total_touch, 1.0)) if total_touch > 0 else 0.0
    time_score = clamp01(1.0 - max(0.0, minutes_to_expiry - 45) / max(SELECTION_MAX_MINUTES - 45, 1))
    return round(clamp01(spread_score * 0.40 + touch_score * 0.30 + balance * 0.20 + time_score * 0.10), 4)


def _complement_binary_price(price):
    price = safe_float(price, None)
    if price is None or price < 0.0 or price > 1.0:
        return None
    return round(1.0 - price, 4)


def normalize_quote_state(market):
    yes_bid = safe_float(market.get("yes_bid"), None)
    raw_yes_ask = safe_float(market.get("yes_ask"), None)
    no_bid = safe_float(market.get("no_bid"), None)
    raw_no_ask = safe_float(market.get("no_ask"), None)
    yes_touch = float(market.get("yes_touch_size") or 0.0)
    no_touch = float(market.get("no_touch_size") or 0.0)
    yes_ask = raw_yes_ask if raw_yes_ask is not None else _complement_binary_price(no_bid)
    no_ask = raw_no_ask if raw_no_ask is not None else _complement_binary_price(yes_bid)
    yes_effective_bid = yes_bid if yes_bid is not None else _complement_binary_price(raw_no_ask)
    no_effective_bid = no_bid if no_bid is not None else _complement_binary_price(raw_yes_ask)
    yes_spread = (yes_ask - yes_effective_bid) if yes_ask is not None and yes_effective_bid is not None else None
    no_spread = (no_ask - no_effective_bid) if no_ask is not None and no_effective_bid is not None else None
    yes_has_real_quote = yes_bid is not None or yes_ask is not None
    no_has_real_quote = no_bid is not None or no_ask is not None
    yes_has_real_book = yes_has_real_quote and (yes_ask is None or yes_effective_bid is None or yes_ask >= yes_effective_bid)
    no_has_real_book = no_has_real_quote and (no_ask is None or no_effective_bid is None or no_ask >= no_effective_bid)
    yes_entry_touch = yes_touch if raw_yes_ask is not None else (no_touch if no_bid is not None else yes_touch)
    no_entry_touch = no_touch if raw_no_ask is not None else (yes_touch if yes_bid is not None else no_touch)
    yes_touch = max(yes_touch, yes_entry_touch)
    no_touch = max(no_touch, no_entry_touch)
    return {
        "yes_bid": yes_effective_bid,
        "yes_ask": yes_ask,
        "no_bid": no_effective_bid,
        "no_ask": no_ask,
        "yes_spread": yes_spread,
        "no_spread": no_spread,
        "yes_touch": yes_touch,
        "no_touch": no_touch,
        "yes_entry_touch": yes_entry_touch,
        "no_entry_touch": no_entry_touch,
        "yes_has_real_quote": yes_has_real_quote,
        "no_has_real_quote": no_has_real_quote,
        "yes_has_real_book": yes_has_real_book,
        "no_has_real_book": no_has_real_book,
        "market_has_any_real_side": yes_has_real_quote or no_has_real_quote,
    }


def side_book_metrics(market, side):
    quotes = normalize_quote_state(market)
    bid = quotes[f"{side}_bid"]
    ask = quotes[f"{side}_ask"]
    spread = quotes[f"{side}_spread"]
    touch = quotes[f"{side}_entry_touch"]
    has_real_quote = quotes[f"{side}_has_real_quote"]
    has_real_book = quotes[f"{side}_has_real_book"]
    if not has_real_quote:
        return {
            "side": side,
            "bid": bid,
            "ask": ask,
            "spread": spread,
            "touch": touch,
            "valid": False,
            "reason": "no_real_quote",
        }
    if ask is None:
        return {
            "side": side,
            "bid": bid,
            "ask": ask,
            "spread": spread,
            "touch": touch,
            "valid": False,
            "reason": "no_executable_ask",
        }
    if not has_real_book:
        return {
            "side": side,
            "bid": bid,
            "ask": ask,
            "spread": spread,
            "touch": touch,
            "valid": False,
            "reason": "no_real_book",
        }
    return {
        "side": side,
        "bid": bid,
        "ask": ask,
        "spread": spread,
        "touch": touch,
        "valid": True,
        "reason": "ok",
    }

def pre_trade_quality_filter(row):
    """Hard and soft pre-trade quality gates applied before edge evaluation.

    Returns (passed: bool, reject_reason: str | None).
    Hard rejects fire unconditionally; soft rejects require the combination
    of both conditions to be true.
    """
    spread    = row["spread"]
    yes_touch = row["yes_touch"]
    no_touch  = row["no_touch"]
    quality   = row.get("quality_score", 0.0)

    # Hard rejects — unconditional
    if spread > 0.08:
        return False, "spread_extreme"
    if max(yes_touch, no_touch) < 10:
        return False, "no_liquidity"

    # Soft rejects — compound condition only
    if spread > 0.06 and quality < 0.30:
        return False, "weak_quality_wide_spread"
    if quality < 0.15:
        return False, "low_quality"

    return True, None

def book_imbalance_score(market, side):
    yes_pressure = safe_float(market.get("yes_pressure"), 0.0) or 0.0
    no_pressure = safe_float(market.get("no_pressure"), 0.0) or 0.0
    total = yes_pressure + no_pressure
    if total <= 0:
        return 0.0
    return clamp01((yes_pressure if side == "yes" else no_pressure) / total)

def is_fragile_book(market, side):
    yes_bid = safe_float(market.get("yes_bid"), None)
    yes_ask = safe_float(market.get("yes_ask"), None)
    no_bid = safe_float(market.get("no_bid"), None)
    no_ask = safe_float(market.get("no_ask"), None)
    if yes_bid is None and no_bid is None:
        return True
    if yes_bid is None:
        yes_bid = _complement_binary_price(no_ask)
    if yes_ask is None:
        yes_ask = _complement_binary_price(no_bid)
    if yes_bid is None or yes_ask is None:
        return True
    if yes_ask < yes_bid:
        return True
    if no_bid is not None and no_ask is not None and no_ask < no_bid:
        return True
    if yes_bid is not None and no_bid is not None and yes_bid + no_bid > 1.02:
        return True
    spread = yes_ask - yes_bid
    touch = safe_float(market.get(f"{side}_touch_size"), 0.0) or 0.0
    depth = (safe_float(market.get("yes_depth_near"), 0.0) or 0.0) + (safe_float(market.get("no_depth_near"), 0.0) or 0.0)
    flip_flop = 0
    hist = _price_history.get((market.get("ticker", ""), side), [])
    if len(hist) >= 4:
        deltas = [hist[i] - hist[i - 1] for i in range(1, len(hist))]
        flip_flop = sum(1 for i in range(1, len(deltas)) if deltas[i] * deltas[i - 1] < 0)
    return touch < 2.0 or (spread > MAX_ENTRY_SPREAD and spread_is_expanding(market.get("ticker", ""))) or depth < 6.0 or flip_flop >= 2

def is_active_market(spot, prev_spot):
    if spot in (None, 0) or prev_spot in (None, 0):
        return True
    move = abs(spot - prev_spot) / prev_spot
    return move >= 0.0005   # 0.05% move

def _exit_bucket(held_secs, time_stop_secs):
    if held_secs < MIN_HOLD_SECONDS:
        return "hold"
    if held_secs < MIN_HOLD_SECONDS + EXIT_WARMUP_SECONDS:
        return "warmup"
    if held_secs < min(time_stop_secs, MIN_HOLD_SECONDS + EXIT_MATURE_SECONDS):
        return "mature"
    return "late"

def derive_exit_plan(entry_meta, spread, held_secs, time_stop_secs, conviction_state="neutral", pnl_pct=0.0, fake_edge_failed=False):
    entry_spread = safe_float(entry_meta.get("entry_spread"), spread)
    entry_spread = max(0.0, entry_spread or 0.0)
    current_spread = max(0.0, safe_float(spread, entry_spread) or 0.0)
    spread_ref = max(entry_spread, current_spread, 0.01)
    quality = safe_float(entry_meta.get("quality_score"), None)
    if quality is None:
        quality = safe_float(entry_meta.get("liquidity_score"), 0.45)
    quality = clamp01(quality or 0.45)
    entry_pressure = clamp01(safe_float(entry_meta.get("entry_pressure"), entry_meta.get("pressure_score", 0.0)) or 0.0)
    bucket = _exit_bucket(held_secs, time_stop_secs)

    tp = max(0.028, TP_PNL + 0.008 + spread_ref * 0.55 + max(0.0, quality - 0.60) * 0.012)
    sl = min(-0.040, STOP_PNL - 0.010 - spread_ref * 0.30 - max(0.0, 0.55 - quality) * 0.015)
    trail_trigger = max(TRAIL_ARM_PNL, TRAIL_TRIGGER_PNL * 2.0, spread_ref * 1.15)
    trail_giveback = max(0.006, TRAIL_GIVEBACK * 2.0, spread_ref * 0.70)
    stale_after = min(time_stop_secs, MIN_HOLD_SECONDS + EXIT_MATURE_SECONDS)
    stall_band = max(0.0020, spread_ref * 0.18)

    if bucket == "warmup":
        tp += 0.006
        sl -= 0.006
        trail_trigger = max(trail_trigger, 0.018)
        trail_giveback = max(trail_giveback, 0.008)
    elif bucket == "late":
        tp = max(0.018, tp - 0.010)
        sl = min(-0.030, sl + 0.010)
        trail_trigger = max(0.012, trail_trigger - 0.003)
        trail_giveback = max(0.005, trail_giveback - 0.002)
        stall_band = max(stall_band, 0.0030)

    pressure_floor = max(0.35, min(PRESSURE_EXIT, entry_pressure - 0.10 + max(0.0, 0.55 - quality) * 0.05))
    pressure_drop_confirm = max(0.08, 0.13 - quality * 0.04)
    hold_longer = pnl_pct > 0.01

    if hold_longer:
        time_stop_secs = int(time_stop_secs * 1.15)
        sl = max(sl, 0.0)

    if pnl_pct > 0:
        if conviction_state == "elite":
            trail_trigger += 0.005
            pressure_drop_confirm += 0.020
            time_stop_secs = int(time_stop_secs * 1.50)
        elif conviction_state == "strong":
            time_stop_secs = int(time_stop_secs * 1.25)
    elif pnl_pct < 0:
        if conviction_state == "weak":
            pressure_drop_confirm = max(0.05, pressure_drop_confirm - 0.015)
        elif conviction_state in {"strong", "elite"} and not fake_edge_failed:
            pressure_drop_confirm = max(pressure_drop_confirm, 0.10)

    return {
        "bucket": bucket,
        "tp": round(tp, 4),
        "sl": round(sl, 4),
        "catastrophic_sl": round(max(CATASTROPHIC_STOP, min(sl - 0.015, CATASTROPHIC_STOP_LOSS)), 4),
        "trail_trigger": round(trail_trigger, 4),
        "trail_giveback": round(trail_giveback, 4),
        "trail_floor": round(max(TRAIL_MIN_LOCK_PNL, trail_trigger * 0.35), 4),
        "pressure_floor": round(pressure_floor, 4),
        "pressure_drop_confirm": round(pressure_drop_confirm, 4),
        "stale_after": max(MIN_HOLD_SECONDS, int(min(stale_after, time_stop_secs))),
        "stall_band": round(stall_band, 4),
        "time_stop_secs": max(MIN_HOLD_SECONDS + 60, int(time_stop_secs)),
    }

def is_micro_active(row):
    spread = row['yes_ask'] - row['yes_bid']
    return spread >= 0.02 and spread <= 0.15

def is_valid_entry(yes_ask):
    return 0.12 <= yes_ask <= 0.18

def is_early_stage(yes_ask):
    return 0.20 <= yes_ask <= 0.45

def has_structure(row):
    return (
        row['yes_bid'] >= 0.15 and
        row['no_bid'] >= 0.15
    )

def extract_strike(ticker):
    return parse_strike(ticker)

def same_strike_cluster(t1, t2):
    return extract_strike(t1) == extract_strike(t2)

def detect_regime(candidates, active):
    if active:
        return "momentum"

    valid_books = 0
    for r in candidates:
        yes_bid = r.get("yes_bid")
        yes_ask = r.get("yes_ask")
        if (
            yes_bid is not None and
            yes_ask is not None and
            0.20 <= yes_ask <= 0.60 and
            (yes_ask - yes_bid) <= 0.08
        ):
            valid_books += 1

    if valid_books >= 2:
        return "sideways_trade"

    return "no_trade"

def classify_regime(candidate):
    pressure = candidate["pressure_score"]
    clean = not candidate["crowd_softening"] and not candidate["spread_expanding"] and not candidate["fragile_book"]
    if pressure >= max(MIN_PRESSURE + 0.08, 0.65) and clean:
        return "momentum_clean"
    if pressure >= 0.45 and clean:
        return "momentum_normal"
    if pressure >= MIN_PRESSURE and (candidate["fragile_book"] or candidate["spread_expanding"] or candidate["crowd_softening"]):
        return "momentum_fragile"
    if candidate["crowd"] >= 0.94 and candidate["pressure_delta"] <= -0.03 and candidate["liquidity_score"] >= max(MIN_LIQUIDITY_SCORE + 0.15, 0.5) and candidate["spread"] <= MAX_ENTRY_SPREAD * 0.8:
        return "mean_revert_candidate"
    return "no_trade"


def elite_entry_allowed(regime, pressure, spread, touch, side_valid=True, executable_valid=True):
    if not side_valid:
        return False, "side_invalid"
    if not executable_valid:
        return False, "non_executable"
    if regime == "no_trade" and not ELITE_ENTRY_ALLOW_IN_NO_TRADE:
        return False, "regime_no_trade"
    if pressure < ELITE_MIN_PRESSURE:
        return False, "low_pressure"
    if spread is None or spread > ELITE_MAX_SPREAD:
        return False, "wide_spread"
    if touch < ELITE_MIN_TOUCH:
        return False, "low_touch"
    return True, "ok"


def final_entry_allowed(candidate, regime, global_regime_name: str = ""):
    """Returns (allowed: bool, reason: str, size_mult: float).
    size_mult < 1.0 means allow but reduce position size.
    global_regime_name: the bot-wide regime string ("ATTACK"/"NORMAL"/etc.)
    """
    side = candidate.get("selected_side") or candidate.get("side")
    pressure = float(candidate.get("pressure_score", 0.0))
    spread = candidate.get("spread")
    touch = float(candidate.get("selected_touch", 0.0) or 0.0)
    edge = float(candidate.get("edge", 0.0))
    model_prob = float(candidate.get("mp", 0.5))
    side_valid = candidate.get("side_valid", True)
    executable_valid = candidate.get("executable_valid", True)
    size_mult = 1.0
    _attack = global_regime_name.upper() == "ATTACK"

    if not side_valid:
        return False, "side_invalid", 1.0
    if not executable_valid:
        return False, "non_executable", 1.0
    if not side:
        return False, "missing_side", 1.0

    gate_label = str(candidate.get("gate", "PASS") or "PASS")

    # ── pressure tiers ───────────────────────────────────────────────────────
    if pressure < 0.45:
        return False, "dead_market", 1.0
    if pressure < 0.52:
        return False, "weak_pressure", 1.0
    if abs(model_prob - 0.50) < 0.08:
        return False, "no_clear_direction", 1.0
    if edge < 0.58:
        return False, "low_edge", 1.0
    yes_pressure = float(candidate.get("yes_pressure_score", pressure))
    no_pressure = float(candidate.get("no_pressure_score", 0.0))
    if abs(yes_pressure - no_pressure) < 0.15:
        return False, "no_side_control", 1.0

    # ── Change 2: tightened mid-tier unlock ──────────────────────────────────
    # ATTACK regime: require pressure >= 0.50 and edge >= 0.40 for mid-tier.
    # In the grey zone (0.45 ≤ pressure < 0.50, ATTACK only) a mid-tier entry
    # is still allowed but ONLY with edge >= 0.55 AND spread <= 0.03.
    # All other regimes keep the original threshold (pressure >= 0.45, edge >= 0.50).
    if _attack:
        if pressure >= 0.50 and edge >= 0.40:
            _mid_tier = True
        elif 0.45 <= pressure < 0.50:
            _mid_tier = (edge >= 0.55 and spread is not None and spread <= 0.03)
            if not _mid_tier:
                return False, "attack_mid_tier_weak", 1.0
        else:
            _mid_tier = False
    else:
        _mid_tier = pressure >= 0.45 and edge >= 0.50
    if _mid_tier:
        return False, "mid_tier_disabled", 1.0
    if gate_label != "PASS":
        return False, "gate_block", 1.0

    # Per-regime adaptive floor.
    # In ATTACK, momentum_normal/fragile floor drops to 0.45 to allow
    # candidates that are just below the normal 0.50 threshold.
    _pressure_floor = (
        0.42 if regime == "momentum_clean"
        else (0.45 if _attack else 0.50) if regime in ("momentum_normal", "momentum_fragile")
        else 0.45  # mean_revert_candidate / no_trade
    )
    if pressure < _pressure_floor and not _mid_tier:
        return False, "low_pressure_gate", 1.0

    # Graduated size multiplier based on pressure tier.
    # In ATTACK, anything in the 0.45–0.75 range gets 0.4× (same as mid-tier).
    if pressure >= 0.75:
        pass                                          # elite — full size
    elif _mid_tier or (_attack and pressure >= 0.45):
        size_mult *= 0.4                              # mid-tier / ATTACK mid-tier

    # no_trade regime: stricter — mid-tier needs edge >= 0.60 to pass
    if regime == "no_trade":
        _nt_mid = pressure >= 0.45 and edge >= 0.60   # tighter bar for no_trade
        if pressure < NO_TRADE_MIN_PRESSURE:
            if _nt_mid:
                size_mult *= 0.5   # high-edge mid-tier allowed, further reduced
            else:
                return False, "no_trade_plus_weak_pressure", 1.0
        else:
            # Adequate pressure in no_trade — mild size penalty
            size_mult *= 0.75

    if spread is None:
        return False, "spread_missing", 1.0
    if spread > FINAL_ENTRY_MAX_SPREAD:
        return False, "final_wide_spread", 1.0
    if touch < FINAL_ENTRY_MIN_TOUCH:
        return False, "final_low_touch", 1.0
    return True, "ok", size_mult

def log_skip(ticker, reason, detail=""):
    _last_skip_reason_counts[reason] = _last_skip_reason_counts.get(reason, 0) + 1
    if LOG_SKIP_REASONS:
        extra = f" | {detail}" if detail else ""
        msg = f"[SKIP] {ticker} | reason={reason}{extra}"
        log.info(yellow(msg))

def purge_expired_runtime_guards(now):
    for store in (_entry_cooldown_by_ticker, _post_exit_cooldown_by_series):
        expired = [k for k, expiry in store.items() if expiry <= now]
        for k in expired:
            store.pop(k, None)

def compute_velocity_metrics(history):
    if len(history) < 2:
        return None
    p1, p2 = history[-2:]
    velocity_short = p2 - p1
    velocity_med   = velocity_short
    acceleration   = 0.0
    deltas = [velocity_short]
    smooth_up = sum(1 for d in deltas if d >= 0) / len(deltas)
    velocity_score = clamp01((velocity_med / 0.01) * 0.65 + smooth_up * 0.35)
    stability_score = 1.0
    max_jump = max(abs(d) for d in deltas)
    if max_jump > 0.03:
        stability_score -= min(0.75, (max_jump - 0.03) / 0.04)
    return {
        "velocity_short": velocity_short,
        "velocity_med": velocity_med,
        "acceleration": acceleration,
        "velocity_score": clamp01(velocity_score),
        "stability_score": clamp01(stability_score),
        "max_jump": max_jump,
    }

def compute_elite_score(crowd, mp, pressure_score, pressure_delta, spread, velocity_metrics):
    return round(
        pressure_score * 0.5 +
        max(0.0, 0.08 - spread) * 2.0 +
        max(0.0, mp - crowd) * 5.0,
        4,
    )

def compute_entry_score(pressure_score, spread, edge, spread_penalty):
    score = (
        pressure_score * 0.4 +
        (1 - spread) * 0.3 +
        edge * 0.3
    ) - spread_penalty
    return round(score, 4)

def load_cooldowns():
    """Return dict of {ticker_side_key: expiry_datetime} from cooldown file."""
    try:
        raw = read_json_state(COOLDOWN_LOG, {})
        now = datetime.now(timezone.utc)
        return {
            k: dt for k, v in raw.items()
            if (dt := normalize_timestamp_utc(v)) is not None and dt > now
        }
    except: return {}

def save_cooldowns(cd):
    write_json_state(COOLDOWN_LOG, {k: v.isoformat() for k, v in cd.items()})


def append_persisted_open_row(open_rows, candidate, written_entry_ts):
    open_rows.append({
        "timestamp": written_entry_ts,
        "ticker": candidate["ticker"],
        "side": candidate["side"],
        "crowd_prob": f"{candidate['crowd']:.4f}",
        "model_prob": f"{candidate['mp']:.4f}",
        "ev": f"{candidate['ev']:.4f}",
        "position_usd": candidate["size"],
        "hours_to_close": f"{candidate['hours']:.2f}",
        "family": candidate["family"],
        "strike": candidate["strike"] if candidate["strike"] is not None else "",
        "spot_price": f"{candidate['spot_price']:.2f}" if candidate["spot_price"] is not None else "",
        "close_time": candidate["close_time"],
        "tier": candidate["tier"],
    })

def get_headers():
    return {"Content-Type":"application/json","Authorization":f"Bearer {API_KEY}"}

def hours_until(ts):
    dt = normalize_timestamp_utc(ts)
    if dt is None:
        return 999.0
    return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds() / 3600)

def get_yes_price(m):
    for field in ["yes_bid","yes_ask","last_price","yes_price"]:
        v = m.get(field)
        if v is not None:
            v = float(v)
            return v/100.0 if v>1.0 else v
    return 0.5

def estimate_model_prob(market, spot_price, minutes_to_expiry):
    """Log-normal probability that spot finishes on the YES side of strike.

    Uses a normal approximation of the log-return distribution:
        z = log(spot / strike) / (vol * sqrt(T))
        P(YES) = Phi(z)   where Phi is the standard normal CDF

    This means: if spot > strike by several sigma → P(YES) close to 1;
    if spot ≈ strike → P(YES) ≈ 0.5; if spot < strike → P(YES) < 0.5.

    Edge confidence boost: when spread is wide OR time is very short,
    require larger z to avoid noise trades.
    """
    ticker = market.get("ticker", "") if isinstance(market, dict) else str(market)
    strike = extract_strike(ticker)
    if strike is None or spot_price is None or spot_price <= 0 or strike <= 0:
        return None

    T = max(minutes_to_expiry / 525600.0, 1e-6)   # minutes → fraction of year
    sigma = MODEL_VOL * math.sqrt(T)
    if sigma <= 0:
        return None

    z = math.log(spot_price / strike) / sigma
    prob = 0.5 * (1.0 + math.erf(z / math.sqrt(2)))
    return max(0.05, min(0.95, prob))

def kelly_size(model_p, crowd_p):
    b=(1.0-crowd_p)/crowd_p
    if b<=0: return 0.0
    f=(model_p*b-(1.0-model_p))/b
    if f<=0: return 0.0
    return round(min(MAX_TRADE,max(1.0,f*0.25*BANKROLL)),2)

def current_total_equity():
    open_exposure = round(sum(safe_float(r.get("position_usd"), 0.0) or 0.0 for r in load_open_rows()), 4)
    return current_cash_balance(open_exposure) + open_exposure

def log_signal(m, side, crowd, model, ev, size, hours, family, strike, spot_price):
    ticker=m.get("ticker","")
    with open(SIGNAL_LOG,"a",newline="") as f:
        csv.writer(f).writerow([
            datetime.now(timezone.utc).isoformat(),
            ticker,
            m.get("title","")[:60],
            side,
            f"{crowd:.4f}",
            f"{model:.4f}",
            f"{ev:.4f}",
            size,
            f"{hours:.2f}",
            family,
            strike if strike is not None else "",
            m.get("yes_bid",""),
            m.get("yes_ask",""),
            m.get("no_bid",""),
            m.get("no_ask",""),
            f"{spot_price:.2f}" if spot_price is not None else "",
        ])

def record_open_trade(m, side, crowd, model, ev, size, hours, family, strike, spot_price, tier=1, replace=False):
    return lifecycle_record_open_trade(
        market=m,
        side=side,
        crowd=crowd,
        model=model,
        ev=ev,
        size=size,
        hours=hours,
        family=family,
        strike=strike,
        spot_price=spot_price,
        tier=tier,
        replace=replace,
        load_rows_fn=load_open_rows,
        save_positions_fn=save_positions,
        last_exit_meta_by_ticker=_last_exit_meta_by_ticker,
        log=log,
    )

async def fetch_market_result(session, ticker):
    """Return 'yes', 'no', or None if not yet resolved."""
    if OFFLINE_DEBUG:
        return get_offline_fixture().get("market_results", {}).get(ticker)
    async with session.get(f"{BASE_URL}/markets/{ticker}", headers=get_headers()) as r:
        if r.status != 200: return None
        data = await r.json()
    m = data.get("market", {})
    result = m.get("result")
    if result in ("yes", "no"): return result
    # fallback: finalized market with settlement price
    if m.get("status") in ("finalized", "settled"):
        for field in ["yes_price","last_price"]:
            v = m.get(field)
            if v is not None:
                v = float(v)
                if v > 1.0: v /= 100.0
                return "yes" if v >= 0.99 else "no"
    return None

async def resolve_trades(session):
    """Check open trades; fetch results for expired ones and write to resolved log."""
    trades = load_open_rows()
    if not trades: return set(), 0
    now = datetime.now(timezone.utc)
    still_open = []
    resolved_count = 0
    exited_tickers = set()
    for trade in trades:
        # parse close_time
        close_dt = normalize_timestamp_utc(trade.get("close_time", ""))
        if close_dt is None:
            still_open.append(trade); continue
        if now < close_dt:
            still_open.append(trade); continue
        # market should be expired — fetch result
        result = await fetch_market_result(session, trade["ticker"])
        if result is None:
            still_open.append(trade); continue
        # compute P&L
        side   = trade["side"]
        crowd  = float(trade["crowd_prob"])
        size   = float(trade["position_usd"])
        won    = (result == side)
        pnl    = round(size * (1.0 - crowd) / crowd, 2) if won else -round(size, 2)
        resolved_yes = 1 if result == "yes" else 0
        resolved_no  = 1 if result == "no"  else 0
        append_resolved_trade(
            RESOLVED_TRADES_LOG,
            OPEN_FIELDS,
            trade,
            resolved_yes,
            resolved_no,
            1 if won else 0,
            pnl,
            "expiry",
        )
        resolved_count += 1
        exited_tickers.add(trade["ticker"])
        global session_trades, session_wins, session_losses, session_pnl, _realized_cash_pnl
        session_trades += 1
        session_pnl    = round(session_pnl + pnl, 2)
        _realized_cash_pnl = round(_realized_cash_pnl + pnl, 2)
        if won: session_wins   += 1
        else:   session_losses += 1
        cleanup_position_state(
            trade["ticker"],
            side,
            reason="expiry",
            expected_entry_ts=trade.get("timestamp", ""),
        )
        rotation_meta = _rotation_book.pop((trade["ticker"], side), None)
        log.info((green if pnl > 0 else red if pnl < 0 else (lambda x: x))(
            f"RESOLVED | {trade['ticker']} | {side.upper()} | {'WON' if won else 'LOST'} | P&L: {fmt_money(pnl)}"
        ))
        _last_exit_meta_by_ticker[trade["ticker"]] = {"ts": now, "edge": safe_float(trade.get("ev"), 0.0) or 0.0}
        if rotation_meta:
            rotation_alpha = round(pnl - rotation_meta["old_pnl"], 2)
            _rotation_perf["count"] += 1
            _rotation_perf["alpha_sum"] = round(_rotation_perf["alpha_sum"] + rotation_alpha, 4)
            if rotation_alpha > 0:
                _rotation_perf["wins"] += 1
            log.info(magenta(
                f"[ROTATION_RESULT] old={rotation_meta['old_ticker']} new={trade['ticker']}"
                f" old_pnl={fmt_money(rotation_meta['old_pnl'])} new_pnl={fmt_money(pnl)} delta={fmt_money(rotation_alpha)}"
            ))
        exit_update = (
            f"CAPITAL_UPDATE | event=exit cash_balance={current_cash_balance(sum(safe_float(r.get('position_usd'), 0.0) or 0.0 for r in still_open)):.2f}"
            f" open_exposure={sum(safe_float(r.get('position_usd'), 0.0) or 0.0 for r in still_open):.2f}"
            f" realized_cash_pnl={fmt_money(_realized_cash_pnl)}"
        )
        log.info(green(exit_update) if pnl > 0 else red(exit_update) if pnl < 0 else cyan(exit_update))
    # rewrite open trades with only still-open rows
    save_positions(still_open)
    if resolved_count:
        persist_runtime_state()
    if resolved_count:
        log.info(f"Resolved {resolved_count} trade(s) this cycle.")
    log.info(f"[EXIT CLEANUP] expiry_cleanup | remaining_open={len(still_open)}")
    return exited_tickers, resolved_count

def check_early_exits(prices):
    """Check open trades for fast exits using current orderbook prices."""
    trades = load_open_rows()
    if not trades:
        return 0, 0, 0.0, set(), set(), {}, set()
    still_open = []
    early_wins = 0
    early_losses = 0
    early_pnl = 0.0
    early_win_exits = set()
    early_loss_exits = set()
    exit_counts = {"tp_hit":0, "pressure_failure":0, "momentum_break":0, "hard_stop":0, "trail_protect":0, "time_stop":0, "exec_deterioration":0, "stale_conviction":0, "stale_break":0, "conviction_decay":0}
    cooldowns = load_cooldowns()
    now = utc_now()
    exited_tickers = set()
    hold_deferred = 0
    for trade in trades:
        ticker = trade.get("ticker","")
        side = trade.get("side","")
        position_id = f"{ticker}|{side}"
        series = ticker_series(ticker)
        try:
            entry_price = float(trade["crowd_prob"])
            size = float(trade["position_usd"])
        except Exception:
            still_open.append(trade)
            continue
        entry_dt = normalize_timestamp_utc(trade.get("timestamp", ""))
        if entry_dt is None:
            age_secs = 9999.0
        else:
            age_secs = (now - entry_dt).total_seconds()
        # Only apply hold-time exits (time_stop, no_momentum, failsafe) to positions
        # entered in this session. Carry-over positions from prior sessions have stale
        # timestamps that would immediately trigger time_stop and get re-entered each cycle.
        session_position = (ticker, side) in _entry_metrics
        if not session_position:
            _msg = (
                f"[SESSION_GUARD] ticker={ticker} side={side}"
                f" missing_entry_metrics={str((ticker, side) not in _entry_metrics)}"
            )
            if OFFLINE_LIFECYCLE_TEST:
                log.info(_msg)
            else:
                log.debug(_msg)
            age_secs = 0.0
        p = prices.get(ticker, {})
        if side == "no":
            exit_price = p.get("no_bid") if p.get("no_bid") is not None else p.get("no_ask")
            pressure_score = p.get("no_pressure_score")
        else:
            exit_price = p.get("yes_bid") if p.get("yes_bid") is not None else p.get("yes_ask")
            pressure_score = p.get("pressure_score")
        if exit_price is None:
            exit_price = entry_price
        pnl_pct = exit_price - entry_price
        log.info(f"[EXIT CHECK] {ticker} pnl={pnl_pct:.4f} held={int(age_secs)}")
        reason = None
        state = _trade_state.setdefault((ticker, side), {"max_favorable_excursion":0.0, "max_adverse_excursion":0.0, "peak_pnl_pct":0.0, "pressure_weak_streak":0})
        state["max_favorable_excursion"] = max(state["max_favorable_excursion"], pnl_pct)
        state["max_adverse_excursion"] = min(state["max_adverse_excursion"], pnl_pct)
        state["peak_pnl_pct"] = max(state["peak_pnl_pct"], pnl_pct)
        _peak_pnl_by_position_id[position_id] = max(_peak_pnl_by_position_id.get(position_id, pnl_pct), pnl_pct)
        peak_pnl = _peak_pnl_by_position_id[position_id]
        spread = None
        if p.get("yes_bid") is not None and p.get("yes_ask") is not None:
            spread = float(p["yes_ask"]) - float(p["yes_bid"])
        entry_meta = _entry_metrics.get((ticker, side), {})
        entry_crowd = safe_float(entry_meta.get("crowd"), entry_price)
        crowd_drop = max(0.0, entry_crowd - exit_price)
        weak_book_now = book_depth_score(p) < max(0.10, MIN_BOOK_DEPTH_SCORE * 0.7)
        fragile_book_now = is_fragile_book(dict(p, ticker=ticker), side)
        time_stop_secs = max(MIN_HOLD_SECONDS + 60, int(safe_float(entry_meta.get("time_stop_secs"), MAX_HOLD_SECONDS) or MAX_HOLD_SECONDS))
        # Broken state: no live book for either side
        state_is_broken = (p.get("yes_bid") is None and p.get("no_bid") is None)
        held_secs = age_secs
        _age_msg = f"[AGE] ticker={ticker} opened_at={trade.get('timestamp', '?')} held_secs={int(held_secs)}"
        if OFFLINE_LIFECYCLE_TEST:
            log.info(_age_msg)
        else:
            log.debug(_age_msg)
        # Dynamic hold floor: high-edge trades get a longer early-protection window.
        # Formula: EARLY_PROTECT_MIN_HOLD + edge × 100 seconds.
        # e.g. edge=0.03 → 48s, edge=0.10 → 55s, edge=0.30 → 75s (caps naturally).
        _entry_edge_val = safe_float(entry_meta.get("entry_edge"), 0.0) or 0.0
        dynamic_hold = EARLY_PROTECT_MIN_HOLD + int(_entry_edge_val * 100)
        pressure_score_now = safe_float(pressure_score, 0.0) or 0.0
        entry_pressure = safe_float(entry_meta.get("entry_pressure"), entry_meta.get("pressure_score", pressure_score_now))
        entry_pressure = pressure_score_now if entry_pressure is None else entry_pressure
        pressure_drop = max(0.0, entry_pressure - pressure_score_now)
        position_intel = compute_position_intel(trade, side, prices, entry_meta)
        fake_edge_failed = (
            (position_intel["current_edge"] >= 0.30 and position_intel["current_quality_score"] < 0.45)
            or (position_intel["current_edge"] >= 0.40 and position_intel["current_touch"] < 10)
            or (position_intel["current_edge"] >= 0.40 and position_intel["current_spread"] >= 0.05 * 0.95)
            or (position_intel["current_edge"] >= 0.45 and position_intel["current_pressure_score"] < 0.45)
        )
        exit_plan = derive_exit_plan(
            entry_meta,
            spread,
            age_secs,
            time_stop_secs,
            conviction_state=position_intel["conviction_state"],
            pnl_pct=pnl_pct,
            fake_edge_failed=fake_edge_failed,
        )
        time_stop_secs = exit_plan["time_stop_secs"]
        if side == "no" and NO_SIDE_HOLD_MULT != 1.0:
            time_stop_secs = int(time_stop_secs * NO_SIDE_HOLD_MULT)
            dynamic_hold   = int(dynamic_hold   * NO_SIDE_HOLD_MULT)
            log.debug(f"[NO_SIDE_HOLD] ticker={ticker} time_stop_secs={time_stop_secs} dynamic_hold={dynamic_hold}")
        spread_deteriorating = spread is not None and safe_float(entry_meta.get("entry_spread"), spread) is not None and spread > (safe_float(entry_meta.get("entry_spread"), spread) or 0.0) * 1.25
        weak_pressure_now = (
            pressure_score is not None
            and pressure_score_now <= exit_plan["pressure_floor"]
            and pressure_drop >= exit_plan["pressure_drop_confirm"]
            and (weak_book_now or fragile_book_now or spread_deteriorating)
        )
        pnl = round(size * pnl_pct, 2)
        if VERBOSE_LOGS:
            log.info(
                f"[POSITION_INTEL] ticker={ticker} pnl={fmt_money(pnl)} held={int(held_secs)}"
                f" edge={position_intel['current_edge']:.3f} killer={position_intel['current_killer_score']:.3f}"
                f" quality={position_intel['current_quality_score']:.3f} pressure={position_intel['current_pressure_score']:.3f}"
                f" spread={position_intel['current_spread']:.3f} touch={position_intel['current_touch']:.0f}"
                f" conviction={position_intel['conviction_state']}"
            )
            log.info(
                f"[CONVICTION_DELTA] ticker={ticker} killer_delta={position_intel['killer_delta']:+.3f}"
                f" quality_delta={position_intel['quality_delta']:+.3f} state={position_intel['conviction_delta_state']}"
            )
        state["pressure_weak_streak"] = state["pressure_weak_streak"] + 1 if weak_pressure_now else 0
        pressure_confirmed = state["pressure_weak_streak"] >= PRESSURE_EXIT_CONFIRM_CYCLES
        # Minimum hold guard — defer ordinary exits until MIN_HOLD_SECONDS unless catastrophic
        spread_blowout_now = spread is not None and spread > HARD_SPREAD_CEIL + 0.02
        catastrophic_now   = pnl_pct <= CATASTROPHIC_STOP or spread_blowout_now
        trail_armed_now    = peak_pnl >= exit_plan["trail_trigger"]
        skip_exit = held_secs < MIN_HOLD_SECONDS and not catastrophic_now
        if skip_exit:
            hold_deferred += 1
            _spread_s = f"{spread:.4f}" if spread is not None else "-"
            _pressure_s = (
                yellow(f"{pressure_score_now:.3f}") if pressure_score_now < 0.40
                else green(f"{pressure_score_now:.3f}") if pressure_score_now > 0.65
                else f"{pressure_score_now:.3f}"
            )
            if VERBOSE_LOGS:
                log.info(
                    f"{yellow('[HOLD]')} ticker={ticker} pnl={fmt_pnl(pnl_pct)} held={int(held_secs)}s"
                    f" pressure={_pressure_s} spread={_spread_s}"
                    f" tp={green(fmt_pnl(exit_plan['tp']))} sl={red(fmt_pnl(exit_plan['sl']))}"
                    f" trail_armed={trail_armed_now} bucket={exit_plan['bucket']}"
                )
            still_open.append(trade)
            continue
        if held_secs < 20 and pnl_pct < -0.006:
            reason = "early_loss"
            won = 0
        elif (
            entry_price <= DEAD_PRICE_MAX
            and size <= DEAD_SIZE_MAX
            and pnl_pct <= DEAD_PNL
            and held_secs >= DEAD_HOLD_SECS
        ):
            reason = "dead_position_kill"
            won = 0
        conviction_state = position_intel.get("conviction_state", "neutral")
        _base_ts = time_stop_secs
        if conviction_state == "elite":
            _time_stop_limit = None                     # no time stop — let dynamic SL / expiry handle
        elif conviction_state == "strong":
            _time_stop_limit = int(_base_ts * 2.0)
        elif conviction_state == "neutral":
            _time_stop_limit = int(_base_ts * 1.5)
        else:                                           # weak
            _time_stop_limit = _base_ts
        if pnl_pct > 0.01:
            _time_stop_limit = None   # winner — let it run, don't force out by clock
        # Suppress time_stop for long-horizon trades with genuine model edge.
        # Rationale: a YES trade at 10¢ with mp=0.60 and 8h to close cannot
        # play out in 60-240s; time_stop kills it before any resolution is possible.
        # Hard_stop, conviction_decay, and DEAD_POSITION_KILL remain active.
        _entry_hours = safe_float(trade.get("hours_to_close"), 0.0) or 0.0
        if _entry_hours > 1.0 and _entry_edge_val > 0.20 and _time_stop_limit is not None:
            log.info(
                f"[TIME_STOP_SUPPRESS] ticker={ticker}"
                f" hours_to_close={_entry_hours:.1f} entry_edge={_entry_edge_val:.3f}"
                f" was={_time_stop_limit}s → suppressed (long-horizon high-edge)"
            )
            _time_stop_limit = None
        if VERBOSE_LOGS:
            log.info(
                f"[TIME_STOP_CHECK] ticker={ticker} held={int(age_secs)}"
                f" conviction={conviction_state} base={_base_ts}"
                f" effective={'disabled' if _time_stop_limit is None else _time_stop_limit}"
            )
        dynamic_sl = exit_plan["sl"]
        if pnl_pct > 0.02:
            dynamic_sl = pnl_pct - 0.01   # trail floor: protect gains, raise sl above base
        flat_stop = None
        if conviction_state == "weak":
            flat_stop = -0.015
        elif conviction_state == "neutral":
            flat_stop = -0.02
        elif conviction_state in ("strong", "elite"):
            flat_stop = None  # let dynamic SL handle

        if flat_stop is not None and pnl_pct <= flat_stop:
            reason = "hard_stop"
            won = 0
        elif pnl_pct > 0.01 and pressure_score_now >= MIN_PRESSURE_ENTRY:
            still_open.append(trade)
            continue
        elif pnl_pct >= exit_plan["tp"]:
            reason = "tp_hit"
            won = 1
        elif pnl_pct <= exit_plan["catastrophic_sl"]:
            if held_secs < dynamic_hold:
                # Within early-protect window: only allow a genuinely catastrophic
                # loss (1.5× the stop level). Shallow stop hits are noise — ignore.
                if pnl_pct <= exit_plan["catastrophic_sl"] * 1.5:
                    reason = "catastrophic_stop"
                    won = 0
                else:
                    log.info(
                        f"[EARLY PROTECT] ticker={ticker} pnl={pnl_pct:.4f}"
                        f" held={int(held_secs)}s dynamic_hold={dynamic_hold}s"
                        f" edge={_entry_edge_val:.4f} ignoring early stop"
                    )
            else:
                reason = "hard_stop"
                won = 0
        elif state_is_broken and age_secs > max(300, MIN_HOLD_SECONDS):
            log.error(f"[STATE ERROR] failsafe_triggered | {ticker} held={int(age_secs)}")
            reason = "exec_deterioration"
            won = 1 if pnl_pct > 0 else 0
        elif pnl_pct <= dynamic_sl:
            if held_secs < dynamic_hold:
                # Within early-protect window: only allow a genuinely catastrophic
                # loss (1.5× the stop level). Shallow stop hits are noise — ignore.
                if pnl_pct <= exit_plan["sl"] * 1.5:
                    reason = "catastrophic_stop"
                    won = 0
                else:
                    log.info(
                        f"[EARLY PROTECT] ticker={ticker} pnl={pnl_pct:.4f}"
                        f" held={int(held_secs)}s dynamic_hold={dynamic_hold}s"
                        f" edge={_entry_edge_val:.4f} ignoring early stop"
                    )
            else:
                reason = "hard_stop"
                won = 0
        elif (
            held_secs >= MIN_HOLD_SECONDS + PRESSURE_EXIT_GRACE_SECONDS
            and pressure_confirmed
            and pnl_pct > exit_plan["sl"] * 0.60
        ):
            reason = "pressure_failure"
            won = 1 if pnl_pct > 0 else 0
        elif (
            USE_TRAIL_PROTECT
            and peak_pnl >= exit_plan["trail_trigger"]
            and pnl_pct <= peak_pnl - exit_plan["trail_giveback"]
            and pnl_pct >= exit_plan["trail_floor"]
        ):
            reason = "trail_protect"
            won = 1
        elif position_intel["conviction_delta_state"] == "degrading" and held_secs > 60 and not (side == "no" and NO_SIDE_SUPPRESS_SOFT_EXITS):
            # Change 5: explicit floor guard — conviction_decay cannot fire < 30s
            if held_secs < 30:
                log.info(f"[EXIT_GUARD] ticker={ticker} reason=min_hold_window held={int(held_secs)} min=30 block=conviction_decay")
            else:
                reason = "conviction_decay"
                won = 1 if pnl_pct > 0 else 0
        elif _time_stop_limit is not None and age_secs >= _time_stop_limit:
            # Change 5: explicit floor guard — time_stop cannot fire < 45s
            if age_secs < 45:
                log.info(f"[EXIT_GUARD] ticker={ticker} reason=min_hold_window held={int(age_secs)} min=45 block=time_stop")
            else:
                reason = "time_stop"
                won = 1 if pnl_pct > 0 else 0
        elif age_secs >= exit_plan["stale_after"] and abs(pnl_pct) <= exit_plan["stall_band"] and (weak_pressure_now or weak_book_now or crowd_drop >= MOMENTUM_BREAK_CROWD_DROP * 0.33) and not (side == "no" and NO_SIDE_SUPPRESS_SOFT_EXITS):
            reason = "momentum_break"
            won = 1 if pnl_pct > 0 else 0
        # ── Stale conviction exit ────────────────────────────────────────────
        # Fires when a held position has degraded past reinforce-eligibility
        # and no normal exit has triggered.  Uses entry_meta + live prices to
        # mirror the killer-filter checks without spamming [KILLER] logs.
        if reason is None and held_secs >= STALE_CONVICTION_MIN_HOLD:
            killer_score_now = position_intel["current_killer_score"]
            reinforce_eligible = (
                position_intel["conviction_state"] == "elite"
                and pnl_pct >= 0.0
                and killer_score_now >= (safe_float(entry_meta.get("entry_killer_score"), 0.0) or 0.0) - 0.03
                and position_intel["current_spread"] <= max(0.05, (safe_float(entry_meta.get("entry_spread"), 0.05) or 0.05) * 1.5)
            )
            winner_protected_stale = pnl_pct > 0.01 and pressure_score_now >= 0.45 and held_secs < 900
            stale_hold_floor = STALE_CONVICTION_MIN_HOLD
            stale_break_floor = STALE_BREAK_MIN_HOLD
            if position_intel["conviction_delta_state"] == "degrading" and pnl_pct <= 0:
                stale_hold_floor = max(MIN_HOLD_SECONDS, int(stale_hold_floor * 0.80))
            elif position_intel["conviction_delta_state"] == "improving" and pnl_pct > -0.01:
                stale_hold_floor = int(stale_hold_floor * 1.20)
            if position_intel["conviction_state"] == "weak" and pnl_pct < 0:
                stale_hold_floor = max(MIN_HOLD_SECONDS, stale_hold_floor - 30)
            if position_intel["conviction_delta_state"] == "degrading" and fake_edge_failed:
                stale_break_floor = max(MIN_HOLD_SECONDS, int(stale_break_floor * 0.85))
            log.info(
                f"[HELD DECAY] ticker={ticker} killer={killer_score_now:.3f}"
                f" fake_edge={fake_edge_failed} reinforce_eligible={reinforce_eligible}"
                f" pnl={pnl_pct:.4f} held={int(held_secs)}"
            )
            if not winner_protected_stale and pnl_pct <= 0.00 and not reinforce_eligible:
                # Stronger stale_break takes priority over stale_conviction
                if (
                    held_secs >= stale_break_floor
                    and pnl_pct < 0.00
                    and killer_score_now < (0.40 if position_intel["conviction_delta_state"] == "degrading" else 0.38)
                    and fake_edge_failed
                ):
                    reason = "stale_break"
                    won = 0
                elif held_secs >= stale_hold_floor and (killer_score_now < 0.42 or fake_edge_failed):
                    reason = "stale_conviction"
                    won = 0
        if reason is None:
            still_open.append(trade)
            continue
        early_loss_safety_reasons = {"catastrophic_stop", "hard_stop", "exec_deterioration"}
        early_loss_eligible = (
            not won
            and reason not in early_loss_safety_reasons
            and held_secs >= MIN_EARLY_LOSS_HOLD_SECONDS
            and pnl_pct < 0.0
        )
        if not won and reason not in early_loss_safety_reasons and not early_loss_eligible:
            log.info(
                f"[EARLY_LOSS_DEFER] ticker={ticker} held={int(held_secs)}"
                f" min_hold={MIN_EARLY_LOSS_HOLD_SECONDS} pnl={pnl_pct:.4f}"
                f" reason={reason}"
            )
            still_open.append(trade)
            continue
        early_pnl += pnl
        if won:
            early_wins += 1
            early_win_exits.add((ticker, side))
        else:
            early_losses += 1
            early_loss_exits.add((ticker, side))
            recent_losses[ticker] = now
        _last_exit_price_by_ticker[ticker] = exit_price
        cooldowns[f"{ticker}|{side}"] = now + timedelta(seconds=POST_EXIT_COOLDOWN_SECS)
        _post_exit_cooldown_by_series[series] = now + timedelta(seconds=POST_EXIT_COOLDOWN_SECS)
        _last_trade_ts_by_ticker[ticker] = now
        _session_hold_secs.append(age_secs)
        resolved_yes = 1 if side == "yes" else 0
        resolved_no = 1 - resolved_yes
        append_resolved_trade(
            RESOLVED_TRADES_LOG,
            OPEN_FIELDS,
            trade,
            resolved_yes,
            resolved_no,
            won,
            pnl,
            reason,
        )
        exit_counts[reason] = exit_counts.get(reason, 0) + 1
        exited_tickers.add(ticker)
        global session_trades, session_wins, session_losses, session_pnl, _realized_cash_pnl
        session_trades += 1
        session_pnl    = round(session_pnl + pnl, 2)
        _realized_cash_pnl = round(_realized_cash_pnl + pnl, 2)
        if won: session_wins   += 1
        else:   session_losses += 1
        if ticker in prices:
            prices.pop(ticker, None)
        cleanup_position_state(
            ticker,
            side,
            reason=reason,
            expected_entry_ts=trade.get("timestamp", ""),
        )
        rotation_meta = _rotation_book.pop((ticker, side), None)
        _last_exit_meta_by_ticker[ticker] = {"ts": now, "edge": safe_float(entry_meta.get("entry_edge"), safe_float(trade.get("ev"), 0.0) or 0.0) or 0.0}
        exit_msg = (
            f"[EXIT] ticker={ticker} reason={reason} bucket={exit_plan['bucket']} pnl={fmt_pnl(pnl_pct)}"
            f" usd={fmt_money(pnl)} held={int(age_secs)}s"
            f" | pressure={pressure_score_now:.3f} | spread={(spread if spread is not None else 0.0):.3f}"
            f" | tp={fmt_pnl(exit_plan['tp'])} | sl={fmt_pnl(exit_plan['sl'])}"
            f" | weak_streak={state['pressure_weak_streak']} | regime={entry_meta.get('regime','unknown')}"
        )
        if reason == "tp_hit" or pnl > 0:
            log.info(green(exit_msg))
        elif reason in ("hard_stop", "exec_deterioration") or pnl < 0:
            log.info(red(exit_msg))
        else:
            log.info(yellow(exit_msg))
        # ── analytics ────────────────────────────────────────────────────────
        _a_side, _a_tier, _a_press, _a_exit, _a_regime, _a_expiry = _analytics.record_exit(
            side=side,
            pnl_usd=pnl,
            held_secs=age_secs,
            exit_reason=reason,
            entry_meta=entry_meta,
        )
        if _dashboard is not None:
            _dashboard.add_event(
                "EXIT", ticker,
                f"side={_a_side} tier={_a_tier} exit={_a_exit}"
                f" pnl={pnl:+.3f} held={int(age_secs)}s",
            )
            _dashboard.add_event(
                "ANALYTICS", ticker,
                f"side={_a_side} tier={_a_tier} press={_a_press}"
                f" exit={_a_exit} regime={_a_regime} pnl={pnl:+.3f}",
            )
            _closed_total, _, _ = _analytics.get_totals()
            _dashboard.add_event("SYSTEM", "CLOSED_TRADES", f"count={_closed_total}")
        log.info(
            f"[EXPECTANCY] side={_a_side} tier={_a_tier} regime={_a_regime}"
            f" exit={_a_exit} expiry={_a_expiry} pnl={pnl:+.3f}"
        )
        if _analytics._exit_count % _analytics.FLUSH_EVERY == 0:
            for _al in _analytics.get_summary_lines():
                log.info(_al)
            for _al in _analytics.get_expiry_lines():
                log.info(_al)
        if rotation_meta:
            rotation_alpha = round(pnl - rotation_meta["old_pnl"], 2)
            _rotation_perf["count"] += 1
            _rotation_perf["alpha_sum"] = round(_rotation_perf["alpha_sum"] + rotation_alpha, 4)
            if rotation_alpha > 0:
                _rotation_perf["wins"] += 1
            log.info(magenta(
                f"[ROTATION_RESULT] old={rotation_meta['old_ticker']} new={ticker}"
                f" old_pnl={fmt_money(rotation_meta['old_pnl'])} new_pnl={fmt_money(pnl)} delta={fmt_money(rotation_alpha)}"
            ))
        exit_update = (
            f"CAPITAL_UPDATE | event=exit cash_balance={current_cash_balance(sum(safe_float(r.get('position_usd'), 0.0) or 0.0 for r in still_open)):.2f}"
            f" open_exposure={sum(safe_float(r.get('position_usd'), 0.0) or 0.0 for r in still_open):.2f}"
            f" realized_cash_pnl={fmt_money(_realized_cash_pnl)}"
        )
        log.info(green(exit_update) if pnl > 0 else red(exit_update) if pnl < 0 else cyan(exit_update))
    save_positions(still_open)
    log.info(f"[EXIT CLEANUP] active_exit_cleanup | remaining_open={len(still_open)}")
    if early_wins + early_losses > 0:
        log.info(cyan(f"Early exits this cycle: {early_wins} win(s), {early_losses} loss(es), pnl={fmt_money(early_pnl)}"))
    print_exit_summary(log, hold_deferred, exit_counts)
    save_cooldowns(cooldowns)
    if early_wins + early_losses > 0:
        persist_runtime_state()
    for k, expiry in cooldowns.items():
        if expiry > now:
            parts = k.split("|", 1)
            if len(parts) == 2:
                early_loss_exits.add((parts[0], parts[1]))
    return early_wins, early_losses, early_pnl, early_win_exits, early_loss_exits, exit_counts, exited_tickers

def print_performance_summary():
    # Detailed breakdown — SESSION/ALL TIME shown in compact dashboard; this is family/tier/hour/top
    if not Path(RESOLVED_TRADES_LOG).exists(): return
    with open(RESOLVED_TRADES_LOG, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows: return
    total = len(rows)
    wins  = [r for r in rows if r.get("won","0")=="1"]
    losses= [r for r in rows if r.get("won","0")=="0"]
    families={}
    for r in rows:
        fam=r.get("family","?") or "?"
        families.setdefault(fam,{"w":0,"l":0,"pnl":0.0})
        if r.get("won","0")=="1": families[fam]["w"]+=1
        else:                      families[fam]["l"]+=1
        families[fam]["pnl"]+=float(r["pnl_usd"])
    fam_parts=[f"{fam}(w={d['w']},l={d['l']},pnl={fmt_money(d['pnl'])})" for fam,d in sorted(families.items())]
    log.info(f"PERF_FAMILY | {' '.join(fam_parts)}")
    buckets={"under_1h":{"w":0,"l":0,"pnl":0.0},"1h_to_24h":{"w":0,"l":0,"pnl":0.0},"over_24h":{"w":0,"l":0,"pnl":0.0}}
    for r in rows:
        try: h=float(r.get("hours_to_close",0) or 0)
        except: h=0.0
        if   h<1:   b="under_1h"
        elif h<=24: b="1h_to_24h"
        else:       b="over_24h"
        if r.get("won","0")=="1": buckets[b]["w"]+=1
        else:                      buckets[b]["l"]+=1
        buckets[b]["pnl"]+=float(r["pnl_usd"])
    bkt_parts=[f"{bkt}(w={d['w']},l={d['l']},pnl={fmt_money(d['pnl'])})" for bkt,d in buckets.items()]
    log.info(f"PERF_HOURS  | {' '.join(bkt_parts)}")
    tiers={}
    for r in rows:
        t=str(r.get("tier","1") or "1")
        tiers.setdefault(t,{"w":0,"l":0,"pnl":0.0})
        if r.get("won","0")=="1": tiers[t]["w"]+=1
        else:                      tiers[t]["l"]+=1
        tiers[t]["pnl"]+=float(r["pnl_usd"])
    tier_parts=[]
    for t,d in sorted(tiers.items()):
        wr=d["w"]/(d["w"]+d["l"]) if (d["w"]+d["l"])>0 else 0.0
        tier_parts.append(f"T{t}(w={d['w']},l={d['l']},pnl={fmt_money(d['pnl'])},wr={wr:.1%})")
    log.info(f"PERF_TIER   | {' '.join(tier_parts)}")
    best=max(rows, key=lambda r: float(r["pnl_usd"]))
    worst=min(rows, key=lambda r: float(r["pnl_usd"]))
    log.info(green(f"TOP WIN     | {best['ticker']} | {best['side'].upper()} | pnl={fmt_money(float(best['pnl_usd']))} | ev={float(best['ev']):.2%} | hours={best['hours_to_close']}"))
    log.info(red(f"TOP LOSS    | {worst['ticker']} | {worst['side'].upper()} | pnl={fmt_money(float(worst['pnl_usd']))} | ev={float(worst['ev']):.2%} | hours={worst['hours_to_close']}"))
    early_rows   = [r for r in rows if r.get("exit_type","expiry") in ("tp_hit","pressure_failure","momentum_break","hard_stop","trail_protect","time_stop","exec_deterioration","stale_conviction","stale_break","conviction_decay")]
    ew_cnt       = sum(1 for r in early_rows if r.get("won","0")=="1")
    el_cnt       = len(early_rows) - ew_cnt
    early_pnl_s  = round(sum(float(r["pnl_usd"]) for r in early_rows), 2)
    log.info(cyan(f"EARLY       | wins={ew_cnt} losses={el_cnt} pnl={fmt_money(early_pnl_s)}"))
    # FRESH_HFT: only rows resolved after this process started, with a valid exit_type
    def _row_ts(r):
        try:
            ts = r.get("timestamp","")
            dt = normalize_timestamp_utc(ts)
            return dt
        except: return None
    fresh = [r for r in rows
             if r.get("exit_type") in ("tp_hit","pressure_failure","momentum_break","hard_stop","trail_protect","time_stop","exec_deterioration","expiry","stale_conviction","stale_break","conviction_decay")
             and _row_ts(r) is not None
             and _row_ts(r) >= STARTUP_TIME]
    f_ew  = sum(1 for r in fresh if r.get("won","0")=="1" and r.get("exit_type") != "expiry")
    f_el  = sum(1 for r in fresh if r.get("won","0")=="0" and r.get("exit_type") != "expiry")
    f_exp = sum(1 for r in fresh if r.get("exit_type")=="expiry")
    f_pnl = round(sum(float(r["pnl_usd"]) for r in fresh), 2)
    f_wr  = (sum(1 for r in fresh if r.get("won","0")=="1") / len(fresh)) if fresh else 0.0
    log.info(cyan(f"FRESH_HFT   | resolved={len(fresh)}  early_wins={f_ew}  early_losses={f_el}  expiry={f_exp}  early_pnl={fmt_money(f_pnl)}  win_rate={f_wr:.1%}"))
    hft_rows  = [r for r in fresh if r.get("exit_type") in ("tp_hit","pressure_failure","momentum_break","hard_stop","trail_protect","time_stop","exec_deterioration","stale_conviction","stale_break","conviction_decay")]
    hft_wins  = [r for r in hft_rows if r.get("won","0")=="1"]
    hft_losses= [r for r in hft_rows if r.get("won","0")=="0"]
    hft_wr    = len(hft_wins)/len(hft_rows) if hft_rows else 0.0
    avg_win_p = round(sum(float(r["pnl_usd"]) for r in hft_wins)/len(hft_wins),3) if hft_wins else 0.0
    avg_los_p = round(sum(float(r["pnl_usd"]) for r in hft_losses)/len(hft_losses),3) if hft_losses else 0.0
    avg_hold = round(sum(_session_hold_secs)/len(_session_hold_secs),1) if _session_hold_secs else 0.0
    log.info(cyan(f"HFT_STATS   | trades={len(hft_rows)}  wins={len(hft_wins)}  losses={len(hft_losses)}  wr={hft_wr:.1%}  avg_hold={avg_hold}s  avg_win={fmt_money(avg_win_p, 3)}  avg_loss={fmt_money(avg_los_p, 3)}"))

PREFERRED_SERIES = ["KXBTC","KXETH"]
STRIKE_RANGE     = 0.20   # ±20% of spot price
MAX_PER_CLUSTER  = 2      # max signals per (family, close_time) group

async def fetch_spot_prices(session):
    """Fetch current spot prices for known series with Coinbase primary, Kraken fallback, and cache."""
    global _last_spot_prices
    if OFFLINE_MODE:
        spot = load_offline_spots()
        for k in ["KXBTC", "KXETH"]:
            if k in spot:
                _last_spot_prices[k] = spot[k]
        log.info(cyan(f"[OFFLINE_DEBUG] scenario={OFFLINE_SCENARIO}"))
        log.info(f"FINAL_SPOT | BTC={spot.get('KXBTC')} ETH={spot.get('KXETH')}")
        return spot

    btc_price = None
    eth_price = None

    try:
        async with session.get(
            "https://api.coinbase.com/v2/prices/BTC-USD/spot",
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            if r.status == 200:
                data = await r.json()
                amt = data.get("data", {}).get("amount")
                if amt is not None:
                    btc_price = float(amt)
            else:
                log.warning(f"Coinbase BTC spot API {r.status}")
    except Exception as e:
        log.warning(f"Coinbase BTC fetch failed: {e}")

    try:
        async with session.get(
            "https://api.coinbase.com/v2/prices/ETH-USD/spot",
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            if r.status == 200:
                data = await r.json()
                amt = data.get("data", {}).get("amount")
                if amt is not None:
                    eth_price = float(amt)
            else:
                log.warning(f"Coinbase ETH spot API {r.status}")
    except Exception as e:
        log.warning(f"Coinbase ETH fetch failed: {e}")

    if btc_price is None:
        try:
            async with session.get(
                "https://api.kraken.com/0/public/Ticker",
                params={"pair":"XBTUSD"},
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    result = data.get("result", {})
                    first = next(iter(result.values()), {})
                    close = first.get("c", [None])[0]
                    if close is not None:
                        btc_price = float(close)
                else:
                    log.warning(f"Kraken BTC spot API {r.status}")
        except Exception as e:
            log.warning(f"Kraken BTC fetch failed: {e}")

    if eth_price is None:
        try:
            async with session.get(
                "https://api.kraken.com/0/public/Ticker",
                params={"pair":"ETHUSD"},
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    result = data.get("result", {})
                    first = next(iter(result.values()), {})
                    close = first.get("c", [None])[0]
                    if close is not None:
                        eth_price = float(close)
                else:
                    log.warning(f"Kraken ETH spot API {r.status}")
        except Exception as e:
            log.warning(f"Kraken ETH fetch failed: {e}")

    spot = {}
    if btc_price:
        spot["KXBTC"] = float(btc_price)
    if eth_price:
        spot["KXETH"] = float(eth_price)

    if not spot.get("KXBTC") and _last_spot_prices["KXBTC"]:
        spot["KXBTC"] = _last_spot_prices["KXBTC"]
    if not spot.get("KXETH") and _last_spot_prices["KXETH"]:
        spot["KXETH"] = _last_spot_prices["KXETH"]

    for k in ["KXBTC", "KXETH"]:
        if k in spot:
            _last_spot_prices[k] = spot[k]

    try:
        async with session.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC",
            params={"interval":"1d","range":"1d"},
            headers={"User-Agent":"Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            if r.status == 200:
                data = await r.json()
                spx = data.get("chart", {}).get("result", [{}])[0].get("meta", {}).get("regularMarketPrice")
                if spx:
                    spot["KXSPX"] = float(spx)
    except Exception as e:
        log.warning(f"Yahoo SPX fetch failed: {e}")

    if not spot.get("KXBTC") or not spot.get("KXETH"):
        log.critical("BTC/ETH missing — using last known")

    log.info(f"FINAL_SPOT | BTC={spot.get('KXBTC')} ETH={spot.get('KXETH')}")
    return spot

def parse_strike(ticker):
    """Parse numeric strike from ticker like KXBTC-26APR0715-B77550 or -T78299.99."""
    parts=ticker.split("-")
    if len(parts)<3: return None
    s=parts[-1]
    if s and s[0] in ("B","T","b","t"): s=s[1:]
    try: return float(s)
    except: return None

def is_near_spot(strike, spot):
    dist = abs(strike - spot) / spot
    return dist <= 0.01  # 1%

def is_tradeable_strike(strike, spot):
    dist = abs(strike - spot) / spot

    return dist <= 0.01   # 1%

def within_strike_range(ticker, series, spots):
    """Return True if strike is within ±STRIKE_RANGE of current spot, or if no spot available."""
    spot=spots.get(series)
    if spot is None: return True   # no spot data for this series — don't filter
    strike=parse_strike(ticker)
    if strike is None: return True
    return spot*(1-STRIKE_RANGE) <= strike <= spot*(1+STRIKE_RANGE)

CRYPTO_SERIES        = {"KXBTC", "KXETH"}
DISCOVERY_HOURS_MIN  = 0.75   # 45 minutes — don't fetch already-about-to-expire markets
DISCOVERY_HOURS_MAX  = 36     # 36 hours — wide enough to capture next-day crypto contracts
# Selection window mirrors discovery so no markets fall through the gap
SELECTION_MIN_MINUTES = int(DISCOVERY_HOURS_MIN * 60)   # 45 min
SELECTION_MAX_MINUTES = int(DISCOVERY_HOURS_MAX * 60)   # 2160 min (36 h)


async def fetch_preferred_series_markets(session):
    """Fetch markets for preferred series tickers, paginating through all results.

    Uses direct series_ticker queries (not broad page scans) for KXBTC/KXETH.
    Close-time window: 45 min → 36 hours, covering next-day crypto contracts.
    Adds a short delay between pages to avoid 429s.
    """
    if OFFLINE_MODE:
        markets = load_offline_markets()
        by_series = defaultdict(list)
        for market in markets:
            by_series[ticker_series(market.get("ticker", ""))].append(market)
        for series in PREFERRED_SERIES:
            series_batch = by_series.get(series, [])
            if series_batch:
                close_times = sorted(m.get("close_time", "") for m in series_batch if m.get("close_time"))
                earliest = close_times[0] if close_times else "?"
                latest = close_times[-1] if close_times else "?"
                log.info(f"[DISCOVERY] series={series} count={len(series_batch)} earliest_close={earliest} latest_close={latest}")
            else:
                log.info(f"[DISCOVERY] series={series} count=0 (offline)")
        return markets, False
    now = datetime.now(timezone.utc)
    close_min = (now + timedelta(hours=DISCOVERY_HOURS_MIN)).strftime("%Y-%m-%dT%H:%M:%SZ")
    close_max = (now + timedelta(hours=DISCOVERY_HOURS_MAX)).strftime("%Y-%m-%dT%H:%M:%SZ")
    markets = []
    discovery_rl = False
    for series in PREFERRED_SERIES:
        cursor = None
        page = 0
        series_batch = []
        while True:
            params = {
                "status": "open",
                "limit": 200,
                "series_ticker": series,
                "close_time_min": close_min,
                "close_time_max": close_max,
            }
            if cursor:
                params["cursor"] = cursor
            if page > 0:
                await asyncio.sleep(0.3)
            async with session.get(f"{BASE_URL}/markets", headers=get_headers(), params=params) as r:
                if r.status == 429:
                    log.warning(f"[DISCOVERY] Markets 429 for {series} page {page}, backing off")
                    discovery_rl = True
                    await asyncio.sleep(1.5)
                    break
                if r.status != 200:
                    log.warning(f"[DISCOVERY] Markets API {r.status} for {series}: {(await r.text())[:200]}")
                    break
                data = await r.json()
            batch = data.get("markets", [])
            series_batch.extend(batch)
            cursor = data.get("cursor")
            page += 1
            if not cursor or not batch:
                break

        # DISCOVERY summary log
        if series_batch:
            close_times = sorted(m.get("close_time", "") for m in series_batch if m.get("close_time"))
            earliest = close_times[0] if close_times else "?"
            latest   = close_times[-1] if close_times else "?"
            log.info(f"[DISCOVERY] series={series} count={len(series_batch)} earliest_close={earliest} latest_close={latest}")
        else:
            log.info(f"[DISCOVERY] series={series} count=0 (no markets in window {close_min} → {close_max})")

        markets.extend(series_batch)
    return markets, discovery_rl

async def fetch_batch_orderbooks(session, tickers):
    """Fetch per-ticker orderbooks from /markets/{ticker}/orderbook.
    Returns dict: ticker -> normalized executable YES/NO quote fields.
    """
    if OFFLINE_MODE:
        fixture_prices = load_offline_prices()
        prices = {ticker: copy.deepcopy(fixture_prices[ticker]) for ticker in tickers if ticker in fixture_prices}
        log.info(f"Orderbooks: 0 single request(s) for {len(tickers)} tickers, got prices for {len(prices)} (rate_limited=0).")
        return prices, 0, 0

    def _extract_levels(book, keys):
        if not isinstance(book, dict):
            return []
        for key in keys:
            value = book.get(key)
            if value:
                return _normalize_level_list(value)
        return []

    prices = {}
    num_requests = 0
    skip_rate_limited = 0
    for i, ticker in enumerate(tickers):
        if i > 0:
            await asyncio.sleep(0.10)
        data = None
        for attempt in range(2):
            async with session.get(f"{BASE_URL}/markets/{ticker}/orderbook", headers=get_headers()) as r:
                num_requests += 1
                if r.status == 429:
                    log.warning(f"Orderbook 429 ticker={ticker} attempt {attempt+1}")
                    if attempt == 0:
                        await asyncio.sleep(1.0)
                        continue
                    skip_rate_limited += 1
                    break
                if r.status != 200:
                    log.warning(f"Orderbook API {r.status} ticker={ticker}: {(await r.text())[:200]}")
                    break
                data = await r.json()
                break
        if data is None:
            continue

        payload = data if isinstance(data, dict) else {}
        print("RAW DATA KEYS:", list(data.keys()) if isinstance(data, dict) else data)
        print("YES LEVELS SAMPLE:", data.get("yes") or data.get("yes_dollars") if isinstance(data, dict) else None)
        print("NO LEVELS SAMPLE:", data.get("no") or data.get("no_dollars") if isinstance(data, dict) else None)
        if VERBOSE_LOGS:
            log.info(f"[RAW_BOOK] ticker={ticker} data={data}")
        fp = payload.get("orderbook_fp", {}) if isinstance(payload, dict) else {}
        yes_bid_levels = _extract_levels(payload, ["yes_bids", "yes_bid_levels", "yes", "yes_dollars"])
        if not yes_bid_levels:
            yes_bid_levels = _extract_levels(fp, ["yes_bids", "yes_bid_levels", "yes", "yes_dollars"])
        no_bid_levels = _extract_levels(payload, ["no_bids", "no_bid_levels", "no", "no_dollars"])
        if not no_bid_levels:
            no_bid_levels = _extract_levels(fp, ["no_bids", "no_bid_levels", "no", "no_dollars"])
        yes_ask_levels = _extract_levels(payload, ["yes_asks", "yes_ask_levels", "sell_yes", "yes_asks_dollars"])
        if not yes_ask_levels:
            yes_ask_levels = _extract_levels(fp, ["yes_asks", "yes_ask_levels", "sell_yes", "yes_asks_dollars"])
        no_ask_levels = _extract_levels(payload, ["no_asks", "no_ask_levels", "sell_no", "no_asks_dollars"])
        if not no_ask_levels:
            no_ask_levels = _extract_levels(fp, ["no_asks", "no_ask_levels", "sell_no", "no_asks_dollars"])

        yes_bid = safe_float(payload.get("yes_bid", payload.get("best_yes_bid")), None) if isinstance(payload, dict) else None
        no_bid = safe_float(payload.get("no_bid", payload.get("best_no_bid")), None) if isinstance(payload, dict) else None
        yes_ask = safe_float(payload.get("yes_ask", payload.get("best_yes_ask")), None) if isinstance(payload, dict) else None
        no_ask = safe_float(payload.get("no_ask", payload.get("best_no_ask")), None) if isinstance(payload, dict) else None

        yes_bid = best_bid(yes_bid_levels)
        no_bid  = best_bid(no_bid_levels)
        if yes_ask is None:
            yes_ask = _best_price_from_levels(yes_ask_levels, side="ask")
        if no_ask is None:
            no_ask = _best_price_from_levels(no_ask_levels, side="ask")

        yes_bid_touch = _touch_size(yes_bid_levels, side="bid")
        yes_ask_touch = _touch_size(yes_ask_levels, side="ask")
        no_bid_touch = _touch_size(no_bid_levels, side="bid")
        no_ask_touch = _touch_size(no_ask_levels, side="ask")

        if yes_bid is None and no_bid is None:
            yes_touch_size = 0.0
            no_touch_size = 0.0
        else:
            yes_touch_size = yes_ask_touch if yes_ask_touch > 0 else (no_bid_touch if no_bid is not None else yes_bid_touch)
            no_touch_size = no_ask_touch if no_ask_touch > 0 else (yes_bid_touch if yes_bid is not None else no_bid_touch)

        prices[ticker] = {
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "yes_pressure": sum(level[1] for level in yes_bid_levels),
            "no_pressure": sum(level[1] for level in no_bid_levels),
            "pressure_score": None,
            "no_pressure_score": None,
            "yes_touch_size": yes_touch_size,
            "no_touch_size": no_touch_size,
            "yes_depth_near": _near_touch_size(yes_bid_levels, side="bid"),
            "no_depth_near": _near_touch_size(no_bid_levels, side="bid"),
        }
        total_pressure = prices[ticker]["yes_pressure"] + prices[ticker]["no_pressure"]
        if total_pressure > 0:
            prices[ticker]["pressure_score"] = prices[ticker]["yes_pressure"] / total_pressure
            prices[ticker]["no_pressure_score"] = prices[ticker]["no_pressure"] / total_pressure
        print("BIDS:", ticker, yes_bid, no_bid)
    log.info(f"Orderbooks: {num_requests} single request(s) for {len(tickers)} tickers, got prices for {len(prices)} (rate_limited={skip_rate_limited}).")
    return prices, num_requests, skip_rate_limited

async def fetch_all_markets(session):
    function_name = "fetch_all_markets"
    print("TRACE: HIT", function_name)
    spots=await fetch_spot_prices(session)
    preferred, discovery_rl=await fetch_preferred_series_markets(session)
    seen=set(); stub_map={}
    skip_not_fam=0; skip_too_soon=0; skip_too_far=0
    skip_unparsed_strike=0; skip_far_strike=0; skip_no_spot=0
    per_series_candidates = defaultdict(lambda: {"above": [], "below": []})
    for m in preferred:
        t=m.get("ticker","")
        if not t or t.upper().startswith("KXMVE"): continue
        if t in seen: continue
        series=t.split("-")[0]
        fam_ok=any(series.upper().startswith(k) for k in HFT_SERIES)
        if not fam_ok:
            skip_not_fam+=1; continue
        h=hours_until(m.get("close_time") or m.get("expiration_time",""))
        minutes=h*60
        if minutes<SELECTION_MIN_MINUTES:
            skip_too_soon+=1; continue
        if minutes>SELECTION_MAX_MINUTES:
            skip_too_far+=1; continue
        # strike proximity: only keep strikes within ±2% of current spot
        spot=spots.get(series)
        strike=parse_strike(t)
        if strike is None:
            skip_unparsed_strike+=1; continue
        if spot is None:
            skip_no_spot+=1
            continue
        strike_distance_pct = abs(strike - spot) / spot
        if strike_distance_pct > 0.030:
            skip_far_strike+=1; continue
        seen.add(t)
        side_bucket = "above" if strike >= spot else "below"
        per_series_candidates[series][side_bucket].append((strike_distance_pct, strike, t, m))
    stubs=[]
    stub_map={}
    for series, sides in per_series_candidates.items():
        above = sorted(sides["above"], key=lambda item: item[0])[:5]
        below = sorted(sides["below"], key=lambda item: item[0])[:5]
        for _, _, t, m in above + below:
            if t in stub_map:
                continue
            stub_map[t]=m
            stubs.append(t)
    log.info(f"PREFILTER SUMMARY | kept={len(stubs)} rejects={{not_fam: {skip_not_fam}, too_soon: {skip_too_soon}, too_far: {skip_too_far}, far_strike: {skip_far_strike}, unparsed: {skip_unparsed_strike}, no_spot: {skip_no_spot}}}")
    log.info(f"Candidates: {len(stubs)} tickers ({skip_far_strike} far_strike, {skip_unparsed_strike} unparsed, {skip_no_spot} no_spot).")
    for ticker in stubs:
        print("TRACE: calling orderbook", ticker)
    prices, num_ob_requests, skip_rate_limited=await fetch_batch_orderbooks(session, stubs)
    if OFFLINE_MODE:
        prices = load_offline_prices()
    markets=[]
    for t,m in stub_map.items():
        p=prices.get(t,{})
        if p.get("yes_bid") is not None:
            m["yes_bid"]=p.get("yes_bid")
        if p.get("yes_ask") is not None:
            m["yes_ask"]=p.get("yes_ask")
        if p.get("no_bid") is not None:
            m["no_bid"]=p.get("no_bid")
        if p.get("no_ask") is not None:
            m["no_ask"]=p.get("no_ask")
        m["yes_pressure"]=p.get("yes_pressure")
        m["no_pressure"]=p.get("no_pressure")
        m["pressure_score"]=p.get("pressure_score")
        m["no_pressure_score"]=p.get("no_pressure_score")
        m["yes_touch_size"]=p.get("yes_touch_size")
        m["no_touch_size"]=p.get("no_touch_size")
        m["yes_depth_near"]=p.get("yes_depth_near")
        m["no_depth_near"]=p.get("no_depth_near")
        quotes = normalize_quote_state(m)
        if VERBOSE_LOGS:
            log.info(
                cyan(
                    f"[QUOTE_NORM] ticker={t}"
                    f" yes_quote={1 if quotes['yes_has_real_quote'] else 0}"
                    f" no_quote={1 if quotes['no_has_real_quote'] else 0}"
                    f" yes_book={1 if quotes['yes_has_real_book'] else 0}"
                    f" no_book={1 if quotes['no_has_real_book'] else 0}"
                )
            )
        if p.get("yes_bid") is None and p.get("no_bid") is None:
            log_skip(t, "no_real_book")
            continue
        if max(quotes["yes_touch"], quotes["no_touch"]) < 1.0:
            log_skip(t, "no_real_liquidity")
            continue
        markets.append(m)
    if any(not normalize_quote_state(m)["market_has_any_real_side"] for m in markets):
        raise AssertionError("TRADEABLE MARKETS contains ticker without any real side")
    log.info(f"TRADEABLE MARKETS: {len(markets)}")
    if len(markets) < 3:
        log.info("Thin market — proceeding anyway")
    log.info(f"RATE_LIMIT_SOURCE | discovery={discovery_rl} orderbooks={skip_rate_limited > 0} ob_requests={num_ob_requests} ob_prices={len(prices)}")
    return markets, prices, num_ob_requests, spots, skip_rate_limited, skip_unparsed_strike, skip_far_strike, skip_no_spot, discovery_rl

async def run_cycle(session, cycle_num):
    print("TRACE: entering run_cycle")
    SEP="══════════════════════════════════════════════════════════"
    global _prev_spot_prices
    ts=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    now = utc_now()
    purge_expired_runtime_guards(now)
    _last_skip_reason_counts.clear()
    _recent_rejects_by_ticker.clear()
    _series_open_count.clear()
    log.info(SEP)
    log.info(f"CYCLE #{cycle_num:<4}  |  {ts}")
    log.info(SEP)
    if _dashboard is not None:
        _dashboard.set_scanning()
        if cycle_num == 1:
            _dashboard.add_event("SYSTEM", "FIRST_SCAN_BEGIN", f"cycle={cycle_num}")
    print("TRACE: entering orderbook for", "market_scan")
    markets, prices, num_ob_requests, spots, skip_rate_limited, skip_unparsed_strike, skip_far_strike, skip_no_spot, discovery_rl = await fetch_all_markets(session)
    # Only skip cycle on genuine rate-limit: must have actually made requests or seen 429
    if (skip_rate_limited > 0 or discovery_rl) and len(prices) == 0 and num_ob_requests > 0:
        log.warning("RATE LIMIT HIT - SKIPPING CYCLE")
        if _dashboard is not None:
            _a_closed, _a_wins, _a_losses = _analytics.get_totals()
            _dashboard.update({
                "bankroll":      BANKROLL,
                "equity":        round(BANKROLL + _realized_cash_pnl, 2),
                "cash":          round(BANKROLL + _realized_cash_pnl, 2),
                "open":          0.0,
                "realized":      _realized_cash_pnl,
                "cycle":         cycle_num,
                "regime":        "normal",
                "positions":     [],
                "candidates":    [],
                "closed_trades": _a_closed,
                "wins":          _a_wins,
                "losses":        _a_losses,
            })
            _dashboard.add_event("SYSTEM", "RATE_LIMIT", f"cycle={cycle_num} skipping")
            _dashboard.set_idle(CYCLE_SECS)
        await asyncio.sleep(2)
        return
    # Snapshot pre-exit state for validate_cycle_state comparison at end of cycle
    open_rows, total_open_exposure = rebuild_open_position_state()
    open_count = len(open_rows)
    expired_exits, expiry_exit_count = await resolve_trades(session)
    btc_spot = spots.get("KXBTC")
    eth_spot = spots.get("KXETH")
    btc_active = is_active_market(btc_spot, _prev_spot_prices.get("KXBTC"))
    eth_active = is_active_market(eth_spot, _prev_spot_prices.get("KXETH"))
    if btc_spot is not None:
        _prev_spot_prices["KXBTC"] = btc_spot
    if eth_spot is not None:
        _prev_spot_prices["KXETH"] = eth_spot
    cycle_regime = detect_regime(markets, btc_active or eth_active)
    _ew, _el, _epnl, early_win_exits, early_loss_exits, exit_counts, early_exited_tickers = check_early_exits(prices)
    # Rebuild from disk after both exit functions have rewritten the file —
    # this ensures open_rows/total_open_exposure are always in sync with disk.
    open_rows, total_open_exposure = rebuild_open_position_state()
    open_count = len(open_rows)
    log.info(f"ACTIVE POSITIONS: {open_count}")
    # ── Compact dashboard ────────────────────────────────────────────────────
    BAR = "─" * 61
    # ALL TIME stats from resolved file
    all_w = all_l = 0; all_pnl = 0.0
    if Path(RESOLVED_TRADES_LOG).exists():
        with open(RESOLVED_TRADES_LOG, newline="") as _f:
            for _r in csv.DictReader(_f):
                all_pnl += safe_float(_r.get("pnl_usd"), 0.0) or 0.0
                if _r.get("won","0") == "1": all_w += 1
                else: all_l += 1
    all_wr  = all_w / (all_w + all_l) if (all_w + all_l) > 0 else 0.0
    cash_balance = current_cash_balance(total_open_exposure)
    s_total = session_wins + session_losses
    s_wr    = session_wins / s_total if s_total > 0 else 0.0
    BAR = print_balance_summary(
        log, cyan, yellow, fmt_money,
        BANKROLL, cash_balance, total_open_exposure, open_count,
        all_w, all_l, all_wr, all_pnl,
        session_wins, session_losses, s_wr, session_pnl,
        realized_cash_pnl=_realized_cash_pnl,
    )
    print_exposure_summary(log, fmt_money, open_rows, prices, now, safe_float, timezone, datetime)
    log.info(cyan(BAR))

    cycle_regime = compute_cycle_regime(markets)
    adaptive_spread_cap = cycle_regime["adaptive_spread_cap"]
    adaptive_touch_req  = cycle_regime["adaptive_touch_req"]
    bad_regime = cycle_regime["bad_regime"]
    # WIDE regime: relax spread cap slightly so marginal trades can still qualify,
    # but tighten edge/quality/size to compensate for worse book conditions.
    if bad_regime:
        adaptive_spread_cap = min(0.10, HARD_SPREAD_CEIL + 0.02)
    if VERBOSE_LOGS:
        log.info(cyan(f"ADAPTIVE_LIMITS | spread_cap={adaptive_spread_cap:.3f} min_edge={MIN_EDGE:.3f} touch_req={adaptive_touch_req}"))
    log.info(f"REGIME | {'WIDE ⚠' if bad_regime else 'NORMAL'}")
    if bad_regime and VERBOSE_LOGS:
        log.info(yellow(
            f"REGIME_ADJUSTMENTS | spread_cap={adaptive_spread_cap:.3f}"
            f" min_edge={round(MIN_EDGE + 0.01, 4):.3f} size_mult=0.5 quality_floor=0.35"
        ))
    adapt_evaluated = adapt_edge_pass = adapt_spread_pass = adapt_touch_pass = adapt_quality_pass = adapt_near_miss = 0
    signals = 0
    buys = 0        # initialized early so end-of-cycle block is safe even if an exception fires before line ~4181
    candidates = []
    _near_miss_pool = []   # soft-blocked candidates surfaced in dashboard for DEFENSIVE/NO_TRADE
    actionable_count = 0
    RELAX_MODE = False
    MAX_ENTRY_SPREAD_TEMP = MAX_ENTRY_SPREAD
    usable_markets = 0
    skip_non_binary = skip_not_open = skip_kxmve = skip_no_price = 0
    skip_hours = skip_hours_strict = skip_hft_hours = 0
    skip_no_liquidity = skip_pressure = skip_velocity = skip_elite_score = 0
    skip_instability = skip_wide_spread = skip_price = skip_volatile = skip_ev = skip_model = 0
    skip_low_volume = 0
    skip_exposure = skip_family_exposure = skip_bucket_exposure = 0
    skip_size = skip_cycle_cap = skip_duplicate_ticker = 0
    skip_series_cap = skip_cooldown = skip_weak_book = skip_edge = 0
    skip_spot_missing = skip_entry_score = skip_stale_signal = 0
    qf_passed = qf_rejected = 0
    qf_breakdown: dict = {}
    killer_passed = killer_blocked = 0
    killer_breakdown: dict = {}
    held_tickers = {r.get("ticker") for r in open_rows}
    min_edge = CHF_MIN_EDGE
    elite_scores = []
    pressure_vals = []
    velocity_vals = []
    spread_vals = []
    sample_fields = ["ticker","title","market_type","status","yes_bid","yes_ask","no_bid","no_ask","volume","liquidity","open_interest","close_time"]
    samples_printed = 0

    for m in markets:
        if m.get("market_type") != "binary":
            skip_non_binary += 1
            continue
        if m.get("status") != "active":
            skip_not_open += 1
            continue
        ticker = m.get("ticker", "")
        if ticker in held_tickers:
            log.info(f"[HELD TRACE] entering pipeline ticker={ticker}")
        if not ticker or ticker.upper().startswith("KXMVE"):
            skip_kxmve += 1
            continue
        series = ticker_series(ticker)
        family = next((k for k in FAMILY_RATES if series.upper().startswith(k)), "DEFAULT")
        if family not in HFT_SERIES:
            skip_hft_hours += 1
            continue
        hours = hours_until(m.get("close_time") or m.get("expiration_time", ""))
        minutes_to_expiry = hours * 60
        if minutes_to_expiry < 45:
            skip_hours += 1
            if ticker in held_tickers:
                log.info(f"[HELD SKIP] {ticker} reason=window minutes={minutes_to_expiry:.0f} min=45")
            log_skip(ticker, "too_late")
            continue
        if minutes_to_expiry > SELECTION_MAX_MINUTES:
            skip_hours_strict += 1
            if ticker in held_tickers:
                log.info(f"[HELD SKIP] {ticker} reason=window minutes={minutes_to_expiry:.0f} max={SELECTION_MAX_MINUTES}")
            log_skip(ticker, "too_far")
            continue
        quotes = normalize_quote_state(m)
        yes_book = side_book_metrics(m, "yes")
        no_book = side_book_metrics(m, "no")
        if not yes_book["valid"]:
            log.info(yellow(f"[SIDE_INVALID] ticker={ticker} side=yes reason={yes_book['reason']}"))
        if not no_book["valid"]:
            log.info(yellow(f"[SIDE_INVALID] ticker={ticker} side=no reason={no_book['reason']}"))
        if not yes_book["valid"] and not no_book["valid"]:
            if quotes["market_has_any_real_side"]:
                log.info(yellow(f"[PIPELINE_MISMATCH] ticker={ticker} reason=no_valid_side_after_tradeable"))
            skip_no_price += 1
            log_skip(ticker, "no_real_book")
            continue
        _yt = float(m.get("yes_touch_size") or 0)
        _nt = float(m.get("no_touch_size") or 0)
        adapt_evaluated += 1
        usable_markets += 1
        if VERBOSE_LOGS and (cycle_num == 1 or cycle_num % 20 == 0) and samples_printed < 10:
            log.info("SAMPLE: " + str({k: m.get(k) for k in sample_fields}))
            samples_printed += 1
        strike = parse_strike(ticker)
        spot_price = spots.get(series)
        pressure_score = m.get("pressure_score")
        prev_pressure = _pressure_history.get(ticker, [])
        if pressure_score is None:
            pressure_score = 0.0
        # Pressure is a ranking signal only — no hard gate here
        yes_ask_f = yes_book["ask"]
        no_ask_f = no_book["ask"]
        yes_spread = yes_book["spread"]
        no_spread = no_book["spread"]
        if VERBOSE_LOGS:
            log.info(
                f"[EVAL] {ticker} | yes_ask={yes_ask_f if yes_ask_f is not None else '-'}"
                f" no_ask={no_ask_f if no_ask_f is not None else '-'}"
                f" yes_spread={yes_spread if yes_spread is not None else '-'}"
                f" no_spread={no_spread if no_spread is not None else '-'}"
                f" pressure={pressure_score:.3f}"
            )
        # Model probability from strike/spot distance
        if spot_price is None:
            skip_spot_missing += 1
            log_skip(ticker, "no_spot")
            continue
        if strike is None:
            skip_model += 1
            log_skip(ticker, "no_strike")
            continue
        mp = estimate_model_prob(m, spot_price, minutes_to_expiry)
        if mp is None:
            skip_model += 1
            log_skip(ticker, "no_strike")
            continue
        # Strike proximity gate
        dist = abs(strike - spot_price) / spot_price
        if dist > 0.030:
            skip_model += 1
            log_skip(ticker, "far_strike", f"dist={dist:.3f}")
            continue
        # Minimum volume filter — only skip if low-volume AND wide spread
        volume = safe_float(m.get("volume_24h") or m.get("volume"), None)
        widest_real_spread = max([s for s in [yes_spread, no_spread] if s is not None], default=0.0)
        if volume is not None and volume < MIN_VOLUME and widest_real_spread > MAX_HARD_SPREAD:
            skip_low_volume += 1
            log_skip(ticker, "low_volume", f"volume={volume:.0f} spread={widest_real_spread:.3f}")
            continue
        # Edge-based side selection using true execution prices only.
        edge_yes = (mp - yes_ask_f) if yes_book["valid"] and yes_ask_f is not None else None
        edge_no  = ((1.0 - mp) - no_ask_f) if no_book["valid"] and no_ask_f is not None else None
        # Spread penalty: half-spread cost charged against raw edge.
        # In WIDE regime the penalty is reduced by 25% — books are structurally
        # wide so a full half-spread deduction would suppress every candidate.
        _pen_factor = 0.75 if bad_regime else 1.0
        eff_edge_yes = round(edge_yes - (yes_spread * 0.5 * _pen_factor), 4) if edge_yes is not None and yes_spread is not None else None
        eff_edge_no  = round(edge_no  - (no_spread * 0.5 * _pen_factor), 4) if edge_no is not None and no_spread is not None else None
        # Relative edge: raw edge expressed as a multiple of spread.
        # Captures "big signal relative to noise" — used for WIDE-regime override.
        rel_edge_yes = round(edge_yes / max(yes_spread, 0.01), 4) if edge_yes is not None and yes_spread is not None else None
        rel_edge_no  = round(edge_no  / max(no_spread, 0.01), 4) if edge_no is not None and no_spread is not None else None
        effective_min_edge = MIN_EDGE
        if cycle_regime["median_spread"] > 0.06:
            effective_min_edge += 0.01
        if minutes_to_expiry < 90:
            effective_min_edge += 0.005
        if max(_yt, _nt) < 10:
            effective_min_edge += 0.005
        if bad_regime:
            effective_min_edge += 0.01   # WIDE regime: require higher edge
        effective_min_edge = round(effective_min_edge, 4)
        _relative_pass = False
        selection_reason = None
        if eff_edge_yes is not None and eff_edge_yes >= effective_min_edge and (eff_edge_no is None or eff_edge_yes >= eff_edge_no):
            selected_side = "yes"
            entry_price   = yes_ask_f
            raw_edge_sel  = edge_yes
            true_edge     = eff_edge_yes
            relative_edge = rel_edge_yes
            selected_spread = yes_spread
            selected_touch = yes_book["touch"]
            selection_reason = "stronger_executable_edge"
        elif eff_edge_no is not None and eff_edge_no >= effective_min_edge:
            selected_side = "no"
            entry_price   = no_ask_f
            raw_edge_sel  = edge_no
            true_edge     = eff_edge_no
            relative_edge = rel_edge_no
            selected_spread = no_spread
            selected_touch = no_book["touch"]
            selection_reason = "stronger_executable_edge"
        else:
            # Normal effective-edge gate failed.  In WIDE regime a relative-edge
            # override admits the trade when raw signal is strong vs spread AND
            # the book quality is high enough to justify the execution risk.
            if (edge_yes is not None) and (edge_no is None or edge_yes >= edge_no):
                _rs, _rp = "yes", yes_ask_f
                _rraw, _reff, _rrel = edge_yes, eff_edge_yes, rel_edge_yes
                _rspread, _rtouch = yes_spread, yes_book["touch"]
            elif edge_no is not None:
                _rs, _rp = "no", no_ask_f
                _rraw, _reff, _rrel = edge_no, eff_edge_no, rel_edge_no
                _rspread, _rtouch = no_spread, no_book["touch"]
            else:
                _rs = _rp = _rraw = _reff = _rrel = _rspread = _rtouch = None
            _rel_q = market_quality_score(m, _rs, _rspread, minutes_to_expiry) if _rs is not None and _rspread is not None else 0.0
            allow_relative_trade = bad_regime and _rrel is not None and _rrel >= 0.6 and _rel_q >= 0.55
            if allow_relative_trade:
                selected_side = _rs
                entry_price   = _rp
                raw_edge_sel  = _rraw
                true_edge     = _reff
                relative_edge = _rrel
                selected_spread = _rspread
                selected_touch = _rtouch
                quality_score = _rel_q
                _relative_pass = True
                selection_reason = "relative_edge_override"
                if VERBOSE_LOGS:
                    log.info(cyan(
                        f"[RELATIVE EDGE PASS] ticker={ticker}"
                        f" raw={raw_edge_sel:.4f} spread={selected_spread:.4f}"
                        f" rel={relative_edge:.2f} q={quality_score:.3f}"
                    ))
            else:
                skip_edge += 1
                _vol_s = f"{int(volume)}" if volume is not None else "-"
                if VERBOSE_LOGS: log.info(f"[GATE] {ticker} sp=- vol={_vol_s} yask={yes_ask_f if yes_ask_f is not None else '-'} nask={no_ask_f if no_ask_f is not None else '-'} mpy={mp:.3f} mpn={1-mp:.3f} ey={eff_edge_yes if eff_edge_yes is not None else '-'} en={eff_edge_no if eff_edge_no is not None else '-'} pr={pressure_score:.3f} vel=- elite=- escore=- side=- block=low_edge(min={effective_min_edge:.3f})")
                _best_edge = max([v for v in [eff_edge_yes, eff_edge_no] if v is not None], default=-999.0)
                if OFFLINE_MODE and OFFLINE_SCENARIO == "edge_floor":
                    if VERBOSE_LOGS:
                        log.info(
                            f"[EDGE CHECK] raw={_best_edge:.4f}"
                            f" effective={_best_edge:.4f} threshold={MIN_EDGE:.4f}"
                        )
                    log.info(yellow(
                        f"[BLOCKED] ticker={ticker} reason=edge_floor"
                        f" final_edge={_best_edge:.4f} min_edge={MIN_EDGE:.4f}"
                    ))
                    log_skip(ticker, "edge_floor",
                             f"final_edge={_best_edge:.4f} min_edge={MIN_EDGE:.4f} raw={_best_edge:.4f}")
                    continue
                if 0 < effective_min_edge - _best_edge < 0.01:
                    adapt_near_miss += 1
                    if VERBOSE_LOGS: log.info(yellow(f"[NEAR_MISS] ticker={ticker} reason=edge value={_best_edge:.4f} threshold={effective_min_edge:.4f}"))
                log_skip(ticker, "low_edge", f"eff_yes={eff_edge_yes if eff_edge_yes is not None else 'na'} eff_no={eff_edge_no if eff_edge_no is not None else 'na'} min={effective_min_edge:.3f}")
                continue
        # Keep side honest: if model probability favours NO, use NO or skip.
        if mp < 0.5:
            if not no_book["valid"]:
                log.info(yellow(
                    f"[ENTRY_BLOCK] ticker={ticker} reason=no_side_not_executable"
                    f" mp={mp:.3f} model_no={(1.0 - mp):.3f}"
                ))
                log_skip(ticker, "no_side_not_executable", f"mp={mp:.3f} model_no={(1.0 - mp):.3f}")
                continue
            selected_side = "no"
            selected_spread = no_spread
            selected_touch = no_book["touch"]
            entry_price = no_ask_f
            raw_edge_sel = edge_no
            true_edge = eff_edge_no
            relative_edge = rel_edge_no
            selection_reason = "model_prefers_no"
        pressure_score = safe_float(m.get("no_pressure_score" if selected_side == "no" else "pressure_score"), 0.0) or 0.0
        if VERBOSE_LOGS:
            log.info(
                cyan(
                    f"[SIDE_SELECT] ticker={ticker}"
                    f" yes_valid={1 if yes_book['valid'] else 0}"
                    f" no_valid={1 if no_book['valid'] else 0}"
                    f" yes_edge={(f'{eff_edge_yes:.4f}' if eff_edge_yes is not None else 'invalid')}"
                    f" no_edge={(f'{eff_edge_no:.4f}' if eff_edge_no is not None else 'invalid')}"
                    f" chosen={selected_side} reason={selection_reason}"
                )
            )
        if VERBOSE_LOGS:
            log.info(
                cyan(
                    f"[BOOK_CHECK] ticker={ticker} side={selected_side}"
                    f" ask={entry_price:.4f} spread={(f'{selected_spread:.4f}' if selected_spread is not None else 'na')}"
                    f" touch={selected_touch:.0f} valid=1"
                )
            )
        _touch_near_miss_floor = max(MIN_BOOK_SIZE, adaptive_touch_req - 2)
        if selected_touch < _touch_near_miss_floor:
            skip_no_liquidity += 1
            if ticker in held_tickers:
                log.info(f"[HELD SKIP] {ticker} reason=liquidity touch={selected_touch:.0f} req={adaptive_touch_req:.0f} side={selected_side}")
            if selected_touch >= adaptive_touch_req * 0.5:
                adapt_near_miss += 1
                if VERBOSE_LOGS: log.info(yellow(f"[NEAR_MISS] ticker={ticker} reason=touch value={selected_touch:.0f} threshold={adaptive_touch_req:.0f}"))
            log_skip(ticker, "bad_liquidity", f"side={selected_side} touch={selected_touch:.0f} req={adaptive_touch_req:.0f}")
            continue
        adapt_touch_pass += 1
        if not quotes["market_has_any_real_side"]:
            raise AssertionError(f"tradeable market without real side quote: {ticker}")
        if selected_spread is not None and selected_spread > HARD_SPREAD_CEIL:
            skip_wide_spread += 1
            if ticker in held_tickers:
                log.info(f"[HELD SKIP] {ticker} reason=spread spread={selected_spread:.3f} cap={HARD_SPREAD_CEIL:.3f} side={selected_side}")
            log_skip(ticker, "wide_spread", f"side={selected_side} spread={selected_spread:.3f} ceil={HARD_SPREAD_CEIL:.3f}")
            continue
        if selected_spread is not None and selected_spread > adaptive_spread_cap:
            skip_wide_spread += 1
            adapt_near_miss += 1
            if ticker in held_tickers:
                log.info(f"[HELD SKIP] {ticker} reason=spread spread={selected_spread:.3f} cap={adaptive_spread_cap:.3f} side={selected_side}")
            if VERBOSE_LOGS: log.info(yellow(f"[NEAR_MISS] ticker={ticker} reason=spread value={selected_spread:.4f} threshold={adaptive_spread_cap:.4f}"))
            log_skip(ticker, "wide_spread", f"side={selected_side} spread={selected_spread:.3f} cap={adaptive_spread_cap:.3f}")
            continue
        adapt_spread_pass += 1
        _prelim_quality = market_quality_score(m, selected_side, selected_spread, minutes_to_expiry)
        _qf_row = {
            "spread": selected_spread,
            "yes_touch": _yt,
            "no_touch": _nt,
            "quality_score": _prelim_quality,
        }
        _qf_ok, _qf_reason = pre_trade_quality_filter(_qf_row)
        if not _qf_ok:
            qf_rejected += 1
            qf_breakdown[_qf_reason] = qf_breakdown.get(_qf_reason, 0) + 1
            log_skip(ticker, f"pre_filter_{_qf_reason}",
                     f"side={selected_side} spread={selected_spread:.3f} yes_touch={_yt:.0f} no_touch={_nt:.0f} prelim_q={_prelim_quality:.3f}")
            continue
        qf_passed += 1
        spread_penalty = round(max(0.0, selected_spread) * 0.5 * _pen_factor, 4)
        net_edge = round(raw_edge_sel - spread_penalty, 4)
        true_edge = net_edge
        # Quality score (already set for relative-pass path; compute now otherwise).
        if not _relative_pass:
            quality_score = market_quality_score(m, selected_side, selected_spread, minutes_to_expiry)
        # Quality boost: ±0.01 premium/penalty centred on quality=0.5.
        quality_boost = max(-0.01, min(0.01, (quality_score - 0.5) * 0.02))
        true_edge = round(true_edge + quality_boost, 4)
        # Full edge decomposition — always logged before negativity check.
        if VERBOSE_LOGS:
            log.info(
                f"[EDGE] raw={raw_edge_sel:.4f} spread={selected_spread:.4f}"
                f" penalty={spread_penalty:.4f} net={net_edge:.4f} rel={relative_edge:.2f}"
                f" quality_boost={quality_boost:.4f} effective={true_edge:.4f}"
            )
        # Informational only — no longer controls pass/block decision.
        allow_soft_negative = (
            bad_regime
            and relative_edge >= 0.75
            and quality_score >= 0.65
            and true_edge >= -0.01
        )
        if VERBOSE_LOGS:
            log.info(
                f"[FINAL EDGE] edge={true_edge:.4f} rel={relative_edge:.2f}"
                f" quality={quality_score:.2f} allow_soft={allow_soft_negative}"
            )
        # Hard floor: MIN_EDGE enforced after ALL adjustments (penalty, quality_boost,
        # relative-pass, soft-negative). No soft path bypasses this gate.
        if VERBOSE_LOGS:
            log.info(
                f"[EDGE CHECK] raw={raw_edge_sel:.4f}"
                f" effective={true_edge:.4f} threshold={MIN_EDGE:.4f}"
            )
        if true_edge < MIN_EDGE:
            skip_edge += 1
            log.info(yellow(
                f"[BLOCKED] ticker={ticker} reason=edge_floor"
                f" final_edge={true_edge:.4f} min_edge={MIN_EDGE:.4f}"
            ))
            log_skip(ticker, "edge_floor",
                     f"final_edge={true_edge:.4f} min_edge={MIN_EDGE:.4f} raw={raw_edge_sel:.4f}")
            continue
        adapt_edge_pass += 1
        log.info(green(f"[EDGE PASS] ticker={ticker} edge={true_edge:.4f} spread={selected_spread:.4f} side={selected_side} mp={mp:.3f} entry={entry_price:.3f}"))
        if VERBOSE_LOGS: log.info(f"[EDGE TRACK] ticker={ticker} true_edge={true_edge} model_prob={mp} entry={entry_price}")
        # ── Change 3: anti-chase filter ───────────────────────────────────────
        # Block late-confirmation YES buys where the market is already extended
        # and the pressure signal is only confirming a move that's priced in.
        if selected_side == "yes" and mp >= 0.68 and pressure_score >= 0.55:
            log.info(yellow(
                f"[ENTRY_BLOCK] ticker={ticker} reason=late_chase"
                f" market_prob={mp:.3f} pressure={pressure_score:.3f} spread={selected_spread:.3f}"
            ))
            log_skip(ticker, "late_chase", f"mp={mp:.3f} pressure={pressure_score:.3f}")
            continue
        if selected_spread > 0.03 and pressure_score >= 0.55:
            log.info(yellow(
                f"[ENTRY_BLOCK] ticker={ticker} reason=late_chase"
                f" market_prob={mp:.3f} pressure={pressure_score:.3f} spread={selected_spread:.3f}"
            ))
            log_skip(ticker, "late_chase", f"spread={selected_spread:.3f} pressure={pressure_score:.3f}")
            continue
        # ── Change 4: execution-quality edge adjustment ───────────────────────
        # Penalise edges on weak books so mid-tier unlock and sizing see realistic
        # expectancy.  The original true_edge already passed the MIN_EDGE floor;
        # this adjustment is applied only for entry-decision purposes.
        _adj_edge = true_edge
        if selected_spread > 0.04 and selected_touch < 15:
            _adj_edge = round(_adj_edge * 0.65, 4)
        elif selected_spread > 0.03:
            _adj_edge = round(_adj_edge * 0.80, 4)
        elif selected_touch < 25:
            _adj_edge = round(_adj_edge * 0.80, 4)
        if _adj_edge != true_edge:
            log.info(
                f"[EDGE_ADJUST] ticker={ticker}"
                f" raw_edge={true_edge:.4f} adj_edge={_adj_edge:.4f}"
                f" spread={selected_spread:.4f} touch={selected_touch:.0f}"
            )
            true_edge = _adj_edge
        yes_pressure_score = safe_float(m.get("pressure_score"), 0.0) or 0.0
        no_pressure_score = safe_float(m.get("no_pressure_score"), 0.0) or 0.0
        side_control_delta = abs(yes_pressure_score - no_pressure_score)
        if pressure_score < 0.45:
            candidate_gate = "DEAD"
        elif pressure_score < 0.52:
            candidate_gate = "WEAK_PRESS"
        elif true_edge < 0.58:
            candidate_gate = "LOW_EDGE"
        elif side_control_delta < 0.15:
            candidate_gate = "NO_SIDE_CONTROL"
        else:
            candidate_gate = "PASS"
        if candidate_gate != "PASS":
            log.info(
                yellow(
                    f"[NO_EN_REASON] ticker={ticker} gate={candidate_gate}"
                    f" pressure={pressure_score:.3f}"
                    f" edge={true_edge:.3f}"
                    f" spread={selected_spread:.3f}"
                    f" touch={selected_touch:.0f}"
                    f" yes_pressure={yes_pressure_score:.3f}"
                    f" no_pressure={no_pressure_score:.3f}"
                )
            )
        crowd = entry_price
        last_p = _last_crowd.get((ticker, selected_side))
        history = _price_history.get((ticker, selected_side), [])
        pressure_delta = pressure_score - prev_pressure[-1] if prev_pressure else 0.0
        vm = compute_velocity_metrics(history) or {"velocity_short":0.0, "velocity_med":0.0, "acceleration":0.0, "velocity_score":0.0, "stability_score":1.0, "max_jump":0.0}
        crowd_softening = last_p is not None and crowd < last_p
        liq_score = liquidity_score(m)
        depth_score = book_depth_score(m)
        imbalance_score = book_imbalance_score(m, selected_side)
        spread_expanding = spread_is_expanding(ticker)
        fragile_book = is_fragile_book(dict(m, ticker=ticker), selected_side)
        provisional_regime_candidate = {
            "pressure_score": pressure_score,
            "crowd_softening": crowd_softening,
            "spread_expanding": spread_expanding,
            "fragile_book": fragile_book,
            "crowd": crowd,
            "pressure_delta": pressure_delta,
            "liquidity_score": liq_score,
            "spread": selected_spread,
        }
        provisional_regime = classify_regime(provisional_regime_candidate) if USE_REGIME_FILTER else "momentum_clean"
        elite_override_allowed, elite_override_reason = elite_entry_allowed(
            provisional_regime,
            pressure_score,
            selected_spread,
            selected_touch,
            side_valid=bool(selected_side),
            executable_valid=entry_price is not None,
        )
        if VERBOSE_LOGS:
            if pressure_score < MIN_PRESSURE_ENTRY:
                log.info(
                    yellow(
                        f"[PIPELINE_PASS] ticker={ticker} disabled_filter=pressure"
                        f" side={selected_side} pressure={pressure_score:.3f}"
                        f" min={MIN_PRESSURE_ENTRY:.2f}"
                    )
                )
            if vm["velocity_med"] <= 0 or crowd_softening:
                log.info(
                    yellow(
                        f"[PIPELINE_PASS] ticker={ticker} disabled_filter=momentum"
                        f" side={selected_side} momentum={vm['velocity_med']:.4f}"
                        f" crowd_softening={1 if crowd_softening else 0}"
                    )
                )
        entry_score = compute_entry_score(pressure_score, selected_spread, true_edge, 0.0)
        elite_score = compute_elite_score(crowd, mp, pressure_score, pressure_delta, selected_spread, vm)
        if VERBOSE_LOGS:
            log.info(
                cyan(
                    f"[EXEC_SCORE] ticker={ticker} score={entry_score:.3f}"
                    f" edge={true_edge:.3f} pressure={pressure_score:.3f}"
                    f" spread={selected_spread:.3f} side={selected_side}"
                )
            )
        if entry_score < EXEC_SCORE_THRESHOLD and pressure_score < 0.45:
            log.info(
                f"[SKIP] {ticker} | mid_signal_low_pressure"
                f" score={entry_score:.3f} pressure={pressure_score:.3f}"
            )
            skip_entry_score += 1
            _near_miss_pool.append({
                "ticker": ticker, "edge": true_edge,
                "pressure_score": pressure_score, "spread": selected_spread,
                "entry_score": entry_score, "tier_name": "",
            })
            continue
        if VERBOSE_LOGS: log.info(f"[QUALITY] ticker={ticker} q={quality_score:.3f} spread={selected_spread:.4f} yes_touch={_yt:.0f} no_touch={_nt:.0f}")
        if quality_score < 0.20:
            skip_entry_score += 1
            if quality_score >= 0.10:
                adapt_near_miss += 1
                if VERBOSE_LOGS: log.info(yellow(f"[NEAR_MISS] ticker={ticker} reason=quality value={quality_score:.3f} threshold=0.20"))
            log_skip(ticker, "low_quality", f"q={quality_score:.3f} side={selected_side} spread={selected_spread:.3f}")
            _near_miss_pool.append({
                "ticker": ticker, "edge": true_edge,
                "pressure_score": pressure_score, "spread": selected_spread,
                "entry_score": entry_score, "tier_name": "",
            })
            continue
        adapt_quality_pass += 1
        # WIDE regime: raise the quality floor from 0.20 → 0.35
        if bad_regime and quality_score < 0.35:
            skip_entry_score += 1
            log_skip(ticker, "bad_regime_low_quality",
                     f"q={quality_score:.3f} side={selected_side} spread={selected_spread:.3f} regime=WIDE")
            _near_miss_pool.append({
                "ticker": ticker, "edge": true_edge,
                "pressure_score": pressure_score, "spread": selected_spread,
                "entry_score": entry_score, "tier_name": "",
            })
            continue
        # Entry price must be in tradeable range for selected side.
        # Strong edge can override the low-price floor.
        price_override = False
        if entry_price < 0.06:
            skip_price += 1
            _vol_s = f"{int(volume)}" if volume is not None else "-"
            if VERBOSE_LOGS: log.info(f"[GATE] {ticker} sp={selected_spread:.3f} vol={_vol_s} yask={yes_ask_f if yes_ask_f is not None else '-'} nask={no_ask_f if no_ask_f is not None else '-'} mpy={mp:.3f} mpn={1-mp:.3f} ey={eff_edge_yes if eff_edge_yes is not None else '-'} en={eff_edge_no if eff_edge_no is not None else '-'} pr={pressure_score:.3f} vel=- elite=- escore=- side={selected_side} block=price({entry_price:.3f})")
            log_skip(ticker, "no_tradeable_side", f"entry_price={entry_price:.3f} side={selected_side} floor=0.06")
            continue
        price_floor = YES_MIN_ENTRY_PRICE if selected_side == "yes" else NO_MIN_ENTRY_PRICE
        price_cap = 0.60 if selected_side == "no" else 0.45
        if entry_price < price_floor:
            if true_edge >= 0.30 and quality_score >= 0.65 and elite_override_allowed:
                price_override = True
                log.info(cyan(f"[PRICE OVERRIDE] strong edge {ticker}"))
            elif true_edge >= 0.30 and quality_score >= 0.65:
                log.info(yellow(
                    f"[ELITE_ENTRY_BLOCKED] ticker={ticker} reason={elite_override_reason}"
                    f" regime={provisional_regime} pressure={pressure_score:.3f}"
                    f" spread={selected_spread:.3f} touch={selected_touch:.0f}"
                ))
            else:
                skip_price += 1
                _vol_s = f"{int(volume)}" if volume is not None else "-"
                if VERBOSE_LOGS: log.info(f"[GATE] {ticker} sp={selected_spread:.3f} vol={_vol_s} yask={yes_ask_f if yes_ask_f is not None else '-'} nask={no_ask_f if no_ask_f is not None else '-'} mpy={mp:.3f} mpn={1-mp:.3f} ey={eff_edge_yes if eff_edge_yes is not None else '-'} en={eff_edge_no if eff_edge_no is not None else '-'} pr={pressure_score:.3f} vel=- elite=- escore=- side={selected_side} block=price({entry_price:.3f})")
                log_skip(ticker, "no_tradeable_side", f"entry_price={entry_price:.3f} side={selected_side} floor={price_floor:.2f}")
                continue
        if selected_side == "yes" and entry_price > 0.20:
            skip_price += 1
            _vol_s = f"{int(volume)}" if volume is not None else "-"
            if VERBOSE_LOGS: log.info(f"[GATE] {ticker} sp={selected_spread:.3f} vol={_vol_s} yask={yes_ask_f if yes_ask_f is not None else '-'} nask={no_ask_f if no_ask_f is not None else '-'} mpy={mp:.3f} mpn={1-mp:.3f} ey={eff_edge_yes if eff_edge_yes is not None else '-'} en={eff_edge_no if eff_edge_no is not None else '-'} pr={pressure_score:.3f} vel=- elite=- escore=- side={selected_side} block=price({entry_price:.3f})")
            log_skip(ticker, "no_tradeable_side", f"entry_price={entry_price:.3f} side=yes cap=0.20")
            continue
        if entry_price > price_cap:
            skip_price += 1
            _vol_s = f"{int(volume)}" if volume is not None else "-"
            if VERBOSE_LOGS: log.info(f"[GATE] {ticker} sp={selected_spread:.3f} vol={_vol_s} yask={yes_ask_f if yes_ask_f is not None else '-'} nask={no_ask_f if no_ask_f is not None else '-'} mpy={mp:.3f} mpn={1-mp:.3f} ey={eff_edge_yes if eff_edge_yes is not None else '-'} en={eff_edge_no if eff_edge_no is not None else '-'} pr={pressure_score:.3f} vel=- elite=- escore=- side={selected_side} block=price({entry_price:.3f})")
            log_skip(ticker, "no_tradeable_side", f"entry_price={entry_price:.3f} side={selected_side} cap={price_cap:.2f}")
            continue
        # T1/T2 tier based on entry price and spread
        # NO side upper bounds extended to match the wider price cap
        tier_name = None
        tier_fallback = False
        tier_size_mult = 1.0
        if selected_spread <= 0.02 and 0.12 <= entry_price <= (0.58 if selected_side == "no" else 0.42):
            tier_name = "T1"
            size = 4
        elif selected_spread <= 0.03 and 0.10 <= entry_price <= (0.60 if selected_side == "no" else 0.45):
            tier_name = "T2"
            size = 2
        elif selected_spread <= adaptive_spread_cap and 0.10 <= entry_price <= (0.60 if selected_side == "no" else 0.45) and quality_score >= 0.20:
            tier_name = "T3"
            size = 1
        elif price_override:
            # Elite edge bypass: assign T3 regardless of price/tier bounds
            tier_name = "T3"
            size = 1
        else:
            tier_name = "STARTER"
            tier_fallback = True
            tier_size_mult = 0.5
            size = 1
            _vol_s = f"{int(volume)}" if volume is not None else "-"
            log.info(
                f"[TIER_FALLBACK] ticker={ticker} reason=no_tier"
                f" spread={selected_spread:.3f} entry_price={entry_price:.3f}"
                f" assigned=STARTER size_mult={tier_size_mult:.2f}"
            )
        if VERBOSE_LOGS:
            log.info(
                f"[TIER] ticker={ticker} assigned={tier_name}"
                f" spread={selected_spread:.3f} entry_price={entry_price:.3f}"
                f" fallback={int(tier_fallback)}"
            )
        if pressure_score < 0.55:
            size = round(size * 0.7, 2)
            if VERBOSE_LOGS:
                log.info(
                    cyan(
                        f"[PRESSURE_SIZE_CUT] ticker={ticker} pressure={pressure_score:.3f}"
                        f" base_size={size / 0.7:.2f} adjusted_size={size:.2f}"
                    )
                )
        if selected_side == "yes":
            ev = mp / entry_price - 1.0 if entry_price > 0 else 0.0
        else:
            ev = (1.0 - mp) / entry_price - 1.0 if entry_price > 0 else 0.0
        edge = true_edge
        accel = pressure_accel(pressure_score, prev_pressure)
        pressure_edge = pressure_score - BASE_PRESSURE
        near_atm = dist <= 0.01
        mid_range = 0.18 <= entry_price <= 0.42
        candidate = {
            "m": m,
            "ticker": ticker,
            "side": selected_side,
            "crowd": crowd,
            "mp": mp,
            "ev": ev,
            "size": 0.0,
            "base_size": size,
            "hours": hours,
            "minutes_to_expiry": minutes_to_expiry,
            "family": family,
            "strike": strike,
            "spot_price": spot_price,
            "close_time": m.get("close_time", ""),
            "tier": 1 if tier_name == "T1" else 2,
            "tier_name": tier_name,
            "tier_fallback": tier_fallback,
            "tier_size_mult": tier_size_mult,
            "pressure_score": pressure_score,
            "yes_pressure_score": yes_pressure_score,
            "no_pressure_score": no_pressure_score,
            "pressure_delta": pressure_delta,
            "pressure_edge": pressure_edge,
            "pressure_accel": accel,
            "velocity": vm["velocity_med"],
            "velocity_short": vm["velocity_short"],
            "acceleration": vm["acceleration"],
            "spread": selected_spread,
            "elite_score": elite_score,
            "elite_tier": "STRONG" if elite_score >= 0.75 else "MEDIUM" if elite_score >= 0.60 else "WEAK",
            "liquidity_score": liq_score,
            "book_depth_score": depth_score,
            "imbalance_score": imbalance_score,
            "entry_score": entry_score,
            "dist_from_spot": dist,
            "near_atm": near_atm,
            "mid_range": mid_range,
            "fragile_book": fragile_book,
            "spread_expanding": spread_expanding,
            "crowd_softening": crowd_softening,
            "edge": edge,            # effective_edge (raw - spread_penalty + quality_boost)
            "raw_edge": raw_edge_sel,
            "net_edge": net_edge,
            "spread_penalty": spread_penalty,
            "quality_boost": quality_boost,
            "relative_edge": relative_edge,
            "quality_score": quality_score,
            "selected_touch": float(selected_touch or 0.0),
            "selected_side": selected_side,
            "gate": candidate_gate,
            "side_valid": bool(selected_side),
            "executable_valid": entry_price is not None,
            "price_override": price_override,
            "hard_elite": False,
        }
        regime = classify_regime(candidate) if USE_REGIME_FILTER else "momentum_clean"
        candidate["regime"] = regime
        elite_force_allowed, elite_force_reason = elite_entry_allowed(
            regime,
            candidate["pressure_score"],
            candidate["spread"],
            candidate["selected_touch"],
            side_valid=bool(candidate["side"]),
            executable_valid=candidate["crowd"] is not None,
        )
        candidate["elite_force_allowed"] = elite_force_allowed
        candidate["elite_force_block_reason"] = elite_force_reason
        candidate["hard_elite"] = edge >= 0.55 and elite_force_allowed
        regime_penalty = -0.05 if regime == "no_trade" else 0.0
        candidate["entry_score"] = round(candidate["entry_score"] + regime_penalty, 4)
        time_stop_secs = MAX_HOLD_SECONDS
        _vol_s = f"{int(volume)}" if volume is not None else "-"
        if size < 1.0:
            original_size = size
            size = max(size, 1)
            if VERBOSE_LOGS:
                log.info(
                    f"[SIZE_ADJUST] ticker={ticker} original={original_size:.2f}"
                    f" adjusted={size:.2f} reason=min_size_floor"
                )
        if VERBOSE_LOGS: log.info(f"[GATE] {ticker} sp={selected_spread:.3f} vol={_vol_s} yask={yes_ask_f if yes_ask_f is not None else '-'} nask={no_ask_f if no_ask_f is not None else '-'} mpy={mp:.3f} mpn={1-mp:.3f} ey={eff_edge_yes if eff_edge_yes is not None else '-'} en={eff_edge_no if eff_edge_no is not None else '-'} pr={pressure_score:.3f} vel={vm['velocity_med']:.3f} elite={elite_score:.3f} escore={entry_score:.3f} side={selected_side} block=PASS")
        candidate["base_size"] = size
        candidate["size"] = size
        # WIDE regime: halve position size to limit exposure in poor book conditions
        if bad_regime:
            candidate["size"] = max(0.5, round(candidate["size"] * 0.5, 2))
        candidate["time_stop_secs"] = time_stop_secs
        elite_scores.append(elite_score)
        pressure_vals.append(pressure_score)
        velocity_vals.append(vm["velocity_med"])
        spread_vals.append(selected_spread)
        ki_pass, ki_reason, ki_diag = killer_instinct_filter(
            candidate, prices, open_rows, now,
            adaptive_touch_req=adaptive_touch_req,
            adaptive_spread_cap=adaptive_spread_cap,
        )
        if not ki_pass:
            killer_blocked += 1
            killer_breakdown[ki_reason] = killer_breakdown.get(ki_reason, 0) + 1
            _ki_ks = ki_diag.get("killer_score", 0.0)
            _ki_th = ki_diag.get("threshold", 0.0)
            log.info(yellow(
                f"[KILLER_BLOCK] ticker={ticker} reason={ki_reason}"
                f" score={_ki_ks:.3f} threshold={_ki_th:.3f}"
                f" lower_bound={_ki_th - 0.03:.3f}"
            ))
            if ticker in held_tickers:
                log.info(f"[HELD SKIP] {ticker} reason=killer score={_ki_ks:.3f}")
            log_skip(ticker, ki_reason)
            skip_entry_score += 1
            continue
        killer_passed += 1
        candidate["killer_score"] = ki_diag.get("killer_score", 0.5)
        candidate["killer_tier"] = ki_diag.get("killer_tier", "KILLER")
        if candidate["killer_tier"] == "STALKER":
            candidate["size"] = max(0.5, round(candidate["size"] * 0.5, 2))
            # base_size intentionally NOT overwritten — compute_trade_size needs the
            # original value (set at candidate construction) to size correctly against
            # capital limits. STALKER halving is re-applied after compute_trade_size.
            log.info(yellow(f"[STALKER] ticker={ticker} size reduced to {candidate['size']} killer_score={candidate['killer_score']:.3f}"))
        elif candidate["killer_tier"] == "NEAR_STALKER":
            candidate["size"] = max(0.5, round(candidate["size"] * 0.5, 2))
            # base_size intentionally NOT overwritten — same reasoning as STALKER above.
            log.info(yellow(
                f"[KILLER_SOFT] ticker={ticker} score={candidate['killer_score']:.3f}"
                f" threshold={ki_diag.get('threshold', 0.0):.3f}"
                f" size_reduced={candidate['size']:.2f}"
            ))
        candidates.append(candidate)
        actionable_count += 1

    if VERBOSE_LOGS: log.info(cyan(f"KILLER_SUMMARY | passed={killer_passed} blocked={killer_blocked} breakdown={killer_breakdown}"))
    for c in candidates:
        base_priority = execution_priority_score(c)
        c["execution_priority"] = round(base_priority * 0.85 + c.get("killer_score", 0.5) * 0.15, 4)
    ranked_candidates = sorted(
        candidates,
        key=lambda c: (c["execution_priority"], c["edge"], c["quality_score"], -c["spread"]),  # edge = effective_edge
        reverse=True
    )
    ranked_candidates = diversify_ranked_candidates(ranked_candidates, open_rows)
    if VERBOSE_LOGS:
        for c in ranked_candidates[: min(5, len(ranked_candidates))]:
            log.info(cyan(
                f"[RANK] ticker={c['ticker']} score={c['execution_priority']:.4f}"
                f" edge={c['edge']:.3f} quality={c['quality_score']:.3f}"
                f" spread={c['spread']:.3f} touch={c['selected_touch']:.0f}"
                f" minutes={int(c['minutes_to_expiry'])}"
            ))
    cluster_counts = dict(_series_open_count)
    skip_cluster = skip_reentry_blocked = 0
    tier1_signals = tier2_signals = 0
    EXPOSURE_CAP = 0.4 * BANKROLL
    open_trades_index = {}
    open_tickers = set()
    for row in open_rows:
        try:
            open_tickers.add(row["ticker"])
            open_trades_index[(row["ticker"], row["side"])] = {
                "ev": float(row.get("ev", 0) or 0),
                "crowd": float(row.get("crowd_prob", 0) or 0),
                "entry_price": float(row.get("crowd_prob", 0) or 0),
                "tier": int(row.get("tier", 1) or 1),
                "elite_score": float(_entry_metrics.get((row["ticker"], row["side"]), {}).get("elite_score", 0.0) or 0.0),
                "pressure_score": float(_entry_metrics.get((row["ticker"], row["side"]), {}).get("pressure_score", 0.0) or 0.0),
            }
        except Exception:
            pass

    skip_counts = dict(_last_skip_reason_counts)
    dead_ratio = skip_counts.get("dead_book", 0) / max(1, usable_markets)
    if dead_ratio > 0.75:
        max_new_trades_this_cycle = 1
        min_edge = 0.015
    else:
        max_new_trades_this_cycle = CHF_MAX_NEW_TRADES_PER_CYCLE
        min_edge = CHF_MIN_EDGE

    log.info("--- SIGNALS ---")
    attempts = 0
    buys = 0
    rotations = 0
    rotation_checked = 0
    rotation_approved = 0
    rotation_blocked_churn = 0
    rotation_blocked_budget = 0
    rotation_blocked_conviction = 0
    rotation_blocked_lock = 0
    rotation_blocked_downgrade = 0
    rotation_blocked_confidence = 0
    rotation_winner_protected = 0
    new_trades_this_cycle = 0
    t3_this_cycle = 0
    blocked_capital = 0
    blocked_open_cap = 0
    skipped_tickers = set()
    cash_balance = current_cash_balance(total_open_exposure)
    allocator_portfolio_state = build_allocator_portfolio_state(open_rows, prices)
    global_regime = compute_global_regime(cycle_regime, candidates, open_rows, prices)
    regime_action = global_regime["action"]
    cycle_cap_limit = cash_balance * MAX_CAPITAL_PER_CYCLE_PCT * regime_action["cycle_cap_mult"]
    cycle_cap_used = 0.0
    effective_min_alloc_score = regime_action["min_alloc_score"]
    effective_kelly_max_mult = regime_action["kelly_max_mult"]
    btc_open = sum(1 for row in open_rows if row.get("family") == "KXBTC")
    eth_open = sum(1 for row in open_rows if row.get("family") == "KXETH")
    log.info(cyan(
        f"PORTFOLIO | bankroll={BANKROLL:.2f} cash_balance={cash_balance:.2f}"
        f" open_exposure={total_open_exposure:.2f} open_positions={len(open_rows)}"
        f" cycle_cap_remaining={cycle_cap_limit:.2f} btc_open={btc_open} eth_open={eth_open}"
    ))
    log_portfolio_intel_summary(open_rows, prices, now)
    log_capital_state("CAPITAL", total_open_exposure, cycle_cap_limit, cycle_cap_used, cash_balance * MAX_CAPITAL_PER_TRADE_PCT)
    if cash_balance <= 0:
        log.warning("No capital available - skipping trades")
        ranked_candidates = []
    else:
        for c in ranked_candidates:
            c["rotation_override"] = bool(c.get("rotation_override"))
            allocation_score, conviction_state = compute_allocation_score(c, allocator_portfolio_state, ticker=c.get("ticker", ""))
            c["allocation_score"] = allocation_score
            c["allocation_conviction_state"] = conviction_state
            c["allocation_size_mult"] = allocation_size_multiplier(allocation_score)
        ranked_candidates.sort(key=lambda c: (c.get("allocation_score", float("-inf")), c.get("execution_priority", 0.0)), reverse=True)
    for c in ranked_candidates:
        if max(0.0, cycle_cap_limit - cycle_cap_used) <= 0:
            break
        if c.get("allocation_score", float("-inf")) < effective_min_alloc_score:
            continue
        ts_key = (c["ticker"], c["side"])
        if c["ticker"] in open_tickers:
            if not regime_action["allow_reinforce"]:
                log.info(yellow(f"[REINFORCE_BLOCKED] ticker={c['ticker']} reason=global_regime_{global_regime['regime'].lower()}"))
                if _dashboard is not None:
                    _dashboard.add_event("REINFORCE_BLOCKED", c["ticker"], f"regime={global_regime['regime']}")
                skipped_tickers.add(c["ticker"])
                continue
            existing_row = next((r for r in open_rows if r.get("ticker") == c["ticker"] and r.get("side") == c["side"]), None)
            existing_entry_ts = parse_utc_timestamp(existing_row.get("timestamp")) if existing_row else None
            existing_held_secs = (now - existing_entry_ts).total_seconds() if existing_entry_ts else 0.0
            existing_entry_meta = _entry_metrics.get(ts_key, {})
            reinforce_intel = compute_position_intel(existing_row or {"ticker": c["ticker"], "crowd_prob": c["crowd"], "model_prob": c["mp"], "hours_to_close": c["hours"]}, c["side"], prices, existing_entry_meta)
            existing_entry_price = safe_float(existing_row.get("crowd_prob"), c["crowd"]) if existing_row else c["crowd"]
            existing_exit_price = reinforce_intel.get("current_exit_price", existing_entry_price)
            existing_pnl_pct = existing_exit_price - existing_entry_price
            entry_killer_score = safe_float(existing_entry_meta.get("entry_killer_score"), existing_entry_meta.get("killer_score", 0.0)) or 0.0
            entry_spread = safe_float(existing_entry_meta.get("entry_spread"), c["spread"]) or c["spread"]
            reinforce_ok = (
                reinforce_intel["conviction_state"] == "elite"
                and existing_pnl_pct >= 0.0
                and reinforce_intel["current_killer_score"] >= entry_killer_score - 0.03
                and reinforce_intel["current_spread"] <= max(0.05, entry_spread * 1.5)
                and _reinforce_count_by_ticker.get(c["ticker"], 0) < MAX_REINFORCE_PER_TICKER
                and existing_held_secs >= REINFORCE_MIN_HOLD_SECS
            )
            if existing_pnl_pct <= 0.0 and reinforce_intel["conviction_delta_state"] != "improving":
                reinforce_ok = False
            log.info(
                f"[REINFORCE_CHECK] ticker={c['ticker']} pnl={existing_pnl_pct:+.4f}"
                f" conviction={reinforce_intel['conviction_state']}"
                f" killer_now={reinforce_intel['current_killer_score']:.3f}"
                f" killer_entry={entry_killer_score:.3f}"
                f" spread_now={reinforce_intel['current_spread']:.3f}"
                f" spread_entry={entry_spread:.3f} allow={1 if reinforce_ok else 0}"
            )
            if not reinforce_ok:
                reinforce_block_reason = None
                if reinforce_intel["conviction_state"] != "elite":
                    reinforce_block_reason = "conviction"
                elif existing_pnl_pct <= 0.0 and reinforce_intel["conviction_delta_state"] != "improving":
                    reinforce_block_reason = "pnl_not_improving"
                elif existing_pnl_pct < 0.0:
                    reinforce_block_reason = "pnl_negative"
                elif reinforce_intel["current_killer_score"] < entry_killer_score - 0.03:
                    reinforce_block_reason = "killer_decay"
                elif reinforce_intel["current_spread"] > max(0.05, entry_spread * 1.5):
                    reinforce_block_reason = "spread_drift"
                elif _reinforce_count_by_ticker.get(c["ticker"], 0) >= MAX_REINFORCE_PER_TICKER:
                    reinforce_block_reason = "cap"
                elif existing_held_secs < REINFORCE_MIN_HOLD_SECS:
                    reinforce_block_reason = "min_hold"
                if reinforce_block_reason:
                    log.info(f"[REINFORCE_BLOCKED] ticker={c['ticker']} reason={reinforce_block_reason}")
                    if _dashboard is not None:
                        _dashboard.add_event("REINFORCE_BLOCKED", c["ticker"], f"reason={reinforce_block_reason}")
            if reinforce_ok:
                _cash = current_cash_balance(total_open_exposure)
                _cycle_cap_remaining = max(0.0, cycle_cap_limit - cycle_cap_used)
                reinforce_size = round(min(c.get("base_size", c["size"]) * REINFORCE_SIZE_MULT, _cycle_cap_remaining), 2)
                if reinforce_size >= 0.5:
                    _old_usd = 0.0
                    for _row in open_rows:
                        if _row.get("ticker") == c["ticker"] and _row.get("side") == c["side"]:
                            _old_usd = safe_float(_row.get("position_usd"), 0.0)
                            _row["position_usd"] = round(_old_usd + reinforce_size, 2)
                            break
                    _new_usd = round(_old_usd + reinforce_size, 2)
                    _next_count = _reinforce_count_by_ticker.get(c["ticker"], 0) + 1
                    log.info(magenta(
                        f"[REINFORCE] ticker={c['ticker']} added={reinforce_size}"
                        f" old_position_usd={_old_usd} new_position_usd={_new_usd}"
                        f" count={_next_count}"
                    ))
                    save_positions(open_rows)
                    total_open_exposure = round(total_open_exposure + reinforce_size, 4)
                    cycle_cap_used += reinforce_size
                    _reinforce_count_by_ticker[c["ticker"]] = _next_count
                    persist_runtime_state()
                    buys += 1
                    continue
                log.info(yellow(f"[REINFORCE BLOCKED] ticker={c['ticker']} reason=insufficient_cap size={reinforce_size:.2f}"))
            else:
                log.info(f"[ENTRY BLOCKED] already holding {c['ticker']}")
            skipped_tickers.add(c["ticker"])
            continue
        series = ticker_series(c["ticker"])
        if not regime_action["allow_new_entries"]:
            _mid_tier_global = c["edge"] >= 0.60 and c.get("pressure_score", 0.0) >= 0.45
            if not _mid_tier_global:
                log.info(yellow(f"[BLOCKED] ticker={c['ticker']} reason=global_regime_{global_regime['regime'].lower()}"))
                skipped_tickers.add(c["ticker"])
                continue
            # High-edge mid-tier allowed even in global NO_TRADE, at 40% size
            c["size"] = max(0.25, round(c["size"] * 0.4, 2))
            log.info(cyan(f"[MT_OVERRIDE] {c['ticker']} edge={c['edge']:.3f} pressure={c['pressure_score']:.3f} bypass global NO_TRADE → size={c['size']}"))
        open_families = {row.get("family", "") for row in open_rows}
        all_same_family = len(open_families) == 1 and len(open_rows) > 0
        diversify_force = (
            all_same_family
            and c["family"] not in open_families
            and c["edge"] >= 0.20
            and c["quality_score"] >= 0.80
        )
        high_edge_override = c["edge"] >= 0.25 and c["quality_score"] >= 0.60
        hard_elite_raw = c.get("hard_elite", False) or c["edge"] >= 0.55
        hard_elite = hard_elite_raw and c.get("elite_force_allowed", True)
        force_rotation_eval = len(open_rows) >= MAX_OPEN_POSITIONS and c["edge"] >= 0.40
        rotation_candidate = None
        if hard_elite_raw and not hard_elite:
            log.info(yellow(
                f"[ELITE_ENTRY_BLOCKED] ticker={c['ticker']}"
                f" reason={c.get('elite_force_block_reason', 'elite_guard')}"
                f" regime={c.get('regime', '-')}"
                f" pressure={c['pressure_score']:.3f}"
                f" spread={c['spread']:.3f} touch={c['selected_touch']:.0f}"
            ))
        if len(open_rows) >= MAX_OPEN_POSITIONS:
            if not regime_action["allow_rotation"] or not ROTATION_ENABLED:
                reason = "rotation_disabled" if ROTATION_ENABLED is False and regime_action["allow_rotation"] else f"global_regime_{global_regime['regime'].lower()}"
                log.info(yellow(f"[BLOCKED] ticker={c['ticker']} reason={reason}"))
                skipped_tickers.add(c["ticker"])
                continue
            rotation_checked += 1
            if hard_elite:
                log.info(magenta(f"[ELITE_ENTRY] forcing execution ticker={c['ticker']}"))
            if diversify_force:
                log.info(magenta(f"[DIVERSIFY_FORCE] ticker={c['ticker']} family={c['family']} edge={c['edge']:.3f} quality={c['quality_score']:.3f}"))
            log.info(magenta(
                f"[ROTATION_CANDIDATE] new={c['ticker']} priority={c['execution_priority']:.4f}"
                f" edge={c['edge']:.3f} quality={c['quality_score']:.3f} spread={c['spread']:.3f}"
            ))
            victim = select_rotation_candidate(
                open_rows, prices, now,
                incoming_family=c["family"],
                cluster_counts=cluster_counts,
                force_cross_family=(diversify_force or hard_elite),
            )
            if victim is None:
                blocked_open_cap += 1
                log.info(yellow(f"[BLOCKED] ticker={c['ticker']} reason=open_cap current_open={len(open_rows)}"))
                if force_rotation_eval:
                    log.info(yellow(f"[ROTATION_MISSED] ticker={c['ticker']} edge={c['edge']:.3f} reason=no_victim"))
                skipped_tickers.add(c["ticker"])
                continue
            victim_row, victim_score, victim_edge, victim_pnl_pct, _victim_exit_price, victim_held_secs, victim_pressure, victim_spread, winner_protected = victim
            victim_quality = position_entry_quality(victim_row)
            victim_side = victim_row.get("side", "")
            victim_entry_meta = _entry_metrics.get((victim_row.get("ticker", ""), victim_side), {})
            victim_intel = compute_position_intel(victim_row, victim_side, prices, victim_entry_meta)
            rotation_score, victim_quality, time_decay_bonus, spread_penalty_score, diversification_boost = compute_rotation_score(c, victim, open_rows)
            upgrade_ok, upgrade_reason, upgrade_alpha = rotation_upgrade_allowed(victim, c)
            rotation_regime = "thin" if adaptive_touch_req >= TOUCH_REQ_WIDE or adaptive_spread_cap >= max(0.07, HARD_SPREAD_CEIL * 0.95) else c.get("regime")
            age_minutes = max(0.0, victim_held_secs / 60.0)
            base_threshold = 0.02 if rotation_regime == "thin" else 0.05
            # Fresh positions are protected; older positions become easier to replace.
            age_penalty = min(0.04, age_minutes * 0.002)   # caps at 0.04 after 20 min
            rotation_threshold = max(0.01, base_threshold - age_penalty)
            forced_upgrade = rotation_score >= 0.10 or upgrade_ok
            elite_force = hard_elite
            elite_override = c["edge"] >= 0.40
            rotation_pressure_override = (
                c["edge"] > victim_intel["current_edge"] + 0.05
                and (c.get("killer_score", 0.0) or 0.0) > victim_intel["current_killer_score"]
            )
            material_rotation_override = upgrade_ok
            force_rotation = hard_elite or diversify_force or rotation_pressure_override or material_rotation_override
            if winner_protected:
                rotation_winner_protected += 1
            log.info(magenta(
                f"[ROTATION_VICTIM] old={victim_row.get('ticker','')} retain={victim_score:.4f}"
                f" pnl={victim_pnl_pct:.4f} held={int(victim_held_secs)} pressure={victim_pressure:.3f} spread={victim_spread:.3f}"
            ))
            log.info(magenta(
                f"[ROTATION_SCORE] old={victim_row.get('ticker','')} new={c['ticker']} score={rotation_score:.4f}"
                f" diversification_bonus={diversification_boost:.4f} time_decay_bonus={time_decay_bonus:.4f}"
                f" spread_penalty={spread_penalty_score:.4f}"
            ))
            log.info(magenta(
                f"[ROTATION_UPGRADE] old={victim_row.get('ticker','')} new={c['ticker']}"
                f" allow={1 if upgrade_ok else 0} reason={upgrade_reason} alpha={upgrade_alpha:.4f}"
            ))
            if upgrade_reason == "rotation_suppressed_long_horizon":
                log.info(yellow(
                    f"[ROTATION_BLOCKED] incumbent={victim_row.get('ticker','')}"
                    f" candidate={c['ticker']} reason=rotation_suppressed_long_horizon"
                ))
                skipped_tickers.add(c["ticker"])
                continue
            log.info(magenta(
                f"[ROTATION_CHECK] incumbent={victim_row.get('ticker','')} candidate={c['ticker']}"
                f" inc_edge={victim_edge:.3f} cand_edge={c['edge']:.3f}"
                f" inc_pnl={victim_pnl_pct:.4f} inc_held={int(victim_held_secs)}"
                f" alpha={upgrade_alpha:.4f} allowed={'True' if upgrade_ok else 'False'}"
                f" reason={upgrade_reason}"
            ))
            if c["edge"] < victim_edge and c["quality_score"] < victim_quality and not force_rotation:
                rotation_blocked_downgrade += 1
                blocked_open_cap += 1
                log.info(yellow(
                    f"[ROTATION_BLOCKED] incumbent={victim_row.get('ticker','')}"
                    f" candidate={c['ticker']} reason=no_downgrade"
                ))
                log.info(yellow(f"[BLOCKED] ticker={c['ticker']} reason=no_downgrade"))
                if force_rotation_eval:
                    log.info(yellow(f"[ROTATION_MISSED] ticker={c['ticker']} edge={c['edge']:.3f} reason=no_downgrade"))
                skipped_tickers.add(c["ticker"])
                continue
            if victim_edge >= 0.20 and victim_quality >= 0.55 and victim_held_secs < 600 and not force_rotation:
                rotation_blocked_conviction += 1
                blocked_open_cap += 1
                log.info(yellow(
                    f"[ROTATION_BLOCKED] incumbent={victim_row.get('ticker','')}"
                    f" candidate={c['ticker']} reason=conviction_hold"
                ))
                log.info(yellow(
                    f"[BLOCKED] reason=conviction_hold old={victim_row.get('ticker','')}"
                    f" edge={victim_edge:.3f} quality={victim_quality:.3f} held={int(victim_held_secs)}"
                ))
                if force_rotation_eval:
                    log.info(yellow(f"[ROTATION_MISSED] ticker={c['ticker']} edge={c['edge']:.3f} reason=conviction_hold"))
                skipped_tickers.add(c["ticker"])
                continue
            if position_expiry_progress(victim_row, now) >= 0.80 and victim_pnl_pct > 0 and victim_pressure >= 0.40 and not force_rotation:
                rotation_blocked_lock += 1
                blocked_open_cap += 1
                log.info(yellow(
                    f"[ROTATION_BLOCKED] incumbent={victim_row.get('ticker','')}"
                    f" candidate={c['ticker']} reason=late_cycle_lock"
                ))
                log.info(yellow(f"[BLOCKED] reason=late_cycle_lock ticker={victim_row.get('ticker','')}"))
                if force_rotation_eval:
                    log.info(yellow(f"[ROTATION_MISSED] ticker={c['ticker']} edge={c['edge']:.3f} reason=late_cycle_lock"))
                skipped_tickers.add(c["ticker"])
                continue
            if c["spread"] > adaptive_spread_cap or c["selected_touch"] < adaptive_touch_req:
                if not force_rotation:
                    rotation_blocked_confidence += 1
                    blocked_open_cap += 1
                    log.info(yellow(
                        f"[ROTATION_BLOCKED] incumbent={victim_row.get('ticker','')}"
                        f" candidate={c['ticker']} reason=rotation_confidence"
                    ))
                    log.info(yellow(
                        f"[BLOCKED] ticker={c['ticker']} reason=rotation_confidence"
                        f" spread={c['spread']:.3f} cap={adaptive_spread_cap:.3f}"
                        f" touch={c['selected_touch']:.0f} req={adaptive_touch_req:.0f}"
                    ))
                    if force_rotation_eval:
                        log.info(yellow(f"[ROTATION_MISSED] ticker={c['ticker']} edge={c['edge']:.3f} reason=rotation_confidence"))
                    skipped_tickers.add(c["ticker"])
                    continue
            if victim_held_secs < 180 and victim_pnl_pct > -0.01 and rotation_score < 0.15 and not force_rotation:
                rotation_blocked_churn += 1
                blocked_open_cap += 1
                log.info(yellow(
                    f"[BLOCKED] ticker={c['ticker']} reason=rotation_churn_guard old={victim_row.get('ticker','')}"
                    f" held={int(victim_held_secs)} pnl={victim_pnl_pct:.4f} advantage={rotation_score:.4f}"
                ))
                if force_rotation_eval:
                    log.info(yellow(f"[ROTATION_MISSED] ticker={c['ticker']} edge={c['edge']:.3f} reason=rotation_churn_guard"))
                skipped_tickers.add(c["ticker"])
                continue
            if rotations >= MAX_ROTATIONS_PER_CYCLE and not forced_upgrade and not force_rotation:
                rotation_blocked_budget += 1
                blocked_open_cap += 1
                log.info(yellow(
                    f"[BLOCKED] ticker={c['ticker']} reason=rotation_budget"
                    f" advantage={rotation_score:.4f} rotations_this_cycle={rotations}"
                ))
                if force_rotation_eval:
                    log.info(yellow(f"[ROTATION_MISSED] ticker={c['ticker']} edge={c['edge']:.3f} reason=rotation_budget"))
                skipped_tickers.add(c["ticker"])
                continue
            if rotation_score < rotation_threshold and not force_rotation:
                blocked_open_cap += 1
                log.info(yellow(
                    f"[BLOCKED] ticker={c['ticker']} reason=open_cap"
                    f" current_open={len(open_rows)} worst_ticker={victim_row.get('ticker','')}"
                    f" retain={victim_score:.4f} rotation_score={rotation_score:.4f}"
                ))
                if force_rotation_eval:
                    log.info(yellow(f"[ROTATION_MISSED] ticker={c['ticker']} edge={c['edge']:.3f} reason=rotation_threshold"))
                skipped_tickers.add(c["ticker"])
                continue
            rotation_candidate = victim
            rotation_approved += 1
            c["rotation_override"] = True
            rotation_reason = classify_rotation_reason(c, victim_row, victim_score, victim_edge, victim_quality, diversification_boost)
            log.info(magenta(
                f"[ROTATION_OVERRIDE] ticker={c['ticker']} reason={rotation_reason} bypassing cycle_cap"
            ))
        elif hard_elite:
            log.info(magenta(f"[ELITE_ENTRY] forcing execution ticker={c['ticker']}"))
        if rotation_candidate is None and new_trades_this_cycle >= max_new_trades_this_cycle:
            skip_cycle_cap += 1
            log_skip(c["ticker"], "cycle_cap", f"max_new={max_new_trades_this_cycle}")
            skipped_tickers.add(c["ticker"])
            continue
        if rotation_candidate is None and c.get("tier_name") == "T3" and t3_this_cycle >= MAX_T3_PER_CYCLE:
            skip_cycle_cap += 1
            log_skip(c["ticker"], "t3_cap", f"max_t3={MAX_T3_PER_CYCLE}")
            skipped_tickers.add(c["ticker"])
            continue
        if c["quality_score"] < 0.40 and c["edge"] < 0.18:
            skip_entry_score += 1
            log.info(yellow(f"[BLOCKED] ticker={c['ticker']} reason=weak_combo edge={c['edge']:.3f} quality={c['quality_score']:.3f}"))
            skipped_tickers.add(c["ticker"])
            continue
        key = c["family"]
        current_cluster_count = cluster_counts.get(key, 0)
        # If this is a rotation, the victim is being removed — adjust the effective
        # count to reflect the post-rotation portfolio so we don't block a same-family
        # replacement that leaves net concentration unchanged.
        if rotation_candidate is not None:
            victim_family = victim_row.get("family", "")
            if victim_family == key:
                current_cluster_count = max(0, current_cluster_count - 1)
        # Hard cap — never overridden regardless of edge or quality.
        # Net count must not exceed the absolute ceiling even after rotation credit.
        if current_cluster_count >= MAX_CLUSTER_HARD_CAP and not diversify_force and not hard_elite:
            skip_cluster += 1
            log.info(yellow(f"[BLOCKED] ticker={c['ticker']} reason=cluster_hard_cap current={current_cluster_count} cap={MAX_CLUSTER_HARD_CAP}"))
            log_skip(c["ticker"], "cluster_hard_cap", f"current={current_cluster_count} cap={MAX_CLUSTER_HARD_CAP}")
            skipped_tickers.add(c["ticker"])
            continue
        # Soft cap — elite trades may bypass with sufficient edge + quality.
        if current_cluster_count >= MAX_POSITIONS_PER_CLUSTER:
            if not high_edge_override and not diversify_force and not hard_elite:
                skip_cluster += 1
                log.info(yellow(f"[BLOCKED] ticker={c['ticker']} reason=cluster_exposure current={current_cluster_count} cap={MAX_POSITIONS_PER_CLUSTER}"))
                log_skip(c["ticker"], "cluster_exposure", f"current={current_cluster_count} cap={MAX_POSITIONS_PER_CLUSTER}")
                skipped_tickers.add(c["ticker"])
                continue
            else:
                log.info(cyan(f"[OVERRIDE] cluster bypass | ticker={c['ticker']} edge={c['edge']:.3f} quality={c['quality_score']:.3f} current={current_cluster_count}"))
        ts_key = (c["ticker"], c["side"])
        if not OFFLINE_LIFECYCLE_TEST and c["ticker"] in _entry_cooldown_by_ticker and now < _entry_cooldown_by_ticker[c["ticker"]]:
            skip_cooldown += 1
            log_skip(c["ticker"], "cooldown")
            skipped_tickers.add(c["ticker"])
            continue
        recent_loss_ts = recent_losses.get(c["ticker"])
        time_since_exit = None
        last_trade_ts = _last_trade_ts_by_ticker.get(c["ticker"])
        if last_trade_ts:
            time_since_exit = (now - last_trade_ts).total_seconds()
        last_exit_price = _last_exit_price_by_ticker.get(c["ticker"])
        last_exit_meta = _last_exit_meta_by_ticker.get(c["ticker"])
        price_changed = last_exit_price is not None and abs(c["crowd"] - last_exit_price) >= 0.01
        reentry_ok = c["spread"] <= 0.03 and price_changed
        if not OFFLINE_LIFECYCLE_TEST and last_exit_meta and (now - last_exit_meta["ts"]).total_seconds() < 120:
            prev_edge = safe_float(last_exit_meta.get("edge"), 0.0) or 0.0
            if c["edge"] < prev_edge + 0.08:
                skip_reentry_blocked += 1
                log.info(yellow(f"[BLOCKED] ticker={c['ticker']} reason=fresh_reentry_cooldown prev_edge={prev_edge:.3f} new_edge={c['edge']:.3f}"))
                skipped_tickers.add(c["ticker"])
                continue
        if not OFFLINE_LIFECYCLE_TEST and recent_loss_ts and time_since_exit is not None and time_since_exit < 15:
            skip_reentry_blocked += 1
            log_skip(c["ticker"], "cooldown_short")
            skipped_tickers.add(c["ticker"])
            continue
        if not OFFLINE_LIFECYCLE_TEST and recent_loss_ts and not reentry_ok:
            skip_reentry_blocked += 1
            log_skip(c["ticker"], "reentry_blk", f"spread={c['spread']:.3f} price_changed={1 if price_changed else 0}")
            skipped_tickers.add(c["ticker"])
            continue
        if _recent_rejects_by_ticker.get(c["ticker"], 0) >= 3:
            skip_weak_book += 1
            log_skip(c["ticker"], "reject_limit")
            skipped_tickers.add(c["ticker"])
            continue
        if c["ticker"] in open_tickers and ts_key not in open_trades_index:
            skip_duplicate_ticker += 1
            skipped_tickers.add(c["ticker"])
            continue
        if ts_key in open_trades_index:
            prev = open_trades_index[ts_key]
            elite_ok = c["elite_score"] >= prev.get("elite_score", 0.0) + 0.05
            pr_ok = c["pressure_score"] >= prev.get("pressure_score", 0.0) + 0.08
            mom_ok = c["crowd"] >= prev["crowd"] + 0.015
            if not (elite_ok or pr_ok or mom_ok):
                skip_reentry_blocked += 1
                skipped_tickers.add(c["ticker"])
                continue
        cash_balance = current_cash_balance(total_open_exposure)
        trade_cap = cash_balance * MAX_CAPITAL_PER_TRADE_PCT
        cycle_cap_remaining = max(0.0, cycle_cap_limit - cycle_cap_used)
        log_capital_state("CAPITAL", total_open_exposure, cycle_cap_limit, cycle_cap_used, trade_cap)
        c["size"] = compute_trade_size(
            c,
            trade_cap,
            cycle_cap_remaining,
            cash_balance,
            allocator_portfolio_state,
            ticker=c["ticker"],
            kelly_max_mult=effective_kelly_max_mult,
        )
        # Re-apply STALKER 50% reduction AFTER compute_trade_size so capital limits
        # are evaluated against the full base, then the final position is halved.
        if c.get("killer_tier") == "STALKER" and c["size"] >= 1.0:
            c["size"] = max(0.5, round(c["size"] * STALKER_SIZE_MULT, 2))
        if c.get("tier_fallback"):
            c["size"] = max(0.5, round(c["size"] * c.get("tier_size_mult", 0.5), 2))
        _size_floor = 0.5 if c.get("killer_tier") == "STALKER" or c.get("tier_fallback") else 1.0
        if c["size"] < _size_floor:
            c["size"] = max(0.50, trade_cap)
            log.info(
                f"[SIZE_CLAMP] ticker={c['ticker']} size clamped to {c['size']:.2f}"
                f" trade_cap={trade_cap:.2f} cycle_cap={cycle_cap_remaining:.2f}"
            )
        if VERBOSE_LOGS:
            log.info(cyan(
                f"[SIZE] ticker={c['ticker']} tier={c['tier_name']} base={c['base_size']:.1f}"
                f" final={c['size']:.1f} edge={c['edge']:.3f} quality={c['quality_score']:.3f}"
                f" spread={c['spread']:.3f} touch={c['selected_touch']:.0f}"
            ))
        cur_exposure = total_open_exposure + c["size"]
        if cur_exposure >= EXPOSURE_CAP:
            skip_exposure += 1
            log.info(yellow(f"[BLOCKED] ticker={c['ticker']} reason=exposure_cap current_exposure={total_open_exposure:.2f} cap={EXPOSURE_CAP:.2f}"))
            skipped_tickers.add(c["ticker"])
            continue
        fam_exp = defaultdict(float)
        short_exp = 0.0
        long_exp = 0.0
        for row in open_rows:
            pos = safe_float(row.get("position_usd"), 0.0) or 0.0
            fam_exp[row.get("family", "OTHER")] += pos
            h = safe_float(row.get("hours_to_close"), 0.0) or 0.0
            if h <= 1:
                short_exp += pos
            else:
                long_exp += pos
        if fam_exp.get(c["family"], 0.0) + c["size"] > CAP_FAMILY_PCT * BANKROLL:
            skip_family_exposure += 1
            log.info(yellow(f"[BLOCKED] ticker={c['ticker']} reason=family_exposure family={c['family']} family_exposure={fam_exp.get(c['family'], 0.0):.2f}"))
            skipped_tickers.add(c["ticker"])
            continue
        if c["hours"] <= 1 and short_exp + c["size"] > CAP_SHORT_PCT * BANKROLL:
            skip_bucket_exposure += 1
            log.info(yellow(f"[BLOCKED] ticker={c['ticker']} reason=short_bucket_exposure short_exposure={short_exp:.2f}"))
            skipped_tickers.add(c["ticker"])
            continue
        if c["hours"] > 1 and long_exp + c["size"] > CAP_LONG_PCT * BANKROLL:
            skip_bucket_exposure += 1
            log.info(yellow(f"[BLOCKED] ticker={c['ticker']} reason=long_bucket_exposure long_exposure={long_exp:.2f}"))
            skipped_tickers.add(c["ticker"])
            continue
        if c["edge"] < MIN_EDGE:
            skip_edge += 1
            log_skip(c["ticker"], "edge_gate", f"edge={c['edge']:.3f}")
            skipped_tickers.add(c["ticker"])
            continue
        attempts += 1
        if rotation_candidate is not None:
            victim_row, _victim_score, _victim_edge, _victim_pnl_pct, _victim_exit_price, _victim_held_secs, _victim_pressure, _victim_spread, _winner_protected = rotation_candidate
            old_open_positions = len(open_rows)
            pre_rotation_exposure = total_open_exposure
            replaced_size = safe_float(victim_row.get("position_usd"), 0.0) or 0.0
            open_rows, _rotation_pnl, rotated_out_edge, rotated_out_pnl, rotated_out_quality = rotate_open_position(open_rows, victim_row, c, prices, now)
            rotations += 1
            total_open_exposure = round(sum(safe_float(r.get("position_usd"), 0.0) or 0.0 for r in open_rows), 4)
            open_tickers = {row.get("ticker") for row in open_rows}
            open_trades_index.pop((victim_row.get("ticker"), victim_row.get("side")), None)
            _series_open_count.clear()
            for row in open_rows:
                fam = row.get("family", "")
                _series_open_count[fam] = _series_open_count.get(fam, 0) + 1
            cluster_counts = dict(_series_open_count)
            log.info(magenta(
                f"CAPITAL_UPDATE | event=rotation cash_balance={current_cash_balance(total_open_exposure):.2f}"
                f" open_exposure={total_open_exposure:.2f} realized_cash_pnl={fmt_money(_realized_cash_pnl)}"
            ))
            _rotation_book[(c["ticker"], c["side"])] = {
                "old_ticker": victim_row.get("ticker", ""),
                "old_side": victim_row.get("side", ""),
                "old_edge": rotated_out_edge,
                "old_quality": rotated_out_quality,
                "old_pnl": _rotation_pnl,
                "old_pnl_pct": rotated_out_pnl,
                "new_ticker": c["ticker"],
                "new_side": c["side"],
                "new_edge": c["edge"],
                "new_quality": c["quality_score"],
                "ts": now,
            }
            log.info(magenta(
                f"[ROTATION_IN] ticker={c['ticker']} replaced={victim_row.get('ticker','')}"
                f" edge={c['edge']:.3f} quality={c['quality_score']:.3f}"
                f" pressure={c['pressure_score']:.3f}"
            ))
            if _dashboard is not None:
                _dashboard.add_event("ROTATION", c["ticker"],
                                     f"replaced={victim_row.get('ticker','')} edge={c['edge']:.3f}")
            persist_runtime_state()
        cluster_counts[key] = cluster_counts.get(key, 0) + 1
        final_entry_ok, final_entry_reason, _entry_size_mult = final_entry_allowed(
            c, c.get("regime", "no_trade"), global_regime.get("regime", "")
        )
        log.info(
            f"[ENTRY_GATE] ticker={c['ticker']} regime={c.get('regime', '-')}"
            f" pressure={c['pressure_score']:.3f}"
            f" spread={c['spread'] if c.get('spread') is not None else '-'}"
            f" touch={c.get('selected_touch', 0.0):.0f}"
            f" allowed={'True' if final_entry_ok else 'False'}"
            f" reason={final_entry_reason}"
            + (f" size_mult={_entry_size_mult:.2f}" if _entry_size_mult < 1.0 else "")
        )
        if not final_entry_ok:
            if final_entry_reason == "attack_mid_tier_weak":
                log.info(yellow(
                    f"[MID_TIER_BLOCK] ticker={c['ticker']} reason=insufficient_pressure_or_edge"
                    f" pressure={c['pressure_score']:.3f} edge={c['edge']:.3f}"
                    f" spread={c.get('spread', '?')}"
                ))
            elif final_entry_reason == "dead_zone":
                log.info(yellow(
                    f"[ENTRY_BLOCK] ticker={c['ticker']} reason=dead_zone"
                    f" pressure={c['pressure_score']:.3f}"
                    f" adj_edge={c['edge']:.3f}"
                    f" spread={c['spread'] if c.get('spread') is not None else '-'}"
                    f" touch={c.get('selected_touch', 0.0):.0f}"
                ))
            elif final_entry_reason == "edge_too_low":
                log.info(yellow(
                    f"[ENTRY_BLOCK] ticker={c['ticker']} reason=edge_too_low"
                    f" raw_edge={c.get('raw_edge', c['edge']):.3f}"
                    f" adj_edge={c['edge']:.3f}"
                ))
            elif final_entry_reason == "no_clear_direction":
                log.info(yellow(
                    f"[ENTRY_BLOCK] ticker={c['ticker']} reason=no_clear_direction"
                    f" model_prob={c['mp']:.3f}"
                ))
            log.info(yellow(
                f"[ENTRY_BLOCK] ticker={c['ticker']} reason={final_entry_reason}"
                f" regime={c.get('regime', '-')}"
                f" pressure={c['pressure_score']:.3f}"
                f" spread={c['spread'] if c.get('spread') is not None else '-'}"
                f" touch={c.get('selected_touch', 0.0):.0f}"
            ))
            if _dashboard is not None:
                _dashboard.add_event("ENTRY_BLOCK", c["ticker"], f"reason={final_entry_reason}")
            log_skip(
                c["ticker"],
                "final_entry_gate",
                f"reason={final_entry_reason} regime={c.get('regime', '-')}"
            )
            skipped_tickers.add(c["ticker"])
            continue
        print("DEBUG: ENTERING EXECUTION", c["ticker"])
        if _entry_size_mult < 1.0:
            c["size"] = max(0.25, round(c["size"] * _entry_size_mult, 2))
            log.info(cyan(
                f"[MID_TIER_UNLOCK] ticker={c['ticker']}"
                f" pressure={c['pressure_score']:.3f}"
                f" edge={c['edge']:.3f}"
                f" size_mult={_entry_size_mult:.2f}"
                f" → size={c['size']}"
            ))
            if _dashboard is not None:
                _dashboard.add_event(
                    "SYSTEM", c["ticker"],
                    f"MID_TIER_UNLOCK pressure={c['pressure_score']:.3f}"
                    f" edge={c['edge']:.3f} size={c['size']}",
                )
        if VERBOSE_LOGS: log.info(f"[TIER] assigned={c['tier_name']}")
        if VERBOSE_LOGS: log.info(f"[ENTRY] {c['ticker']} size={c['size']} spread={c['spread']:.3f} pressure={c['pressure_score']:.3f}")
        log.info(green(f"[ENTRY READY] {c['ticker']} | side={c['side'].upper()} | price={c['crowd']:.3f} spread={c['spread']:.3f}"))
        log.info(f"===> [ATTEMPTING BUY {c['side'].upper()}] {c['ticker']} | regime={c['regime']} | score={c['entry_score']:.3f} | edge={c['edge']:.3f} | pressure={c['pressure_score']:.3f} | spread={c['spread']:.3f} | liq={c['liquidity_score']:.3f}")
        if LOG_ENTRY_COMPONENTS:
            log.info(
                f"[ENTRY COMPONENTS] {c['ticker']} | "
                f"pressure={c['pressure_score']:.3f} edge={c['edge']:.3f} "
                f"liq={c['liquidity_score']:.3f} spread={c['spread']:.3f} "
                f"score={c['entry_score']:.3f}"
            )
        signals += 1
        if c["tier"] == 1:
            tier1_signals += 1
        else:
            tier2_signals += 1
        log_signal(c["m"], c["side"], c["crowd"], c["mp"], c["ev"], c["size"], c["hours"], c["family"], c["strike"], c["spot_price"])
        print("DEBUG: EXECUTING TRADE", c["ticker"], c["size"])
        written_entry_ts = record_open_trade(c["m"], c["side"], c["crowd"], c["mp"], c["ev"], c["size"], c["hours"], c["family"], c["strike"], c["spot_price"], c["tier"], replace=ts_key in open_trades_index)
        log.info(green(f"[BUY {c['side'].upper()}] {c['ticker']} | size={c['size']} | entry={c['crowd']:.3f} | edge={c['edge']:.3f}"))
        if _dashboard is not None:
            _dashboard.add_event(
                "BUY", c["ticker"],
                f"side={c['side'].upper()} tier={c['tier_name']} size={c['size']} edge={c['edge']:.3f}",
            )
        # Derive analytics entry-tier label from size multiplier and elite score.
        _analytics_tier = (
            "MID_TIER" if _entry_size_mult < 1.0
            else "ELITE" if (c.get("hard_elite") or c["elite_score"] >= 0.75)
            else "NORMAL"
        )
        _entry_metrics[ts_key] = {
            "elite_score": c["elite_score"],
            "pressure_score": c["pressure_score"],
            "crowd": c["crowd"],
            "velocity": c["velocity"],
            "entry_pressure": c["pressure_score"],
            "entry_spread": c["spread"],
            "entry_score": c["entry_score"],
            "regime": c["regime"],
            "entry_edge": c["edge"],
            "entry_killer_score": c.get("killer_score", 0.0),
            "killer_score": c.get("killer_score", 0.0),
            "liquidity_score": c["liquidity_score"],
            "quality_score": c["quality_score"],
            "series_key": series,
            "time_stop_secs": c["time_stop_secs"],
            "entry_ts": written_entry_ts,
            "entry_tier": _analytics_tier,
            "hours_to_close": safe_float(c.get("hours"), 0.0) or 0.0,
        }
        _msg = (
            f"[ENTRY_METRICS_SET] ticker={c['ticker']} side={c['side']}"
            f" entry_ts={written_entry_ts}"
        )
        if OFFLINE_LIFECYCLE_TEST:
            log.info(_msg)
        else:
            log.debug(_msg)
        _trade_state[ts_key] = {"max_favorable_excursion":0.0, "max_adverse_excursion":0.0, "peak_pnl_pct":0.0}
        _entry_cooldown_by_ticker[c["ticker"]] = now + timedelta(seconds=ENTRY_COOLDOWN_SECS)
        _last_trade_ts_by_ticker[c["ticker"]] = now
        _peak_pnl_by_position_id[f"{c['ticker']}|{c['side']}"] = 0.0
        _series_open_count[series] = _series_open_count.get(series, 0) + 1
        open_tickers.add(c["ticker"])
        append_persisted_open_row(open_rows, c, written_entry_ts)
        total_open_exposure += c["size"]
        if rotation_candidate is not None:
            log.info(magenta(
                f"[ROTATION_POST] open_positions={len(open_rows)} open_exposure={total_open_exposure:.2f} cash_balance={current_cash_balance(total_open_exposure):.2f}"
            ))
            expected_exposure = round(pre_rotation_exposure - replaced_size + c["size"], 4)
            if len(open_rows) != old_open_positions or abs(total_open_exposure - expected_exposure) > 0.0001:
                log.error(
                    f"[ROTATION_POST] validation_failed open_positions={len(open_rows)} expected_positions={old_open_positions}"
                    f" open_exposure={total_open_exposure:.2f} expected_exposure={expected_exposure:.2f}"
                    f" size_delta={(c['size'] - replaced_size):.2f} cash_balance={current_cash_balance(total_open_exposure):.2f}"
                )
        if rotation_candidate is None:
            cycle_cap_used += c["size"]
        log.info(cyan(
            f"CAPITAL_UPDATE | event=buy cash_balance={current_cash_balance(total_open_exposure):.2f}"
            f" open_exposure={total_open_exposure:.2f} realized_cash_pnl={fmt_money(_realized_cash_pnl)}"
        ))
        if c.get("tier_name") == "T3":
            t3_this_cycle += 1
        if rotation_candidate is None:
            new_trades_this_cycle += 1
        buys += 1
        persist_runtime_state()

    # FORCED FLOW DISABLED — bypassed safety gates (cooldown, reentry, cluster, series_cap)
    # and was the source of mystery fast_stop losses. Main scan loop is the sole entry path.

    if signals == 0:
        log.info("  (no signals this cycle)")
    else:
        log.info(f"  tier1={tier1_signals}  tier2={tier2_signals}")

    elite_strong = sum(1 for s in elite_scores if s >= 0.85)
    elite_medium = sum(1 for s in elite_scores if 0.78 <= s < 0.85)
    elite_weak = sum(1 for s in elite_scores if 0.70 <= s < 0.78)
    elite_avg = (sum(elite_scores) / len(elite_scores)) if elite_scores else 0.0
    avg_pressure = (sum(pressure_vals) / len(pressure_vals)) if pressure_vals else 0.0
    avg_velocity = (sum(velocity_vals) / len(velocity_vals)) if velocity_vals else 0.0
    avg_spread = (sum(spread_vals) / len(spread_vals)) if spread_vals else 0.0
    if VERBOSE_LOGS:
        log.info(f"ELITE_STATS | elite={len(elite_scores)} strong={elite_strong} medium={elite_medium} weak={elite_weak} avg_score={elite_avg:.3f}")
        log.info(f"EXECUTION   | tp={exit_counts.get('tp_hit',0)} pressure_exit={exit_counts.get('pressure_failure',0)} momentum_break={exit_counts.get('momentum_break',0)} hard_stop={exit_counts.get('hard_stop',0)} trail_exit={exit_counts.get('trail_protect',0)}")
        log.info(f"MICROSTATE  | avg_pressure={avg_pressure:.3f} avg_velocity={avg_velocity:.3f} avg_spread={avg_spread:.3f}")
        log.info(cyan(
            f"QUALITY_FILTER_SUMMARY | passed={qf_passed} rejected={qf_rejected}"
            f" breakdown={qf_breakdown}"
        ))
        print_candidate_summary(
            log, cyan,
            adapt_evaluated, adapt_touch_pass, adapt_spread_pass, adapt_edge_pass, adapt_quality_pass, adapt_near_miss,
            len(candidates), len(ranked_candidates), attempts, buys, skip_cluster, blocked_capital, blocked_open_cap, rotations,
        )
        log.info(magenta(
            f"ROTATION_SUMMARY | checked={rotation_checked} approved={rotation_approved} executed={rotations}"
            f" blocked_churn={rotation_blocked_churn} blocked_budget={rotation_blocked_budget}"
            f" blocked_conviction={rotation_blocked_conviction} blocked_late_lock={rotation_blocked_lock}"
            f" blocked_downgrade={rotation_blocked_downgrade} blocked_confidence={rotation_blocked_confidence}"
            f" winner_protected={rotation_winner_protected}"
        ))
    rotation_count = _rotation_perf["count"]
    avg_rotation_alpha = (_rotation_perf["alpha_sum"] / rotation_count) if rotation_count else 0.0
    rotation_win_rate = (_rotation_perf["wins"] / rotation_count) if rotation_count else 0.0
    if VERBOSE_LOGS:
        log.info(magenta(
            f"ROTATION_PERF | count={rotation_count} avg_alpha={avg_rotation_alpha:.4f} win_rate={rotation_win_rate:.1%}"
        ))
    exited_this_cycle = len(set(expired_exits) | set(early_exited_tickers))
    log.info(cyan(
        f"CAPITAL_SUMMARY | cash_balance={current_cash_balance(total_open_exposure):.2f}"
        f" open_exposure={total_open_exposure:.2f} realized_cash_pnl={fmt_money(_realized_cash_pnl)}"
        f" bought_this_cycle={buys} exited_this_cycle={exited_this_cycle} rotated_this_cycle={rotations}"
    ))
    if OFFLINE_MODE:
        log.info(cyan(
            f"[OFFLINE_RESULT] scenario={OFFLINE_SCENARIO} candidates={len(candidates)}"
            f" passed={len(ranked_candidates)} bought={buys} rotated={rotations} blocked={len(skipped_tickers)}"
        ))
    if LOG_DIAGNOSTICS:
        log.info(f"SCAN SUMMARY | fetched={len(markets)} usable={usable_markets} actionable={len(candidates)} attempts={attempts} buys={buys} open={open_count + buys}")
        if _last_skip_reason_counts:
            skip_counts = dict(_last_skip_reason_counts)
            dead_books = skip_counts.get("dead_book", 0)
            inactive = skip_counts.get("inactive_zone", 0)
            total_candidates = len(markets)
            market_dead = (dead_books + inactive) > (total_candidates * 0.8)
            MODE = "CHF" if CHF_MODE else "NORMAL"
            order = ["spread","hard_spread","pressure","liquidity","depth","cooldown","series_cap","weak_book","edge","spot_missing","entry_score","regime","open_cap","churn_block","reject_limit"]
            skip_parts = [f"{key}={_last_skip_reason_counts[key]}" for key in order if _last_skip_reason_counts.get(key)]
            if skip_parts:
                log.info("SKIPS | " + " ".join(skip_parts))
    _sc = dict(_last_skip_reason_counts)
    _bp = skip_pressure + _sc.get("pressure", 0)
    _bs = skip_wide_spread + _sc.get("spread", 0) + _sc.get("hard_spread", 0)
    _bl = skip_no_liquidity + _sc.get("liquidity", 0) + _sc.get("depth", 0)
    _be = skip_entry_score + _sc.get("entry_score", 0)
    log.info(f"BLOCK_SUMMARY | pressure={_bp} spread={_bs} liquidity={_bl} score={_be}")
    if cycle_num % 10 == 0:
        print_performance_summary()

    for t, p in prices.items():
        yb = p.get("yes_bid")
        ya = p.get("yes_ask")
        yes_p = float(yb) if yb is not None else (float(ya) if ya is not None else None)
        if yes_p is None:
            continue
        _last_crowd[(t, "yes")] = yes_p
        _last_crowd[(t, "no")] = round(1.0 - yes_p, 4)
        yes_hist = _price_history.get((t, "yes"), [])
        yes_hist.append(yes_p)
        _price_history[(t, "yes")] = yes_hist[-5:]
        no_hist = _price_history.get((t, "no"), [])
        no_hist.append(round(1.0 - yes_p, 4))
        _price_history[(t, "no")] = no_hist[-5:]
        if p.get("pressure_score") is not None:
            ph = _pressure_history.get(t, [])
            ph.append(float(p["pressure_score"]))
            _pressure_history[t] = ph[-5:]
        if p.get("yes_bid") is not None and p.get("yes_ask") is not None:
            sh = _spread_history.get(t, [])
            sh.append(float(p["yes_ask"]) - float(p["yes_bid"]))
            _spread_history[t] = sh[-5:]

    skip_pairs = [
        ("non_binary  ", skip_non_binary),
        ("not_open    ", skip_not_open),
        ("kxmve       ", skip_kxmve),
        ("no_price    ", skip_no_price),
        ("hours       ", skip_hours),
        ("hours_strict", skip_hours_strict),
        ("hft_hours   ", skip_hft_hours),
        ("unparsed_str", skip_unparsed_strike),
        ("far_strike  ", skip_far_strike),
        ("no_spot     ", skip_no_spot),
        ("no_liquidity", skip_no_liquidity),
        ("pressure    ", skip_pressure),
        ("velocity    ", skip_velocity),
        ("elite_score ", skip_elite_score),
        ("instability ", skip_instability),
        ("wide_spread ", skip_wide_spread),
        ("exposure    ", skip_exposure),
        ("fam_exp     ", skip_family_exposure),
        ("bucket_exp  ", skip_bucket_exposure),
        ("price       ", skip_price),
        ("volatile    ", skip_volatile),
        ("ev          ", skip_ev),
        ("low_volume  ", skip_low_volume),
        ("model       ", skip_model),
        ("cluster     ", skip_cluster),
        ("reentry_blk ", skip_reentry_blocked),
        ("cycle_cap   ", skip_cycle_cap),
        ("dup_ticker  ", skip_duplicate_ticker),
        ("size        ", skip_size),
        ("rate_limited", skip_rate_limited),
        ("series_cap  ", skip_series_cap),
        ("cooldown    ", skip_cooldown),
        ("weak_book   ", skip_weak_book),
        ("edge_gate   ", skip_edge),
        ("spot_missing", skip_spot_missing),
        ("entry_score ", skip_entry_score),
        ("stale_sig   ", skip_stale_signal),
    ]
    if VERBOSE_LOGS:
        log.info("--- FILTERS ---")
        print_skip_reason_summary(log, SEP, skip_pairs, signals, cycle_num)
    else:
        _nz = [(k.strip(), v) for k, v in skip_pairs if v > 0]
        if _nz:
            log.info("SKIPS | " + "  ".join(f"{k}={v}" for k, v in _nz))
        log.info(f"CYCLE #{cycle_num} done — {signals} signal(s)")
    exits_this_cycle = expiry_exit_count + (_ew + _el)
    validate_cycle_state(open_rows, total_open_exposure, set(expired_exits) | set(early_exited_tickers), exits_this_cycle, buys)
    if _dashboard is not None:
        _regime_now = global_regime["regime"]
        # In defensive/no_trade regimes with no actionable candidates, surface near-miss
        # pool so the dashboard shows *what was close* rather than a blank list.
        _defensive = _regime_now.lower() in ("defensive", "no_trade", "wide")
        _dash_cands = ranked_candidates
        if not _dash_cands and _near_miss_pool and _defensive:
            _dash_cands = sorted(_near_miss_pool, key=lambda c: c.get("edge", 0.0), reverse=True)
        _a_closed, _a_wins, _a_losses = _analytics.get_totals()
        if cycle_num == 1:
            _dashboard.add_event("SYSTEM", "FIRST_SCAN_DONE", f"markets={len(markets)} signals={signals}")
        log.info(f"[DASH_UPDATE] cycle={cycle_num} markets={len(markets)} signals={signals} buys={buys}")
        _dashboard.update(snapshot_from_bot_state(
            BANKROLL, current_cash_balance(total_open_exposure), total_open_exposure,
            _realized_cash_pnl, cycle_num, _regime_now,
            open_rows, _dash_cands, prices,
            safe_float, now, normalize_timestamp_utc,
            closed_trades=_a_closed, wins=_a_wins, losses=_a_losses,
        ))
        if signals == 0:
            # Derive the single largest block reason for the NO_CANDIDATES event.
            if _bp >= max(_bs, _bl, _be, 1):
                _no_cand_reason = "low_pressure"
            elif _bs >= max(_bp, _bl, _be, 1):
                _no_cand_reason = "spread"
            elif _bl >= max(_bp, _bs, _be, 1):
                _no_cand_reason = "liquidity"
            elif _be > 0:
                _no_cand_reason = "score"
            else:
                _no_cand_reason = "no_signal"
            _dashboard.add_event("SYSTEM", "NO_CANDIDATES", f"reason={_no_cand_reason} regime={_regime_now}")
        _dashboard.add_event("SYSTEM", "SCAN", f"markets={len(markets)} | actionable={signals} | buys={buys}")
        _dashboard.set_idle(CYCLE_SECS)
    persist_runtime_state()

async def main():
    global _offline_lifecycle_cycle, _dashboard
    if _DASHBOARD_AVAILABLE:
        _dashboard = BotDashboard()
        _dashboard.start()
        # Strip stdout handlers so log spam doesn't corrupt the Rich screen buffer
        import logging as _logging
        _root_logger = _logging.getLogger()
        for _h in list(_root_logger.handlers):
            if isinstance(_h, _logging.StreamHandler) and not isinstance(_h, _logging.FileHandler):
                _root_logger.removeHandler(_h)
        _dashboard.add_event("SYSTEM", "BOOT", "Dashboard started")
        # Push a partial startup state immediately so header is not all zeros
        # while the first network scan is in-flight.
        _dashboard.update({
            "bankroll":      BANKROLL,
            "equity":        BANKROLL,
            "cash":          BANKROLL,
            "open":          0.0,
            "realized":      0.0,
            "cycle":         0,
            "regime":        "normal",
            "positions":     [],
            "candidates":    [],
            "closed_trades": 0,
            "wins":          0,
            "losses":        0,
        })
        _dashboard.add_event("SYSTEM", "INIT_CONFIG", f"bankroll=${BANKROLL:.2f} mode={'OFFLINE' if OFFLINE_MODE else 'LIVE'}")
    print_cycle_header(
        log,
        "Kalshi High-Confidence Insurance Bot",
        BANKROLL,
        MIN_EV,
        MIN_CROWD,
        MAX_CROWD,
        HFT_TAKE_PROFIT,
        HFT_STOP_LOSS,
        SELECTION_MIN_MINUTES,
        SELECTION_MAX_MINUTES,
    )
    if OFFLINE_MODE:
        prepare_offline_debug_state()
        log.info(cyan(f"[OFFLINE_DEBUG] scenario={'lifecycle' if OFFLINE_LIFECYCLE_TEST else OFFLINE_SCENARIO}"))
    if not API_KEY and not OFFLINE_MODE:
        log.error("No KALSHI_API_KEY in .env")
        return
    if PAPER_STATE_RESET:
        reset_paper_state()
    elif not OFFLINE_MODE:
        restore_runtime_state()
    if _dashboard is not None:
        _dashboard.add_event("SYSTEM", "RESTORE_STATE", f"realized_pnl=${_realized_cash_pnl:+.2f}")
        # Refresh equity/cash now that realized_pnl is loaded from persisted state
        _dashboard.update({
            "bankroll":      BANKROLL,
            "equity":        round(BANKROLL + _realized_cash_pnl, 2),
            "cash":          round(BANKROLL + _realized_cash_pnl, 2),
            "open":          0.0,
            "realized":      _realized_cash_pnl,
            "cycle":         0,
            "regime":        "normal",
            "positions":     [],
            "candidates":    [],
            "closed_trades": 0,
            "wins":          0,
            "losses":        0,
        })
    log.info(
        f"NO_SIDE_CONFIG | hold_mult={NO_SIDE_HOLD_MULT} suppress_soft_exits={NO_SIDE_SUPPRESS_SOFT_EXITS}"
    )
    # ── Startup purge ────────────────────────────────────────────────────────
    now_startup = utc_now()
    existing = load_open_rows()
    reconciled_rows, reconcile_counts = reconcile_open_positions(existing, now_startup, log=log)
    save_positions(reconciled_rows)
    log.info(
        f"[STARTUP PURGE] complete | surviving_positions={len(reconciled_rows)}"
        f" open={reconcile_counts.get('open', 0)}"
        f" expired_unresolved={reconcile_counts.get('expired_unresolved', 0)}"
        f" corrupt={reconcile_counts.get('corrupt', 0)}"
    )
    if _dashboard is not None:
        _startup_open_exp = sum(
            safe_float(r.get("position_usd"), 0.0) or 0.0 for r in reconciled_rows
        )
        _dashboard.add_event(
            "SYSTEM", "STARTUP_PURGE",
            f"surviving={len(reconciled_rows)} open_exp=${_startup_open_exp:.2f}",
        )
        # Final pre-scan update: real carry-over positions are now known
        _dashboard.update({
            "bankroll":      BANKROLL,
            "equity":        round(current_cash_balance(_startup_open_exp) + _startup_open_exp, 2),
            "cash":          round(current_cash_balance(_startup_open_exp), 2),
            "open":          _startup_open_exp,
            "realized":      _realized_cash_pnl,
            "cycle":         0,
            "regime":        "normal",
            "positions":     positions_from_open_trades(reconciled_rows, {}, safe_float, now_startup, normalize_timestamp_utc),
            "candidates":    [],
            "closed_trades": 0,
            "wins":          0,
            "losses":        0,
        })
    # Seed _entry_metrics for carry-over positions so session guard passes and they age correctly.
    # Preserve the persisted CSV/state timestamp instead of assigning a fresh startup timestamp.
    for _r in reconciled_rows:
        _ticker = _r.get("ticker", "")
        _side = _r.get("side", "")
        _entry_dt = normalize_timestamp_utc(_r.get("timestamp", ""))
        if not _ticker or not _side or _entry_dt is None:
            log.info(f"[CARRY_SKIP] ticker={_ticker or '?'} reason=corrupt_row")
            continue
        _key = (_ticker, _side)
        if _key not in _entry_metrics:
            _entry_metrics[_key] = {"entry_ts": _r.get("timestamp", "")}
            log.info(
                f"[CARRY-OVER] seeded entry_ts={_r.get('timestamp', '')} "
                f"ticker={_r.get('ticker','')} side={_r.get('side','')}"
            )
    if OFFLINE_LIFECYCLE_TEST:
        for cycle_num in range(1, 7):
            _offline_lifecycle_cycle = cycle_num
            await run_cycle(None, cycle_num)
            advance_offline_lifecycle_state(cycle_num)
        return
    if OFFLINE_DEBUG:
        _offline_lifecycle_cycle = 1
        await run_cycle(None, 1)
        return
    cycle=0
    try:
        async with aiohttp.ClientSession() as session:
            while True:
                cycle+=1
                try:
                    await run_cycle(session,cycle)
                except aiohttp.ClientError as e:
                    log.error(f"KALSHI FETCH FAILED | {e}")
                    if _dashboard is not None:
                        _a_closed, _a_wins, _a_losses = _analytics.get_totals()
                        _dashboard.update({
                            "bankroll": BANKROLL,
                            "equity":   round(BANKROLL + _realized_cash_pnl, 2),
                            "cash":     round(BANKROLL + _realized_cash_pnl, 2),
                            "open":     0.0,
                            "realized": _realized_cash_pnl,
                            "cycle":    cycle,
                            "regime":   "normal",
                            "positions":     [],
                            "candidates":    [],
                            "closed_trades": _a_closed,
                            "wins":          _a_wins,
                            "losses":        _a_losses,
                        })
                        _dashboard.add_event("SYSTEM", "FETCH_ERROR", f"cycle={cycle} {str(e)[:40]}")
                        _dashboard.set_idle(CYCLE_SECS)
                    await asyncio.sleep(CYCLE_SECS)
                    continue
                except Exception as e:
                    log.error(f"Error: {e}",exc_info=True)
                    if _dashboard is not None:
                        _a_closed, _a_wins, _a_losses = _analytics.get_totals()
                        _dashboard.update({
                            "bankroll": BANKROLL,
                            "equity":   round(BANKROLL + _realized_cash_pnl, 2),
                            "cash":     round(BANKROLL + _realized_cash_pnl, 2),
                            "open":     0.0,
                            "realized": _realized_cash_pnl,
                            "cycle":    cycle,
                            "regime":   "normal",
                            "positions":     [],
                            "candidates":    [],
                            "closed_trades": _a_closed,
                            "wins":          _a_wins,
                            "losses":        _a_losses,
                        })
                        _dashboard.add_event("SYSTEM", "CYCLE_ERROR", f"cycle={cycle} {str(e)[:40]}")
                        _dashboard.set_idle(CYCLE_SECS)
                await asyncio.sleep(CYCLE_SECS)
    finally:
        if _dashboard is not None:
            _dashboard.stop()

if __name__=="__main__":
    asyncio.run(main())
