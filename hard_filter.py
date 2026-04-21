"""
hard_filter.py — Phase 1 pre-scoring kill system.

Returns (allowed, reason, details) for every market before it reaches scoring.
First failure wins. No scoring logic lives here — fast, deterministic checks only.
"""
from __future__ import annotations

from typing import Any

# ── kill reason analytics ─────────────────────────────────────────────────────

kill_stats: dict[str, int] = {
    "low_volume":           0,
    "wide_spread":          0,
    "bad_price_range":      0,
    "too_close_to_expiry":  0,
    "missing_orderbook":    0,
    "stale_data":           0,
    "insufficient_depth":   0,
    "other":                0,
}

# ── per-cycle debug limiter for missing_orderbook ─────────────────────────────
# Reset to 0 at the start of each scan cycle in main.py.
missing_orderbook_debug_count: int = 0
MISSING_ORDERBOOK_DEBUG_MAX: int = 3


def record_kill(reason: str) -> None:
    """Increment the counter for the given kill reason."""
    if reason in kill_stats:
        kill_stats[reason] += 1
    else:
        kill_stats["other"] += 1


# ── main filter ───────────────────────────────────────────────────────────────

def hard_filter_market(
    market: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> tuple[bool, str | None, dict]:
    """
    Pre-scoring kill filter.

    Returns:
        (allowed: bool, reason: str | None, details: dict)

    First failure wins. Safe defaults used for any missing config key.
    """
    if config is None:
        config = {}

    try:
        # ── 1. missing orderbook ─────────────────────────────────────────────
        # Only fail if ALL four bid/ask fields are absent — normalize_quote_state
        # can reconstruct yes_ask from no_bid and vice-versa, so a single
        # non-None field means the book is usable.
        yes_ask = market.get("yes_ask")
        no_ask  = market.get("no_ask")
        yes_bid = market.get("yes_bid")
        no_bid  = market.get("no_bid")
        if yes_ask is None and no_ask is None and yes_bid is None and no_bid is None:
            global missing_orderbook_debug_count
            details: dict[str, Any] = {
                "yes_ask": yes_ask, "no_ask": no_ask,
                "yes_bid": yes_bid, "no_bid": no_bid,
            }
            if missing_orderbook_debug_count < MISSING_ORDERBOOK_DEBUG_MAX:
                missing_orderbook_debug_count += 1
                available_keys = [k for k, v in market.items() if v is not None][:20]
                details["_debug_keys"] = available_keys
                details["_debug_sample"] = missing_orderbook_debug_count
            return False, "missing_orderbook", details

        # ── 2. low volume ────────────────────────────────────────────────────
        volume = market.get("volume_24h") or market.get("volume") or 0
        min_vol = config.get("min_volume", 500)
        try:
            volume = float(volume)
        except (TypeError, ValueError):
            volume = 0.0
        if volume < min_vol:
            return False, "low_volume", {"volume": volume, "min_volume": min_vol}

        # ── 3. wide spread ───────────────────────────────────────────────────
        max_spread = config.get("max_spread", 0.12)
        spread = None
        try:
            if yes_ask is not None and yes_bid is not None:
                spread = float(yes_ask) - float(yes_bid)
            elif no_ask is not None and no_bid is not None:
                spread = float(no_ask) - float(no_bid)
        except (TypeError, ValueError):
            spread = None
        if spread is not None and spread > max_spread:
            return False, "wide_spread", {"spread": round(spread, 4), "max_spread": max_spread}

        # ── 4. bad price range ───────────────────────────────────────────────
        price_floor = config.get("price_floor", 0.01)
        price_ceil  = config.get("price_ceil",  0.99)
        ref_price = None
        try:
            if no_ask is not None:
                ref_price = float(no_ask)
            elif yes_ask is not None:
                ref_price = float(yes_ask)
        except (TypeError, ValueError):
            ref_price = None
        if ref_price is not None and not (price_floor <= ref_price <= price_ceil):
            return False, "bad_price_range", {"price": ref_price, "floor": price_floor, "ceil": price_ceil}

        # ── 5. too close to expiry ───────────────────────────────────────────
        min_minutes = config.get("min_minutes_to_expiry", 30)
        minutes_to_expiry = market.get("minutes_to_expiry")
        if minutes_to_expiry is None:
            # try to derive from hours field if pre-computed
            hours = market.get("hours_to_expiry") or market.get("hours")
            if hours is not None:
                try:
                    minutes_to_expiry = float(hours) * 60
                except (TypeError, ValueError):
                    minutes_to_expiry = None
        if minutes_to_expiry is not None:
            try:
                if float(minutes_to_expiry) < min_minutes:
                    return False, "too_close_to_expiry", {
                        "minutes_to_expiry": float(minutes_to_expiry),
                        "min_minutes": min_minutes,
                    }
            except (TypeError, ValueError):
                pass

        # ── 6. stale data ────────────────────────────────────────────────────
        # Flag markets where last_trade_price is identical to yes_ask with zero
        # volume — a simple proxy for stale/untouched books.
        last_price = market.get("last_price") or market.get("last_trade_price")
        if last_price is not None and volume == 0:
            return False, "stale_data", {"last_price": last_price, "volume": volume}

        # ── 7. insufficient depth ────────────────────────────────────────────
        min_depth = config.get("min_depth", 5)
        yes_touch = market.get("yes_touch_size") or market.get("yes_depth") or 0
        no_touch  = market.get("no_touch_size")  or market.get("no_depth")  or 0
        try:
            depth = max(float(yes_touch), float(no_touch))
        except (TypeError, ValueError):
            depth = 0.0
        if depth < min_depth:
            return False, "insufficient_depth", {"depth": depth, "min_depth": min_depth}

        # ── all checks passed ────────────────────────────────────────────────
        return True, None, {}

    except Exception as exc:
        # Never crash the caller — log the anomaly and allow through.
        return True, None, {"hard_filter_error": str(exc)}
