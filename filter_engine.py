"""
filter_engine.py — Layer 1: silent pre-filter.

Rejects candidates before they enter decision logic.
No logging. No context. First failure wins.
"""
from __future__ import annotations
from typing import Any

from hard_filter import hard_filter_market


# Skip codes (used by compressed logger)
SKIP_CODES: dict[str, str] = {
    "weak_pressure":          "P",
    "low_pressure":           "P",
    "no_pressure":            "P",
    "low_edge":               "E",
    "edge_floor":             "E",
    "edge_gate":              "E",
    "bad_liquidity":          "L",
    "low_liquidity":          "L",
    "no_real_book":           "L",
    "no_real_liquidity":      "L",
    "insufficient_depth":     "L",
    "wide_spread":            "S",
    "spread_too_wide":        "S",
    "low_volume":             "V",
    "low_quality":            "Q",
    "bad_regime_low_quality": "Q",
    "missing_orderbook":      "B",
    "too_close_to_expiry":    "T",
    "bad_price_range":        "X",
    "stale_data":             "Z",
}


def pre_filter(
    market: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> tuple[bool, str | None]:
    """
    Hard pre-filter gate. Returns (pass, reason).
    Wraps hard_filter_market — no new logic added.
    """
    ok, reason, _ = hard_filter_market(market, config)
    return ok, reason


def to_minimal(c: dict[str, Any]) -> dict[str, Any]:
    """Strips candidate to the minimal structured payload for decision engine."""
    return {
        "ticker":  c.get("ticker", ""),
        "side":    c.get("side", ""),
        "edge":    c.get("edge", 0.0),
        "price":   c.get("crowd", c.get("price", 0.0)),
        "liq":     c.get("liquidity_score", 0.0),
        "spread":  c.get("spread", 0.0),
    }


def top_candidates(
    candidates: list[dict[str, Any]],
    n: int = 5,
) -> list[dict[str, Any]]:
    """Return top-N candidates by edge, already past the filter."""
    return sorted(candidates, key=lambda c: c.get("edge", 0.0), reverse=True)[:n]
