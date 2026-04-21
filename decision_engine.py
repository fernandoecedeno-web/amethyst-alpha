"""
decision_engine.py — Layer 2: decision context.

Receives only top-N filtered candidates in minimal form.
Renders clean terminal display. No strategy logic lives here.
"""
from __future__ import annotations
from typing import Any

from filter_engine import top_candidates, to_minimal


def build_decision_context(
    candidates: list[dict[str, Any]],
    state: dict[str, Any],
    n: int = 5,
) -> dict[str, Any]:
    """
    Build minimal decision context from filtered candidates.

    state must contain: bankroll, open, risk, regime
    """
    top = top_candidates(candidates, n)
    return {
        "state": {
            "bankroll": state.get("bankroll", 0.0),
            "open":     state.get("open", 0),
            "risk":     state.get("risk", 0.0),
            "regime":   state.get("regime", "unknown"),
        },
        "candidates": [to_minimal(c) for c in top],
    }


def render_cycle_display(
    cycle_num: int,
    state: dict[str, Any],
    candidates: list[dict[str, Any]],
    n: int = 5,
) -> str:
    """
    Returns clean terminal output string. No logging side effects.

    Example:
        CYCLE #42
        BANKROLL: $91.20 | OPEN: 2 | RISK: 4.4%
        TOP:
          1. KXBTC-NO  e=0.31
          2. KXETH-NO  e=0.22
    """
    bankroll = state.get("bankroll", 0.0)
    open_pos = state.get("open", 0)
    risk_pct = state.get("risk", 0.0) * 100

    top = top_candidates(candidates, n)

    lines = [
        f"CYCLE #{cycle_num}",
        f"BANKROLL: ${bankroll:.2f} | OPEN: {open_pos} | RISK: {risk_pct:.1f}%",
    ]
    if top:
        lines.append("TOP:")
        for i, c in enumerate(top, 1):
            ticker = c.get("ticker", "?")
            side   = c.get("side", "?").upper()
            edge   = c.get("edge", 0.0)
            lines.append(f"  {i}. {ticker}-{side:<4} e={edge:.2f}")
    else:
        lines.append("TOP: none")

    return "\n".join(lines)
