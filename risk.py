from __future__ import annotations

from typing import Any


def can_open_trade(
    candidate: dict[str, Any],
    state: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> tuple[bool, str | None]:
    bankroll = state.get("bankroll", 0)
    max_loss_allowed = bankroll * 0.02

    trade_size = candidate.get("size", 0)
    price = candidate.get("price", 1)
    exposure = trade_size * price

    if exposure > max_loss_allowed:
        return False, "exceeds_2pct_risk"

    if state.get("open_positions", 0) >= 3:
        return False, "max_positions_reached"

    return True, None


def compute_position_size(
    candidate: dict[str, Any],
    state: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> float:
    bankroll = state.get("bankroll", 0)
    price = candidate.get("price", 1)

    max_size = (bankroll * 0.15) / price

    return min(max_size, candidate.get("size", max_size))
