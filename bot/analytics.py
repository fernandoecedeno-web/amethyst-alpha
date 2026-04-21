"""
Trade-performance analytics — observability only, no strategy changes.

Aggregates realized results into six dimensions:
  side | tier | pressure_band | exit_reason | regime_at_entry | expiry_horizon

Results are flushed to trade_analytics.json (repo root) every FLUSH_EVERY exits
and are also available as formatted log lines via get_summary_lines().
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

# Writes to <repo_root>/trade_analytics.json
_ANALYTICS_FILE = Path(__file__).resolve().parents[1] / "trade_analytics.json"

FLUSH_EVERY = 5   # flush JSON + log summary after this many exits

_lock       = threading.Lock()
_exit_count = 0

_buckets: dict[str, dict[str, dict]] = {
    "side":     {},
    "tier":     {},
    "pressure": {},
    "exit":     {},
    "regime":   {},
    "expiry":   {},
}


# ── bucket key helpers ────────────────────────────────────────────────────────

def _pressure_band(p: float) -> str:
    if p < 0.40: return "LT_040"
    if p < 0.45: return "P040_045"
    if p < 0.55: return "P045_055"
    if p < 0.75: return "P055_075"
    return "GE_075"


def _exit_bucket(reason: str) -> str:
    _map = {
        "hard_stop":            "hard_stop",
        "catastrophic_stop":    "hard_stop",
        "time_stop":            "time_stop",
        "conviction_decay":     "conviction_decay",
        "tp_hit":               "tp",
        "trail_protect":        "trail",
        "pressure_failure":     "pressure",
        "momentum_break":       "momentum",
        "stale_conviction":     "other",
        "stale_break":          "other",
        "exec_deterioration":   "other",
    }
    return _map.get(reason, "other")


def _expiry_bucket(hours_to_close: float) -> str:
    if hours_to_close < 0.25: return "LT_15M"
    if hours_to_close < 1.0:  return "M15_60"
    if hours_to_close < 4.0:  return "H1_4"
    if hours_to_close < 12.0: return "H4_12"
    return "GE_12H"


def _regime_bucket(regime: str) -> str:
    r = (regime or "").lower()
    if "attack"          in r: return "ATTACK"
    if "no_trade"        in r: return "NO_TRADE"
    if "defensive"       in r: return "DEFENSIVE"
    if "momentum_clean"  in r: return "MOM_CLEAN"
    if "momentum"        in r: return "MOM_FRAGILE"
    if "normal"          in r: return "NORMAL"
    return "OTHER"


def _empty_bucket() -> dict:
    return {"trades": 0, "wins": 0, "losses": 0, "total_pnl": 0.0, "total_hold": 0}


def _update(b: dict, pnl: float, held: int) -> None:
    b["trades"]    += 1
    b["total_pnl"]  = round(b["total_pnl"] + pnl, 4)
    b["total_hold"] += int(held)
    if pnl >= 0:
        b["wins"]   += 1
    else:
        b["losses"] += 1


# ── public API ────────────────────────────────────────────────────────────────

def record_exit(
    side: str,
    pnl_usd: float,
    held_secs: float,
    exit_reason: str,
    entry_meta: dict,
) -> tuple[str, str, str, str, str, str]:
    """Record one completed trade; return the six bucket labels used."""
    global _exit_count

    pressure      = float(
        entry_meta.get("entry_pressure")
        or entry_meta.get("pressure_score")
        or 0.0
    )
    tier          = str(entry_meta.get("entry_tier", "NORMAL"))
    regime        = str(entry_meta.get("regime", ""))
    hours_to_close = float(entry_meta.get("hours_to_close") or 0.0)

    side_b   = side.upper()
    tier_b   = tier
    press_b  = _pressure_band(pressure)
    exit_b   = _exit_bucket(exit_reason)
    regime_b = _regime_bucket(regime)
    expiry_b = _expiry_bucket(hours_to_close)

    with _lock:
        for dim, key in (
            ("side",     side_b),
            ("tier",     tier_b),
            ("pressure", press_b),
            ("exit",     exit_b),
            ("regime",   regime_b),
            ("expiry",   expiry_b),
        ):
            if key not in _buckets[dim]:
                _buckets[dim][key] = _empty_bucket()
            _update(_buckets[dim][key], pnl_usd, held_secs)

        _exit_count += 1
        should_flush = (_exit_count % FLUSH_EVERY == 0)
        snap = _snapshot() if should_flush else None

    if snap is not None:
        _write_json(snap)

    return side_b, tier_b, press_b, exit_b, regime_b, expiry_b


def get_totals() -> tuple[int, int, int]:
    """Return (total_closed, total_wins, total_losses) across all dimensions combined."""
    with _lock:
        side_buckets = _buckets["side"]
        total_closed = sum(b["trades"] for b in side_buckets.values())
        total_wins   = sum(b["wins"]   for b in side_buckets.values())
        total_losses = sum(b["losses"] for b in side_buckets.values())
    return total_closed, total_wins, total_losses


def get_expiry_lines() -> list[str]:
    """Return compact [EXPIRY_AUDIT] lines, one per bucket, ordered near→far."""
    with _lock:
        snap = _snapshot()

    order = ["LT_15M", "M15_60", "H1_4", "H4_12", "GE_12H"]
    exp   = snap.get("expiry", {})
    lines = []
    for bkt in order:
        b = exp.get(bkt)
        if not b or b["trades"] == 0:
            continue
        n   = b["trades"]
        wr  = b["wins"] / n
        avg = b["total_pnl"] / n
        lines.append(
            f"[EXPIRY_AUDIT] bucket={bkt:<7} trades={n:>4}"
            f"  wr={wr:>5.1%}  tot={b['total_pnl']:>+7.2f}  avg={avg:>+7.4f}"
        )
    return lines


def get_summary_lines() -> list[str]:
    """Return formatted lines suitable for log.info() calls."""
    with _lock:
        snap = _snapshot()

    dim_order = ["side", "tier", "pressure", "exit", "regime", "expiry"]
    lines = ["── TRADE ANALYTICS ──────────────────────────────────────────────"]
    for dim in dim_order:
        dim_buckets = snap.get(dim, {})
        if not dim_buckets:
            continue
        lines.append(f"  [{dim.upper()}]")
        for key in sorted(dim_buckets):
            b = dim_buckets[key]
            n = b["trades"]
            if n == 0:
                continue
            wr       = b["wins"] / n
            avg_pnl  = b["total_pnl"] / n
            avg_hold = b["total_hold"] // n
            lines.append(
                f"    {key:<16} n={n:>3}  wr={wr:>5.0%}"
                f"  tot={b['total_pnl']:+.3f}"
                f"  avg={avg_pnl:+.4f}"
                f"  hold={avg_hold}s"
            )
    lines.append("─────────────────────────────────────────────────────────────")
    return lines


# ── internal ──────────────────────────────────────────────────────────────────

def _snapshot() -> dict:
    """Caller must hold _lock."""
    return {dim: {k: dict(v) for k, v in buckets.items()}
            for dim, buckets in _buckets.items()}


def _write_json(snap: dict) -> None:
    """Serialise bucket snapshot to trade_analytics.json. Failures are silently swallowed."""
    try:
        out: dict = {}
        for dim, buckets in snap.items():
            out[dim] = {}
            for key, b in buckets.items():
                n = b["trades"]
                out[dim][key] = {
                    "trades":        n,
                    "wins":          b["wins"],
                    "losses":        b["losses"],
                    "win_rate":      round(b["wins"] / n, 4) if n else 0.0,
                    "total_pnl":     round(b["total_pnl"], 4),
                    "avg_pnl":       round(b["total_pnl"] / n, 4) if n else 0.0,
                    "avg_hold_secs": b["total_hold"] // n if n else 0,
                }
        _ANALYTICS_FILE.write_text(json.dumps(out, indent=2))
    except Exception:
        pass  # analytics write failure must never kill the bot
