"""
smart_money.py — Phase 1 smart-money signal detector.

Non-invasive: produces signals only. Does NOT force trades or alter scores.
Attach results to market dict; scoring layer reads them optionally in Phase 2.
"""
from __future__ import annotations

from typing import Any


def detect_smart_money(
    current: dict[str, Any],
    previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Detect smart-money signatures from market data.

    Returns:
        {
            "entry_boost":  float,   # suggested additive boost (0.0 = none)
            "exit_flag":    bool,    # True = smart money may be exiting
            "reason":       str,     # human-readable description
            "confidence":   float,   # 0.0–1.0
        }

    Defaults to neutral if data is missing. Never forces a trade.
    """
    result: dict[str, Any] = {
        "entry_boost": 0.0,
        "exit_flag":   False,
        "reason":      "neutral",
        "confidence":  0.0,
    }

    try:
        signals: list[str] = []
        boost    = 0.0
        conf     = 0.0
        exit_flg = False

        # ── 1. volume spike ──────────────────────────────────────────────────
        vol_now = _safe_float(current.get("volume_24h") or current.get("volume"))
        vol_avg = _safe_float(current.get("avg_volume") or current.get("volume_7d_avg"))
        if vol_now > 0 and vol_avg > 0:
            spike_ratio = vol_now / vol_avg
            if spike_ratio >= 3.0:
                boost  += 0.04
                conf   += 0.30
                signals.append(f"vol_spike_{spike_ratio:.1f}x")
            elif spike_ratio >= 2.0:
                boost  += 0.02
                conf   += 0.15
                signals.append(f"vol_spike_{spike_ratio:.1f}x")
        elif vol_now > 0 and previous is not None:
            # fallback: compare to previous snapshot
            vol_prev = _safe_float(previous.get("volume_24h") or previous.get("volume"))
            if vol_prev > 0 and vol_now > vol_prev * 2.0:
                boost  += 0.02
                conf   += 0.15
                signals.append("vol_spike_vs_prev")

        # ── 2. pressure shift ────────────────────────────────────────────────
        press_now  = _safe_float(current.get("pressure_score") or current.get("pressure"))
        press_prev = _safe_float(
            (previous or {}).get("pressure_score") or (previous or {}).get("pressure")
        )
        if press_now > 0 and press_prev > 0:
            delta = press_now - press_prev
            if delta >= 0.08:
                boost  += 0.03
                conf   += 0.20
                signals.append(f"pressure_up_{delta:.2f}")
            elif delta <= -0.08:
                exit_flg = True
                conf     += 0.15
                signals.append(f"pressure_drop_{delta:.2f}")

        # ── 3. spread compression ────────────────────────────────────────────
        spread_now  = _spread(current)
        spread_prev = _spread(previous) if previous else None
        if spread_now is not None and spread_prev is not None and spread_prev > 0:
            compression = (spread_prev - spread_now) / spread_prev
            if compression >= 0.20:
                boost  += 0.02
                conf   += 0.15
                signals.append(f"spread_compress_{compression:.0%}")

        # ── 4. depth improvement (smart money providing liquidity) ───────────
        depth_now  = _depth(current)
        depth_prev = _depth(previous) if previous else None
        if depth_now is not None and depth_prev is not None and depth_prev > 0:
            depth_ratio = depth_now / depth_prev
            if depth_ratio >= 1.5:
                boost  += 0.01
                conf   += 0.10
                signals.append(f"depth_up_{depth_ratio:.1f}x")

        # ── compile ──────────────────────────────────────────────────────────
        result["entry_boost"] = round(min(boost, 0.10), 4)   # cap boost at 0.10
        result["exit_flag"]   = exit_flg
        result["confidence"]  = round(min(conf, 1.0), 3)
        result["reason"]      = ", ".join(signals) if signals else "neutral"

    except Exception as exc:
        result["reason"] = f"error:{exc}"

    return result


# ── helpers ───────────────────────────────────────────────────────────────────

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _spread(m: dict[str, Any] | None) -> float | None:
    if not m:
        return None
    try:
        ya, yb = m.get("yes_ask"), m.get("yes_bid")
        if ya is not None and yb is not None:
            return float(ya) - float(yb)
        na, nb = m.get("no_ask"), m.get("no_bid")
        if na is not None and nb is not None:
            return float(na) - float(nb)
    except (TypeError, ValueError):
        pass
    return None


def _depth(m: dict[str, Any] | None) -> float | None:
    if not m:
        return None
    try:
        yt = m.get("yes_touch_size") or m.get("yes_depth") or 0
        nt = m.get("no_touch_size")  or m.get("no_depth")  or 0
        return max(float(yt), float(nt))
    except (TypeError, ValueError):
        return None
