def clamp01(x):
    return max(0.0, min(1.0, x))


def compute_killer_score_components(edge, quality, touch, spread, pressure, adaptive_touch_req=10, adaptive_spread_cap=0.05):
    touch_norm = min(1.0, max(0.0, touch) / max(1.0, adaptive_touch_req * 2))
    spread_norm = max(0.0, 1.0 - (max(0.0, spread) / max(0.001, adaptive_spread_cap)))
    killer_score = clamp01(
        edge * 0.40 +
        quality * 0.25 +
        touch_norm * 0.15 +
        spread_norm * 0.10 +
        pressure * 0.10
    )
    return killer_score, touch_norm, spread_norm


def classify_conviction_state(killer_score, edge):
    if killer_score >= 0.65 and edge >= 0.44:
        return "elite"
    if killer_score >= 0.57 and edge >= 0.33:
        return "strong"
    if killer_score >= 0.49 and edge >= 0.18:
        return "neutral"
    return "weak"


def classify_conviction_delta(killer_delta):
    if killer_delta >= 0.025:
        return "improving"
    if killer_delta <= -0.025:
        return "degrading"
    return "flat"
