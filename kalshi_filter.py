def market_quality_filter(implied_prob, market_volume):
    if implied_prob <= 0.20 or implied_prob >= 0.80:
        return "skip_extreme"

    if market_volume < 100:
        return "skip_low_quality"

    if 0.47 <= implied_prob <= 0.53 and market_volume >= 200:
        return "tier1"

    if 0.40 <= implied_prob <= 0.60 and market_volume >= 125:
        return "tier2"

    return "skip_low_quality"


if __name__ == "__main__":
    tests = [
        (0.50, 500000),
        (0.07, 3000000),
        (0.62, 800000),
        (0.48, 50000),
        (0.30, 100000),
    ]

    for prob, vol in tests:
        result = market_quality_filter(prob, vol)
        print(f"prob={prob:.3f} volume={vol:,.0f} -> {result}")
