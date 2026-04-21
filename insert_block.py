        # Polymarket-inspired market quality filter
        implied_prob = mp
        market_volume = candidate.get("selected_touch", 0.0) or 0.0
        quality_tier = market_quality_filter(implied_prob, market_volume)

        if quality_tier == "skip_extreme":
            log_skip(ticker, "extreme_market")
            continue

        if quality_tier == "skip_low_quality":
            log_skip(ticker, "low_quality_market")
            continue

        candidate["quality_tier"] = quality_tier

        if quality_tier == "tier1":
            candidate["killer_score"] = min(0.99, candidate.get("killer_score", 0.5) + 0.05)
            candidate["size"] = round(candidate["size"] * 1.10, 2)

        elif quality_tier == "tier2":
            candidate["killer_score"] = min(0.99, candidate.get("killer_score", 0.5) + 0.02)
