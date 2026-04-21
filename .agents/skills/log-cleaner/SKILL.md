---
name: log-cleaner
description: Simplify noisy trading bot logs into clean, readable summaries. Use when logs are cluttered or hard to interpret.
---

When invoked:
- Remove repetitive lines (rate limits, skips, duplicates)
- Group logs into:
  1. Trades opened
  2. Trades closed
  3. Key errors
  4. Summary stats
- Output clean, minimal dashboard-style text
- Highlight mismatches (e.g., bankroll inconsistencies)
- Keep output readable in terminal (no clutter)
