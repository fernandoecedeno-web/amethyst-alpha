"""
probe_trading_api.py — confirm the configured Kalshi API serves KXBTC/KXETH

Run:
    python3 probe_trading_api.py

Uses the same KALSHI_API_KEY from .env.  No changes to main.py.
"""

import asyncio
from datetime import datetime, timezone

import aiohttp
from bot.config import BASE_URL, KALSHI_API_KEY

API_KEY      = KALSHI_API_KEY
CANONICAL_BASE = BASE_URL
PAGE_DELAY   = 0.35


def headers():
    return {"Content-Type": "application/json", "Authorization": f"Bearer {API_KEY}"}


async def fetch_series(session, base_url, series_ticker):
    """GET /markets?series_ticker=X&status=open&limit=10"""
    async with session.get(
        f"{base_url}/markets",
        headers=headers(),
        params={"series_ticker": series_ticker, "status": "open", "limit": 10},
        timeout=aiohttp.ClientTimeout(total=8),
    ) as r:
        status = r.status
        if status == 200:
            data = await r.json()
            markets = data.get("markets", [])
            return status, markets
        return status, []


async def fetch_one(session, base_url, ticker):
    """GET /markets/{ticker}"""
    async with session.get(
        f"{base_url}/markets/{ticker}",
        headers=headers(),
        timeout=aiohttp.ClientTimeout(total=8),
    ) as r:
        status = r.status
        if status == 200:
            data = await r.json()
            return status, data.get("market", {})
        return status, {}


async def count_open(session, base_url):
    """Fetch first page and return count + sample ticker."""
    async with session.get(
        f"{base_url}/markets",
        headers=headers(),
        params={"status": "open", "limit": 1},
        timeout=aiohttp.ClientTimeout(total=8),
    ) as r:
        if r.status == 200:
            data = await r.json()
            sample = (data.get("markets") or [{}])[0].get("ticker", "n/a")
            has_more = bool(data.get("cursor"))
            return r.status, sample, has_more
        return r.status, "n/a", False


async def main():
    if not API_KEY:
        print("ERROR: KALSHI_API_KEY not set in .env")
        return

    print(f"Kalshi Endpoint Probe — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()

    async with aiohttp.ClientSession() as session:

        # ── configured host at /markets?limit=1 ──────────────────────────────
        print("── Reachability check ─────────────────────────────────────────")
        status, sample, has_more = await count_open(session, CANONICAL_BASE)
        mark = "✓" if status == 200 else "✗"
        print(f"  {mark} BASE_URL={CANONICAL_BASE}  HTTP {status}  sample_ticker={sample}  more_pages={has_more}")
        print()

        # ── KXBTC series on configured host ──────────────────────────────────
        print("── KXBTC series probe ─────────────────────────────────────────")
        await asyncio.sleep(PAGE_DELAY)
        status, markets = await fetch_series(session, CANONICAL_BASE, "KXBTC")
        if status == 200 and markets:
            print(f"  BASE_URL [{status}]  {len(markets)} markets returned:")
            for m in markets[:5]:
                ticker     = m.get("ticker", "?")
                close_time = m.get("close_time", "?")
                yes_bid    = m.get("yes_bid")
                yes_ask    = m.get("yes_ask")
                spread     = f"{float(yes_ask)-float(yes_bid):.3f}" if yes_bid and yes_ask else "n/a"
                print(f"    {ticker:<42}  close={close_time}  spread={spread}")
        else:
            print(f"  BASE_URL [{status}]  0 markets (series not found or not accessible)")
        print()

        # ── KXETH series on configured host ──────────────────────────────────
        print("── KXETH series probe ─────────────────────────────────────────")
        await asyncio.sleep(PAGE_DELAY)
        status, markets = await fetch_series(session, CANONICAL_BASE, "KXETH")
        if status == 200 and markets:
            print(f"  BASE_URL [{status}]  {len(markets)} markets returned:")
            for m in markets[:5]:
                ticker     = m.get("ticker", "?")
                close_time = m.get("close_time", "?")
                yes_bid    = m.get("yes_bid")
                yes_ask    = m.get("yes_ask")
                spread     = f"{float(yes_ask)-float(yes_bid):.3f}" if yes_bid and yes_ask else "n/a"
                print(f"    {ticker:<42}  close={close_time}  spread={spread}")
        else:
            print(f"  BASE_URL [{status}]  0 markets (series not found or not accessible)")
        print()

        print("── Verdict ────────────────────────────────────────────────────")
        print(f"  Using canonical BASE_URL: {CANONICAL_BASE}")
        print("  If 0 markets return here, either the series is inactive or")
        print("  the configured key/host combination is not valid for read access.")


if __name__ == "__main__":
    asyncio.run(main())
