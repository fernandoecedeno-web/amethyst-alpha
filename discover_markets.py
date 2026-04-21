"""
discover_markets.py — market universe & endpoint audit

Answers four questions:
  1. Which Kalshi base URL actually serves crypto (BTC/ETH) markets?
  2. Do KXBTC / KXETH tickers exist at all on any known endpoint?
  3. What series / event_ticker values are present in the current scan?
  4. Are there duplicate tickers across pages (cursor reliability check)?

Run:
    python3 discover_markets.py

Set KALSHI_API_KEY in .env or environment before running.
"""

import asyncio
import os
from collections import Counter
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv
from bot.config import BASE_URL

load_dotenv()

API_KEY  = os.getenv("KALSHI_API_KEY", "")

# ── canonical base URL to probe ──────────────────────────────────────────────
CANDIDATE_URLS = [BASE_URL]

# ── known crypto tickers to probe directly ───────────────────────────────────
PROBE_TICKERS = [
    "KXBTC-26APR1117-B73000",
    "KXBTC-26APR1700-B73000",
    "KXBTC",                   # series-level lookup (may 404, that's useful data)
    "KXETH-26APR1117-B2300",
]

# ── scan limits ───────────────────────────────────────────────────────────────
MAX_PAGES       = 25
BTC_MATCH_LIMIT = 20
PAGE_DELAY      = 0.35
# ─────────────────────────────────────────────────────────────────────────────


def get_headers():
    return {"Content-Type": "application/json", "Authorization": f"Bearer {API_KEY}"}


def _haystack(m):
    return " ".join([
        m.get("ticker", ""),
        m.get("title", ""),
        m.get("subtitle", ""),
        m.get("series_ticker", ""),
        m.get("event_ticker", ""),
    ]).lower()


def is_btc(m):
    h = _haystack(m)
    return "btc" in h or "bitcoin" in h


def is_eth(m):
    h = _haystack(m)
    return "eth" in h or "ethereum" in h


# ── 1. probe each base URL for reachability and market count ──────────────────

async def probe_url(session, base_url):
    """GET /markets?limit=1 and report status + market count hint."""
    try:
        async with session.get(
            f"{base_url}/markets",
            headers=get_headers(),
            params={"status": "open", "limit": 1},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            if r.status == 200:
                data = await r.json()
                has_cursor = bool(data.get("cursor"))
                count = len(data.get("markets", []))
                sample_ticker = (data.get("markets") or [{}])[0].get("ticker", "?")
                return r.status, f"ok  cursor={'yes' if has_cursor else 'no'}  sample_ticker={sample_ticker}"
            else:
                text = (await r.text())[:120]
                return r.status, text
    except Exception as e:
        return None, str(e)[:120]


# ── 2. probe individual ticker endpoints ──────────────────────────────────────

async def probe_ticker(session, base_url, ticker):
    """GET /markets/{ticker} — 200 means it exists, 404 means it doesn't."""
    try:
        async with session.get(
            f"{base_url}/markets/{ticker}",
            headers=get_headers(),
            timeout=aiohttp.ClientTimeout(total=6),
        ) as r:
            if r.status == 200:
                data = await r.json()
                m = data.get("market", {})
                return r.status, f"EXISTS  status={m.get('status')}  close={m.get('close_time','?')}"
            return r.status, (await r.text())[:80]
    except Exception as e:
        return None, str(e)[:80]


# ── 3. bounded page scan on a given base URL ──────────────────────────────────

async def scan_pages(session, base_url):
    all_markets  = []
    btc_matches  = []
    seen_tickers = set()
    dupes        = 0
    cursor       = None
    page         = 0

    while page < MAX_PAGES:
        params = {"status": "open", "limit": 200}
        if cursor:
            params["cursor"] = cursor
        if page > 0:
            await asyncio.sleep(PAGE_DELAY)

        async with session.get(
            f"{base_url}/markets",
            headers=get_headers(),
            params=params,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status == 429:
                print(f"    [429] page {page+1} — sleeping 2s")
                await asyncio.sleep(2.0)
                continue
            if r.status != 200:
                print(f"    [ERROR] HTTP {r.status}")
                break
            data = await r.json()

        batch  = data.get("markets", [])
        cursor = data.get("cursor")
        page  += 1

        page_dupes = sum(1 for m in batch if m.get("ticker") in seen_tickers)
        dupes += page_dupes
        for m in batch:
            seen_tickers.add(m.get("ticker"))

        page_btc = [m for m in batch if is_btc(m)]
        btc_matches.extend(page_btc)
        all_markets.extend(batch)

        print(f"    page {page:3d}: {len(batch):4d} mkts  btc={len(page_btc)}"
              f"  btc_total={len(btc_matches)}  dupes={page_dupes}"
              f"  cursor={'yes' if cursor else 'end'}")

        if len(btc_matches) >= BTC_MATCH_LIMIT:
            print(f"    [STOP] BTC match limit reached")
            break
        if not cursor or not batch:
            break

    return all_markets, btc_matches, dupes, page


# ── main ──────────────────────────────────────────────────────────────────────

async def main():
    if not API_KEY:
        print("ERROR: KALSHI_API_KEY not set")
        return

    print(f"Kalshi Market Universe Audit — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()

    async with aiohttp.ClientSession() as session:

        # ── Step 1: probe each candidate base URL ──────────────────────────
        print("══ 1. BASE URL PROBE ══════════════════════════════════════════")
        reachable = []
        for url in CANDIDATE_URLS:
            status, note = await probe_url(session, url)
            mark = "✓" if status == 200 else "✗"
            print(f"  {mark} [{status}]  {url}")
            print(f"       {note}")
            if status == 200:
                reachable.append(url)
        print()

        # ── Step 2: probe known KXBTC/KXETH tickers on each reachable URL ──
        print("══ 2. DIRECT TICKER PROBE ════════════════════════════════════")
        for url in reachable:
            print(f"  {url}")
            for ticker in PROBE_TICKERS:
                status, note = await probe_ticker(session, url, ticker)
                print(f"    [{status}]  {ticker:<40} {note}")
                await asyncio.sleep(0.2)
        print()

        # ── Step 3: bounded page scan + series breakdown ───────────────────
        current_url = CANDIDATE_URLS[0]   # what the bot currently uses
        print(f"══ 3. PAGE SCAN (current bot URL — max {MAX_PAGES} pages) ════════")
        print(f"  URL: {current_url}")
        all_markets, btc_matches, dupes, pages = await scan_pages(session, current_url)
        print()

        # series/event_ticker breakdown
        series_counts = Counter(
            m.get("series_ticker") or m.get("event_ticker") or "UNKNOWN"
            for m in all_markets
        )
        event_counts = Counter(
            m.get("event_ticker") or "UNKNOWN"
            for m in all_markets
        )
        eth_matches = [m for m in all_markets if is_eth(m)]

        print(f"  Total markets scanned : {len(all_markets)}  (pages={pages})")
        print(f"  Duplicate tickers     : {dupes}  "
              f"{'← cursor unreliable for exhaustive scan' if dupes else '← cursor looks clean'}")
        print(f"  BTC matches           : {len(btc_matches)}")
        print(f"  ETH matches           : {len(eth_matches)}")
        print()

        print(f"  Top 30 series_ticker values:")
        for s, c in series_counts.most_common(30):
            print(f"    {s:<25} {c:5d}")
        print()

        print(f"  Top 20 event_ticker values (first 5000 markets):")
        for e, c in event_counts.most_common(20):
            print(f"    {e:<35} {c:5d}")
        print()

        if btc_matches:
            print(f"  BTC markets found:")
            for m in sorted(btc_matches, key=lambda x: x.get("close_time", ""))[:BTC_MATCH_LIMIT]:
                ticker = m.get("ticker", "?")
                close  = m.get("close_time", "?")
                series = m.get("series_ticker", "?")
                print(f"    {ticker:<42} series={series}  close={close}")
        else:
            print("  *** No BTC markets found on this endpoint. ***")
            print("  Check Step 1 results — a different base URL may be required.")


if __name__ == "__main__":
    asyncio.run(main())
