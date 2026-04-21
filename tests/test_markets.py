import asyncio
import aiohttp
import os
from dotenv import load_dotenv
from bot.config import BASE_URL

load_dotenv()

API_KEY = os.getenv("KALSHI_API_KEY")


async def fetch_all_btc():
    headers = {"Authorization": f"Bearer {API_KEY}"}
    cursor = None
    page = 0
    total_markets = 0
    total_btc = 0

    async with aiohttp.ClientSession() as session:
        while page < 10:
            params = {}
            if cursor:
                params["cursor"] = cursor

            async with session.get(f"{BASE_URL}/markets", headers=headers, params=params) as r:
                print(f"PAGE {page+1} STATUS:", r.status)
                data = await r.json()

            markets = data.get("markets", [])
            cursor = data.get("cursor")
            total_markets += len(markets)

            btc = [m for m in markets if "KXBTC" in (m.get("ticker", "") or "")]
            total_btc += len(btc)

            print(f"PAGE {page+1}: markets={len(markets)} btc={len(btc)}")

            for m in btc[:10]:
                print(
                    m.get("ticker"),
                    "| status=", m.get("status"),
                    "| close_time=", m.get("close_time"),
                )

            page += 1
            if not cursor or not markets:
                break

    print("TOTAL SCANNED:", total_markets)
    print("TOTAL BTC FOUND:", total_btc)


asyncio.run(fetch_all_btc())
