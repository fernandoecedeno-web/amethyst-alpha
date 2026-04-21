import asyncio
import os
from pathlib import Path
from typing import Any

import aiohttp
from dotenv import load_dotenv

from bot.config import BASE_URL, KALSHI_API_KEY


ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"
TIMEOUT_SECS = 5


def load_env_safely() -> None:
    load_dotenv(dotenv_path=str(ENV_PATH), override=False)


def mask_secret(value: str) -> str:
    if not value:
        return "(missing)"
    if len(value) <= 4:
        return "*" * len(value)
    return f"{'*' * max(0, len(value) - 4)}{value[-4:]}"


def discover_api_vars() -> dict[str, str]:
    api_vars: dict[str, str] = {}
    for key, value in os.environ.items():
        upper = key.upper()
        if any(token in upper for token in ("API", "KEY", "SECRET", "TOKEN")):
            api_vars[key] = value
    return dict(sorted(api_vars.items()))


async def request_json(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
) -> tuple[bool, int | None, str]:
    try:
        async with session.request(
            method,
            url,
            headers=headers,
            params=params,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECS),
        ) as response:
            text = await response.text()
            return True, response.status, text[:300]
    except asyncio.TimeoutError:
        return False, None, "timeout"
    except aiohttp.ClientConnectorError as exc:
        return False, None, f"dns/connect error: {exc}"
    except aiohttp.ClientError as exc:
        return False, None, f"client error: {exc}"
    except Exception as exc:
        return False, None, f"unexpected error: {exc}"


async def test_kalshi(session: aiohttp.ClientSession) -> tuple[str, str]:
    api_key = KALSHI_API_KEY.strip() if KALSHI_API_KEY else ""
    if not api_key:
        return "❌ Failed", "missing KALSHI_API_KEY"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    ok, status, body = await request_json(
        session,
        "GET",
        f"{BASE_URL}/markets",
        headers=headers,
        params={"limit": 1, "status": "open"},
    )
    if not ok:
        return "❌ Failed", f"{BASE_URL} -> {body}"
    if status == 200:
        return "✅ Connected", f"runtime auth OK via {BASE_URL} (key …{api_key[-4:]})"
    if status in (401, 403):
        return "❌ Failed", f"{BASE_URL} -> HTTP {status} auth rejected"
    return "❌ Failed", f"{BASE_URL} -> HTTP {status} {body}"


async def test_coinbase(session: aiohttp.ClientSession) -> tuple[str, str]:
    ok, status, body = await request_json(
        session,
        "GET",
        "https://api.coinbase.com/v2/prices/BTC-USD/spot",
    )
    if not ok:
        return "❌ Failed", body
    if status == 200:
        return "✅ Connected", "public spot endpoint reachable"
    return "❌ Failed", f"HTTP {status} {body}"


async def test_kraken(session: aiohttp.ClientSession) -> tuple[str, str]:
    ok, status, body = await request_json(
        session,
        "GET",
        "https://api.kraken.com/0/public/Ticker",
        params={"pair": "XBTUSD"},
    )
    if not ok:
        return "❌ Failed", body
    if status == 200:
        return "✅ Connected", "public ticker endpoint reachable"
    return "❌ Failed", f"HTTP {status} {body}"


async def test_yahoo(session: aiohttp.ClientSession) -> tuple[str, str]:
    ok, status, body = await request_json(
        session,
        "GET",
        "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC",
        headers={"User-Agent": "Mozilla/5.0"},
        params={"interval": "1d", "range": "1d"},
    )
    if not ok:
        return "❌ Failed", body
    if status == 200:
        return "✅ Connected", "public chart endpoint reachable"
    return "❌ Failed", f"HTTP {status} {body}"


async def main() -> None:
    load_env_safely()

    print(f"Loaded .env from: {ENV_PATH}")
    print()
    print("API VARIABLES:")
    api_vars = discover_api_vars()
    if not api_vars:
        print("- none found")
    else:
        for key, value in api_vars.items():
            print(f"- {key} = {mask_secret(value)}")

    print()

    async with aiohttp.ClientSession() as session:
        kalshi_status, kalshi_reason = await test_kalshi(session)
        coinbase_status, coinbase_reason = await test_coinbase(session)
        kraken_status, kraken_reason = await test_kraken(session)
        yahoo_status, yahoo_reason = await test_yahoo(session)

    print("API STATUS:")
    print(f"- Kalshi:   {kalshi_status} ({kalshi_reason})")
    print(f"- Coinbase: {coinbase_status} ({coinbase_reason})")
    print(f"- Kraken:   {kraken_status} ({kraken_reason})")
    print(f"- Yahoo:    {yahoo_status} ({yahoo_reason})")
    print()
    print("Run with:")
    print("python3 test_api_connections.py")


if __name__ == "__main__":
    asyncio.run(main())
