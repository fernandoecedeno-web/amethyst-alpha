import asyncio

import aiohttp

from bot.config import BASE_URL, KALSHI_API_KEY


TIMEOUT_SECS = 5


async def main() -> None:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {KALSHI_API_KEY}",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{BASE_URL}/markets",
                headers=headers,
                params={"limit": 1, "status": "open"},
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECS),
            ) as response:
                print(f"BASE_URL: {BASE_URL}")
                print("DNS/connect: OK")
                print(f"HTTP status: {response.status}")
                print(f"Bearer auth accepted: {'yes' if response.status == 200 else 'no'}")
    except asyncio.TimeoutError:
        print(f"BASE_URL: {BASE_URL}")
        print("DNS/connect: failed (timeout)")
        print("HTTP status: n/a")
        print("Bearer auth accepted: no")
    except aiohttp.ClientConnectorError as exc:
        print(f"BASE_URL: {BASE_URL}")
        print(f"DNS/connect: failed ({exc})")
        print("HTTP status: n/a")
        print("Bearer auth accepted: no")
    except Exception as exc:
        print(f"BASE_URL: {BASE_URL}")
        print(f"DNS/connect: failed ({exc})")
        print("HTTP status: n/a")
        print("Bearer auth accepted: no")


if __name__ == "__main__":
    asyncio.run(main())
