"""Four ways to fetch every feed: the I/O-bound half of the paper.

Each strategy returns (vehicles, per_feed) where per_feed maps
agency -> {"seconds", "start", "end", "vehicles", "error"}; start/end are
time.perf_counter() readings so a caller can lay the requests on a timeline.
A failed feed is recorded and skipped; it never fails the whole snapshot.
"""
import asyncio
import ssl
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx

from feeds import FEEDS, HEADERS, HTTP_TIMEOUT, MAX_RETRIES, RETRY_BACKOFF, fetch_feed, parse_vehicles


def _stats(start: float, vehicles: list, error: Exception | None = None) -> dict:
    end = time.perf_counter()
    return {"seconds": end - start, "start": start, "end": end, "vehicles": len(vehicles),
            "error": type(error).__name__ if error else None}


def _fetch_one(agency: str, url: str):
    start = time.perf_counter()
    try:
        vehicles = parse_vehicles(agency, fetch_feed(url))
        return agency, vehicles, _stats(start, vehicles)
    except Exception as e:
        return agency, [], _stats(start, [], e)


def fetch_sequential(feeds=FEEDS):
    vehicles, per_feed = [], {}
    for agency, url in feeds.items():
        _, vs, stats = _fetch_one(agency, url)
        vehicles.extend(vs)
        per_feed[agency] = stats
    return vehicles, per_feed


def fetch_threading(feeds=FEEDS):
    results = {}

    def worker(agency, url):
        results[agency] = _fetch_one(agency, url)   # distinct key per thread: no lock needed

    threads = [threading.Thread(target=worker, args=item) for item in feeds.items()]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    vehicles = [v for _, vs, _ in results.values() for v in vs]
    return vehicles, {agency: stats for agency, _, stats in results.values()}


def fetch_thread_pool(feeds=FEEDS, max_workers=6):
    vehicles, per_feed = [], {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch_one, agency, url) for agency, url in feeds.items()]
        for future in as_completed(futures):            # completion order: no head-of-line blocking
            agency, vs, stats = future.result()
            vehicles.extend(vs)                          # only the main thread mutates the list
            per_feed[agency] = stats
    return vehicles, per_feed


async def _fetch_one_async(client: httpx.AsyncClient, agency: str, url: str):
    start = time.perf_counter()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            vehicles = parse_vehicles(agency, resp.content)
            return agency, vehicles, _stats(start, vehicles)
        except Exception as e:
            if attempt == MAX_RETRIES:
                return agency, [], _stats(start, [], e)
            await asyncio.sleep(RETRY_BACKOFF * 2 ** (attempt - 1))   # non-blocking backoff


# Built once: httpx's default loads the certifi bundle on every client (~0.6 s here),
# which costs more than all six requests. The stdlib context uses the OS trust store.
_SSL_CONTEXT = ssl.create_default_context()


async def _fetch_all_async(feeds):
    async with httpx.AsyncClient(headers=HEADERS, timeout=HTTP_TIMEOUT, follow_redirects=True,
                                 verify=_SSL_CONTEXT) as client:
        return await asyncio.gather(*(_fetch_one_async(client, a, u) for a, u in feeds.items()))


def fetch_asyncio(feeds=FEEDS):
    results = asyncio.run(_fetch_all_async(feeds))
    vehicles = [v for _, vs, _ in results for v in vs]
    return vehicles, {agency: stats for agency, _, stats in results}


STRATEGIES = {
    "sequential": fetch_sequential,
    "threading": fetch_threading,
    "ThreadPoolExecutor": fetch_thread_pool,
    "asyncio": fetch_asyncio,
}
