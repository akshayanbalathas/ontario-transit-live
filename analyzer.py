"""Nearest stop for every vehicle: the CPU-bound half of the paper.

The kernel is a deliberately plain brute-force search in pure Python
(every vehicle against every stop of its agency), so it holds the GIL the
whole time. The strategies differ only in how the vehicles are split into
tasks, which is the same question as OpenMP's schedule clause:

    by_agency      one task per agency           ~ uneven static partition
    static_chunks  WORKERS equal-count chunks    ~ schedule(static)
    dynamic        many 25-vehicle tasks         ~ schedule(dynamic, 25)

Every strategy returns (enriched_vehicles, task_seconds) where task_seconds
lists (label, seconds) for each task, measured inside the worker.
"""
import csv
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

STOPS_DIR = Path(__file__).parent / "data" / "stops"
WORKERS = max(1, (os.cpu_count() or 2) // 2)   # physical cores on an SMT machine
DYNAMIC_CHUNK = 25
EARTH_RADIUS_M = 6_371_000

_stops: dict[str, list[tuple[str, float, float]]] = {}   # per process, loaded once


def load_stops() -> dict:
    stops = {}
    for path in STOPS_DIR.glob("*.csv"):
        with open(path, newline="", encoding="utf-8") as f:
            stops[path.stem] = [(r["stop_name"], float(r["stop_lat"]), float(r["stop_lon"]))
                                for r in csv.DictReader(f)]
    return stops


def init_worker():
    """Pool initializer: each worker reads the stop lists from disk once,
    so they are never pickled and sent with a task."""
    global _stops
    _stops = load_stops()


def haversine_m(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def nearest_stops(vehicles: list[dict]) -> list[dict]:
    if not _stops:
        init_worker()
    out = []
    for v in vehicles:
        best, best_d = None, math.inf
        for name, lat, lon in _stops.get(v["agency"], ()):
            d = haversine_m(v["lat"], v["lon"], lat, lon)
            if d < best_d:
                best, best_d = name, d
        out.append({**v, "nearest_stop": best, "stop_distance_m": round(best_d, 1)})
    return out


def timed_task(label: str, vehicles: list[dict]):
    start = time.perf_counter()
    result = nearest_stops(vehicles)
    return label, result, time.perf_counter() - start


def _run(tasks, executor):
    futures = [executor.submit(timed_task, label, vs) for label, vs in tasks]
    enriched, task_seconds = [], []
    for f in futures:
        label, result, secs = f.result()
        enriched.extend(result)
        task_seconds.append((label, secs))
    return enriched, task_seconds


def _by_agency(vehicles):
    groups = {}
    for v in vehicles:
        groups.setdefault(v["agency"], []).append(v)
    return list(groups.items())


def _chunks(vehicles, size):
    return [(f"chunk {i // size}", vehicles[i:i + size]) for i in range(0, len(vehicles), size)]


def analyze_sequential(vehicles, executor=None):
    label, result, secs = timed_task("all", vehicles)
    return result, [(label, secs)]


def analyze_by_agency(vehicles, executor):
    return _run(_by_agency(vehicles), executor)


def analyze_static_chunks(vehicles, executor):
    size = math.ceil(len(vehicles) / WORKERS)
    return _run(_chunks(vehicles, size), executor)


def analyze_dynamic(vehicles, executor):
    return _run(_chunks(vehicles, DYNAMIC_CHUNK), executor)


def make_process_pool(workers=WORKERS) -> ProcessPoolExecutor:
    return ProcessPoolExecutor(max_workers=workers, initializer=init_worker)


def make_thread_pool(workers=WORKERS) -> ThreadPoolExecutor:
    init_worker()   # threads share this process's copy
    return ThreadPoolExecutor(max_workers=workers)


def _warm(_):
    time.sleep(0.2)


def warm_up(executor, workers=WORKERS):
    """Make every worker start (and run its initializer) before timing."""
    list(executor.map(_warm, range(workers * 2)))
