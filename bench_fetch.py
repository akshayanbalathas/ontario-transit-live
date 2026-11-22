"""Benchmark the four fetch strategies against the live Ontario feeds.

Each round runs every strategy once, in a rotated order so no strategy always
goes first (warm caches) or last. Rounds are spaced out to stay polite to
the agencies' servers.

Run:  python bench_fetch.py [rounds]
Writes results/fetch_runs.csv (one row per strategy per round) and
results/fetch_feeds.csv (one row per feed per strategy per round).
"""
import csv
import statistics
import sys
import time
from pathlib import Path

from fetchers import STRATEGIES

ROUNDS = int(sys.argv[1]) if len(sys.argv) > 1 else 10
PAUSE = 5  # seconds between rounds

out = Path(__file__).parent / "results"
out.mkdir(exist_ok=True)
names = list(STRATEGIES)
runs, feeds = [], []

for rnd in range(ROUNDS):
    order = names[rnd % len(names):] + names[:rnd % len(names)]
    for name in order:
        start = time.perf_counter()
        vehicles, per_feed = STRATEGIES[name]()
        wall = time.perf_counter() - start
        failed = [a for a, s in per_feed.items() if s["error"]]
        runs.append({"round": rnd, "strategy": name, "seconds": round(wall, 3),
                     "vehicles": len(vehicles), "failed_feeds": ";".join(failed)})
        for agency, s in per_feed.items():
            feeds.append({"round": rnd, "strategy": name, "agency": agency,
                          "seconds": round(s["seconds"], 3), "vehicles": s["vehicles"], "error": s["error"] or ""})
        print(f"round {rnd} {name:20} {wall:6.2f}s {len(vehicles):5} vehicles"
              + (f"  failed: {', '.join(failed)}" if failed else ""), flush=True)
    if rnd < ROUNDS - 1:
        time.sleep(PAUSE)

with open(out / "fetch_runs.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(runs[0]))
    w.writeheader()
    w.writerows(runs)
with open(out / "fetch_feeds.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(feeds[0]))
    w.writeheader()
    w.writerows(feeds)

print("\nmedian wall time per strategy:")
for name in names:
    times = [r["seconds"] for r in runs if r["strategy"] == name]
    print(f"  {name:20} median {statistics.median(times):5.2f}s  max {max(times):5.2f}s")
print("\nmedian latency per feed (sequential runs):")
for agency in sorted({r["agency"] for r in feeds}):
    times = [r["seconds"] for r in feeds if r["agency"] == agency and r["strategy"] == "sequential"]
    print(f"  {agency:8} median {statistics.median(times):5.2f}s  max {max(times):5.2f}s")
