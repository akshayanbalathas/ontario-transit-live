"""Benchmark the nearest-stop analysis on one frozen live snapshot.

Run:  python bench_analyze.py [repeats]
Writes results/snapshot.json (the vehicles analysed, so runs are comparable),
results/analyze_runs.csv (one row per strategy per repeat) and
results/analyze_tasks.csv (per-task worker times, which show load imbalance).
"""
import csv
import json
import statistics
import sys
import time
from pathlib import Path

import analyzer
from fetchers import fetch_thread_pool

REPEATS = int(sys.argv[1]) if len(sys.argv) > 1 else 3


def main():
    out = Path(__file__).parent / "results"
    out.mkdir(exist_ok=True)
    vehicles, _ = fetch_thread_pool()
    vehicles.sort(key=lambda v: (v["agency"], v["route"]))   # agencies contiguous, as a naive split sees them
    (out / "snapshot.json").write_text(json.dumps(vehicles))
    counts = {}
    for v in vehicles:
        counts[v["agency"]] = counts.get(v["agency"], 0) + 1
    print(f"{len(vehicles)} vehicles: {counts}; workers={analyzer.WORKERS}", flush=True)

    start = time.perf_counter()
    procs = analyzer.make_process_pool()
    analyzer.warm_up(procs)
    pool_startup = time.perf_counter() - start
    threads = analyzer.make_thread_pool()
    print(f"process pool startup (incl. loading stops): {pool_startup:.2f}s", flush=True)

    cases = [
        ("sequential", analyzer.analyze_sequential, None),
        ("threads, dynamic", analyzer.analyze_dynamic, threads),
        ("processes, by agency", analyzer.analyze_by_agency, procs),
        ("processes, static chunks", analyzer.analyze_static_chunks, procs),
        ("processes, dynamic", analyzer.analyze_dynamic, procs),
    ]
    runs, tasks = [], []
    for rep in range(REPEATS):
        for name, fn, executor in cases:
            t0 = time.perf_counter()
            enriched, task_seconds = fn(vehicles, executor)
            wall = time.perf_counter() - t0
            assert len(enriched) == len(vehicles)
            runs.append({"repeat": rep, "strategy": name, "seconds": round(wall, 3),
                         "tasks": len(task_seconds), "vehicles": len(vehicles)})
            tasks += [{"repeat": rep, "strategy": name, "task": label, "seconds": round(s, 3)}
                      for label, s in task_seconds]
            print(f"repeat {rep} {name:26} {wall:6.2f}s  ({len(task_seconds)} tasks)", flush=True)
    procs.shutdown()
    threads.shutdown()

    for fname, rows in (("analyze_runs.csv", runs), ("analyze_tasks.csv", tasks)):
        with open(out / fname, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)

    base = statistics.median(r["seconds"] for r in runs if r["strategy"] == "sequential")
    print("\nmedian wall time:")
    for name, _, _ in cases:
        med = statistics.median(r["seconds"] for r in runs if r["strategy"] == name)
        print(f"  {name:26} {med:6.2f}s  speedup {base / med:4.1f}x")
    print("\nby-agency task times (repeat 0):")
    for t in tasks:
        if t["repeat"] == 0 and t["strategy"] == "processes, by agency":
            print(f"  {t['task']:8} {t['seconds']:6.2f}s")


if __name__ == "__main__":   # required on Windows: worker processes re-import this module
    main()
