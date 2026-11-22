"""Live dashboard: the whole pipeline running, plus the benchmark charts.

Run:  python server.py     then open http://localhost:8000

Concurrency in this file:
  poller thread    producer: fetches all feeds every POLL_SECONDS (thread pool inside)
  analyzer thread  consumer: takes the newest snapshot from a queue and runs the
                   nearest-stop analysis on a persistent process pool
  HTTP server      ThreadingHTTPServer: one thread per request, reading shared
                   state under a lock
"""
import csv
import json
import queue
import statistics
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import analyzer
import fetchers
from feeds import FEEDS

POLL_SECONDS = 30
HISTORY = 120
PORT = 8000
ROOT = Path(__file__).parent

snapshots: queue.Queue = queue.Queue(maxsize=1)   # bounded buffer between producer and consumer
state = {"snapshot": None, "history": []}
state_lock = threading.Lock()
race_lock = threading.Lock()                      # one race at a time; politeness to the feeds


def offer(item):
    """Put the newest snapshot in the queue, dropping a stale one the consumer hasn't taken."""
    while True:
        try:
            snapshots.put_nowait(item)
            return
        except queue.Full:
            try:
                snapshots.get_nowait()
            except queue.Empty:
                pass


def poller(stop: threading.Event):
    while not stop.is_set():
        t0 = time.perf_counter()
        vehicles, per_feed = fetchers.fetch_thread_pool()
        offer({"polled_at": time.time(), "vehicles": vehicles,
               "fetch_seconds": time.perf_counter() - t0,
               "feeds": {a: {"seconds": s["seconds"], "vehicles": s["vehicles"], "error": s["error"]}
                         for a, s in per_feed.items()}})
        stop.wait(POLL_SECONDS)


def consumer(pool):
    while True:
        item = snapshots.get()
        t0 = time.perf_counter()
        enriched, tasks = analyzer.analyze_dynamic(item["vehicles"], pool)
        item.update(vehicles=enriched, analyze_seconds=time.perf_counter() - t0, analyze_tasks=len(tasks))
        with state_lock:
            state["snapshot"] = item
            state["history"].append({"polled_at": item["polled_at"], "fetch_seconds": item["fetch_seconds"],
                                     "analyze_seconds": item["analyze_seconds"], "vehicles": len(enriched)})
            del state["history"][:-HISTORY]


def run_race():
    """Run each fetch strategy once and return every request's start/end on a shared timeline."""
    rows = []
    for name, fn in fetchers.STRATEGIES.items():
        t0 = time.perf_counter()
        _, per_feed = fn()
        total = time.perf_counter() - t0
        rows.append({"strategy": name, "total": total,
                     "feeds": [{"agency": a, "start": s["start"] - t0, "end": s["end"] - t0, "error": s["error"]}
                               for a, s in per_feed.items()]})
    return rows


def _read_csv(name):
    path = ROOT / "results" / name
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _medians(rows, key):
    groups = {}
    for r in rows:
        groups.setdefault(r[key], []).append(float(r["seconds"]))
    return [{"name": k, "seconds": statistics.median(v)} for k, v in groups.items()]


def bench_summary():
    tasks = _read_csv("analyze_tasks.csv")
    return {
        "fetch": _medians(_read_csv("fetch_runs.csv"), "strategy"),
        "analyze": _medians(_read_csv("analyze_runs.csv"), "strategy"),
        "agency_tasks": _medians([t for t in tasks if t["strategy"] == "processes, by agency"], "task"),
    }


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT / "web"), **kwargs)

    def _json(self, payload, status=200):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/api/snapshot":
            with state_lock:
                snap, history = state["snapshot"], list(state["history"])
            return self._json({"snapshot": snap, "history": history, "poll_seconds": POLL_SECONDS,
                               "agencies": list(FEEDS), "workers": analyzer.WORKERS})
        if self.path == "/api/bench":
            return self._json(bench_summary())
        return super().do_GET()

    def do_POST(self):
        if self.path == "/api/race":
            if not race_lock.acquire(blocking=False):
                return self._json({"error": "a race is already running"}, 409)
            try:
                return self._json(run_race())
            finally:
                race_lock.release()
        self.send_error(404)

    def log_message(self, fmt, *args):
        pass   # keep the console for pipeline output


def main():
    pool = analyzer.make_process_pool()
    analyzer.warm_up(pool)
    stop = threading.Event()
    threading.Thread(target=poller, args=(stop,), daemon=True, name="poller").start()
    threading.Thread(target=consumer, args=(pool,), daemon=True, name="analyzer").start()
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"dashboard at http://localhost:{PORT}  (Ctrl+C to stop)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()
        pool.shutdown(cancel_futures=True)


if __name__ == "__main__":   # required on Windows: pool workers re-import this module
    main()
