# Ontario Transit, Live

Polls six open GTFS-Realtime feeds (TTC, MiWay, YRT, HSR, LTC, DRT), finds the
nearest stop for every vehicle, and shows it all on a live map. Built to
demonstrate Python concurrency for CSCI 4060U.

| Part | Workload | Mechanism |
| --- | --- | --- |
| `fetchers.py` | I/O-bound: 6 HTTP requests | sequential, `threading`, `ThreadPoolExecutor`, `asyncio` |
| `analyzer.py` | CPU-bound: ~1,400 vehicles x ~23,500 stops | sequential, threads, `ProcessPoolExecutor` (by agency, static chunks, dynamic chunks) |
| `server.py` | Live pipeline | producer thread -> `queue.Queue` -> consumer thread + process pool; `ThreadingHTTPServer` |

## Run

Needs Python 3.10 or newer. No API keys.

```
git clone https://github.com/akshayanbalathas/ontario-transit-live.git
cd ontario-transit-live
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python stops.py          # one-time: download each agency's stop list
.venv\Scripts\python bench_fetch.py    # results/fetch_*.csv
.venv\Scripts\python bench_analyze.py  # results/analyze_*.csv
.venv\Scripts\python server.py         # http://localhost:8000  (add #race to auto-start the race)
```

`results/` holds the benchmark CSVs from our runs (Python 3.13, 8-core laptop).

## Data and licence

Code: MIT (see `LICENSE`). Transit data is fetched live from each agency's
open-data program (TTC via City of Toronto Open Data, MiWay, YRT, HSR, LTC, DRT)
and is not stored in this repository; it remains under each agency's terms.
Map tiles: © OpenStreetMap contributors.
