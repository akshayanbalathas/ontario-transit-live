"""Download each agency's static GTFS once and keep just its stop list.

Run:  python stops.py        (re-downloads anything older than 7 days)
Writes data/stops/<agency>.csv with stop_id, stop_name, stop_lat, stop_lon.
"""
import csv
import io
import time
import zipfile
from pathlib import Path
from urllib.request import Request, urlopen

from feeds import HEADERS

STATIC_FEEDS = {
    "TTC": "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/7795b45e-e65a-4465-81fc-c36b9dfff169/resource/cfb6b2b8-6191-41e3-bda1-b175c51148cb/download/opendata_ttc_schedules.zip",
    "MiWay": "https://www.miapp.ca/GTFS/google_transit.zip",
    "YRT": "https://www.yrt.ca/google/google_transit.zip",
    "HSR": "https://opendata.hamilton.ca/GTFS-Static/google_transit.zip",
    "LTC": "https://www.londontransit.ca/gtfsfeed/google_transit.zip",
    "DRT": "https://maps.durham.ca/OpenDataGTFS/GTFS_Durham_TXT.zip",
}

STOPS_DIR = Path(__file__).parent / "data" / "stops"
MAX_AGE = 7 * 24 * 3600


def _find_stops_txt(archive: zipfile.ZipFile) -> str:
    # Some agencies nest the GTFS files one folder down.
    names = [n for n in archive.namelist() if n.rsplit("/", 1)[-1] == "stops.txt"]
    if not names:
        raise ValueError("no stops.txt in archive")
    return min(names, key=len)


def download_stops(agency: str, url: str) -> int:
    with urlopen(Request(url, headers=HEADERS), timeout=120) as resp:
        archive = zipfile.ZipFile(io.BytesIO(resp.read()))
    with archive.open(_find_stops_txt(archive)) as raw:
        rows = list(csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig")))
    # Keep boarding stops only; stations and entrances (location_type 1-4) aren't where vehicles stop.
    stops = [r for r in rows if r.get("location_type", "") in ("", "0") and r.get("stop_lat")]
    STOPS_DIR.mkdir(parents=True, exist_ok=True)
    with open(STOPS_DIR / f"{agency}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["stop_id", "stop_name", "stop_lat", "stop_lon"])
        for r in stops:
            w.writerow([r["stop_id"], r["stop_name"].strip(), r["stop_lat"], r["stop_lon"]])
    return len(stops)


def ensure_stops():
    for agency, url in STATIC_FEEDS.items():
        path = STOPS_DIR / f"{agency}.csv"
        if path.exists() and time.time() - path.stat().st_mtime < MAX_AGE:
            continue
        print(f"downloading {agency} stops...", flush=True)
        print(f"  {download_stops(agency, url)} stops", flush=True)


if __name__ == "__main__":
    ensure_stops()
