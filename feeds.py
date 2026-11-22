"""Open Ontario GTFS-Realtime vehicle-position feeds (no API key needed).

Verified working on 2026-11-21. Check each agency's open-data licence before
publishing anything built from these feeds.
"""
import time
from urllib.request import Request, urlopen

from google.transit import gtfs_realtime_pb2

FEEDS = {
    "TTC": "https://bustime.ttc.ca/gtfsrt/vehicles",
    "MiWay": "https://www.miapp.ca/GTFS_RT/Vehicle/VehiclePositions.pb",
    "YRT": "https://rtu.york.ca/gtfsrealtime/VehiclePositions",
    "HSR": "https://opendata.hamilton.ca/GTFS-RT/GTFS_VehiclePositions.pb",
    "LTC": "http://gtfs.ltconline.ca/Vehicle/VehiclePositions.pb",
    "DRT": "https://drtonline.durhamregiontransit.com/gtfsrealtime/VehiclePositions",
}

HEADERS = {"User-Agent": "csci4060u-transit-demo/1.0 (student project)"}
HTTP_TIMEOUT = 20
MAX_RETRIES = 3
RETRY_BACKOFF = 0.8


def fetch_feed(url: str) -> bytes:
    """Blocking GET with retry and exponential backoff (0.8 s, 1.6 s)."""
    attempt = 0
    while True:
        attempt += 1
        try:
            with urlopen(Request(url, headers=HEADERS), timeout=HTTP_TIMEOUT) as resp:
                return resp.read()
        except Exception:
            if attempt >= MAX_RETRIES:
                raise
            time.sleep(RETRY_BACKOFF * 2 ** (attempt - 1))


def parse_vehicles(agency: str, body: bytes) -> list[dict]:
    """Decode a feed into plain dicts, dropping vehicles not on a route."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(body)
    vehicles = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        if not v.trip.route_id:
            continue
        vehicles.append({
            "agency": agency,
            "vehicle_id": v.vehicle.id or entity.id,
            "route": v.trip.route_id,
            "lat": v.position.latitude,
            "lon": v.position.longitude,
            "timestamp": v.timestamp or feed.header.timestamp,
        })
    return vehicles
