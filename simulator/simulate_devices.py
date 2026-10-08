#!/usr/bin/env python3
"""
Simulates two Smart Meter devices in one R&D lab, pushing sensor readings to
the Lab Edge Server via its REST Data API.

Env vars:
  LAB_EDGE_URL            default https://localhost:59840
  EDGE_DB                 default labdata
  EDGE_USER / EDGE_PASSWORD  default empty = anonymous access (matches the
                             fallback 'db' database, which has no users file)
  SENSOR_INTERVAL_SECONDS  default 5
  SITE_NAME / LAB_NAME     default uk / lab1

Writes TWO documents per reading:
  <site>::<lab>::<device_id>::<timestamp>   immutable, one per reading
                                             (this is what proves no data is
                                             lost while the lab is offline)
  <site>::<lab>::<device_id>::latest        overwritten every time, cheap
                                             "current value" pointer
"""
import os
import time
import random
import requests
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

EDGE_URL = os.environ.get("LAB_EDGE_URL", "")
DB = os.environ.get("EDGE_DB", "")  # fallback: image's default anonymous database, default collection
USER = os.environ.get("EDGE_USER", "")  # empty = send no Authorization header (anonymous db has no users file)
PASSWORD = os.environ.get("EDGE_PASSWORD", "")
AUTH = (USER, PASSWORD) if USER else None
INTERVAL = int(os.environ.get("SENSOR_INTERVAL_SECONDS", "5"))
SITE = os.environ.get("SITE_NAME", "uk")
LAB = os.environ.get("LAB_NAME", "lab1")

DEVICES = [
    "ambr250-bioreactor01-vessel01",
    "ambr250-bioreactor02-vessel01",
]

def history_doc_id(device_id: str, ts: int) -> str:
    return f"{SITE}::{LAB}::{device_id}::{ts}"


def latest_doc_id(device_id: str) -> str:
    return f"{SITE}::{LAB}::{device_id}::latest"


def reading(device_id: str, ts: int) -> dict:
    return {
        "site": SITE,
        "lab": LAB,
        "device_id": device_id,
        "timestamp": ts,
        "metrics": [
            {"name": "stir_speed", "value": round(random.uniform(780.0, 820.0), 1), "unit": "rpm"}
        ],
    }


# Edge Server uses optimistic concurrency control: updating an EXISTING
# document requires its current revision number (?rev=... or If-Match).
# The history doc is always brand new (unique timestamp per write), so it
# never needs one. The 'latest' doc is deliberately overwritten every
# interval, so we must track and send its revision each time.
_latest_rev: dict[str, str] = {}


def put(url: str, doc: dict):
    """Returns (status_string, parsed_json_body_or_None)."""
    try:
        r = requests.put(url, json=doc, auth=AUTH, verify=False, timeout=5)
        if r.status_code < 300:
            try:
                return "OK", r.json()
            except ValueError:
                return "OK", None
        return f"HTTP {r.status_code}: {r.text[:200]}", None
    except Exception as e:  # noqa: BLE001 - demo script, keep it simple and visible
        return f"FAILED ({e})", None


def put_latest(device_id: str, doc: dict) -> str:
    base_url = f"{EDGE_URL}/{DB}/{latest_doc_id(device_id)}"
    rev = _latest_rev.get(device_id)
    url = f"{base_url}?rev={rev}" if rev else base_url
    status, body = put(url, doc)

    if status.startswith("HTTP 409"):
        # Doc already existed with a revision we didn't know about (e.g. left
        # over from a previous run of this demo) -- fetch the current
        # revision once and retry, instead of failing forever.
        try:
            g = requests.get(base_url, auth=AUTH, verify=False, timeout=5)
            current_rev = g.json().get("_rev") or g.json().get("rev") if g.status_code == 200 else None
        except Exception:  # noqa: BLE001
            current_rev = None
        if current_rev:
            status, body = put(f"{base_url}?rev={current_rev}", doc)

    if body:
        new_rev = body.get("rev") or body.get("_rev")
        if new_rev:
            _latest_rev[device_id] = new_rev
    return status


def push(device_id: str) -> None:
    ts = int(time.time())
    doc = reading(device_id, ts)
    history_status, _ = put(f"{EDGE_URL}/{DB}/{history_doc_id(device_id, ts)}", doc)
    latest_status = put_latest(device_id, doc)
    print(
        f"[{time.strftime('%H:%M:%S')}] {device_id:35s} "
        f"history={history_status:6s} latest={latest_status:6s}  {doc['metrics'][0]}"
    )


if __name__ == "__main__":
    print(f"Simulating {len(DEVICES)} devices every {INTERVAL}s -> {EDGE_URL}/{DB}")
    print("Ctrl+C to stop.\n")
    try:
        while True:
            for dev in DEVICES:
                push(dev)
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        print("\nStopped.")