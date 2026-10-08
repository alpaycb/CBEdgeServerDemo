#!/usr/bin/env python3
"""
Minimal live dashboard for the Edge Server edge-sync demo.

Polls the Lab Edge Server, Site Edge Server, and (optionally) Capella App
Services server-side, and serves a single auto-refreshing HTML page from the
same origin -- avoids any browser CORS issues with the Edge Servers'
self-signed TLS certs.

Env vars:
  LAB_EDGE_URL              default https://localhost:59840
  SITE_EDGE_URL             default https://localhost:59841
  CAPELLA_APP_SERVICES_URL  optional, e.g. https://<host>.apps.cloud.couchbase.com/<app-endpoint>
  EDGE_USER / EDGE_PASSWORD           default demo / demo123
  CAPELLA_USER / CAPELLA_PASSWORD     required if CAPELLA_APP_SERVICES_URL is set

Run:  python3 dashboard_server.py   then open http://localhost:8080
"""
import os
import json
import requests
import urllib3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

LAB_URL = os.environ.get("LAB_EDGE_URL", "")
SITE_URL = os.environ.get("SITE_EDGE_URL", "")
CAPELLA_URL = os.environ.get("CAPELLA_APP_SERVICES_URL", "")
EDGE_USER = os.environ.get("EDGE_USER", "")
EDGE_PASSWORD = os.environ.get("EDGE_PASSWORD", "")
AUTH = (EDGE_USER, EDGE_PASSWORD) if EDGE_USER and EDGE_PASSWORD else None
CAPELLA_USER = os.environ.get("CAPELLA_USER", "")
CAPELLA_PASSWORD = os.environ.get("CAPELLA_PASSWORD", "")

SITE = os.environ.get("SITE_NAME", "uk")
LAB = os.environ.get("LAB_NAME", "lab1")

DEVICE_IDS = ["ambr250-bioreactor01-vessel01", "ambr250-bioreactor02-vessel01"]


def fetch_doc(base_url, db, doc_id, user, pwd):
    if not base_url:
        return {"error": "not configured"}
    path = f"{base_url}/{db}/{doc_id}" if db else f"{base_url}/{doc_id}"
    auth = (user, pwd) if user and pwd else AUTH
    try:
        r = requests.get(path, auth=auth, verify=False, timeout=3)
        if r.status_code == 200:
            return r.json()
        return {"error": f"HTTP {r.status_code}"}
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


def list_history_doc_ids(base_url, db, prefix, user, pwd):
    """Best-effort: list document IDs in a keyspace matching a prefix.

    Endpoint path is our best reading of the Edge Server REST API reference's
    "Get all documents in the keyspace" operation -- NOT confirmed by running
    it. Verify the real path/response shape during your spike (curl it and
    look at the JSON) and adjust this function if it differs. Returns None on
    any failure so the dashboard degrades gracefully instead of crashing.
    """
    if not base_url:
        return None
    try:
        r = requests.get(f"{base_url}/{db}/_all_docs", auth=AUTH, verify=False, timeout=3)
        if r.status_code != 200:
            return None
        data = r.json()
        rows = data.get("rows", data if isinstance(data, list) else [])
        ids = [row.get("id") or row.get("key") for row in rows]
        # exclude the ::latest pointer doc -- only count individual readings
        return {i for i in ids if i and i.startswith(prefix) and not i.endswith("::latest")}
    except Exception:  # noqa: BLE001
        return None


def build_pending():
    pending = {}
    for dev in DEVICE_IDS:
        prefix = f"{SITE}::{LAB}::{dev}::"
        lab_ids = list_history_doc_ids(LAB_URL, "labdata.metrics", prefix, EDGE_USER, EDGE_PASSWORD)
        site_ids = list_history_doc_ids(SITE_URL, "sitedata.metrics", prefix, EDGE_USER, EDGE_PASSWORD)
        #lab_ids = list_history_doc_ids(LAB_URL, "db", prefix, EDGE_USER, EDGE_PASSWORD)
        #site_ids = list_history_doc_ids(SITE_URL, "db", prefix, EDGE_USER, EDGE_PASSWORD)
        if lab_ids is None or site_ids is None:
            pending[dev] = {"count": None, "note": "endpoint unavailable -- see README"}
        else:
            missing = sorted(lab_ids - site_ids)
            pending[dev] = {"count": len(missing), "sample": missing[-5:]}
    return pending


def build_status():
    # Reads the cheap 'latest' pointer doc per device -- see simulator/simulate_devices.py.
    # The immutable <device_id>::<timestamp> history docs are what actually prove no
    # reading was lost during an outage; inspect those directly (e.g. via curl or the
    # Edge Server's changes feed) rather than through this dashboard if you need to
    # verify completeness, not just "what's current."
    out = {"lab": {}, "site": {}}
    for dev in DEVICE_IDS:
        doc_id = f"{SITE}::{LAB}::{dev}::latest"
        out["lab"][dev] = fetch_doc(LAB_URL, "labdata.metrics", doc_id, EDGE_USER, EDGE_PASSWORD)
        out["site"][dev] = fetch_doc(SITE_URL, "sitedata.metrics", doc_id, EDGE_USER, EDGE_PASSWORD)
        #out["lab"][dev] = fetch_doc(LAB_URL, "db", doc_id, EDGE_USER, EDGE_PASSWORD)
        #out["site"][dev] = fetch_doc(SITE_URL, "db", doc_id, EDGE_USER, EDGE_PASSWORD)
    if CAPELLA_URL:
        out["capella"] = {}
        for dev in DEVICE_IDS:
            doc_id = f"{SITE}::{LAB}::{dev}::latest"
            out["capella"][dev] = fetch_doc(CAPELLA_URL, "", doc_id, CAPELLA_USER, CAPELLA_PASSWORD)
    out["_pending"] = build_pending()
    return out


PAGE = """<!doctype html><html><head><meta charset="utf-8">
<title>Edge Server Sync Demo - Data Synchronization Dashboard</title>
<style>
body{font-family:-apple-system,sans-serif;margin:2rem;background:#fafafa;color:#222}
h1{font-size:1.2rem;margin-bottom:0.2rem}
p{color:#555;font-size:0.9rem}
table{border-collapse:collapse;width:100%;margin-top:1rem;background:#fff}
td,th{border:1px solid #ddd;padding:10px;text-align:left;font-size:0.9rem}
th{background:#f0f0f0;text-transform:uppercase;font-size:0.75rem;letter-spacing:0.03em}
.stale{color:#a32d2d;font-weight:500} .fresh{color:#0f6e56}
</style></head><body>
<h1>Lab &rarr; Site &rarr; Capella: live sync status</h1>
<p>Auto-refreshes every 3s. Disconnect the lab-to-site Docker network to simulate an
outage and watch the Site / Capella columns stop advancing while Lab keeps writing.</p>
<div id="root">Loading...</div>
<script>
async function tick(){
  const r = await fetch('/api/status'); const d = await r.json();
  const pending = d['_pending'] || {};
  const tiers = Object.keys(d).filter(t => t !== '_pending');
  const devices = new Set();
  tiers.forEach(t => Object.keys(d[t]).forEach(x => devices.add(x)));
  let html = '<table><tr><th>Device</th>' + tiers.map(t => '<th>' + t + '</th>').join('') + '<th>Pending sync</th></tr>';
  devices.forEach(dev => {
    html += '<tr><td>' + dev + '</td>';
    tiers.forEach(t => {
      const v = d[t][dev];
      if (v && v.error) {
        html += '<td class="stale">' + v.error + '</td>';
      } else if (v && v.metrics) {
        const ts = new Date(v.timestamp * 1000).toLocaleTimeString();
        html += '<td class="fresh">' + v.metrics[0].value + ' ' + v.metrics[0].unit + ' @ ' + ts + '</td>';
      } else {
        html += '<td>-</td>';
      }
    });
    const p = pending[dev];
    if (!p || p.count === null || p.count === undefined) {
      html += '<td>n/a</td>';
    } else if (p.count === 0) {
      html += '<td class="fresh">0 - fully synced</td>';
    } else {
      html += '<td class="stale">' + p.count + ' reading(s) waiting</td>';
    }
    html += '</tr>';
  });
  html += '</table>';
  document.getElementById('root').innerHTML = html;
}
tick();
setInterval(tick, 3000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/status":
            body = json.dumps(build_status()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        else:
            body = PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, *args):  # quiet the default request logging
        pass


if __name__ == "__main__":
    print("Dashboard running at http://localhost:8081")
    ThreadingHTTPServer(("0.0.0.0", 8081), Handler).serve_forever()