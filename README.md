# Edge Server Sync demo — Lab Edge Server → Site Edge Server → Capella App Services → Capella

Minimal, single-R&D-site demo proving the sync mechanism end to end, including
offline buffering and catch-up. Runs on a MacBook (Apple Silicon), Docker for
the two Edge Servers only; the simulator and dashboard run natively in Python.

## What this proves
- Two simulated Smart Meter devices write to a **Lab Edge Server** (Docker).
- The Lab Edge Server continuously pushes to a **Site Edge Server** (Docker).
- The Site Edge Server continuously pushes to **Capella App Services**, landing
  in **Couchbase Capella**.
- Killing the Docker network between Lab and Site simulates the lab losing
  internet: the lab keeps accepting writes locally; nothing propagates
  upstream until the link is restored, at which point it catches up.

## Why named collections, not `_default`
Capella App Services App Endpoints are scoped to specific named collections.
If an Edge Server replication doesn't specify `collections` (or the local
database doesn't have that collection declared), Edge Server falls back to
`_default`, which your App Endpoint doesn't recognize — this fails with
`"Collection property not specified and default collection is not configured
for this database"`. Fix: declare the same named collection
(`iotdata.metrics`, matching the Capella bucket/scope/collection you
created) on **every** database and replication in the chain — both local
databases (`labdata`, `sitedata`) and both replication legs (lab→site,
site→Capella). REST calls then address it as one dot-joined keyspace segment:
`{db}.{scope}.{collection}`, e.g. `labdata.iotdata.metrics`, not
`{db}/{scope}.{collection}` as separate path segments.

## Document schema
Each reading writes two documents (see `simulator/simulate_devices.py`):
- `uk::lab1::<device_id>::<timestamp>` — immutable, one per reading. This is
  what proves no reading is lost while the lab is offline: every one exists
  independently and syncs on reconnect, in order.
- `uk::lab1::<device_id>::latest` — overwritten every write, a cheap pointer
  for "what's the current value" (used by the dashboard). No query
  infrastructure needed to read it.

The JSON payload (`device_id`/`timestamp`/`metrics`, no site/lab)
doesn't need to change for this — `site`/`lab` are added and the compound key
is built at the ingestion boundary (the simulator here; a thin adapter in
front of the Lab Edge Server in a real deployment), not in device firmware.

## What this deliberately does NOT do (yet)
- No conflict resolution logic beyond Couchbase's default (not exercised here
  — single writer per document, one-way replication).
- No query/changes-feed browsing of the full history docs from the dashboard
  — it only reads the `::latest` pointer. To verify *nothing* was lost during
  an outage (not just that the latest value is current), inspect the
  timestamped docs directly (curl, or the Edge Server's changes feed) rather
  than relying on the dashboard.

## Mac-specific gotcha: use 127.0.0.1, not localhost
Docker Desktop for Mac publishes ports on both IPv4 and IPv6, but the IPv6
loopback (`::1`) path can complete a TCP connection without actually
delivering data into the container — `curl https://localhost:...` can hang
indefinitely as a result, since `localhost` resolves to `::1` first on most
Macs. Always use `https://127.0.0.1:<port>` in this project, on the host side
(container-to-container calls using the service names `lab-edge-server` /
`site-edge-server` are unaffected — that's a different, working, network
path). All the defaults below have been set to `127.0.0.1` for this reason.

## Before you start: a 10-minute spike (recommended)
Two config details below (the exact `users.json` schema, and the REST write
path `/​<db>​/​<doc-id>`) are my best reading of Couchbase's documented
examples, not something I could verify by running the software myself. Before
wiring the full demo:
```
docker run -d --name spike -p 59840:59840 couchbase/edge-server
curl -4 -k https://127.0.0.1:59840/
docker logs spike
```
Confirms the image runs on Apple Silicon and shows you the real default
config/log format. Then try one write against the pattern in
`simulator/simulate_devices.py` before trusting the multi-container setup.
`docker rm -f spike` when done.

## Prerequisites
1. Docker Desktop (or OrbStack/Colima) running on the Mac.
2. Virtual Environment Setup: initialize your virtual environment and install the required dependencies:
```bash
# Create and activate environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```
Verify your .env contains valid credentials for Couchbase Capella, AWS S3, and your LLM provider.

3. A Couchbase Capella account with:
   - A database (bucket), scope, and collection created — this project assumes
     `<customer>-poc` / `iotdata` / `metrics` throughout. If you named yours
     differently, update the `collections` arrays in `config/lab-config.json`
     and `config/site-config.json`, plus `EDGE_DB` in the simulator and the
     keyspace strings in the dashboard, to match.
   - **App Services** enabled on that database.
   - An **App Endpoint** created against that collection, with a sync
     function/access rule permissive enough for the demo (e.g. allow all
     authenticated writes — tighten later, not needed for a demo).
   - A database credential (user/password) with read/write access, for the
     Site Edge Server to authenticate as.
   - The App Endpoint's public WebSocket URL (Capella UI shows this — looks
     like `wss://<host>.apps.cloud.couchbase.com/<app-endpoint-name>`).
   - Capella's **Allowed IP** list updated to include your current public IP
     (Capella blocks all traffic by default).

If you don't have this set up yet and want to run the Lab→Site half first
without Capella, just leave `config/site-config.json`'s `replications` block
in place with placeholder values — Site Edge Server will retry and fail
quietly in the logs; Lab→Site sync still works independently.

## Setup
```
cd <your-folder>
# Fill in real values:
#   config/site-config.json   -> target URL, auth user/password (Capella App Endpoint)
#   config/lab-config.json    -> auth password (must match lab-replicator below)
#   config/site-users.json    -> lab-replicator password (must match lab-config.json)
docker compose up -d
docker compose logs -f
```
Watch the logs for both containers starting cleanly and (once Capella is
configured) the site→Capella replication connecting.

## Run the demo
(If you edit the environment variables in your local .env file, you can skip the inline variables. Make a copy from .env.example and edit all necessary values before running the scripts.)

Terminal 1 — devices:
```
cd simulator
SENSOR_INTERVAL_SECONDS=5 
python3 simulate_devices.py
```
Terminal 2 — dashboard:
```
cd dashboard
CAPELLA_APP_SERVICES_URL="https://<host>.apps.cloud.couchbase.com/<app-endpoint>" \
CAPELLA_USER="<user>" CAPELLA_PASSWORD="<password>" \
python3 dashboard_server.py
```
Open **http://localhost:8080** — you should see all three columns (Lab, Site,
Capella) advancing together every few seconds.

## Simulate a lost connection
```
docker network disconnect <your_folder>_edge-link lab-edge-server
```
(Confirm the exact network name first with `docker network ls` — Compose
prefixes it with the project folder name.)

Watch the dashboard: **Lab** keeps advancing (devices are still writing
locally), **Site** and **Capella** freeze at their last synced value.

Reconnect:
```
docker network connect <your_folder>_edge-link lab-edge-server
```
Site and Capella should catch up to Lab's current value within a few seconds
— that catch-up moment is the one to narrate live in the room.

## Live demo script: showing the outage and the catch-up

The dashboard's rightmost column, **Pending sync**, counts readings that exist
on the Lab Edge Server but haven't reached the Site Edge Server yet. Suggested
narration flow:

1. Let it run normally for ~30s — point out all three tiers advancing
   together, Pending sync at 0.
2. Disconnect:
   ```
   docker network disconnect <your_folder>_edge-link lab-edge-server
   ```
3. Point out: Lab keeps advancing, Site/Capella freeze, **Pending sync starts
   climbing** — "these N readings are safely stored locally and will sync the
   moment connectivity returns, nothing is lost."
4. Optional, for a technical audience — show the replication task itself
   reporting the outage, straight from Edge Server's own REST API:
   ```
   curl -4 -sk -u demo:demo123 https://127.0.0.1:59840/_active_tasks | python3 -m json.tool
   ```
   (Exact field names not yet confirmed against a live server — inspect the
   output during your spike and note the field that shows connection state;
   that's the one to point at live.)
5. Reconnect:
   ```
   docker network connect <your_folder>_edge-link lab-edge-server
   ```
6. Watch Site/Capella catch up to Lab's latest value within a few seconds,
   and **Pending sync drop back to 0** — that's the "zero data loss" proof
   point for the PoC scorecard.

## Post-sync purge worker (`purger/purge_synced.py`)
Frees edge storage by removing readings that Capella has confirmed.

**How it stays safe.** Edge Server has no local-only purge endpoint: a REST
DELETE creates a tombstone, and tombstones replicate. So the worker:
- confirms each reading against **Capella App Services**, the final
  destination, never against Site only. A Lab tombstone sent before Capella
  has the reading would overwrite Site's copy before it reached the cloud,
  and the reading would be lost on every tier.
- deletes on the **Lab only**. The tombstone replicates to Site and cleans
  Site's copy automatically.
- relies on the App Endpoint's sync function to **reject** the tombstone
  when it reaches App Services, so the central copy survives.
- never touches the `::latest` pointer docs.

**Mandatory: add the deletion guard to the App Endpoint's sync function**
(Capella UI → App Services → your App Endpoint → Access Control / sync
function). Keep your existing channel logic and add the `_deleted` check
at the top:
```js
function (doc, oldDoc, meta) {
  // Edge tiers free storage by deleting after Capella confirms receipt.
  // Reject those tombstones so the central copy is never removed.
  if (doc._deleted) {
    throw({forbidden: "deletions from edge replication are not accepted"});
  }
  channel("iotdata");
}
```
The app user the worker and dashboard use must be able to read channel
`iotdata` (or grant `*` for the demo). Otherwise every check returns 403,
nothing is ever purged, and the worker's log will say so.

**Prerequisite:** the Site→Capella replication must be re-enabled (it is
disabled in the current anonymous fallback). The local `db` uses the default
collection, so either bind the App Endpoint to the bucket's default
collection or re-add `"collections": ["iotdata.metrics"]` on both tiers (this
works on the Windows machine).

**Run** (third terminal). Preview first, then go live:
```
cd purger
CAPELLA_APP_SERVICES_URL="https://<host>.apps.cloud.couchbase.com:4984/<app-endpoint>" \
CAPELLA_USER="<user>" CAPELLA_PASSWORD="<password>" \
DRY_RUN=true python3 purge_synced.py

# once the sync guard is in place:
SYNC_GUARD_CONFIRMED=yes PURGE_MIN_AGE_SECONDS=30 \
CAPELLA_APP_SERVICES_URL=... CAPELLA_USER=... CAPELLA_PASSWORD=... \
python3 purge_synced.py
```
On Windows PowerShell, set the variables with `$env:NAME="value"` first,
then run `python purge_synced.py`.

**Verify the guard before trusting it:** after the first live cycle, check
that a purged reading still exists in Capella (Capella UI → Documents), and
look for the rejected tombstone in `docker logs site-edge-server`. If the
reading is gone from Capella, stop the worker: the sync function isn't
rejecting deletions.

**Demo narration:** during an outage the worker logs "not yet in Capella"
and purges nothing, since the readings exist only at the edge. After
reconnect, Capella confirms them and the worker frees Lab and Site storage.

**Known limitations, and questions for the Edge Server PM:**
- Tombstones stay on Lab and Site. They are small, but they accumulate.
  Ask: is there a local purge, document expiration (TTL), or tombstone
  compaction option for Edge Server?
- Ask: does the replication config support a push filter that skips
  deletions? That would remove the dependency on the sync-function guard.
- Checks are one GET per reading, capped by `PURGE_BATCH_LIMIT`. Fine for
  the demo; production volumes need a bulk check.

## Troubleshooting notes
- **404 on simulator writes**: the REST path assumption
  (`<edge-server>/<db>/<doc-id>`) is the most likely thing to need a tweak —
  check the Edge Server logs for the actual route, or the Edge Server REST API
  reference under docs.couchbase.com/couchbase-edge-server.
- **"Password of user 'x' is not a valid bcrypt hash"**: `users.json` stores
  bcrypt hashes, not plaintext, confirmed by this exact error. The files in
  this project already use real hashes for `demo`/`demo123` (both servers)
  and `lab-replicator`/`labsync123` (site server only). If you want different
  passwords, regenerate the hash and update `config/*-users.json`:
  ```
  pip3 install bcrypt
  python3 -c "import bcrypt; print(bcrypt.hashpw(b'yourpassword', bcrypt.gensalt()).decode())"
  ```
  The **plaintext** password still goes in two other places, unchanged: the
  `EDGE_USER`/`EDGE_PASSWORD` env vars the simulator/dashboard use (HTTP Basic
  Auth sends plaintext over TLS), and the `replications[].auth.password`
  field in `lab-config.json` (the live credential Lab sends when it connects
  out to Site) — that field must match whatever plaintext password hashes to
  the value stored in `site-users.json`, not be a hash itself.
- **TLS warnings in the simulator/dashboard**: expected — Edge Server uses a
  self-signed cert by default. `verify=False` is fine for a local demo, never
  for anything real.
- **Site→Capella not connecting**: almost always Capella's Allowed IP list
  (add your current IP) or the App Endpoint not being Active yet.

## Checking what's actually in the anonymous 'db' database
Don't assume default scope/collection — check it:
```
curl -4 -k -v --max-time 10 https://127.0.0.1:59840/db
```
Returns a `collections` object listing what's actually there (per Edge
Server's REST API reference). Test without `-u` first, since no users file is
configured in this fallback setup — worth confirming the server handles a
missing Authorization header gracefully before assuming it's needed at all.

## Reverting to the full setup (named collections + auth)
Once Lab↔Site sync is stable on this anonymous fallback, revert deliberately,
one variable at a time, rather than all at once:
1. Re-add `"collections": ["iotdata.metrics"]` to both databases and the
   lab→site replication first. Confirm sync still works before continuing.
2. Re-add the `users` file references and bcrypt-hashed credentials. Confirm
   again.
3. Only then re-add the Site→Capella replication block, with its own
   `"collections": ["iotdata.metrics"]`, matching your App Endpoint's scope.
This turns "did the last fix work" back into a single-variable question at
each step, instead of the multi-variable guessing we were doing before.
