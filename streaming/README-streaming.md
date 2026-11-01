# Streaming extension: Capella → local Kafka (CDC) + Edge latency

Extends the edge-sync demo without changing its architecture. The only edit to
existing files is one added field in `simulator/simulate_devices.py`:
`edge_ts_ms`, the edge write time in milliseconds. The dashboard and purger
ignore it.

```
Smart Meters → Lab Edge Server → Site Edge Server → App Services → Capella
                                                                     │  CDC (DCP)
                                       Couchbase Kafka Source Connector
                                                                     ▼
                                    local Kafka topic <customer>.iot.metrics
                                                                     ▼
                          latency_consumer.py  (local IoT processing + latency view)
```

## What it shows
1. **Near-real-time extraction:** every reading that lands in Capella is
   streamed to a local Kafka topic by Couchbase's supported Kafka Source
   Connector, which reads the database change stream (DCP).
2. **Latency from the edge, per hop:**

| Segment | Computed from | Clock(s) |
|---|---|---|
| Edge → Capella (Lab → Site → App Services → Capella) | CAS − `edge_ts_ms` | laptop vs Capella |
| Capella → Kafka (CDC) | Kafka LogAppendTime − CAS | Capella vs Docker VM |
| Kafka → consumer | receive time − LogAppendTime | Docker VM vs laptop |
| **End-to-end** | receive time − `edge_ts_ms` | **laptop only: exact** |

The CAS is Couchbase's per-document hybrid logical clock: the server's wall-clock
write time in nanoseconds. That gives a Capella-side timestamp without changing
any data. End-to-end is measured on a single clock, so it carries no skew
error. Skew between machines moves the split between the middle segments, not
their total.

## Plan B: `changes_bridge.py`
The connector connects to the Capella cluster directly on TLS port 11207.
Corporate networks and VPNs often block that outbound. The bridge reads the App
Services changes feed over HTTPS (443) with the dashboard's app user and writes
to the same topic in the same envelope, so the consumer works unchanged. It
can't see CAS, so the view shows end-to-end latency only. Test the connector on
the network you'll present from; switch if it's blocked.

## Capella prerequisites (connector only)
- A **database credential** (cluster → Settings → Cluster Access) with read
  access to `<bucket>`. This is not the App Services app user.
- Your current public IP in the **cluster's** Allowed IP list. App Services has
  its own list, so having added your IP there isn't enough.
- The connection string from the cluster's **Connect** tab
  (`couchbases://cb.<id>.cloud.couchbase.com`).

## Setup (once)
```
cd streaming
pip3 install -r requirements.txt
python3 setup_connector_plugin.py            # downloads connector 4.3.4 into ./plugins
docker compose -f docker-compose.streaming.yml up -d
```
Wait ~30s for Kafka Connect, then register the connector:
```
CB_CONNECTION="couchbases://cb.<id>.cloud.couchbase.com" \
CB_USERNAME="<db-credential>" CB_PASSWORD="<password>" \
python3 register_connector.py
```
Defaults: bucket `<bucket>`, collection `iotdata.metrics`, topic
`<bucket>.iot.metrics`. Override with `CB_BUCKET`, `CB_COLLECTIONS`, `KAFKA_TOPIC`.

Windows PowerShell: set variables with `$env:CB_CONNECTION="..."` and so on,
then `python register_connector.py`.

## Run
Restart the simulator so new readings carry `edge_ts_ms` (older readings are
counted as "no edge timestamp" and skipped). Then:
```
python3 latency_consumer.py          # open http://127.0.0.1:8081
```
Plan B instead of the connector:
```
CAPELLA_APP_SERVICES_URL=... CAPELLA_USER=... CAPELLA_PASSWORD=... \
python3 changes_bridge.py
```
Don't run both at once: every reading would arrive twice. To pause the
connector: `curl -X PUT http://127.0.0.1:8083/connectors/customer-capella-source/pause`
(and `/resume`).

## Demo flow
1. Open the edge dashboard (8080) and the latency view (8081) side by side.
   Normal operation: readings flow through every tier; point at the
   end-to-end median and p95.
2. Cut the Lab: `./scripts/outage.sh cut`. Lab keeps writing, Pending sync
   climbs, the stream goes quiet.
3. Restore: `./scripts/outage.sh restore`. The backlog arrives in Kafka flagged
   **delayed**, with end-to-end latency roughly equal to the outage length.
   Every reading is there, in order per device.
4. If the purger is running, mention that edge storage is freed while the
   central record and the stream are untouched. Edge tombstones are rejected at
   App Services, so no deletions reach Capella or Kafka.

## Talking points
- **Executives:** edge readings reach the systems you already run, in near
  real time, with measured latency and no loss during outages. Use the figures
  you measure tonight, not estimates.
- **Architects:** CDC from the database change stream (DCP), resumable from
  saved offsets, ordered per document. It feeds the Kafka that already sits in
  your architecture, so the downstream pipeline stays as it is.
- **Latency trade-off:** `CB_PERSISTENCE_POLLING` (default 100ms) holds each
  change until it's persisted on all replicas, so a failover rollback can never
  leak into Kafka. Setting it to `0` publishes immediately, at the cost of
  that guarantee. Present it as a choice they control.
- **Suggested extra PoC success criteria:** p95 edge-to-stream latency under an
  agreed target; zero readings missing from the topic after an outage.

## If something looks wrong
- **Connector task FAILED:** the registration script prints the error. Usual
  causes: IP not in the cluster's Allowed IP list, the credential lacks bucket
  read access, or port 11207 is blocked (use plan B).
  `docker logs customer-connect` shows the full detail.
- **Negative or odd Capella → Kafka values:** clock skew. Docker Desktop's VM
  clock can drift after the laptop sleeps; restarting Docker Desktop fixes it.
  End-to-end is unaffected.
- **Consumer shows nothing:** check the topic has data:
  `docker exec customer-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic customer.iot.metrics --max-messages 1`
