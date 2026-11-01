#!/usr/bin/env python3
"""Create or update the Couchbase source connector (Capella -> local Kafka).

Uses PUT /connectors/<name>/config, so running it again updates the config
instead of failing. Then waits for the connector and its task to report RUNNING.

Env vars:
  CB_CONNECTION        REQUIRED: Capella connection string or hostname,
                       e.g. couchbases://cb.abc123.cloud.couchbase.com
  CB_USERNAME / CB_PASSWORD
                       REQUIRED: a Capella *database* credential with read
                       access to the bucket (not the App Services app user)
  CB_BUCKET            default syngenta-poc
  CB_COLLECTIONS       default iotdata.metrics (scope.collection, comma-separated)
  KAFKA_TOPIC          default syngenta.iot.metrics
  CB_PERSISTENCE_POLLING
                       default 100ms. The connector waits until a change is
                       persisted on all replicas before publishing, so a
                       failover rollback can't leak into Kafka. 0 publishes
                       immediately (lower latency, rollback risk).
  CONNECT_URL          default http://127.0.0.1:8083
  CONNECTOR_NAME       default syngenta-capella-source
"""
import os
import sys
import time

import requests

CONNECT_URL = os.environ.get("CONNECT_URL", "http://127.0.0.1:8083").rstrip("/")
NAME = os.environ.get("CONNECTOR_NAME", "syngenta-capella-source")


def required(var):
    value = os.environ.get(var, "").strip()
    if not value:
        sys.exit(f"{var} is not set (see the docstring at the top of this file).")
    return value


host = required("CB_CONNECTION")
for prefix in ("couchbases://", "couchbase://"):
    if host.startswith(prefix):
        host = host[len(prefix):]
host = host.split("?")[0].rstrip("/")

config = {
    "connector.class": "com.couchbase.connect.kafka.CouchbaseSourceConnector",
    "tasks.max": "1",
    "couchbase.seed.nodes": host,
    "couchbase.enable.tls": "true",          # Capella: TLS on; Capella CA is bundled and trusted
    "couchbase.bootstrap.timeout": "30s",
    "couchbase.username": required("CB_USERNAME"),
    "couchbase.password": required("CB_PASSWORD"),
    "couchbase.bucket": os.environ.get("CB_BUCKET", "syngenta-poc"),
    "couchbase.collections": os.environ.get("CB_COLLECTIONS", "_default.metrics"),
    "couchbase.topic": os.environ.get("KAFKA_TOPIC", "syngenta.iot.metrics"),
    # Envelope with event type, key, cas and the document under "content".
    # The cas is what gives us a Capella-side write time per reading.
    "couchbase.source.handler": "com.couchbase.connect.kafka.handler.source.RawJsonWithMetadataSourceHandler",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "couchbase.event.filter": "com.couchbase.connect.kafka.filter.AllPassFilter",
    # Resume where it left off; on first start begin with new changes only, so
    # old readings don't flood the latency figures.
    "couchbase.stream.from": "SAVED_OFFSET_OR_NOW",
    "couchbase.persistence.polling.interval": os.environ.get("CB_PERSISTENCE_POLLING", "100ms"),
    "couchbase.flow.control.buffer": "16m",
    "couchbase.compression": "ENABLED",
}

try:
    r = requests.put(f"{CONNECT_URL}/connectors/{NAME}/config", json=config, timeout=30)
except requests.ConnectionError:
    sys.exit(f"Kafka Connect is not reachable at {CONNECT_URL}. Is the streaming stack up? "
             "It can take ~30s after 'docker compose up' before the REST API answers.")
if r.status_code not in (200, 201):
    sys.exit(f"Connect rejected the config: HTTP {r.status_code}\n{r.text}")
print(f"Connector '{NAME}' {'created' if r.status_code == 201 else 'updated'}. Waiting for RUNNING ...")

state = None
for _ in range(30):
    time.sleep(2)
    s = requests.get(f"{CONNECT_URL}/connectors/{NAME}/status", timeout=10).json()
    tasks = s.get("tasks", [])
    state = s.get("connector", {}).get("state"), [t.get("state") for t in tasks]
    if state[0] == "RUNNING" and tasks and all(t == "RUNNING" for t in state[1]):
        print(f"RUNNING. Capella changes now stream to topic '{config['couchbase.topic']}'.")
        sys.exit(0)
    failed = [t for t in tasks if t.get("state") == "FAILED"]
    if failed:
        trace = failed[0].get("trace", "")
        print("Task FAILED. First lines of the error:\n" + "\n".join(trace.splitlines()[:8]))
        print("\nUsual causes: this machine's public IP not in the Capella cluster's Allowed IP "
              "list; a database credential without read access to the bucket; outbound TLS "
              "port 11207 blocked by the network (then use changes_bridge.py instead).")
        sys.exit(1)

print(f"Not RUNNING after 60s, last state: {state}. Check: docker logs syngenta-connect")
sys.exit(1)
