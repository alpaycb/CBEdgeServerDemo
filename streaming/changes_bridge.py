#!/usr/bin/env python3
"""
Plan B for streaming Capella changes to local Kafka: App Services changes feed.

Use this instead of the Kafka connector when the network blocks the
connector's direct cluster connection (TLS port 11207). This bridge only needs
HTTPS (443) to App Services, using the same app user as the dashboard.

It long-polls the App Endpoint's _changes feed and publishes each reading to
the same topic, in the same envelope as the connector, so latency_consumer.py
works unchanged. There is no CAS in the changes feed, so the consumer shows
end-to-end latency only, not the Capella-side split.

Positioning: the connector (DCP) is the production CDC path. This bridge is an
application-tier alternative: it sees only documents the app user has channel
access to, and it is a polling client rather than a database change stream.

Env vars:
  CAPELLA_APP_SERVICES_URL  REQUIRED, same as the dashboard, e.g.
                            https://<host>.apps.cloud.couchbase.com:4984/<app-endpoint>
  CAPELLA_USER / CAPELLA_PASSWORD   App Services app user
  KAFKA_BOOTSTRAP           default 127.0.0.1:29092
  KAFKA_TOPIC               default syngenta.iot.metrics
"""
import json
import os
import sys
import time

import requests
from kafka import KafkaProducer

URL = os.environ.get("CAPELLA_APP_SERVICES_URL", "").rstrip("/")
USER = os.environ.get("CAPELLA_USER", "")
PASSWORD = os.environ.get("CAPELLA_PASSWORD", "")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:29092")
TOPIC = os.environ.get("KAFKA_TOPIC", "syngenta.iot.metrics")

if not URL:
    sys.exit("CAPELLA_APP_SERVICES_URL is not set.")


def is_history_doc(doc_id):
    _, sep, tail = (doc_id or "").rpartition("::")
    return bool(sep) and tail.isdigit()


session = requests.Session()
if USER:
    session.auth = (USER, PASSWORD)


def current_seq():
    """Start from 'now': the endpoint's current update_seq, else the end of the feed."""
    r = session.get(f"{URL}/", timeout=15)
    r.raise_for_status()
    seq = r.json().get("update_seq")
    if seq is not None:
        return seq
    r = session.get(f"{URL}/_changes", params={"since": 0}, timeout=60)
    r.raise_for_status()
    return r.json().get("last_seq", 0)


producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, linger_ms=0,
                         key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode())
since = current_seq()
print(f"Bridging {URL}/_changes -> Kafka topic '{TOPIC}' from seq {since}", flush=True)

while True:
    try:
        r = session.get(f"{URL}/_changes",
                        params={"feed": "longpoll", "since": since, "include_docs": "true",
                                "timeout": 25000},
                        timeout=40)
        if r.status_code in (401, 403):
            sys.exit(f"App Services rejected the app user (HTTP {r.status_code}).")
        r.raise_for_status()
        body = r.json()
        sent = 0
        for change in body.get("results", []):
            doc = change.get("doc")
            doc_id = change.get("id")
            if change.get("deleted") or not doc or not is_history_doc(doc_id):
                continue
            producer.send(TOPIC, key=doc_id, value={
                "event": "mutation", "key": doc_id, "content": doc,
                "source": "app-services-changes (bridge)",
            })
            sent += 1
        if sent:
            producer.flush()
            print(f"{time.strftime('%H:%M:%S')} forwarded {sent} reading(s)", flush=True)
        since = body.get("last_seq", since)
    except KeyboardInterrupt:
        print("stopped")
        break
    except Exception as e:  # noqa: BLE001 - keep the bridge alive across transient errors
        print(f"bridge error, retrying in 3s: {e}", flush=True)
        time.sleep(3)
