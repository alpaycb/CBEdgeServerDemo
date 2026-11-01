#!/usr/bin/env python3
"""
Local IoT consumer for the Syngenta demo: reads Capella changes from Kafka and
measures latency from the edge.

Each reading carries up to four timestamps:
  t_edge     edge_ts_ms, stamped when the reading is written at the Lab (laptop clock)
  t_capella  the document's CAS. Couchbase CAS is a hybrid logical clock: the
             wall-clock write time in ns, with the low 16 bits used as a
             logical counter (Capella's clock). Connector only.
  t_kafka    the broker's LogAppendTime (Docker VM clock, synced to the laptop)
  t_local    when this consumer received it (laptop clock)

Segments shown:
  edge -> Capella        Lab -> Site -> App Services -> Capella
  Capella -> Kafka       CDC (DCP) to the local broker
  Kafka -> consumer      local delivery
  end-to-end             t_local - t_edge. Same clock at both ends, so it has
                         no clock-skew error. The two middle segments cross
                         machines: clock skew shifts the split between them,
                         not their sum.

Works with both producers on the same topic:
  - the Couchbase Kafka connector (RawJsonWithMetadataSourceHandler envelope)
  - changes_bridge.py (same envelope, no cas -> only end-to-end is measured)

Env vars:
  KAFKA_BOOTSTRAP        default 127.0.0.1:29092
  KAFKA_TOPIC            default syngenta.iot.metrics
  KAFKA_GROUP            default syngenta-latency-demo
  LATENCY_UI_PORT        default 8082
  LATENCY_WINDOW         default 200 readings for the statistics
  DELAYED_THRESHOLD_MS   default 5000; end-to-end above this is flagged "delayed"

Run:  python3 latency_consumer.py   then open http://127.0.0.1:8082
"""
import json
import os
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from kafka import KafkaConsumer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:29092")
TOPIC = os.environ.get("KAFKA_TOPIC", "syngenta.iot.metrics")
GROUP = os.environ.get("KAFKA_GROUP", "syngenta-latency-demo")
PORT = int(os.environ.get("LATENCY_UI_PORT", "8082"))
WINDOW = int(os.environ.get("LATENCY_WINDOW", "200"))
DELAYED_MS = int(os.environ.get("DELAYED_THRESHOLD_MS", "5000"))

SEGMENTS = ["edge_to_capella", "capella_to_kafka", "kafka_to_consumer", "end_to_end"]

recent = deque(maxlen=WINDOW)
counts = {"readings": 0, "ignored": 0, "no_edge_ts": 0}
lock = threading.Lock()
status = {"connected": False, "error": None, "source": None}


def is_history_doc(doc_id):
    _, sep, tail = (doc_id or "").rpartition("::")
    return bool(sep) and tail.isdigit()


def cas_to_ms(cas):
    """CAS (HLC) -> wall-clock ms; None if absent or implausible."""
    try:
        ms = ((int(cas) >> 16) << 16) // 1_000_000
    except (TypeError, ValueError):
        return None
    return ms if 1_500_000_000_000 < ms < 5_000_000_000_000 else None


def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    k = min(len(s) - 1, max(0, round(p / 100 * (len(s) - 1))))
    return s[k]


def handle(msg):
    t_local = int(time.time() * 1000)
    try:
        envelope = json.loads(msg.value)
    except (TypeError, ValueError):
        counts["ignored"] += 1
        return

    # Connector/bridge envelope: has "event" (deletions carry no "content").
    if isinstance(envelope, dict) and ("event" in envelope or "content" in envelope):
        event = envelope.get("event", "mutation")
        doc_id = envelope.get("key") or (msg.key.decode() if msg.key else None)
        doc = envelope.get("content") or {}
        t_capella = cas_to_ms(envelope.get("cas"))
        source = envelope.get("source", "kafka-connector (DCP)")
    else:  # plain document (e.g. RawJsonSourceHandler)
        event, doc = "mutation", envelope if isinstance(envelope, dict) else {}
        doc_id = msg.key.decode() if msg.key else doc.get("_id")
        t_capella, source = None, "kafka-connector (DCP)"

    # Only individual readings; skip ::latest pointers, deletions, other docs.
    if event != "mutation" or not is_history_doc(doc_id):
        counts["ignored"] += 1
        return
    t_edge = doc.get("edge_ts_ms")
    if not isinstance(t_edge, (int, float)):
        counts["no_edge_ts"] += 1
        return

    t_kafka = msg.timestamp if msg.timestamp and msg.timestamp > 0 else None
    row = {
        "doc_id": doc_id,
        "device": doc.get("device_id", "?"),
        "value": (doc.get("metrics") or [{}])[0].get("value"),
        "unit": (doc.get("metrics") or [{}])[0].get("unit", ""),
        "t_edge": t_edge,
        "edge_to_capella": (t_capella - t_edge) if t_capella else None,
        "capella_to_kafka": (t_kafka - t_capella) if (t_kafka and t_capella) else None,
        "kafka_to_consumer": (t_local - t_kafka) if t_kafka else None,
        "end_to_end": t_local - t_edge,
        "source": source,
    }
    with lock:
        recent.append(row)
        counts["readings"] += 1
        status["source"] = source

    def fmt(v):
        return "   n/a" if v is None else f"{v:6d}"
    print(f"{time.strftime('%H:%M:%S')} {row['device']:32s} "
          f"edge->capella {fmt(row['edge_to_capella'])} ms | capella->kafka {fmt(row['capella_to_kafka'])} ms | "
          f"kafka->local {fmt(row['kafka_to_consumer'])} ms | END-TO-END {fmt(row['end_to_end'])} ms"
          f"{'  <- delayed' if row['end_to_end'] > DELAYED_MS else ''}", flush=True)


def consume_forever():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP,
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
            status.update(connected=True, error=None)
            print(f"Consuming '{TOPIC}' from {BOOTSTRAP} (group {GROUP})", flush=True)
            for msg in consumer:
                handle(msg)
        except Exception as e:  # noqa: BLE001 - keep the demo consumer alive
            status.update(connected=False, error=str(e))
            print(f"Kafka error, retrying in 5s: {e}", flush=True)
            time.sleep(5)


def snapshot():
    with lock:
        rows = list(recent)
        c = dict(counts)
    stats = {}
    for seg in SEGMENTS:
        vals = [r[seg] for r in rows if r[seg] is not None]
        stats[seg] = {"p50": percentile(vals, 50), "p95": percentile(vals, 95),
                      "max": max(vals) if vals else None, "n": len(vals)}
    delayed = sum(1 for r in rows if r["end_to_end"] > DELAYED_MS)
    return {"stats": stats, "rows": rows[-25:][::-1], "counts": c, "delayed": delayed,
            "delayed_threshold_ms": DELAYED_MS, "window": WINDOW, "status": status, "topic": TOPIC}


PAGE = """<!doctype html><html><head><meta charset="utf-8">
<title>Syngenta demo - edge-to-stream latency</title>
<style>
body{font-family:-apple-system,Segoe UI,sans-serif;margin:2rem;background:#fafafa;color:#222}
h1{font-size:1.2rem;margin:0 0 .2rem} p{color:#555;font-size:.9rem;margin:.2rem 0 1rem}
.cards{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:1rem}
.card{background:#fff;border:1px solid #ddd;border-radius:8px;padding:10px 14px;min-width:190px}
.card h2{font-size:.72rem;text-transform:uppercase;letter-spacing:.04em;color:#666;margin:0 0 6px}
.big{font-size:1.5rem;font-weight:600} .sub{font-size:.8rem;color:#666}
.e2e{border-color:#0f6e56} .warn{color:#a32d2d}
table{border-collapse:collapse;width:100%;background:#fff}
td,th{border:1px solid #ddd;padding:7px 9px;text-align:left;font-size:.85rem}
th{background:#f0f0f0;text-transform:uppercase;font-size:.7rem;letter-spacing:.03em}
tr.delayed td{background:#fdf0f0}
</style></head><body>
<h1>Edge &rarr; Capella &rarr; local stream: latency</h1>
<p id="meta">Connecting...</p>
<div class="cards" id="cards"></div>
<table><thead><tr><th>Device</th><th>Reading</th><th>Edge write</th><th>Edge&rarr;Capella</th>
<th>Capella&rarr;Kafka</th><th>Kafka&rarr;consumer</th><th>End-to-end</th></tr></thead>
<tbody id="rows"></tbody></table>
<script>
const LABELS={edge_to_capella:'Edge \\u2192 Capella',capella_to_kafka:'Capella \\u2192 Kafka (CDC)',
 kafka_to_consumer:'Kafka \\u2192 consumer',end_to_end:'End-to-end'};
const ms=v=>v===null||v===undefined?'n/a':v+' ms';
async function tick(){
  const d=await (await fetch('/api')).json();
  const st=d.status;
  document.getElementById('meta').innerHTML =
    (st.connected?'Topic <b>'+d.topic+'</b> via '+(st.source||'waiting for data'):'<span class="warn">Kafka not connected: '+(st.error||'')+'</span>')+
    ' &middot; '+d.counts.readings+' readings &middot; stats over last '+d.window+
    ' &middot; <span class="'+(d.delayed?'warn':'')+'">'+d.delayed+' delayed &gt; '+d.delayed_threshold_ms/1000+'s</span>';
  document.getElementById('cards').innerHTML=Object.keys(LABELS).map(k=>{
    const s=d.stats[k];
    return '<div class="card '+(k==='end_to_end'?'e2e':'')+'"><h2>'+LABELS[k]+'</h2><div class="big">'+ms(s.p50)+
      '</div><div class="sub">median &middot; p95 '+ms(s.p95)+' &middot; max '+ms(s.max)+'</div></div>';}).join('');
  document.getElementById('rows').innerHTML=d.rows.map(r=>
    '<tr class="'+(r.end_to_end>d.delayed_threshold_ms?'delayed':'')+'"><td>'+r.device+'</td><td>'+r.value+' '+r.unit+
    '</td><td>'+new Date(r.t_edge).toLocaleTimeString()+'</td><td>'+ms(r.edge_to_capella)+'</td><td>'+ms(r.capella_to_kafka)+
    '</td><td>'+ms(r.kafka_to_consumer)+'</td><td><b>'+ms(r.end_to_end)+'</b></td></tr>').join('');
}
tick(); setInterval(tick,2000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/api"):
            body, ctype = json.dumps(snapshot()).encode(), "application/json"
        else:
            body, ctype = PAGE.encode(), "text/html; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    threading.Thread(target=consume_forever, daemon=True).start()
    print(f"Latency view at http://127.0.0.1:{PORT}", flush=True)
    try:
        ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
    except KeyboardInterrupt:
        print("stopped")
