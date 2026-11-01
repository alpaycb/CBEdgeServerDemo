#!/usr/bin/env python3
"""Download and unpack the Couchbase Kafka Connector into ./plugins (run once).

The Kafka Connect container mounts ./plugins and loads the connector from it.
Version 4.3.4 is pinned: Capella requires 4.0.1 or later.
"""
import os
import sys
import urllib.request
import zipfile

VERSION = os.environ.get("CB_CONNECTOR_VERSION", "4.3.4")
NAME = f"couchbase-kafka-connect-couchbase-{VERSION}"
URL = f"https://packages.couchbase.com/clients/kafka/{VERSION}/{NAME}.zip"
HERE = os.path.dirname(os.path.abspath(__file__))
PLUGINS = os.path.join(HERE, "plugins")
TARGET = os.path.join(PLUGINS, NAME)

if os.path.isdir(TARGET):
    print(f"Already installed: {TARGET}")
    sys.exit(0)

os.makedirs(PLUGINS, exist_ok=True)
zip_path = os.path.join(PLUGINS, f"{NAME}.zip")
print(f"Downloading {URL} ...")
urllib.request.urlretrieve(URL, zip_path)
with zipfile.ZipFile(zip_path) as z:
    z.extractall(PLUGINS)
os.remove(zip_path)

jar = os.path.join(TARGET, "lib", f"kafka-connect-couchbase-{VERSION}.jar")
if not os.path.isfile(jar):
    sys.exit(f"Unexpected archive layout, connector jar not found at {jar}")
print(f"Installed connector {VERSION} into {TARGET}")
