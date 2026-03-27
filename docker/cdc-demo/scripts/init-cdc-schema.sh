#!/usr/bin/env bash
# Creates the sidecar_internal schema and CDC demo keyspace/table.
set -euo pipefail

CASSANDRA_HOST=${CASSANDRA_HOST:-cassandra}

cqlsh "${CASSANDRA_HOST}" <<'CQL'
CREATE KEYSPACE IF NOT EXISTS sidecar_internal
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

CREATE TABLE IF NOT EXISTS sidecar_internal.configs (
  service text,
  config  map<text, text>,
  PRIMARY KEY (service)
);

CREATE KEYSPACE IF NOT EXISTS cdc_demo
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

CREATE TABLE IF NOT EXISTS cdc_demo.events (
  id  uuid      PRIMARY KEY,
  msg text,
  ts  timestamp
) WITH cdc = true;
CQL

echo "Schema initialised."
