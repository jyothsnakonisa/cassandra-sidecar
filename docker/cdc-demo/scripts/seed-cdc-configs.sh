#!/usr/bin/env bash
# Seeds CDC and Kafka configuration into sidecar_internal.configs.
# IF NOT EXISTS makes each insert idempotent across restarts.
set -euo pipefail

CASSANDRA_HOST=${CASSANDRA_HOST:-cassandra}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}
CDC_TOPIC=${CDC_TOPIC:-cdc-mutations}
CDC_DATACENTER=${CDC_DATACENTER:-datacenter1}

cqlsh "${CASSANDRA_HOST}" <<CQL
INSERT INTO sidecar_internal.configs (service, config)
VALUES ('cdc', {
  'cdc_enabled':              'true',
  'topic':                    '${CDC_TOPIC}',
  'jobid':                    'docker-demo-job',
  'datacenter':               '${CDC_DATACENTER}',
  'watermark_seconds':        '259200',
  'micro_batch_delay_millis': '1000',
  'max_commit_logs':          '4',
  'persist_state':            'true',
  'fail_kafka_errors':        'true',
  'fail_kafka_too_large_errors': 'false'
}) IF NOT EXISTS;

INSERT INTO sidecar_internal.configs (service, config)
VALUES ('kafka', {
  'bootstrap.servers': '${KAFKA_BOOTSTRAP}',
  'key.serializer':    'org.apache.kafka.common.serialization.StringSerializer',
  'value.serializer':  'org.apache.kafka.common.serialization.ByteArraySerializer',
  'acks':              'all',
  'retries':           '3',
  'linger.ms':         '5',
  'batch.size':        '16384'
}) IF NOT EXISTS;
CQL

echo "Configs seeded."
