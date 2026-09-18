/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.sidecar.cdc;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.sidecar.tasks.CassandraClusterSchemaMonitor;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterCdcSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.testing.TestCdcEventConsumer;
import org.apache.cassandra.spark.utils.TableIdentifier;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the CDC batch-write bug: a {@code BEGIN BATCH} statement that mixes a
 * CDC-enabled table and a CDC-disabled table under the same partition key produces a single
 * commit log {@code Mutation} spanning both tables. Before the fix, the sidecar's
 * {@code Schema.instance} only knew about CDC-enabled tables, so deserializing that Mutation
 * threw {@code UnknownTableException} and dropped the WHOLE mutation — including the
 * CDC-enabled table's update, not just the non-CDC part.
 *
 * <p>This test exercises the real, wired-up sidecar CDC pipeline (real in-JVM Cassandra cluster,
 * real {@link org.apache.cassandra.sidecar.cdc.CdcSchemaSupplier}, real
 * {@link org.apache.cassandra.sidecar.tasks.CassandraClusterSchemaMonitor}, default
 * {@code batch_statements_enabled=true}) rather than constructing a {@code Mutation} directly,
 * so it validates the fix end-to-end against production wiring. It also covers the new
 * partition-key-structure risk analysis (see {@link org.apache.cassandra.sidecar.cdc.CdcBatchRiskAnalyzer}):
 * a non-CDC table with a DIFFERENT partition-key type than any CDC-enabled table in its keyspace
 * is excluded from the registered schema entirely, since it could never be co-located with a
 * CDC-enabled table's update in the same Mutation.
 */
public class CdcBatchWithMixedTablesIntegrationTest extends SharedClusterCdcSidecarIntegrationTestBase
{
    private static final QualifiedName CDC_TABLE = new QualifiedName("cdc_batch_test_ks", "cdc_table");
    private static final QualifiedName NON_CDC_TABLE = new QualifiedName("cdc_batch_test_ks", "non_cdc_table");
    private static final QualifiedName MISMATCHED_PK_TABLE = new QualifiedName("cdc_batch_test_ks", "mismatched_pk_table");

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(CDC_TABLE, DC1_RF1);

        createTestTable(CDC_TABLE, "CREATE TABLE IF NOT EXISTS %s "
                                    + "(id int PRIMARY KEY, value int) "
                                    + "WITH cdc = true");
        createTestTable(NON_CDC_TABLE, "CREATE TABLE IF NOT EXISTS %s "
                                        + "(id int PRIMARY KEY, value int) "
                                        + "WITH cdc = false");
        // Partition key type (text) differs from CDC_TABLE's (int) — this table could never be
        // batched with CDC_TABLE under a shared partition key, so it must never be registered.
        createTestTable(MISMATCHED_PK_TABLE, "CREATE TABLE IF NOT EXISTS %s "
                                              + "(id text PRIMARY KEY, value int) "
                                              + "WITH cdc = false");
    }

    @Override
    protected void beforeTestStart()
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
    }

    @Test
    void testBatchMixingCdcAndNonCdcTableDoesNotDropCdcMutation()
    {
        // A single BEGIN BATCH writing to both tables produces one commit log Mutation
        // spanning both — exactly the scenario that used to drop the CDC table's update.
        String batch = String.format(
            "BEGIN BATCH " +
            "INSERT INTO %s (id, value) VALUES (1, 100); " +
            "INSERT INTO %s (id, value) VALUES (1, 200); " +
            "APPLY BATCH",
            CDC_TABLE, NON_CDC_TABLE);

        cluster.getFirstRunningInstance()
               .coordinator()
               .execute(batch, ConsistencyLevel.ONE);

        // Seal the active commit log segment so CDC can find the mutation in cdc_raw
        cluster.getFirstRunningInstance().flush(CDC_TABLE.keyspace());

        TestCdcEventConsumer consumer = getTestEventConsumer();
        waitUntil(() -> !consumer.getEvents().isEmpty(), 120, 1000);

        List<CdcEvent> events = consumer.getEvents();

        // The core regression assertion: the CDC-enabled table's mutation must NOT be dropped
        // just because it shared a commit log Mutation with a non-CDC table in the batch.
        assertThat(events)
        .as("CDC-enabled table's mutation must be published even though it shared a batch "
          + "Mutation with a CDC-disabled table")
        .anySatisfy(event -> {
            assertThat(event.keyspace).isEqualTo(CDC_TABLE.keyspace());
            assertThat(event.table).isEqualTo(CDC_TABLE.table());
            assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.INSERT);
        });

        // The non-CDC table must never be published — it's only present in Schema.instance
        // (because it shares CDC_TABLE's partition-key structure) so the shared Mutation can
        // deserialize; it is still correctly excluded at the per-PartitionUpdate CDC-flag gate.
        assertThat(events)
        .as("Non-CDC table's mutation must never be published, even though it's registered in "
          + "Schema.instance because it shares CDC_TABLE's partition-key structure")
        .noneMatch(event -> event.table.equals(NON_CDC_TABLE.table()));
    }

    @Test
    void testTableWithMismatchedPartitionKeyIsNeverRegistered()
    {
        CassandraClusterSchemaMonitor schemaMonitor = serverWrapper.injector.getInstance(CassandraClusterSchemaMonitor.class);
        waitUntil(() -> !schemaMonitor.getRegisteredTables().isEmpty(), 30, 1000);

        Set<TableIdentifier> registered = schemaMonitor.getRegisteredTables();

        assertThat(registered)
        .as("CDC-enabled table must always be registered")
        .contains(TableIdentifier.of(CDC_TABLE.keyspace(), CDC_TABLE.table()));
        assertThat(registered)
        .as("Non-CDC table sharing CDC_TABLE's partition-key structure must be registered — "
          + "it could be co-located with CDC_TABLE's update in the same batch Mutation")
        .contains(TableIdentifier.of(NON_CDC_TABLE.keyspace(), NON_CDC_TABLE.table()));
        assertThat(registered)
        .as("Table with a different partition-key type than any CDC-enabled table in its "
          + "keyspace could never be co-located with a CDC-enabled table's update in the same "
          + "Mutation, so it must never be registered")
        .doesNotContain(TableIdentifier.of(MISMATCHED_PK_TABLE.keyspace(), MISMATCHED_PK_TABLE.table()));
    }
}
