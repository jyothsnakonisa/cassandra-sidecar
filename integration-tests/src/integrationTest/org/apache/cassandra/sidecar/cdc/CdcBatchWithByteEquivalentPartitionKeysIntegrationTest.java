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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterCdcSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.testing.TestCdcEventConsumer;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.assertj.core.api.Assertions.assertThat;

class CdcBatchWithByteEquivalentPartitionKeysIntegrationTest extends SharedClusterCdcSidecarIntegrationTestBase
{
    private static final QualifiedName CDC_TABLE = new QualifiedName("cdc_batch_bytes_ks", "cdc_bigint_table");
    private static final QualifiedName NON_CDC_TABLE = new QualifiedName("cdc_batch_bytes_ks", "non_cdc_timestamp_table");

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(CDC_TABLE, DC1_RF1);
        createTestTable(CDC_TABLE, "CREATE TABLE IF NOT EXISTS %s "
                                    + "(id bigint PRIMARY KEY, value int) WITH cdc = true");
        createTestTable(NON_CDC_TABLE, "CREATE TABLE IF NOT EXISTS %s "
                                        + "(id timestamp PRIMARY KEY, value int) WITH cdc = false");
    }

    @Override
    protected void beforeTestStart()
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
    }

    @Test
    void testBatchWithBigintAndTimestampKeysPublishesCdcEvent()
    {
        TestCdcEventConsumer consumer = getTestEventConsumer();
        cluster.getFirstRunningInstance().coordinator().execute(
        String.format("INSERT INTO %s (id, value) VALUES (2, 99)", CDC_TABLE), ConsistencyLevel.ONE);
        cluster.getFirstRunningInstance().flush(CDC_TABLE.keyspace());
        waitUntil(() -> hasCdcValue(consumer, 99), 120, 1000);
        assertThat(consumer.getEvents()).anySatisfy(event -> assertThat(value(event)).isEqualTo(99));

        String batch = String.format("BEGIN BATCH "
                                     + "INSERT INTO %s (id, value) VALUES (1, 100); "
                                     + "INSERT INTO %s (id, value) VALUES (1, 200); "
                                     + "APPLY BATCH", CDC_TABLE, NON_CDC_TABLE);
        cluster.getFirstRunningInstance().coordinator().execute(batch, ConsistencyLevel.ONE);
        cluster.getFirstRunningInstance().flush(CDC_TABLE.keyspace());

        assertThat(waitForCdcValue(consumer, 100, 120, 1000))
        .as("bigint 1 and timestamp 1 share the bytes 0000000000000001, so the CDC batch event must publish")
        .isTrue();

        List<CdcEvent> events = consumer.getEvents();
        assertThat(events).anySatisfy(event -> {
            assertThat(event.keyspace).isEqualTo(CDC_TABLE.keyspace());
            assertThat(event.table).isEqualTo(CDC_TABLE.table());
            assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.INSERT);
            assertThat(value(event)).isEqualTo(100);
        });
        assertThat(events).noneMatch(event -> event.table.equals(NON_CDC_TABLE.table()));
    }

    private static boolean waitForCdcValue(TestCdcEventConsumer consumer, int expectedValue,
                                            long timeoutSeconds, long pollIntervalMillis)
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (!hasCdcValue(consumer, expectedValue) && System.nanoTime() <= deadline)
        {
            try
            {
                Thread.sleep(pollIntervalMillis);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return hasCdcValue(consumer, expectedValue);
    }

    private static boolean hasCdcValue(TestCdcEventConsumer consumer, int expectedValue)
    {
        return consumer.getEvents().stream().anyMatch(event -> event.keyspace.equals(CDC_TABLE.keyspace())
                                                              && event.table.equals(CDC_TABLE.table())
                                                              && value(event) == expectedValue);
    }

    private static int value(CdcEvent event)
    {
        return ByteBuffer.wrap(Objects.requireNonNull(event.getValueColumns().get(0).getBytes())).getInt();
    }
}
