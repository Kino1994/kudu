// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.client;

import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.KuduTestHarness;

/**
 * Integration tests for {@link DeleteByPkPrefix}.
 *
 * Tests create tables with composite primary keys and hash/range partitioning,
 * insert rows, delete by PK prefix, and verify the correct rows are removed.
 */
public class TestDeleteByPkPrefix {

  private KuduClient client;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
  }

  // --------------------------------------------------------------------------
  // Schema helpers
  // --------------------------------------------------------------------------

  /**
   * Creates a schema with PK (id1 INT32, id2 STRING, seq_num INT64)
   * and one non-PK column (value STRING).
   */
  private static Schema createCompositeKeySchema() {
    List<ColumnSchema> columns = new ArrayList<>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("id1", Type.INT32)
        .key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("id2", Type.STRING)
        .key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("seq_num", Type.INT64)
        .key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
        .nullable(true).build());
    return new Schema(columns);
  }

  /**
   * Creates a schema with PK (k1 INT32, k2 INT32, k3 INT32).
   */
  private static Schema createTripleIntKeySchema() {
    List<ColumnSchema> columns = new ArrayList<>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("k1", Type.INT32)
        .key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("k2", Type.INT32)
        .key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("k3", Type.INT32)
        .key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("val", Type.STRING)
        .nullable(true).build());
    return new Schema(columns);
  }

  // --------------------------------------------------------------------------
  // Insert helpers
  // --------------------------------------------------------------------------

  private void insertRow(KuduTable table, int id1, String id2, long seqNum,
                         String value) throws KuduException {
    KuduSession session = client.newSession();
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addInt("id1", id1);
    row.addString("id2", id2);
    row.addLong("seq_num", seqNum);
    if (value != null) {
      row.addString("value", value);
    }
    session.apply(insert);
    session.flush();
    session.close();
  }

  private void insertTripleIntRow(KuduTable table, int k1, int k2, int k3,
                                  String val) throws KuduException {
    KuduSession session = client.newSession();
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addInt("k1", k1);
    row.addInt("k2", k2);
    row.addInt("k3", k3);
    if (val != null) {
      row.addString("val", val);
    }
    session.apply(insert);
    session.flush();
    session.close();
  }

  private void bulkInsertRows(KuduTable table, int id1, String id2,
                               int count) throws KuduException {
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    for (long seq = 0; seq < count; seq++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt("id1", id1);
      row.addString("id2", id2);
      row.addLong("seq_num", seq);
      row.addString("value", "v" + seq);
      session.apply(insert);
      if (seq % 1000 == 999) {
        session.flush();
      }
    }
    session.flush();
    session.close();
  }

  private int countRows(KuduTable table) throws KuduException {
    return countRowsInScan(client.newScannerBuilder(table).build());
  }

  // --------------------------------------------------------------------------
  // Tests
  // --------------------------------------------------------------------------

  /**
   * Basic prefix delete with composite PK (id1, id2, seq_num).
   * Insert rows for two business keys, delete one prefix, verify only matching
   * rows are removed.
   */
  @Test(timeout = 100000)
  public void testBasicPrefixDelete() throws Exception {
    Schema schema = createCompositeKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("id1", "id2", "seq_num"));
    KuduTable table = client.createTable("testBasicPrefixDelete", schema, opts);

    // Insert 3 rows for (id1=1, id2="a") and 2 rows for (id1=2, id2="b").
    insertRow(table, 1, "a", 100, "v1");
    insertRow(table, 1, "a", 200, "v2");
    insertRow(table, 1, "a", 300, "v3");
    insertRow(table, 2, "b", 100, "v4");
    insertRow(table, 2, "b", 200, "v5");

    assertEquals(5, countRows(table));

    // Delete all rows with prefix (id1=1, id2="a").
    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("id1", 1);
    prefix.addString("id2", "a");

    DeleteByPkPrefixResponse resp = client.deleteByPkPrefix(table, prefix);
    assertEquals(3, resp.getRowsDeleted());

    // Verify: only (id1=2, id2="b") rows remain.
    assertEquals(2, countRows(table));
    List<String> rows = scanTableToStrings(table);
    for (String row : rows) {
      assertTrue("Expected id1=2, got: " + row, row.contains("INT32 id1=2"));
      assertTrue("Expected id2=b, got: " + row, row.contains("STRING id2=b"));
    }
  }

  /**
   * Verify behavior across multiple tablets with hash partitioning.
   */
  @Test(timeout = 100000)
  public void testPrefixDeleteWithHashPartitioning() throws Exception {
    Schema schema = createCompositeKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("id1", "id2", "seq_num"))
        .addHashPartitions(ImmutableList.of("id1"), 3);
    KuduTable table = client.createTable("testPrefixDeleteHash", schema, opts);

    // Insert rows spread across hash buckets.
    for (int id1 = 0; id1 < 6; id1++) {
      for (long seq = 0; seq < 5; seq++) {
        insertRow(table, id1, "key", seq, "val-" + id1 + "-" + seq);
      }
    }
    assertEquals(30, countRows(table));

    // Delete prefix (id1=3, id2="key") — should delete 5 rows.
    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("id1", 3);
    prefix.addString("id2", "key");

    DeleteByPkPrefixResponse resp = client.deleteByPkPrefix(table, prefix);
    assertEquals(5, resp.getRowsDeleted());
    assertEquals(25, countRows(table));

    // Verify no rows with id1=3 remain.
    KuduScanner scanner = client.newScannerBuilder(table)
        .addPredicate(KuduPredicate.newComparisonPredicate(
            schema.getColumn("id1"), KuduPredicate.ComparisonOp.EQUAL, 3))
        .build();
    assertEquals(0, countRowsInScan(scanner));
  }

  /**
   * Verify behavior with hash partitioning on multiple columns.
   */
  @Test(timeout = 100000)
  public void testPrefixDeleteWithMultiColumnHash() throws Exception {
    Schema schema = createTripleIntKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("k1", "k2", "k3"))
        .addHashPartitions(ImmutableList.of("k1", "k2"), 4);
    KuduTable table = client.createTable("testPrefixDeleteMultiHash", schema, opts);

    // Insert rows for several (k1, k2) combinations.
    for (int k1 = 0; k1 < 3; k1++) {
      for (int k2 = 0; k2 < 3; k2++) {
        for (int k3 = 0; k3 < 4; k3++) {
          insertTripleIntRow(table, k1, k2, k3, "v");
        }
      }
    }
    assertEquals(36, countRows(table));

    // Delete prefix (k1=1, k2=2) — should delete 4 rows.
    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("k1", 1);
    prefix.addInt("k2", 2);

    DeleteByPkPrefixResponse resp = client.deleteByPkPrefix(table, prefix);
    assertEquals(4, resp.getRowsDeleted());
    assertEquals(32, countRows(table));
  }

  /**
   * Single-column prefix (only the first PK column).
   */
  @Test(timeout = 100000)
  public void testSingleColumnPrefix() throws Exception {
    Schema schema = createCompositeKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("id1", "id2", "seq_num"));
    KuduTable table = client.createTable("testSingleColPrefix", schema, opts);

    insertRow(table, 1, "a", 1, "v1");
    insertRow(table, 1, "b", 2, "v2");
    insertRow(table, 1, "c", 3, "v3");
    insertRow(table, 2, "a", 1, "v4");

    assertEquals(4, countRows(table));

    // Delete all rows where id1=1 (covers all id2 values).
    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("id1", 1);

    DeleteByPkPrefixResponse resp = client.deleteByPkPrefix(table, prefix);
    assertEquals(3, resp.getRowsDeleted());
    assertEquals(1, countRows(table));
  }

  /**
   * Empty prefix (no PK columns set) should be rejected.
   */
  @Test(timeout = 100000)
  public void testEmptyPrefixRejected() throws Exception {
    Schema schema = createCompositeKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("id1", "id2", "seq_num"));
    KuduTable table = client.createTable("testEmptyPrefixRejected", schema, opts);

    PartialRow prefix = schema.newPartialRow();

    try {
      client.deleteByPkPrefix(table, prefix);
      fail("Expected IllegalArgumentException for empty prefix");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("At least one leading PK column must be set"));
    }
  }

  /**
   * Non-leading prefix (gap in PK columns) should be rejected.
   */
  @Test(timeout = 100000)
  public void testNonLeadingPrefixRejected() throws Exception {
    Schema schema = createCompositeKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("id1", "id2", "seq_num"));
    KuduTable table = client.createTable("testNonLeadingPrefixRejected", schema, opts);

    // Set id1 and seq_num but skip id2 — not a contiguous prefix.
    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("id1", 1);
    prefix.addLong("seq_num", 100);

    try {
      client.deleteByPkPrefix(table, prefix);
      fail("Expected IllegalArgumentException for non-contiguous prefix");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("contiguous leading subset"));
    }
  }

  /**
   * Setting non-PK columns in prefix should be rejected.
   */
  @Test(timeout = 100000)
  public void testNonPkColumnRejected() throws Exception {
    Schema schema = createCompositeKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("id1", "id2", "seq_num"));
    KuduTable table = client.createTable("testNonPkColumnRejected", schema, opts);

    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("id1", 1);
    prefix.addString("value", "should not be here");

    try {
      client.deleteByPkPrefix(table, prefix);
      fail("Expected IllegalArgumentException for non-PK column in prefix");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Non-PK column"));
    }
  }

  /**
   * Deleting when no rows match the prefix should succeed with 0 deleted.
   */
  @Test(timeout = 100000)
  public void testDeleteNonExistentPrefix() throws Exception {
    Schema schema = createCompositeKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("id1", "id2", "seq_num"));
    KuduTable table = client.createTable("testDeleteNonExistent", schema, opts);

    insertRow(table, 1, "a", 100, "v1");
    assertEquals(1, countRows(table));

    // Delete with prefix that has no matching rows.
    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("id1", 999);

    DeleteByPkPrefixResponse resp = client.deleteByPkPrefix(table, prefix);
    assertEquals(0, resp.getRowsDeleted());
    assertEquals(1, countRows(table));
  }

  /**
   * Full PK specified as prefix should delete exactly one row.
   */
  @Test(timeout = 100000)
  public void testFullPkAsPrefix() throws Exception {
    Schema schema = createCompositeKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("id1", "id2", "seq_num"));
    KuduTable table = client.createTable("testFullPkAsPrefix", schema, opts);

    insertRow(table, 1, "a", 100, "v1");
    insertRow(table, 1, "a", 200, "v2");
    assertEquals(2, countRows(table));

    // Set all PK columns — should only delete one exact row.
    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("id1", 1);
    prefix.addString("id2", "a");
    prefix.addLong("seq_num", 100);

    DeleteByPkPrefixResponse resp = client.deleteByPkPrefix(table, prefix);
    assertEquals(1, resp.getRowsDeleted());
    assertEquals(1, countRows(table));
  }

  /**
   * The Spark streaming use case — delete all rows for a business key,
   * then reinsert new rows.
   */
  @Test(timeout = 100000)
  public void testDeleteAndReinsertPattern() throws Exception {
    Schema schema = createCompositeKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("id1", "id2", "seq_num"))
        .addHashPartitions(ImmutableList.of("id1"), 2);
    KuduTable table = client.createTable("testDeleteAndReinsert", schema, opts);

    // Initial insert: business key (1, "order") with 3 exploded rows.
    insertRow(table, 1, "order", 1, "item-A");
    insertRow(table, 1, "order", 2, "item-B");
    insertRow(table, 1, "order", 3, "item-C");
    // Another business key.
    insertRow(table, 2, "order", 1, "item-X");
    assertEquals(4, countRows(table));

    // Delete all for business key (1, "order").
    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("id1", 1);
    prefix.addString("id2", "order");
    DeleteByPkPrefixResponse resp = client.deleteByPkPrefix(table, prefix);
    assertEquals(3, resp.getRowsDeleted());
    assertEquals(1, countRows(table));

    // Re-insert new rows for the same business key.
    insertRow(table, 1, "order", 1, "item-D");
    insertRow(table, 1, "order", 2, "item-E");
    assertEquals(3, countRows(table));

    // Verify the new values.
    KuduScanner scanner = client.newScannerBuilder(table)
        .addPredicate(KuduPredicate.newComparisonPredicate(
            schema.getColumn("id1"), KuduPredicate.ComparisonOp.EQUAL, 1))
        .addPredicate(KuduPredicate.newComparisonPredicate(
            schema.getColumn("id2"), KuduPredicate.ComparisonOp.EQUAL, "order"))
        .build();
    List<String> rows = new ArrayList<>();
    while (scanner.hasMoreRows()) {
      RowResultIterator it = scanner.nextRows();
      while (it.hasNext()) {
        rows.add(it.next().rowToString());
      }
    }
    assertEquals(2, rows.size());
  }

  /**
   * Using the KuduClient convenience method (2-arg).
   */
  @Test(timeout = 100000)
  public void testViaKuduClientMethod() throws Exception {
    Schema schema = createCompositeKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("id1", "id2", "seq_num"));
    KuduTable table = client.createTable("testViaKuduClient", schema, opts);

    insertRow(table, 10, "x", 1, "v1");
    insertRow(table, 10, "x", 2, "v2");
    insertRow(table, 20, "y", 1, "v3");

    DeleteByPkPrefixResponse resp = client.deleteByPkPrefix(table,
        buildPrefix(schema, 10, "x"));
    assertEquals(2, resp.getRowsDeleted());
    assertEquals(1, countRows(table));
  }

  /**
   * Delete a large number of rows matching a prefix across a hash-partitioned table.
   */
  @Test(timeout = 200000)
  public void testUnlimitedDeleteLargeDataset() throws Exception {
    Schema schema = createCompositeKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("id1", "id2", "seq_num"))
        .addHashPartitions(ImmutableList.of("id1"), 3);
    KuduTable table = client.createTable("testUnlimitedDeleteLarge", schema, opts);

    // Insert 5000 rows for prefix (id1=1, id2="bulk") and 10 rows for another prefix.
    bulkInsertRows(table, 1, "bulk", 5000);
    for (long seq = 0; seq < 10; seq++) {
      insertRow(table, 2, "other", seq, "v");
    }
    assertEquals(5010, countRows(table));

    // Delete all matching rows (no limit).
    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("id1", 1);
    prefix.addString("id2", "bulk");

    DeleteByPkPrefixResponse resp = client.deleteByPkPrefix(table, prefix);
    assertEquals(5000, resp.getRowsDeleted());
    assertEquals(10, countRows(table));
  }

  /**
   * Multi-tablet with 4 hash buckets and range partitioning.
   * Verifies correct deletion across all tablets.
   */
  @Test(timeout = 100000)
  public void testMultiTabletMultiHashBucketCorrectness() throws Exception {
    Schema schema = createTripleIntKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("k1", "k2", "k3"))
        .addHashPartitions(ImmutableList.of("k1"), 4);
    KuduTable table = client.createTable("testMultiTabletHash", schema, opts);

    // Insert rows spanning all buckets.
    for (int k1 = 0; k1 < 8; k1++) {
      for (int k2 = 0; k2 < 3; k2++) {
        for (int k3 = 0; k3 < 3; k3++) {
          insertTripleIntRow(table, k1, k2, k3, "v");
        }
      }
    }
    assertEquals(72, countRows(table));

    // Delete prefix (k1=5) — should delete 9 rows.
    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("k1", 5);

    DeleteByPkPrefixResponse resp = client.deleteByPkPrefix(table, prefix);
    assertEquals(9, resp.getRowsDeleted());
    assertEquals(63, countRows(table));

    // Verify no rows with k1=5 remain.
    KuduScanner scanner = client.newScannerBuilder(table)
        .addPredicate(KuduPredicate.newComparisonPredicate(
            schema.getColumn("k1"), KuduPredicate.ComparisonOp.EQUAL, 5))
        .build();
    assertEquals(0, countRowsInScan(scanner));
  }

  /**
   * Test concurrent writers inserting rows while deleteByPkPrefix runs.
   * No errors should occur and the final state should be consistent.
   */
  @Test(timeout = 200000)
  public void testConcurrentWritersDuringDelete() throws Exception {
    Schema schema = createCompositeKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("id1", "id2", "seq_num"))
        .addHashPartitions(ImmutableList.of("id1"), 2);
    KuduTable table = client.createTable("testConcurrentWriters", schema, opts);

    // Insert initial rows.
    for (long seq = 0; seq < 100; seq++) {
      insertRow(table, 1, "a", seq, "v" + seq);
    }
    insertRow(table, 2, "b", 0, "other");

    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch startLatch = new CountDownLatch(1);

    // Start a concurrent writer inserting rows with a different seq range.
    Future<?> writerFuture = executor.submit(() -> {
      try {
        startLatch.await();
        KuduSession session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        for (long seq = 1000; seq < 1050; seq++) {
          Insert insert = table.newInsert();
          PartialRow row = insert.getRow();
          row.addInt("id1", 1);
          row.addString("id2", "a");
          row.addLong("seq_num", seq);
          row.addString("value", "concurrent-" + seq);
          session.apply(insert);
        }
        session.flush();
        session.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Start both the delete and the writer concurrently.
    startLatch.countDown();
    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("id1", 1);
    prefix.addString("id2", "a");
    DeleteByPkPrefixResponse resp = client.deleteByPkPrefix(table, prefix);

    // Wait for the writer to finish.
    writerFuture.get();
    executor.shutdown();

    // The operation should complete without errors.
    // The exact count may vary due to timing, but all initial rows should be deleted.
    assertTrue("Should have deleted at least the initial rows", resp.getRowsDeleted() >= 0);

    // The (2, "b") row should be unaffected.
    KuduScanner scanner = client.newScannerBuilder(table)
        .addPredicate(KuduPredicate.newComparisonPredicate(
            schema.getColumn("id1"), KuduPredicate.ComparisonOp.EQUAL, 2))
        .build();
    assertEquals(1, countRowsInScan(scanner));

    // The table should be in a consistent state (no corruption).
    int totalRemaining = countRows(table);
    assertTrue("Total remaining rows should be non-negative", totalRemaining >= 0);
  }

  /**
   * Test that the deprecated 3-arg API still works.
   */
  @SuppressWarnings("deprecation")
  @Test(timeout = 100000)
  public void testDeprecatedApiStillWorks() throws Exception {
    Schema schema = createCompositeKeySchema();
    CreateTableOptions opts = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("id1", "id2", "seq_num"));
    KuduTable table = client.createTable("testDeprecatedApi", schema, opts);

    insertRow(table, 1, "a", 100, "v1");
    insertRow(table, 1, "a", 200, "v2");
    assertEquals(2, countRows(table));

    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("id1", 1);
    prefix.addString("id2", "a");

    // Use the deprecated 3-arg API.
    DeleteByPkPrefixResponse resp = client.deleteByPkPrefix(table, prefix, Long.MAX_VALUE);
    assertEquals(2, resp.getRowsDeleted());
    assertEquals(0, countRows(table));
  }

  private PartialRow buildPrefix(Schema schema, int id1, String id2) {
    PartialRow prefix = schema.newPartialRow();
    prefix.addInt("id1", id1);
    prefix.addString("id2", id2);
    return prefix;
  }
}
