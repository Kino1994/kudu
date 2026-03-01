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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;

/**
 * Utility to delete all rows from a Kudu table whose primary key starts with a
 * given prefix (a leading subset of PK columns).
 *
 * <p>This is useful in streaming workloads where a table has a composite
 * primary key like {@code (id1, id2, seq_num)} and the caller needs to delete
 * all rows for a given {@code (id1, id2)} before re-inserting new rows.
 *
 * <h3>How it works</h3>
 * <p>When connected to a server that supports the DeleteByPkPrefix RPC, the
 * operation is performed entirely server-side: one RPC is sent per tablet that
 * may contain matching rows. If the server does not support the RPC, the client
 * falls back to a scan + delete approach.
 *
 * <h3>Consistency</h3>
 * <ul>
 *   <li><b>Per-tablet snapshot consistency:</b> Rows to delete are determined
 *       by an MVCC snapshot taken at the start of the operation on each tablet.
 *       Rows inserted after the snapshot are NOT deleted.</li>
 *   <li><b>Per-tablet non-atomicity:</b> Deletes are applied in batches through
 *       Raft. Each batch is individually atomic; the full operation within a
 *       tablet is NOT a single atomic transaction.</li>
 *   <li><b>Cross-tablet non-atomicity:</b> One RPC per tablet; no cross-tablet
 *       atomicity. Use transactions if needed.</li>
 *   <li><b>Concurrent deletes:</b> Uses DELETE_IGNORE internally so concurrent
 *       deletes of the same rows are harmless.</li>
 * </ul>
 *
 * <h3>Usage example</h3>
 * <pre>{@code
 * KuduClient client = ...;
 * KuduTable table = client.openTable("my_table");
 *
 * // PK is (id1 INT32, id2 STRING, seq_num INT64).
 * // Delete all rows where id1=42 and id2="abc".
 * PartialRow prefix = table.getSchema().newPartialRow();
 * prefix.addInt("id1", 42);
 * prefix.addString("id2", "abc");
 *
 * DeleteByPkPrefixResponse resp = DeleteByPkPrefix.execute(client, table, prefix);
 * System.out.println("Deleted " + resp.getRowsDeleted() + " rows");
 * }</pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class DeleteByPkPrefix {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteByPkPrefix.class);

  /** Default batch size for flushing deletes in client-side fallback. */
  private static final int DEFAULT_BATCH_SIZE = 1000;

  private DeleteByPkPrefix() {
    // Utility class.
  }

  /**
   * Delete all rows from {@code table} whose primary key starts with the
   * columns set in {@code pkPrefix}.
   *
   * @param client    the Kudu client
   * @param table     the table to delete from
   * @param pkPrefix  a {@link PartialRow} with a leading subset of the PK
   *                  columns set. Must have at least one PK column set, and
   *                  the set columns must be a contiguous prefix of the PK.
   * @return a {@link DeleteByPkPrefixResponse} with the number of rows deleted
   * @throws KuduException         if an RPC error occurs
   * @throws IllegalArgumentException if the prefix is invalid
   */
  public static DeleteByPkPrefixResponse execute(
      KuduClient client,
      KuduTable table,
      PartialRow pkPrefix) throws KuduException {
    Preconditions.checkNotNull(client, "client must not be null");
    Preconditions.checkNotNull(table, "table must not be null");
    Preconditions.checkNotNull(pkPrefix, "pkPrefix must not be null");

    Schema schema = table.getSchema();
    List<ColumnSchema> pkColumns = schema.getPrimaryKeyColumns();
    int prefixLen = validateAndGetPrefixLength(pkPrefix, schema, pkColumns);

    LOG.info("Deleting rows from table '{}' with PK prefix of {} column(s)",
        table.getName(), prefixLen);

    // Try server-side RPC first, fall back to client-side scan+delete.
    try {
      return executeViaServerRpc(client, table, pkPrefix, pkColumns, prefixLen);
    } catch (KuduException e) {
      if (isNotSupportedError(e)) {
        LOG.info("Server does not support DeleteByPkPrefix RPC, falling back to scan+delete");
        return executeViaClientScanDelete(client, table, schema, pkColumns, pkPrefix, prefixLen);
      }
      throw e;
    }
  }

  /**
   * Delete all rows from {@code table} whose primary key starts with the
   * columns set in {@code pkPrefix}.
   *
   * @param client    the Kudu client
   * @param table     the table to delete from
   * @param pkPrefix  a {@link PartialRow} with a leading subset of the PK
   *                  columns set. Must have at least one PK column set, and
   *                  the set columns must be a contiguous prefix of the PK.
   * @param limit     ignored; retained for backward compatibility
   * @return a {@link DeleteByPkPrefixResponse} with the number of rows deleted
   * @throws KuduException         if an RPC error occurs
   * @throws IllegalArgumentException if the prefix is invalid
   * @deprecated Use {@link #execute(KuduClient, KuduTable, PartialRow)} instead.
   *             The limit parameter is no longer enforced.
   */
  @Deprecated
  public static DeleteByPkPrefixResponse execute(
      KuduClient client,
      KuduTable table,
      PartialRow pkPrefix,
      long limit) throws KuduException {
    return execute(client, table, pkPrefix);
  }

  /**
   * Executes the delete via server-side DeleteByPkPrefix RPCs, one per tablet.
   * Uses partition pruning to only send RPCs to tablets that may contain
   * matching rows, based on the prefix's equality predicates.
   */
  private static DeleteByPkPrefixResponse executeViaServerRpc(
      KuduClient client,
      KuduTable table,
      PartialRow pkPrefix,
      List<ColumnSchema> pkColumns,
      int prefixLen) throws KuduException {
    // Build a prefix-only schema containing just the leading PK columns.
    List<ColumnSchema> prefixCols = new ArrayList<>(prefixLen);
    for (int i = 0; i < prefixLen; i++) {
      prefixCols.add(pkColumns.get(i));
    }
    Schema prefixSchema = new Schema(prefixCols);

    // Build a prefix PartialRow using the prefix-only schema.
    PartialRow prefixForRpc = prefixSchema.newPartialRow();
    Schema fullSchema = table.getSchema();
    for (int i = 0; i < prefixLen; i++) {
      ColumnSchema col = pkColumns.get(i);
      int fullIdx = fullSchema.getColumnIndex(col.getName());
      int prefixIdx = i; // In the prefix schema, columns are indexed 0..prefixLen-1.
      copyColumnValue(pkPrefix, fullIdx, prefixForRpc, prefixIdx, col);
    }

    // Build equality predicates from the prefix to enable partition pruning.
    // This lets us skip tablets that cannot contain matching rows (e.g., hash
    // buckets that don't match or range partitions outside the prefix).
    List<KuduPredicate> predicates = new ArrayList<>(prefixLen);
    for (int i = 0; i < prefixLen; i++) {
      ColumnSchema col = pkColumns.get(i);
      int colIdx = fullSchema.getColumnIndex(col.getName());
      Object value = pkPrefix.getObject(colIdx);
      predicates.add(
          KuduPredicate.newComparisonPredicate(col, KuduPredicate.ComparisonOp.EQUAL, value));
    }

    // Create a partition pruner using a scanner builder with our predicates.
    KuduScanner.KuduScannerBuilder scanBuilder = client.newScannerBuilder(table);
    for (KuduPredicate pred : predicates) {
      scanBuilder.addPredicate(pred);
    }
    PartitionPruner pruner = PartitionPruner.create(scanBuilder);

    // Get all tablet locations for the table.
    List<LocatedTablet> tablets;
    try {
      tablets = table.getTabletsLocations(client.getDefaultAdminOperationTimeoutMs());
    } catch (Exception e) {
      throw KuduException.transformException(e);
    }

    long totalDeleted = 0;
    long timeoutMs = client.getDefaultOperationTimeoutMs();
    int prunedCount = 0;

    for (LocatedTablet tablet : tablets) {
      // Skip tablets that cannot contain matching rows.
      if (pruner.shouldPruneForTests(tablet.getPartition())) {
        prunedCount++;
        continue;
      }

      byte[] partitionKey = tablet.getPartition().getPartitionKeyStart();

      DeleteByPkPrefixRequest rpc = new DeleteByPkPrefixRequest(
          table,
          prefixSchema,
          prefixForRpc,
          partitionKey,
          client.asyncClient.getTimer(),
          timeoutMs);

      DeleteByPkPrefixRpcResponse rpcResp = KuduClient.joinAndHandleException(
          client.asyncClient.sendRpcToTablet(rpc));
      totalDeleted += rpcResp.getRowsDeleted();
    }

    LOG.info("Deleted {} rows from table '{}' via server-side RPC (pruned {}/{} tablets)",
        totalDeleted, table.getName(), prunedCount, tablets.size());
    return new DeleteByPkPrefixResponse(totalDeleted);
  }

  /**
   * Fallback: executes the delete via client-side scan + delete operations.
   */
  private static DeleteByPkPrefixResponse executeViaClientScanDelete(
      KuduClient client,
      KuduTable table,
      Schema schema,
      List<ColumnSchema> pkColumns,
      PartialRow pkPrefix,
      int prefixLen) throws KuduException {
    KuduScanner scanner = buildPrefixScanner(client, table, schema, pkColumns,
                                             pkPrefix, prefixLen);
    long totalDeleted = 0;
    KuduSession session = client.newSession();
    try {
      session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
      int batchCount = 0;

      while (scanner.hasMoreRows()) {
        RowResultIterator results = scanner.nextRows();
        while (results.hasNext()) {
          RowResult row = results.next();
          Delete delete = table.newDelete();
          PartialRow deleteRow = delete.getRow();
          copyPrimaryKey(row, deleteRow, pkColumns);
          session.apply(delete);
          totalDeleted++;
          batchCount++;

          if (batchCount >= DEFAULT_BATCH_SIZE) {
            session.flush();
            checkSessionErrors(session);
            batchCount = 0;
          }
        }
      }

      if (batchCount > 0) {
        session.flush();
        checkSessionErrors(session);
      }
    } finally {
      try {
        session.close();
      } catch (KuduException e) {
        LOG.warn("Error closing session", e);
      }
      scanner.close();
    }

    LOG.info("Deleted {} rows from table '{}' via client-side scan+delete",
        totalDeleted, table.getName());
    return new DeleteByPkPrefixResponse(totalDeleted);
  }

  /**
   * Checks if the exception indicates the server does not support the
   * DeleteByPkPrefix RPC.
   */
  static boolean isNotSupportedError(KuduException e) {
    Status status = e.getStatus();
    if (status.isNotSupported()) {
      return true;
    }
    // Also check for the specific error returned by old server stubs.
    if (status.isRemoteError() || status.isRuntimeError()) {
      String msg = status.getMessage();
      if (msg != null && msg.contains("not yet implemented")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Validates that the set columns in {@code pkPrefix} form a contiguous
   * leading prefix of the primary key columns and returns the prefix length.
   */
  static int validateAndGetPrefixLength(PartialRow pkPrefix,
                                        Schema schema,
                                        List<ColumnSchema> pkColumns) {
    Preconditions.checkArgument(!pkColumns.isEmpty(),
        "Table has no primary key columns");

    int prefixLen = 0;
    for (int i = 0; i < pkColumns.size(); i++) {
      int colIdx = schema.getColumnIndex(pkColumns.get(i).getName());
      if (pkPrefix.isSet(colIdx)) {
        prefixLen++;
      } else {
        break;
      }
    }

    Preconditions.checkArgument(prefixLen > 0,
        "At least one leading PK column must be set in the prefix row");

    // Verify no non-leading PK columns are set beyond the prefix.
    for (int i = prefixLen; i < pkColumns.size(); i++) {
      int colIdx = schema.getColumnIndex(pkColumns.get(i).getName());
      Preconditions.checkArgument(!pkPrefix.isSet(colIdx),
          "PK column '%s' at position %s is set but column '%s' at position %s " +
          "is not set. The prefix must be a contiguous leading subset of the PK.",
          pkColumns.get(i).getName(), i,
          pkColumns.get(prefixLen - 1).getName(), prefixLen - 1);
    }

    // Verify no non-PK columns are set.
    for (int i = pkColumns.size(); i < schema.getColumnCount(); i++) {
      Preconditions.checkArgument(!pkPrefix.isSet(i),
          "Non-PK column '%s' must not be set in the prefix row",
          schema.getColumnByIndex(i).getName());
    }

    return prefixLen;
  }

  /**
   * Builds a scanner with equality predicates on the PK prefix columns,
   * projecting only the PK columns (no limit).
   */
  private static KuduScanner buildPrefixScanner(
      KuduClient client,
      KuduTable table,
      Schema schema,
      List<ColumnSchema> pkColumns,
      PartialRow pkPrefix,
      int prefixLen) {
    KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table);

    List<String> projectedColumns = new ArrayList<>(pkColumns.size());
    for (ColumnSchema col : pkColumns) {
      projectedColumns.add(col.getName());
    }
    builder.setProjectedColumnNames(projectedColumns);

    for (int i = 0; i < prefixLen; i++) {
      ColumnSchema col = pkColumns.get(i);
      int colIdx = schema.getColumnIndex(col.getName());
      Object value = pkPrefix.getObject(colIdx);
      builder.addPredicate(
          KuduPredicate.newComparisonPredicate(col, KuduPredicate.ComparisonOp.EQUAL, value));
    }

    builder.readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT);
    return builder.build();
  }

  /**
   * Copies a single column value from one PartialRow to another.
   */
  private static void copyColumnValue(PartialRow src, int srcIdx,
                                       PartialRow dst, int dstIdx,
                                       ColumnSchema col) {
    if (src.isNull(srcIdx)) {
      dst.setNull(dstIdx);
      return;
    }
    switch (col.getType()) {
      case BOOL:
        dst.addBoolean(dstIdx, src.getBoolean(srcIdx));
        break;
      case INT8:
        dst.addByte(dstIdx, src.getByte(srcIdx));
        break;
      case INT16:
        dst.addShort(dstIdx, src.getShort(srcIdx));
        break;
      case INT32:
        dst.addInt(dstIdx, src.getInt(srcIdx));
        break;
      case INT64:
        dst.addLong(dstIdx, src.getLong(srcIdx));
        break;
      case UNIXTIME_MICROS:
        dst.addTimestamp(dstIdx, src.getTimestamp(srcIdx));
        break;
      case DATE:
        dst.addDate(dstIdx, src.getDate(srcIdx));
        break;
      case FLOAT:
        dst.addFloat(dstIdx, src.getFloat(srcIdx));
        break;
      case DOUBLE:
        dst.addDouble(dstIdx, src.getDouble(srcIdx));
        break;
      case STRING:
        dst.addString(dstIdx, src.getString(srcIdx));
        break;
      case VARCHAR:
        dst.addVarchar(dstIdx, src.getVarchar(srcIdx));
        break;
      case BINARY:
        dst.addBinary(dstIdx, src.getBinaryCopy(srcIdx));
        break;
      case DECIMAL:
        dst.addDecimal(dstIdx, src.getDecimal(srcIdx));
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported PK column type: " + col.getType());
    }
  }

  /**
   * Copies all primary key column values from a scanned {@link RowResult} into
   * a {@link PartialRow} for a DELETE operation.
   */
  private static void copyPrimaryKey(RowResult src, PartialRow dst,
                                     List<ColumnSchema> pkColumns) {
    for (int i = 0; i < pkColumns.size(); i++) {
      ColumnSchema col = pkColumns.get(i);
      if (src.isNull(i)) {
        dst.setNull(i);
        continue;
      }
      switch (col.getType()) {
        case BOOL:
          dst.addBoolean(i, src.getBoolean(i));
          break;
        case INT8:
          dst.addByte(i, src.getByte(i));
          break;
        case INT16:
          dst.addShort(i, src.getShort(i));
          break;
        case INT32:
          dst.addInt(i, src.getInt(i));
          break;
        case INT64:
          dst.addLong(i, src.getLong(i));
          break;
        case UNIXTIME_MICROS:
          dst.addTimestamp(i, src.getTimestamp(i));
          break;
        case DATE:
          dst.addDate(i, src.getDate(i));
          break;
        case FLOAT:
          dst.addFloat(i, src.getFloat(i));
          break;
        case DOUBLE:
          dst.addDouble(i, src.getDouble(i));
          break;
        case STRING:
          dst.addString(i, src.getString(i));
          break;
        case VARCHAR:
          dst.addVarchar(i, src.getVarchar(i));
          break;
        case BINARY:
          dst.addBinary(i, src.getBinaryCopy(i));
          break;
        case DECIMAL:
          dst.addDecimal(i, src.getDecimal(i));
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported PK column type: " + col.getType());
      }
    }
  }

  /**
   * Checks the session for pending errors and throws if any are found.
   */
  private static void checkSessionErrors(KuduSession session) throws KuduException {
    if (session.countPendingErrors() > 0) {
      RowErrorsAndOverflowStatus errors = session.getPendingErrors();
      RowError[] rowErrors = errors.getRowErrors();
      StringBuilder sb = new StringBuilder("Errors during DeleteByPkPrefix:");
      for (RowError err : rowErrors) {
        sb.append("\n  ").append(err.toString());
      }
      throw new NonRecoverableException(Status.RuntimeError(sb.toString()));
    }
  }
}
