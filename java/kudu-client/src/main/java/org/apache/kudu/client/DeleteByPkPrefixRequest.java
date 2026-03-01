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

import com.google.protobuf.Message;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.util.Timer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.Schema;
import org.apache.kudu.security.Token;
import org.apache.kudu.tserver.Tserver;
import org.apache.kudu.util.Pair;

/**
 * RPC to delete all rows in a tablet matching a primary key prefix.
 *
 * <p>The server scans for rows whose leading PK columns match the provided
 * prefix, then deletes them internally using DELETE_IGNORE operations.
 */
@InterfaceAudience.Private
class DeleteByPkPrefixRequest extends KuduRpc<DeleteByPkPrefixRpcResponse> {

  private final Schema prefixSchema;
  private final PartialRow pkPrefix;
  private final byte[] partitionKey;

  /** The token with which to authorize this RPC. */
  private Token.SignedTokenPB authzToken;

  /**
   * Create a new DeleteByPkPrefix RPC request.
   *
   * @param table the table to delete from
   * @param prefixSchema the schema containing only the prefix columns
   * @param pkPrefix the partial row with prefix column values set
   * @param partitionKey the partition key of the target tablet
   * @param timer Timer to monitor RPC timeouts
   * @param timeoutMillis the timeout of the request in milliseconds
   */
  DeleteByPkPrefixRequest(KuduTable table,
                          Schema prefixSchema,
                          PartialRow pkPrefix,
                          byte[] partitionKey,
                          Timer timer,
                          long timeoutMillis) {
    super(table, timer, timeoutMillis);
    this.prefixSchema = prefixSchema;
    this.pkPrefix = pkPrefix;
    this.partitionKey = partitionKey;
  }

  @Override
  Message createRequestPB() {
    RemoteTablet tablet = super.getTablet();
    final Tserver.DeleteByPkPrefixRequestPB.Builder builder =
        Tserver.DeleteByPkPrefixRequestPB.newBuilder();
    builder.setTabletId(UnsafeByteOperations.unsafeWrap(tablet.getTabletIdAsBytes()));
    builder.setSchema(ProtobufHelper.schemaToPb(prefixSchema));

    // Encode the prefix row using RANGE_LOWER_BOUND format.
    builder.setPkPrefix(new Operation.OperationsEncoder()
        .encodeSingleRow(pkPrefix, Operation.ChangeType.RANGE_LOWER_BOUND));

    if (authzToken != null) {
      builder.setAuthzToken(authzToken);
    }

    return builder.build();
  }

  @Override
  boolean needsAuthzToken() {
    return true;
  }

  @Override
  void bindAuthzToken(Token.SignedTokenPB token) {
    authzToken = token;
  }

  @Override
  String serviceName() {
    return TABLET_SERVER_SERVICE_NAME;
  }

  @Override
  String method() {
    return "DeleteByPkPrefix";
  }

  @Override
  boolean isRequestTracked() {
    return true;
  }

  @Override
  Pair<DeleteByPkPrefixRpcResponse, Object> deserialize(CallResponse callResponse,
                                                         String tsUuid) {
    final Tserver.DeleteByPkPrefixResponsePB.Builder respBuilder =
        Tserver.DeleteByPkPrefixResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);

    long rowsDeleted = respBuilder.hasRowsDeleted() ? respBuilder.getRowsDeleted() : 0;
    DeleteByPkPrefixRpcResponse response = new DeleteByPkPrefixRpcResponse(
        timeoutTracker.getElapsedMillis(), tsUuid, rowsDeleted);
    return new Pair<>(response,
                      respBuilder.hasError() ? respBuilder.getError() : null);
  }

  @Override
  byte[] partitionKey() {
    return this.partitionKey;
  }
}
