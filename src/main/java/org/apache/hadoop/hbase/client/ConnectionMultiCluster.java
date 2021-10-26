/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;

public class ConnectionMultiCluster implements Connection {

  Connection primaryConnection;
  Connection[] failoverConnections;
  Configuration originalConfiguration;
  boolean isMasterMaster;
  int waitTimeBeforeAcceptingResults;
  int waitTimeBeforeRequestingFailover;
  int waitTimeBeforeMutatingFailover;
  int waitTimeBeforeMutatingFailoverWithPrimaryException;
  int waitTimeBeforeAcceptingBatchResults;
  int waitTimeBeforeRequestingBatchFailover;
  int waitTimeBeforeMutatingBatchFailover;
  int waitTimeFromLastPrimaryFail;

  static final Log LOG = LogFactory.getLog(ConnectionMultiCluster.class);

  ExecutorService executor;

  public ConnectionMultiCluster(Configuration originalConfiguration,
    Connection primaryConnection, Connection[] failoverConnections) {
    this.primaryConnection = primaryConnection;
    this.failoverConnections = failoverConnections;
    this.originalConfiguration = originalConfiguration;
    this.isMasterMaster = originalConfiguration
      .getBoolean(
        ConfigConst.HBASE_FAILOVER_MODE_CONFIG,
        false);
    this.waitTimeBeforeAcceptingResults = originalConfiguration
      .getInt(
        ConfigConst.HBASE_WAIT_TIME_BEFORE_ACCEPTING_FAILOVER_RESULT_CONFIG,
        100);
    this.waitTimeBeforeMutatingFailover = originalConfiguration
      .getInt(
        ConfigConst.HBASE_WAIT_TIME_BEFORE_MUTATING_FAILOVER_CONFIG,
        100);
    this.waitTimeBeforeMutatingFailoverWithPrimaryException = originalConfiguration
      .getInt(
        ConfigConst.HBASE_WAIT_TIME_BEFORE_MUTATING_FAILOVER_WITH_PRIMARY_EXCEPTION_CONFIG,
        0);
    this.waitTimeBeforeRequestingFailover = originalConfiguration
      .getInt(
        ConfigConst.HBASE_WAIT_TIME_BEFORE_REQUEST_FAILOVER_CONFIG,
        100);
    this.waitTimeBeforeAcceptingBatchResults = originalConfiguration
      .getInt(
        ConfigConst.HBASE_WAIT_TIME_BEFORE_ACCEPTING_FAILOVER_BATCH_RESULT_CONFIG,
        100);
    this.waitTimeBeforeRequestingBatchFailover = originalConfiguration
      .getInt(
        ConfigConst.HBASE_WAIT_TIME_BEFORE_MUTATING_BATCH_FAILOVER_CONFIG,
        100);
    this.waitTimeBeforeMutatingBatchFailover = originalConfiguration
      .getInt(
        ConfigConst.HBASE_WAIT_TIME_BEFORE_REQUEST_BATCH_FAILOVER_CONFIG,
        100);
    this.waitTimeFromLastPrimaryFail = originalConfiguration
      .getInt(ConfigConst.HBASE_WAIT_TIME_BEFORE_TRYING_PRIMARY_AFTER_FAILURE, 5000);

    executor = Executors.newFixedThreadPool(originalConfiguration.getInt(ConfigConst.HBASE_MULTI_CLUSTER_CONNECTION_POOL_SIZE, 20));
  }

  public void abort(String why, Throwable e) {
    primaryConnection.abort(why, e);
    for (Connection failOverConnection : failoverConnections) {
      failOverConnection.abort(why, e);
    }
  }

  public boolean isAborted() {
    return primaryConnection.isAborted();
  }

  public void close() throws IOException {

    Exception lastException = null;
    try {
      primaryConnection.close();
    } catch (Exception e) {
      LOG.error("Exception while closing primary", e);
      lastException = e;
    }
    for (Connection failOverConnection : failoverConnections) {
      try {
        failOverConnection.close();
      } catch (Exception e) {
        LOG.error("Exception while closing primary", e);
        lastException = e;
      }
    }
    if (lastException != null) {
      throw new IOException(lastException);
    }
  }

  public Configuration getConfiguration() {
    return originalConfiguration;
  }

  @Override
  public Table getTable(TableName tableName) throws IOException {
    LOG.info(" -- getting primary table" + primaryConnection.getConfiguration().get("hbase.zookeeper.quorum"));
    Table primaryTable = primaryConnection.getTable(tableName);

    LOG.info(" --- got primary table");
    ArrayList<Table> failoverTables = new ArrayList<Table>();
    for (Connection failOverConnection : failoverConnections) {
      LOG.info(" -- getting failover table:" + failOverConnection.getConfiguration().get("hbase.zookeeper.quorum"));

      Table table = failOverConnection.getTable(tableName);

      failoverTables.add(table);
      LOG.info(" --- got failover table");
    }

    return new HTableMultiCluster(originalConfiguration, primaryTable,
      failoverTables, isMasterMaster,
      waitTimeBeforeAcceptingResults,
      waitTimeBeforeRequestingFailover,
      waitTimeBeforeMutatingFailover,
      waitTimeBeforeMutatingFailoverWithPrimaryException,
      waitTimeBeforeAcceptingBatchResults,
      waitTimeBeforeRequestingBatchFailover,
      waitTimeBeforeMutatingBatchFailover,
      waitTimeFromLastPrimaryFail);
  }


  public Table getTable(TableName tableName, ExecutorService pool)
    throws IOException {
    Table primaryTable = primaryConnection.getTable(tableName, pool);
    ArrayList<Table> failoverTables = new ArrayList<Table>();
    for (Connection failOverConnection : failoverConnections) {
      failoverTables.add(failOverConnection.getTable(tableName, pool));
    }

    return new HTableMultiCluster(originalConfiguration, primaryTable,
      failoverTables, isMasterMaster,
      waitTimeBeforeAcceptingResults,
      waitTimeBeforeRequestingFailover,
      waitTimeBeforeMutatingFailover,
      waitTimeBeforeMutatingFailoverWithPrimaryException,
      waitTimeBeforeAcceptingBatchResults,
      waitTimeBeforeRequestingBatchFailover,
      waitTimeBeforeMutatingBatchFailover,
      waitTimeFromLastPrimaryFail);
  }

  @Override public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return null;
  }

  @Override public BufferedMutator getBufferedMutator(BufferedMutatorParams params)
    throws IOException {
    return null;
  }

  @Override public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public void clearRegionLocationCache() {
    primaryConnection.clearRegionLocationCache();
    Arrays.stream(failoverConnections).forEach(connection -> connection.clearRegionLocationCache());
  }

  @Override
  public Admin getAdmin() throws IOException {
    return primaryConnection.getAdmin();
  }

  @Deprecated
  public
  TableDescriptor getTableDescriptor(byte[] tableName) throws IOException {
    return primaryConnection.getAdmin().getTableDescriptor(TableName.valueOf(tableName));
  }

  public HRegionLocation locateRegion(TableName tableName, byte[] row)
    throws IOException {
    return primaryConnection.getRegionLocator(tableName).getRegionLocation(row);
  }

  @Deprecated
  public HRegionLocation locateRegion(byte[] tableName, byte[] row)
    throws IOException {
    return primaryConnection.getRegionLocator(TableName.valueOf(tableName)).getRegionLocation(row);
  }

  public boolean isClosed() {
    return primaryConnection.isClosed();
  }

  @Override
  public TableBuilder getTableBuilder(TableName tableName, ExecutorService pool) {
    return primaryConnection.getTableBuilder(tableName, pool);
  }

  @Override
  public Hbck getHbck() throws IOException {
    return primaryConnection.getHbck();
  }

  @Override public Hbck getHbck(ServerName masterServer) throws IOException {
    return primaryConnection.getHbck(masterServer);
  }

}
