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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class HBaseAdminMultiCluster extends HBaseAdmin {

  Logger LOG = Logger.getLogger(HBaseAdminMultiCluster.class);

  Map<String, HBaseAdmin> failoverAdminMap = new HashMap<String, HBaseAdmin>();

  public HBaseAdminMultiCluster(Configuration c)
      throws MasterNotRunningException, ZooKeeperConnectionException,
      IOException {
    super((ClusterConnection) ConnectionFactory.createConnection(
      HBaseMultiClusterConfigUtil.splitMultiConfigFile(c)
        .get(HBaseMultiClusterConfigUtil.PRIMARY_NAME)));

    Map<String, Configuration> configs = HBaseMultiClusterConfigUtil
      .splitMultiConfigFile(c);

    for (Entry<String, Configuration> entry : configs.entrySet()) {

      if (!entry.getKey().equals(HBaseMultiClusterConfigUtil.PRIMARY_NAME)) {
        HBaseAdmin admin = new HBaseAdmin((ClusterConnection) ConnectionFactory.createConnection(
          entry.getValue()));
        LOG.info("creating HBaseAdmin for : " + entry.getKey());
        failoverAdminMap.put(entry.getKey(), admin);
        LOG.info(" - successfully creating HBaseAdmin for : " + entry.getKey());
      }
    }
    LOG.info("Successful loaded all HBaseAdmins");

  }

  @Override
  /**
   * This will only return tables that all three HBase clusters have
   */
  public HTableDescriptor[] listTables() throws IOException {
    Map<TableName, HTableDescriptor> tableMap = new HashMap<TableName, HTableDescriptor>();

    HTableDescriptor[] primaryList = super.listTables();

    for (HTableDescriptor table : primaryList) {
      tableMap.put(table.getTableName(), table);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      Map<TableName, HTableDescriptor> tempTableMap = new HashMap<TableName, HTableDescriptor>();

      TableDescriptor[] failureList = super.listTables();

      for (TableDescriptor table : failureList) {
        TableName tableName = table.getTableName();
        if (tableMap.containsKey(tableName)) {
          tempTableMap.put(tableName, tableMap.get(tableName));
        }
      }
      tableMap = tempTableMap;
    }

    HTableDescriptor[] results = new HTableDescriptor[tableMap.size()];
    int counter = 0;
    for (HTableDescriptor table : tableMap.values()) {
      results[counter++] = table;
    }

    return results;
  }

  @Override
  public void createTable(final TableDescriptor desc) throws IOException {

    HBaseAdminMultiCluster.super.createTable(desc, null);

    /*

    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.createTable(desc, null);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().createTable(desc, null);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "createTable");
    */
  }

  @Override
  public void createTable(final TableDescriptor desc, final byte[] startKey,
      final byte[] endKey, final int numRegions) throws IOException {

    HBaseAdminMultiCluster.super.createTable(desc, startKey, endKey,
            numRegions);

    /*

    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.createTable(desc, startKey, endKey,
            numRegions);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().createTable(desc, startKey, endKey, numRegions);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "createTable");
    */
  }

  @Override
  public void createTable(final TableDescriptor desc, 
      final byte[][] splitKeys) throws IOException {

    HBaseAdminMultiCluster.super.createTable(desc, splitKeys);

    /*

    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 2];
    int counter = 0;

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          LOG.info("-- Create table: " + desc.getNameAsString() + " for cluster " + entry.getKey());
          LOG.info("zk.quorum: " + entry.getValue().getConfiguration().get("hbase.zookeeper.quorum"));
          for (TableName tableName : entry.getValue().listTableNames()) {
            LOG.info(entry.getKey() + " PreCreate- " + Bytes.toString(tableName.getName()));
          }
          LOG.info("-<");
          //entry.getValue().createTable(desc, splitKeys);
          LOG.info(">-");
          for (TableName tableName : entry.getValue().listTableNames()) {
            LOG.info(entry.getKey() + " PostCreate- " + Bytes.toString(tableName.getName()));
          }
          LOG.info("-- Created table: " + desc.getNameAsString() + " for cluster " + entry.getKey());
          return null;
        }
      };
    }
    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        LOG.info("-- Create table: " + desc.getNameAsString() + " for primary cluster");
        LOG.info("zk.quorum: " + HBaseAdminMultiCluster.super.getConfiguration().get("hbase.zookeeper.quorum"));
        for (TableName tableName : HBaseAdminMultiCluster.super.listTableNames()) {
          LOG.info("Primary PreCreate- " + Bytes.toString(tableName.getName()));
        }
        HBaseAdminMultiCluster.super.createTable(desc, splitKeys);
        for (TableName tableName : HBaseAdminMultiCluster.super.listTableNames()) {
          LOG.info("Primary PostCreate- " + Bytes.toString(tableName.getName()));
        }
        LOG.info("-- Created table: " + desc.getNameAsString() + " for primary cluster");
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          LOG.info("-- Create table: " + desc.getNameAsString() + " for cluster " + entry.getKey());
          LOG.info("zk.quorum: " + entry.getValue().getConfiguration().get("hbase.zookeeper.quorum"));
          for (TableName tableName : entry.getValue().listTableNames()) {
            LOG.info(entry.getKey() + " PreCreate- " + Bytes.toString(tableName.getName()));
          }
          LOG.info("-<");
          //entry.getValue().createTable(desc, splitKeys);
          LOG.info(">-");
          for (TableName tableName : entry.getValue().listTableNames()) {
            LOG.info(entry.getKey() + " PostCreate- " + Bytes.toString(tableName.getName()));
          }
          LOG.info("-- Create table: " + desc.getNameAsString() + " for cluster " + entry.getKey());
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "createTable");
    */
  }

  @Override
  public Future<Void> createTableAsync(final TableDescriptor desc,
      final byte[][] splitKeys) throws IOException {
    
    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        LOG.info("createTableAsync: " + desc.getTableName() + " for cluster: primary");
        HBaseAdminMultiCluster.super.createTableAsync(desc, splitKeys);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          LOG.info("createTableAsync: " + desc.getTableName() + " for cluster: " + entry.getKey());
          entry.getValue().createTableAsync(desc, splitKeys);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "createTableAsync");
    LOG.info("Sucessfully called createTableAsync");
    return null;
  }

  @Override
  public void deleteTable(final TableName tableName) throws IOException {
    
    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        LOG.info("Delete Table: " + tableName + " for cluster: Primary");
        HBaseAdminMultiCluster.super.deleteTable(tableName);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          LOG.info("Delete Table: " + tableName + " for cluster: " + entry.getKey());
          for (TableName tableName : entry.getValue().listTableNames()) {
            LOG.info(entry.getKey() + " PreDelete- " + Bytes.toString(tableName.getName()));
          }
          entry.getValue().deleteTable(tableName);
          LOG.info("Deleted Table: " + tableName + " for cluster: " + entry.getKey());
          for (TableName tableName : entry.getValue().listTableNames()) {
            LOG.info(entry.getKey() + " PostDelete- " + Bytes.toString(tableName.getName()));
          }
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "deleteTable");
  }

  @Override
  public HTableDescriptor[] deleteTables(String regex) throws IOException {
    return deleteTables(Pattern.compile(regex));
  }

  @Override
  public HTableDescriptor[] deleteTables(final Pattern pattern) throws IOException {
    
    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;
    final Value<HTableDescriptor[]> results = new Value<HTableDescriptor[]>();
    
    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        results.setValue(HBaseAdminMultiCluster.super.deleteTables(pattern));
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().deleteTables(pattern);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "deleteTables");
    
    return results.getValue();
  }

  @Override
  public void enableTable(final TableName tableName) throws IOException {
    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.enableTable(tableName);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().enableTable(tableName);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "enableTable");
  }

  public Future<Void> enableTableAsync(final TableName tableName) throws IOException {
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.enableTableAsync(tableName);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().enableTableAsync(tableName);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "enableTableAsync");
    return null;
  }


  @Override
  public HTableDescriptor[] enableTables(String regex) throws IOException {
    return enableTables(Pattern.compile(regex));
  }

  @Override
  public HTableDescriptor[] enableTables(final Pattern pattern) throws IOException {
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;
    final Value<HTableDescriptor[]> results = new Value<HTableDescriptor[]>();

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        results.setValue(HBaseAdminMultiCluster.super.enableTables(pattern));
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().enableTables(pattern);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "enableTables");
    
    return results.getValue();
  }

  @Override
  public Future<Void> disableTableAsync(final TableName tableName) throws IOException {
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.disableTableAsync(tableName);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().disableTableAsync(tableName);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "disableTableAsync");
    return null;
  }

  @Override
  public void disableTable(final TableName tableName) throws IOException {

    HBaseAdminMultiCluster.super.disableTable(tableName);
    /*
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        LOG.info("Disable Table: " + tableName + " for cluster: primary");
        HBaseAdminMultiCluster.super.disableTable(tableName);
        LOG.info("Disabled Table: " + tableName + " for cluster: primary");
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          LOG.info("Disable Table: " + tableName + " for cluster: " + entry.getKey());
          entry.getValue().disableTable(tableName);
          LOG.info("Disabled Table: " + tableName + " for cluster: " + entry.getKey());
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "disableTable");
    */
  }

  /**
   * @param tableName
   *          Table to check.
   * @return True if table exists already.
   * @throws IOException
   */
  public Map<String, Boolean> tableExistMultiCluster(final TableName tableName)
      throws IOException {
    Map<String, Boolean> results = new HashMap<String, Boolean>();
    results.put(HBaseMultiClusterConfigUtil.PRIMARY_NAME,
        super.tableExists(tableName));

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      results.put(entry.getKey(), entry.getValue().tableExists(tableName));
    }
    return results;
  }

  public void close() throws IOException {
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.close();
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().close();
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "close");
    
  }

  public void abort(String why, Throwable e) {
    try {
      super.abort(why, e);
    } catch (Exception e2) {
      LOG.error("Unable to abort in primary.", e2);
    }
    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().abort(why, e);
      } catch (Exception e2) {
        LOG.error("Unable to abort  " + entry.getKey(), e2);
      }
    }

  }

  public boolean isAborted() {
    // TODO Auto-generated method stub
    return false;
  }

  private void replicationClusterExecute(Callable<Void>[] callArray,
      String actionMethodName) throws IOException {
    Exception exp = null;
    int expCounter = 0;

    for (Callable<Void> call : callArray) {
      try {
        call.call();
      } catch (Exception e) {
        if (exp == null) {
          exp = e;
        }
        expCounter++;
      }
    }
    if (expCounter > 0) {
      throw new IOException("Got " + expCounter
          + " exceptions trying to execute " + actionMethodName, exp);
    }
  }
  
  private static class Value<T extends Object> {
    T value;
    public void setValue(T value) {
      this.value = value;
    }
    
    public T getValue() {
      return value;
    }
  }
}
