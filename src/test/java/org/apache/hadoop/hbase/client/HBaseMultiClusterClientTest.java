/**
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * this test class tests client fails over to the second cluster without setting up
 * kerberos and bi-directional replicas
 */
public class HBaseMultiClusterClientTest {

  private static HBaseTestingUtility HTU1 = new HBaseTestingUtility();
  private static HBaseTestingUtility HTU2 = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    //Cluster1
    HTU1 = new HBaseTestingUtility();
    HTU1.getConfiguration().set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/x1");
    HTU1.getConfiguration().set(HConstants.ZOOKEEPER_CLIENT_PORT,
      "64410");
    HTU1.getConfiguration().set(HConstants.MASTER_INFO_PORT, "64310");
    //HTU1.setZkCluster(htu0.getZkCluster());


    // Cluster 2
    HTU2 = new HBaseTestingUtility();
    HTU2.getConfiguration().set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/x2");
    HTU2.getConfiguration().set(HConstants.ZOOKEEPER_CLIENT_PORT,
      "64110");
    HTU2.getConfiguration().set(HConstants.MASTER_INFO_PORT, "64210");

    HTU1.startMiniCluster();

    System.out.println("------------------------1");

    HTU2.startMiniCluster();

    System.out.println("------------------------2");
  }

  @AfterClass
  public static void after() throws Exception {
    HTU2.shutdownMiniCluster();
    HTU1.shutdownMiniCluster();
  }

  @Test
  public void testHBaseMultiClusterClientTest() throws Exception {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("{Start}");

    System.out.println(strBuilder.toString());

    final TableName TABLE_NAME = TableName.valueOf("test");
    final byte[] FAM_NAME = Bytes.toBytes("fam");
    final byte[] QUAL_NAME = Bytes.toBytes("qual");
    final byte[] VALUE = Bytes.toBytes("value");

    System.out.println(strBuilder.toString());

    Table table1 = HTU1.createTable(TABLE_NAME, FAM_NAME);
    Table table2 = HTU2.createTable(TABLE_NAME, FAM_NAME);

    Configuration combinedConfig = HBaseMultiClusterConfigUtil.combineConfigurations(HTU1.getConfiguration(),
      HTU2.getConfiguration());

    combinedConfig.setInt(ConfigConst.HBASE_WAIT_TIME_BEFORE_TRYING_PRIMARY_AFTER_FAILURE, 0);

    Connection connection = ConnectionManagerMultiClusterWrapper.createConnection(combinedConfig);

    Table multiTable = connection.getTable(TABLE_NAME);

    Put put1 = new Put(Bytes.toBytes("A1"));
    put1.addColumn(FAM_NAME, QUAL_NAME, VALUE);
    multiTable.put(put1);
    // multiTable.flushCommits();

    Get get1 = new Get(Bytes.toBytes("A1"));
    Result r1_1 = table1.get(get1);
    Result r1_2 = table2.get(get1);

    System.out.println("----------------------------");
    System.out.println(r1_1 + " " + r1_2);
    System.out.println(r1_1.isEmpty() + " " + r1_2.isEmpty());
    System.out.println("----------------------------");
    assertFalse("A1 not found in HTU1", r1_1.isEmpty());
    assertTrue("A1 found in HTU2", r1_2.isEmpty());

    strBuilder.append("{r1_1.isEmpty():" + r1_1.isEmpty() + "}");
    strBuilder.append("{r1_2.isEmpty():" + r1_2.isEmpty() + "}");

    System.out.println(strBuilder.toString());

    HTU1.deleteTable(TABLE_NAME);
    System.out.println("------------2");

    Put put2 = new Put(Bytes.toBytes("A2"));
    put2.addColumn(FAM_NAME, QUAL_NAME, VALUE);
    System.out.println("------------3");
    table2.put(put2);

    Get get2 = new Get(Bytes.toBytes("A2"));
    System.out.println(table2.get(get2));

    System.out.println("------------5");
    multiTable.put(put2);
    // multiTable.flushCommits();
    //Get get2 = new Get(Bytes.toBytes("A2"));
    Result r2_2 = table2.get(get2);
    assertFalse("A2 not found in HTU2", r2_2.isEmpty());

    strBuilder.append("{r2_2.getExists():" + r2_2.isEmpty() + "}");

    System.out.println(strBuilder.toString());

    System.out.println("------------6");
    table1 = HTU1.createTable(TABLE_NAME, FAM_NAME);

    System.out.println("------------7");

    Put put3 = new Put(Bytes.toBytes("A3"));
    put3.addColumn(FAM_NAME, QUAL_NAME, VALUE);
    multiTable = connection.getTable(TABLE_NAME);
    multiTable.put(put3);
    // multiTable.flushCommits();
    System.out.println("------------8");

    Get get3 = new Get(Bytes.toBytes("A3"));
    Result r3_1 = table1.get(get3);
    Result r3_2 = table2.get(get3);

    System.out.println("----------------------------");
    System.out.println(r3_1 + " " + r3_2);
    System.out.println(r3_1.isEmpty() + " " + r3_2.isEmpty());
    System.out.println("----------------------------");

    assertFalse("A3 not found in HTU1", r3_1.isEmpty());

    assertTrue("A3 found in HTU2", r3_2.isEmpty());

    strBuilder.append("{r3_1.isEmpty():" + r3_1.isEmpty() + "}");
    strBuilder.append("{r3_2.isEmpty():" + r3_2.isEmpty() + "}");

    table1.close();
    table2.close();

    System.out.println(strBuilder.toString());
  }

}
