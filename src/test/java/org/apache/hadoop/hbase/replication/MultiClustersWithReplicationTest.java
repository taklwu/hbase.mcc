package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConfigConst;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ConnectionManagerMultiClusterWrapper;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseMultiClusterConfigUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, LargeTests.class})
public class MultiClustersWithReplicationTest extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(MultiClustersWithReplicationTest.class);

  @Rule
  public TestName name = new TestName();

  private static final byte[] count = Bytes.toBytes("count");
  private static final byte[] row1 = Bytes.toBytes("row1");
  private static final byte[] put = Bytes.toBytes("put");
  private static final byte[] delete = Bytes.toBytes("delete");
  private static final byte[] famName1 = Bytes.toBytes("f1");
  HBaseTestingUtility[] utilities;
  Configuration[] configurations;


  private static final Logger LOG = LoggerFactory.getLogger(MultiClustersWithReplicationTest.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // reused the CoprocessorCounter for counting operations, e.g. put and get for replications
    UTIL1.getConfiguration().setStrings(
      CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      TestMasterReplication.CoprocessorCounter.class.getName());
    UTIL2.getConfiguration().setStrings(
      CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      TestMasterReplication.CoprocessorCounter.class.getName());
    configureClusters(UTIL1, UTIL2);

    startClusters();
  }

  @Before
  public void setUpBase() throws Exception {
    utilities = new HBaseTestingUtility[]{UTIL1, UTIL2};
    configurations = new Configuration[] {UTIL1.getConfiguration(), UTIL2.getConfiguration()};
    // don't setup the peer here.
  }


  @Test
  public void testHFileReplicationForConfiguredTableCfs() throws Exception {
    // do nothing
  }

  /**
   * It tests the replication scenario involving 0 -> 1 -> 0. It does it by
   * adding and deleting a row to a table in each cluster, checking if it's
   * replicated. It also tests that the puts and deletes are not replicated back
   * to the originating cluster.
   */
  @Test
  public void testCyclicReplication1() throws Exception {
    LOG.info("testSimplePutDelete");
    int numClusters = 2;
    Table[] htables = null;
    try {
      htables = setUpClusterTablesAndPeers(numClusters);

      int[] expectedCounts = new int[] { 2, 2 };

      // add rows to both clusters,
      // make sure they are both replication
      putAndWait(row, famName, htables[0], htables[1]);
      putAndWait(row1, famName, htables[1], htables[0]);
      validateCounts(htables, put, expectedCounts);

      deleteAndWait(row, htables[0], htables[1]);
      deleteAndWait(row1, htables[1], htables[0]);
      validateCounts(htables, delete, expectedCounts);
    } finally {
      close(htables);
    }
  }

  @Test
  public void testCyclicReplicationWithMultiClusterClient() throws Exception {
    LOG.info("testSimplePutDelete");
    int numClusters = 2;
    Table[] htables = null;
    try {
      htables = setUpClusterTablesAndPeers(numClusters);

      int[] expectedCounts = new int[] { 2, 2 };

      // add rows to both clusters,
      // make sure they are both replication
//      putAndWait(row, famName, htables[0], htables[1]);
//      putAndWait(row1, famName, htables[1], htables[0]);

      Configuration combinedConfig = HBaseMultiClusterConfigUtil
        .combineConfigurations(UTIL1.getConfiguration(), UTIL2.getConfiguration());

      combinedConfig.setInt(ConfigConst.HBASE_WAIT_TIME_BEFORE_TRYING_PRIMARY_AFTER_FAILURE, 0);

      Connection connection = ConnectionManagerMultiClusterWrapper.createConnection(combinedConfig);

      TableName tableName = TableName.valueOf(name.getMethodName());
      Table multiTable = connection.getTable(tableName);
      assertTrue(connection.getAdmin().isTableEnabled(tableName));

      // do a put
      Put put1 = new Put(row);
      put1.addColumn(famName, row, row);
      multiTable.put(put1);
      wait(row, htables[0], false);
      wait(row, htables[1], false);


      Get get1 = new Get(row);
      Result r1_1 = htables[0].get(get1);
      Result r1_2 = htables[1].get(get1);
      Result all = multiTable.get(get1);

      System.out.println("----------------------------");
      System.out.println(r1_1 + " " + r1_2);
      System.out.println(r1_1.isEmpty() + " " + r1_2.isEmpty());
      System.out.println("----------------------------");
      assertFalse("row not found in multiTable", all.isEmpty());
      assertFalse("row not found in cluster1", r1_1.isEmpty());
      assertFalse("row not found in cluster2", r1_2.isEmpty());

      // do a put
      Put put2 = new Put(row1);
      put2.addColumn(famName, row1, row1);
      multiTable.put(put2);
      // TODO if we don't wait for the edit shows on the clusters, two cluster has a differnet view
      // here the assumption of MCC is trying first on primary then go to failover
      // if replication stop worker from the primary and do not sync the edits to replication
      // fail over does not work , or the primary exists but failover still not having the
      // edits because of some reasons, then the request failed.
      // in other words, failover depends on the cyclic or master-master replications
      // should we introduce a new strong consistency model ? but how?
      // 1. by updating the replication module and check replication status ?
      // 2. [time out] or delay ?
      // 3. or verification for each put ? this is slow...
      wait(row1, htables[0], false);
      wait(row1, htables[1], false);

      Get get2 = new Get(row1);
      r1_1 = htables[0].get(get2);
      r1_2 = htables[1].get(get2);
      all = multiTable.get(get2);

      System.out.println("----------------------------");
      System.out.println(r1_1 + " " + r1_2);
      System.out.println(r1_1.isEmpty() + " " + r1_2.isEmpty());
      System.out.println("----------------------------");
      assertFalse("row not found in multiTable", all.isEmpty());
      assertFalse("row not found in cluster1", r1_1.isEmpty());
      assertFalse("row not found in cluster2", r1_2.isEmpty());

      validateCounts(htables, put, expectedCounts);

//      deleteAndWait(row, htables[0], htables[1]);
//      deleteAndWait(row1, htables[1], htables[0]);
//      validateCounts(htables, delete, expectedCounts);
    } finally {
      close(htables);
    }
  }

  private void validateCounts(Table[] htables, byte[] type,
    int[] expectedCounts) throws IOException {
    for (int i = 0; i < htables.length; i++) {
      assertEquals(Bytes.toString(type) + " were replicated back ",
        expectedCounts[i], getCount(htables[i], type));
    }
  }

  private void close(Closeable... closeables) {
    try {
      if (closeables != null) {
        for (Closeable closeable : closeables) {
          closeable.close();
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception occurred while closing the object:", e);
    }
  }

  private void deleteAndWait(byte[] row, Table source, Table target)
    throws Exception {
    Delete del = new Delete(row);
    source.delete(del);
    wait(row, target, true);
  }

  private void putAndWait(byte[] row, byte[] fam, Table source, Table target)
    throws Exception {
    Put put = new Put(row);
    put.addColumn(fam, row, row);
    source.put(put);
    wait(row, target, false);
  }

  private Table[] setUpClusterTablesAndPeers(int numClusters) throws Exception {
    Table[] htables;
    // startMiniClusters(numClusters);
    TableName tableName = TableName.valueOf(name.getMethodName());
    createTableOnClusters(name.getMethodName(), utilities);

    htables = getHTablesOnClusters(tableName);
    // htables = new Table[] { htable1, htable2 };
    // Test the replication scenarios of 0 -> 1 -> 0
    addPeer("1", 0, 1);
    addPeer("1", 1, 0);
    return htables;
  }

  private Table[] getHTablesOnClusters(TableName tableName) throws Exception {
    int numClusters = utilities.length;
    Table[] htables = new Table[numClusters];
    for (int i = 0; i < numClusters; i++) {
      Table htable = ConnectionFactory.createConnection(configurations[i]).getTable(tableName);
      htables[i] = htable;
    }
    return htables;
  }

  private void addPeer(String id, int masterClusterNumber,
    int slaveClusterNumber) throws Exception {

    try (Connection conn = ConnectionFactory.createConnection(configurations[masterClusterNumber]);
      Admin admin = conn.getAdmin()) {
      if (!peerExist(admin, id)) {
        LOG.info("setting peer from clusterA {} to clusterB to {}, id = {}",
          masterClusterNumber, slaveClusterNumber, id);
        admin.addReplicationPeer(id,
          new ReplicationPeerConfig().setClusterKey(utilities[slaveClusterNumber].getClusterKey()));
      }
    }
  }

  private void wait(byte[] row, Table target, boolean isDeleted) throws Exception {
    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for replication. Row:" + Bytes.toString(row)
          + ". IsDeleteReplication:" + isDeleted);
      }
      Result res = target.get(get);
      boolean sleep = isDeleted ? res.size() > 0 : res.isEmpty();
      if (sleep) {
        LOG.info("Waiting for more time for replication. Row:"
          + Bytes.toString(row) + ". IsDeleteReplication:" + isDeleted);
        Thread.sleep(SLEEP_TIME);
      } else {
        if (!isDeleted) {
          assertArrayEquals(res.value(), row);
        }
        LOG.info("Obtained row:"
          + Bytes.toString(row) + ". IsDeleteReplication:" + isDeleted);
        break;
      }
    }
  }

  private void rollWALAndWait(final HBaseTestingUtility utility, final TableName table,
    final byte[] row) throws IOException {
    final Admin admin = utility.getAdmin();
    final MiniHBaseCluster cluster = utility.getMiniHBaseCluster();

    // find the region that corresponds to the given row.
    HRegion region = null;
    for (HRegion candidate : cluster.getRegions(table)) {
      if (HRegion.rowIsInRange(candidate.getRegionInfo(), row)) {
        region = candidate;
        break;
      }
    }
    assertNotNull("Couldn't find the region for row '" + Arrays.toString(row) + "'", region);

    final CountDownLatch latch = new CountDownLatch(1);

    // listen for successful log rolls
    final WALActionsListener listener = new WALActionsListener() {
      @Override
      public void postLogRoll(final Path oldPath, final Path newPath) throws IOException {
        latch.countDown();
      }
    };
    region.getWAL().registerWALActionsListener(listener);

    // request a roll
    admin.rollWALWriter(cluster.getServerHoldingRegion(region.getTableDescriptor().getTableName(),
      region.getRegionInfo().getRegionName()));

    // wait
    try {
      latch.await();
    } catch (InterruptedException exception) {
      LOG.warn("Interrupted while waiting for the wal of '" + region + "' to roll. If later " +
        "replication tests fail, it's probably because we should still be waiting.");
      Thread.currentThread().interrupt();
    }
    region.getWAL().unregisterWALActionsListener(listener);
  }

  private int getCount(Table t, byte[] type) throws IOException {
    Get test = new Get(row);
    test.setAttribute("count", new byte[] {});
    Result res = t.get(test);
    return Bytes.toInt(res.getValue(count, type));
  }

  private boolean peerExist(Admin admin, String peer) throws IOException {
    return admin.listReplicationPeers().stream().anyMatch((p) -> peer.equals(p.getPeerId()));
  }

  private void createTableOnClusters(String tableName, HBaseTestingUtility[] utilities)
    throws Exception {
    TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(famName)
        .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(famName1)
        .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(noRepfamName)).build();
    for (HBaseTestingUtility utility : utilities) {
      utility.getAdmin().createTable(table);
    }
  }

}
