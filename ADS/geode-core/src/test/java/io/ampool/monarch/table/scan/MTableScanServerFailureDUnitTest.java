/*
 * Copyright (c) 2017 Ampool, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */
package io.ampool.monarch.table.scan;

import io.ampool.internal.AmpoolOpType;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MScanSplitUnavailableException;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.Table;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(MonarchTest.class)
public class MTableScanServerFailureDUnitTest extends MTableDUnitHelper {

  public MTableScanServerFailureDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();
  Host host = Host.getHost(0);
  private VM vm0 = host.getVM(0);
  private VM vm1 = host.getVM(1);
  private VM vm2 = host.getVM(2);
  private VM client1 = host.getVM(3);

  protected final int NUM_OF_COLUMNS = 10;
  protected final String FTABLE_NAME_PREFIX = "FTable_";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS_PER_BUCKET = 10;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected final int LATEST_TIMESTAMP = 300;
  protected final int MAX_VERSIONS = 5;
  protected final int TABLE_MAX_VERSIONS = 7;
  protected final int NUM_BUCKETS = 113;
  private static final Map<String, Integer> vmToPortMap = new HashMap<>();
  private static final Map<Integer, byte[]> bucketIdToOneRowKeyMap = new HashMap<>();

  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();
    vmToPortMap.put("vm0", (Integer) startServerOn(this.vm0, DUnitLauncher.getLocatorString()));
    vmToPortMap.put("vm1", (Integer) startServerOn(this.vm1, DUnitLauncher.getLocatorString()));
    vmToPortMap.put("vm2", (Integer) startServerOn(this.vm2, DUnitLauncher.getLocatorString()));
    createClientCache(this.client1);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);
    super.tearDown2();
  }

  private void createTableClient(String tableName) {
    createTableClient(0, tableName);
  }

  private void createTableClient(int numRedundantCopies, String tableName) {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setTotalNumOfSplits(NUM_BUCKETS);
    tableDescriptor.setRedundantCopies(numRedundantCopies);
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(tableName, tableDescriptor);
    assertEquals(table.getName(), tableName);
    assertNotNull(table);

    FTableDescriptor fd = new FTableDescriptor();
    fd.setBlockSize(1000);
    for (int i = 0; i < NUM_OF_COLUMNS; i++) {
      fd = fd.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + i));
    }
    fd.setRedundantCopies(numRedundantCopies);
    fd.setTotalNumOfSplits(NUM_BUCKETS);

    FTable ftable = admin.createFTable(FTABLE_NAME_PREFIX + tableName, fd);
    assertNotNull(ftable);
    assertEquals(ftable.getName(), FTABLE_NAME_PREFIX + tableName);
  }

  private void createTableFromClient(VM vm, String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTableClient(tableName);
        return null;
      }
    });
  }

  private void createSingleBucketTableFromClient(VM vm, int numSplits, int numRedundantCopies,
      String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTableClient(numSplits, numRedundantCopies, tableName);
        return null;
      }
    });
  }

  private void createTableClient(int numSplits, int numRedundantCopies, String tableName) {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setTotalNumOfSplits(numSplits);
    tableDescriptor.setRedundantCopies(numRedundantCopies);
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(tableName, tableDescriptor);
    assertEquals(table.getName(), tableName);
    assertNotNull(table);
  }

  private void deleteTable(String tableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    admin.deleteMTable(tableName);
  }

  private void doPuts(String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    Map<Integer, Pair<byte[], byte[]>> splitsMap = MTableUtils.getUniformKeySpaceSplit(NUM_BUCKETS);

    Set<ByteArrayKey> inputStartKeys = new HashSet<>();
    Set<ByteArrayKey> inputEndKeys = new HashSet<>();

    for (int bucketId = 0; bucketId < NUM_BUCKETS; bucketId++) {
      Pair<byte[], byte[]> pair = splitsMap.get(bucketId);
      List<byte[]> keysInRange =
          getKeysInRange(pair.getFirst(), pair.getSecond(), NUM_OF_ROWS_PER_BUCKET);
      bucketIdToOneRowKeyMap.put(bucketId, keysInRange.get(bucketId % NUM_OF_ROWS_PER_BUCKET));
      keysInRange.forEach((K) -> {
        Put record = new Put(K);
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex));
        }
        table.put(record);
      });

    }
    Assert.assertTrue(bucketIdToOneRowKeyMap.size() == NUM_BUCKETS);
  }

  private void doPuts(int rowsPerBucket, String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    Map<Integer, Pair<byte[], byte[]>> splitsMap = MTableUtils.getUniformKeySpaceSplit(NUM_BUCKETS);

    Set<ByteArrayKey> inputStartKeys = new HashSet<>();
    Set<ByteArrayKey> inputEndKeys = new HashSet<>();

    for (int bucketId = 0; bucketId < NUM_BUCKETS; bucketId++) {
      Pair<byte[], byte[]> pair = splitsMap.get(bucketId);
      List<byte[]> keysInRange = getKeysInRange(pair.getFirst(), pair.getSecond(), rowsPerBucket);
      bucketIdToOneRowKeyMap.put(bucketId, keysInRange.get(bucketId % rowsPerBucket));
      keysInRange.forEach((K) -> {
        Put record = new Put(K);
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex));
        }
        table.put(record);
      });

    }
    Assert.assertTrue(bucketIdToOneRowKeyMap.size() == NUM_BUCKETS);
  }

  private void doAppend(int numOfRows, String tableName) {
    FTable table = MClientCacheFactory.getAnyInstance().getFTable(FTABLE_NAME_PREFIX + tableName);
    assertNotNull(table);

    for (int j = 0; j < numOfRows; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_OF_COLUMNS; i++) {
        record.add(COLUMN_NAME_PREFIX + i, Bytes.toBytes(VALUE_PREFIX + i));
      }
      table.append(record);
    }
  }

  private void doPuts2(long totalRecords, String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);

    for (int rowIndex = 0; rowIndex < totalRecords; rowIndex++) {
      Put record = new Put(Bytes.toBytes(String.valueOf(rowIndex)));
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex),
            Bytes.toBytes(VALUE_PREFIX + colIndex));
      }
      table.put(record);
    }
    System.out.println("TEST doPuts2 puts done successfully!");
  }

  private void doPutFrom(VM vm, String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPuts(tableName);
        return null;
      }
    });
  }

  private void verifyBucketActuallyMoved(
      final Map<String, ServerLocation> oldServerLocationOfMovedBucket, String tableName) {
    this.vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        PartitionedRegion pr = (PartitionedRegion) cache.getRegion(tableName);
        assertNotNull(pr);

        oldServerLocationOfMovedBucket.forEach((ID, SL) -> {

          String[] keyPart = ID.split(":");
          assertTrue(keyPart.length == 2);

          Bucket bucket = pr.getRegionAdvisor().getBucket(Integer.valueOf(keyPart[1]));

          assertFalse("VM0 member is still hosting moved bucket", bucket.isHosting());

          BucketRegion bucketRegion =
              bucket.getBucketAdvisor().getProxyBucketRegion().getHostedBucketRegion();

          assertNull("BucketRegion is not null on VM0 member", bucketRegion);
        });

        return null;
      }
    });
  }

  @Test
  public void testBuckedMoved() {
    // create table from client-1
    String tableName = getTestMethodName();
    createTableFromClient(this.client1, tableName);
    /* put from client 2 */
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    MTable table = cache.getTable(tableName);
    assertNotNull(table);
    doPuts(tableName);

    int count = 0;
    int expectedTotalRows = NUM_OF_ROWS_PER_BUCKET * NUM_BUCKETS;
    Scanner scanner = table.getScanner(new Scan());
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);

    // Testing with scan in streming mode
    Scan scan = new Scan();
    scanner = table.getScanner(scan);
    movedRandomBuckets(table, this.vm0, this.vm1, tableName);
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);

    // Testing with scan in batch mode
    scan = new Scan();
    scan.enableBatchMode();
    scanner = table.getScanner(scan);
    movedRandomBuckets(table, this.vm1, this.vm0, tableName);
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);
  }

  private int validateScan(Scanner scanner) {
    int count = 0;
    Row result = null;
    do {
      result = scanner.next();
      if (result != null) {
        int columnIndex = 0;
        for (int i = 0; i < result.getCells().size() - 1; i++) {
          assertEquals(new ByteArrayKey(result.getCells().get(i).getColumnName()),
              new ByteArrayKey(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex)));
          assertEquals(new ByteArrayKey((byte[]) result.getCells().get(i).getColumnValue()),
              new ByteArrayKey(Bytes.toBytes(VALUE_PREFIX + columnIndex)));
          columnIndex++;
        }
        count++;
      }
    } while (result != null);
    scanner.close();
    return count;
  }

  @Test
  public void testScanServerDownWORedundancy() {
    testScanServerDownWORedundancy(false, getTestMethodName());
  }

  @Test
  public void testScanServerDownWORedundancyInBatchMode() {
    testScanServerDownWORedundancy(true, getTestMethodName());
  }

  public void testScanServerDownWORedundancy(boolean batchMode, String tableName) {
    // create table from client-1
    createTableFromClient(this.client1, tableName);
    /* put from client 2 */
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    MTable table = cache.getTable(tableName);
    assertNotNull(table);
    doPuts(tableName);

    int expectedTotalRows = NUM_OF_ROWS_PER_BUCKET * NUM_BUCKETS;

    Scan scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    Scanner scanner = table.getScanner(scan);
    int count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);
    System.out.println(
        "MTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: number of rows (1): "
            + count);

    // Finding number of buckets hosted by vm0
    HashSet<Integer> bucketIds = new HashSet<>();
    for (int bucketId = 0; bucketId < NUM_BUCKETS; bucketId++) {
      bucketIds.add(bucketId);
    }

    Map<Integer, Set<ServerLocation>> bucketToServerMap =
        MTableUtils.getBucketToServerMap(tableName, bucketIds, AmpoolOpType.SCAN_OP);

    int numBucketsOnVm0 = 0;
    for (Map.Entry<Integer, Set<ServerLocation>> locationEntry : bucketToServerMap.entrySet()) {
      for (ServerLocation serverLocation : locationEntry.getValue()) {
        if (serverLocation.getPort() == vmToPortMap.get("vm0")) {
          numBucketsOnVm0++;
        }
      }
    }

    System.out.println(
        "MTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: number of buckets on VM0 = "
            + numBucketsOnVm0);

    expectedTotalRows = expectedTotalRows - numBucketsOnVm0 * NUM_OF_ROWS_PER_BUCKET;
    scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    scanner = table.getScanner(scan);
    System.out.println("MTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: "
        + "Stopping server on vm0");

    stopServerOn(this.vm0);

    boolean caughtException = false;

    try {
      validateScan(scanner);
    } catch (MScanSplitUnavailableException ex) {
      // This is excepted exception
      caughtException = true;
    }
    assertTrue(caughtException);

    // Scan after vm0 is down
    scanner = table.getScanner(new Scan());
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);
    System.out.println(
        "MTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: number of rows (2): "
            + count);
  }

  @Test
  public void testScanServerDownWithRedundancyInBatchMode() {
    testScanServerDownWithRedundancy(true);
  }

  @Test
  public void testTruncateTableServerDownWithRedundancy() throws InterruptedException {
    testTruncateTableServerDownWithRedundancy(false);
  }


  @Test
  public void testTruncateTablesWithRedundancy() {
    testTruncateTablesWithRedundancy(false);
  }

  @Test
  public void testInProgressDefaultScanWithServerDown() {
    testInProgressScanWithServerDown(false, getTestMethodName());
  }

  @Test
  public void testInProgressBatchScanWithServerDown() {
    testInProgressScanWithServerDown(true, getTestMethodName());
  }

  /**
   * Testcase: batch mode with no redundancy. No secondary buckets
   */
  @Test
  public void testScanWithStaleMetaDataServerDown_1() {
    testScanWithStaleMetaDataServerDown(true, 0, getTestMethodName());
  }

  /**
   * Testcase: batch mode with redundancy. Secondary buckets available
   */
  @Test
  public void testScanWithStaleMetaDataServerDown_2() {
    testScanWithStaleMetaDataServerDown(true, 2, getTestMethodName());
  }

  /**
   * Testcase: streaming mode with No redundancy. only primary buckets available
   */
  @Test
  public void testScanWithStaleMetaDataServerDown_3() {
    testScanWithStaleMetaDataServerDown(false, 0, getTestMethodName());
  }

  /**
   * Testcase: stream mode with redundancy. secondary buckets available
   */
  @Test
  public void testScanWithStaleMetaDataServerDown_4() {
    testScanWithStaleMetaDataServerDown(false, 2, getTestMethodName());
  }

  /**
   * #splits = 1 and #redundantCopies = 2
   *
   */
  public void testScanWithStaleMetaDataServerDown(boolean batchMode, int redundantCopies,
      String tableName) {
    // create table from client-1
    createSingleBucketTableFromClient(this.client1, NUM_BUCKETS, redundantCopies, tableName);

    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    MTable table = cache.getTable(tableName);
    assertNotNull(table);
    int expectedTotalRows = NUM_BUCKETS * NUM_OF_ROWS_PER_BUCKET;
    /* put from client 2 */
    doPuts(tableName);

    Scan scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    Scanner scanner = table.getScanner(scan);
    int totalRecordsScaned = validateScan(scanner);
    assertEquals(expectedTotalRows, totalRecordsScaned);
    System.out.println("TEST  :: number of rows Scanned: " + totalRecordsScaned);

    scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    scanner = table.getScanner(scan);
    boolean caughtException = false;

    // Finding number of buckets hosted by vm0
    HashSet<Integer> bucketIds = new HashSet<>();
    for (int bucketId = 0; bucketId < NUM_BUCKETS; bucketId++) {
      bucketIds.add(bucketId);
    }

    Map<Integer, Set<ServerLocation>> bucketToServerMap =
        MTableUtils.getBucketToServerMap(tableName, bucketIds, AmpoolOpType.SCAN_OP);
    int numBucketsOnVm0 = 0;
    int numBucketsOnVm1 = 0;
    int numBucketsOnVm2 = 0;
    for (Map.Entry<Integer, Set<ServerLocation>> locationEntry : bucketToServerMap.entrySet()) {
      for (ServerLocation serverLocation : locationEntry.getValue()) {
        if (serverLocation.getPort() == vmToPortMap.get("vm0")) {
          numBucketsOnVm0++;
        } else if (serverLocation.getPort() == vmToPortMap.get("vm1")) {
          numBucketsOnVm1++;
        } else if (serverLocation.getPort() == vmToPortMap.get("vm2")) {
          numBucketsOnVm2++;
        } else {
          System.out.println("TEST ERROR");
        }
      }
    }

    System.out.println("NNNN TEST :: [#VM0 bkts = " + numBucketsOnVm0 + "] [#VM1 bkts = "
        + numBucketsOnVm1 + "]" + "[#VM2 bkts = " + numBucketsOnVm2 + "]");
    if (redundantCopies == 0) {
      try {
        validateBucketScanWithServerDown2(scanner);
      } catch (MScanSplitUnavailableException ex) {
        // This is excepted exception
        caughtException = true;
      }
      assertTrue(caughtException);
    } else {
      int count = validateBucketScanWithServerDown2(scanner);
      assertEquals(expectedTotalRows, count);
    }

    deleteTable(tableName);
  }

  public void testInProgressScanWithServerDown(boolean batchMode, String tableName) {
    // create table from client-1
    int totalSplits = 1;
    // createTableFromClient(this.client1);
    createSingleBucketTableFromClient(this.client1, 1, 0, tableName);
    /* put from client 2 */
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    MTable table = cache.getTable(tableName);
    assertNotNull(table);
    int expectedTotalRows = 500000;
    doPuts2(expectedTotalRows, tableName);

    Scan scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    Scanner scanner = table.getScanner(scan);
    int totalRecordsScaned = validateScan(scanner);
    assertEquals(expectedTotalRows, totalRecordsScaned);
    System.out.println("TEST  :: number of rows Scanned: " + totalRecordsScaned);

    Map<Integer, ServerLocation> primaryBucketMap = new HashMap<>(113);
    MTableUtils.getLocationMap(table, null, primaryBucketMap, null, AmpoolOpType.SCAN_OP);
    String locationString = primaryBucketMap.toString();
    System.out.println("TEST primaryBucketMap = " + locationString);
    String requiredString =
        locationString.substring(locationString.indexOf(":") + 1, locationString.indexOf("}"));

    // Iterate over vmToPortMap and find the targeted server
    Map<Integer, String> portToVMMap = new HashMap<>();
    for (Map.Entry<String, Integer> nodeEntry : vmToPortMap.entrySet()) {
      portToVMMap.put(nodeEntry.getValue(), nodeEntry.getKey());
    }
    String node = portToVMMap.get(Integer.valueOf(requiredString));
    System.out.println("TEST Targetted node to down = " + node);

    scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    scanner = table.getScanner(scan);
    boolean caughtException = false;
    try {
      validateBucketScanWithServerDown(scanner, expectedTotalRows, node);
    } catch (MScanSplitUnavailableException ex) {
      // This is excepted exception
      caughtException = true;
    }
    assertTrue(caughtException);
    deleteTable(tableName);
  }

  private void validateBucketScanWithServerDown(Scanner scanner, long expectedTotalRows,
      String node) {
    int count = 0;
    Row result = null;
    do {
      result = scanner.next();
      if (result != null) {
        int columnIndex = 0;
        List<Cell> row = result.getCells();
        for (int i = 0; i < row.size() - 1; i++) {
          assertEquals(new ByteArrayKey(row.get(i).getColumnName()),
              new ByteArrayKey(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex)));
          assertEquals(new ByteArrayKey((byte[]) row.get(i).getColumnValue()),
              new ByteArrayKey(Bytes.toBytes(VALUE_PREFIX + columnIndex)));
          columnIndex++;
        }
        count++;
      }
      if (count == expectedTotalRows / 2) {
        // server down
        System.out.println("TEST Scan --> progress, shuting node = " + node);
        stopServer(node);
      }
    } while (result != null);
    scanner.close();
  }

  private int validateBucketScanWithServerDown2(Scanner scanner) {
    int count = 0;
    Row result = null;

    // Simulate stale metadata - down a specific server (vm0) so all buckets holding by this server
    // will be offline.
    stopServerOn(this.vm0);

    // Try to scan after primary bucket server is down.
    do {
      result = scanner.next();
      if (result != null) {
        int columnIndex = 0;
        List<Cell> row = result.getCells();
        for (int i = 0; i < row.size() - 1; i++) {
          assertEquals(new ByteArrayKey(row.get(i).getColumnName()),
              new ByteArrayKey(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex)));
          assertEquals(new ByteArrayKey((byte[]) row.get(i).getColumnValue()),
              new ByteArrayKey(Bytes.toBytes(VALUE_PREFIX + columnIndex)));
          columnIndex++;
        }
        count++;
        // System.out.println("NNNNN Iteration count = " + count);
      }
    } while (result != null);
    return count;
  }

  private void stopServer(String node) {
    switch (node) {
      case "vm0":
        stopServerOn(this.vm0);
        break;
      case "vm1":
        stopServerOn(this.vm1);
        break;
      case "vm2":
        stopServerOn(this.vm2);
        break;
      default:
        throw new IllegalArgumentException("Unexpected Invalid node found while server down!");
    }
  }

  public void testScanServerDownWithRedundancy(boolean batchMode) {
    // create table from client-1
    String tableName = getTestMethodName();
    createTableClient(1, tableName);
    /* put from client 2 */
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    MTable table = cache.getTable(tableName);
    assertNotNull(table);
    doPuts(tableName);

    int expectedTotalRows = NUM_OF_ROWS_PER_BUCKET * NUM_BUCKETS;

    Scan scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    Scanner scanner = table.getScanner(scan);
    int count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);
    System.out.println(
        "MTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: number of rows (1): "
            + count);

    scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    scanner = table.getScanner(scan);
    System.out.println("MTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: "
        + "Stopping server on vm0");

    stopServerOn(this.vm0);

    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);
    System.out.println(
        "MTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: number of rows (2): "
            + count);
  }

  class TruncateTable implements Runnable {

    Table m_table;
    Admin m_admin;

    TruncateTable(Table table, Admin admin) {
      this.m_table = table;
      this.m_admin = admin;
    }

    @Override
    public void run() {

      try {
        System.out.println("TruncateTable.run table name = " + m_table.getName());
        if (m_table instanceof MTable) {
          this.m_admin.truncateMTable(m_table.getName());
        } else {
          this.m_admin.truncateFTable(m_table.getName(), null);
        }
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
    }
  }

  public void testTruncateTableServerDownWithRedundancy(boolean batchMode)
      throws InterruptedException {
    // create table from client-1
    String tableName = getTestMethodName();
    createTableClient(3, tableName);
    /* put from client 2 */
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    MTable table = cache.getMTable(tableName);
    assertNotNull(table);

    doPuts(100, tableName);

    Exception exception = null;

    stopServerOn(this.vm0);

    startServerOn(this.vm0, DUnitLauncher.getLocatorString());

    try {
      cache.getAdmin().truncateMTable(table.getName());
    } catch (Exception e) {
      exception = e;
    }

    stopServerOn(this.vm0);

    startServerOn(this.vm0, DUnitLauncher.getLocatorString());

    try {
      cache.getAdmin().truncateMTable(table.getName());
    } catch (Exception e) {
      exception = e;
    }

    stopServerOn(this.vm1);

    startServerOn(this.vm1, DUnitLauncher.getLocatorString());

    try {
      cache.getAdmin().truncateMTable(table.getName());
    } catch (Exception e) {
      exception = e;
    }

    stopServerOn(this.vm2);

    startServerOn(this.vm2, DUnitLauncher.getLocatorString());

    try {
      cache.getAdmin().truncateMTable(table.getName());
    } catch (Exception e) {
      exception = e;
    }

    // after changes in GEN-1988, now when data is not found then no row is returned
    // against earlier it was returning empty row.
    // assertNotNull(exception); // This means that we have got the exception during truncateTable
    // with
    // server restart.
    // because secondaries for some buckets would not have been fully
    // initialized and we are trying to delete those keys

    // assertTrue(exception instanceof TruncateTableException);
    // assertTrue(exception.getMessage().contains(RowKeyDoesNotExistException.MSG));

    exception = null;

    doPuts(100, tableName);

    stopServerOn(this.vm0);

    startServerOn(this.vm0, DUnitLauncher.getLocatorString());

    Thread.sleep(60000);

    try {
      cache.getAdmin().truncateMTable(table.getName());
    } catch (Exception e) {
      exception = e;
    }

    stopServerOn(this.vm0);

    startServerOn(this.vm0, DUnitLauncher.getLocatorString());

    Thread.sleep(60000);

    try {
      cache.getAdmin().truncateMTable(table.getName());
    } catch (Exception e) {
      exception = e;
    }

    stopServerOn(this.vm1);

    startServerOn(this.vm1, DUnitLauncher.getLocatorString());

    Thread.sleep(60000);

    try {
      cache.getAdmin().truncateMTable(table.getName());
    } catch (Exception e) {
      exception = e;
    }

    stopServerOn(this.vm2);

    startServerOn(this.vm2, DUnitLauncher.getLocatorString());

    Thread.sleep(60000);

    try {
      cache.getAdmin().truncateMTable(table.getName());
    } catch (Exception e) {
      exception = e;
    }

    assertNull(exception);
  }


  public void testTruncateTablesWithRedundancy(boolean batchMode) {
    // create table from client-1
    String tableName = getTestMethodName();
    createTableClient(3, tableName);
    /* put from client 2 */
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    MTable table = cache.getMTable(tableName);
    assertNotNull(table);

    FTable fTable = cache.getFTable(FTABLE_NAME_PREFIX + tableName);
    assertNotNull(fTable);

    int noOfRows = 100;
    int expected = noOfRows * NUM_BUCKETS;
    doPuts(noOfRows, tableName);
    doAppend(expected, tableName);

    assertEquals(expected, table.getKeyCount());

    Scan scan = new Scan();
    // mtable scan
    // Scanner scanner = table.getScanner(scan);
    // Iterator<Row> iterator = scanner.iterator();
    // int count = 0;
    // while (iterator.hasNext()) {
    // Row next = iterator.next();
    // System.out
    // .println("Key: " + TypeHelper.deepToString(next.getRowId()) + " TS:
    // ["+next.getRowTimeStamp()+"]");
    // count++;
    // }
    // assertEquals(expected,count);
    if (batchMode) {
      scan.enableBatchMode();
    }
    Scanner scanner = fTable.getScanner(scan);
    int totalRecordsScaned = validateScan(scanner);
    assertEquals(expected, totalRecordsScaned);

    TruncateTable truncateTable = new TruncateTable(table, cache.getAdmin());
    Thread t1 = new Thread(truncateTable);

    TruncateTable truncateTable2 = new TruncateTable(fTable, cache.getAdmin());
    Thread t2 = new Thread(truncateTable2);

    t1.start();
    t2.start();

    try {
      t1.join();

      t2.join();

    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    assertEquals(0, table.getKeyCount());

    scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    scanner = fTable.getScanner(scan);
    totalRecordsScaned = validateScan(scanner);
    assertEquals(0, totalRecordsScaned);

  }

  private void movedRandomBuckets(MTable table, VM source, VM destination, String tableName) {
    String[] bucketsToMov = new String[5];
    /* Get some random bucket Ids from source to make Move to another node */
    String[] bucketMovedKeys = (String[]) source.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        PartitionedRegion pr =
            (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(tableName);
        Set<BucketRegion> allLocalBucketRegions =
            pr.getDataStore().getAllLocalPrimaryBucketRegions();

        int index = 0;
        for (BucketRegion BR : allLocalBucketRegions) {
          if (index >= 5) {
            break;
          }
          if (pr.getDataStore().isManagingBucket(BR.getId())) {
            bucketsToMov[index++] = MTableUtils.getTableToBucketIdKey(tableName, BR.getId());
          }
        }
        return bucketsToMov;
      }
    });

    /*
     * Make sure that none of the bucket id is null If that is the case then we need to change test
     * logic of having 5 bucket movement.
     */
    Map<String, ServerLocation> oldSlOfBucketsTobeMoved = new HashMap<>();
    for (String bucketId : bucketMovedKeys) {
      assertNotNull(bucketId);
      /* store the old bucket id and its server location */
      // oldSlOfBucketsTobeMoved.put(bucketId,(ServerLocation) metaRegion.get(bucketId));
    }

    /* Perform Bucket Move from soure */

    final InternalDistributedMember sourceMemberId =
        (InternalDistributedMember) source.invoke(new SerializableCallable() {
          public Object call() throws Exception {
            return InternalDistributedSystem.getAnyInstance().getDistributedMember();
          }
        });

    System.out
        .println("MTableScanServerFailureDUnitTest.movedRandomBuckets # 227 :: Buckets to move: "
            + oldSlOfBucketsTobeMoved);

    /* Move buckets from source to destination */
    destination.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        PartitionedRegion pr =
            (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(tableName);
        for (int i = 0; i < bucketsToMov.length; i++) {
          String[] keys = bucketMovedKeys[i].split(":");
          System.out.println("MTableLocationInfoDUnitTest.call:: Moving Bucket:" + keys[1]);
          assertTrue(pr.getDataStore().moveBucket(Integer.valueOf(keys[1]), sourceMemberId, true));
        }
        return null;
      }
    });

    // now query the meta region for the new bucket-ids.
    for (int i = 0; i < table.getTableDescriptor().getTotalNumOfSplits(); i++) {
      // assertNotNull(metaRegion.get(MTableUtils.getTableToBucketIdKey(TABLE_NAME, i)));
    }
    // search the old bucketId-ServerLocation in new
    // They should not match, since the bucket is moved
    // and our logic to update should have executed.

    verifyBucketActuallyMoved(oldSlOfBucketsTobeMoved, tableName);

    /* Verify the actual value is changed or not */
    oldSlOfBucketsTobeMoved.forEach((KEY, SL) -> {
      ServerLocation oldSL = SL;
      // ServerLocation newSL = (ServerLocation) metaRegion.get(KEY);
      System.out.println("Old server location - " + oldSL);
      // System.out.println("New server location - " + newSL);
      // assertFalse(oldSL.equals(newSL));

    });
  }
}
