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

package io.ampool.monarch.table.facttable.dunit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import io.ampool.internal.AmpoolOpType;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MScanSplitUnavailableException;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(FTableTest.class)
public class FTableScanServerFailureDUnitTest extends MTableDUnitHelper {
  public FTableScanServerFailureDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();
  Host host = Host.getHost(0);
  private VM vm0 = host.getVM(0);
  private VM vm1 = host.getVM(1);
  private VM vm2 = host.getVM(2);
  private VM client1 = host.getVM(3);

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "FTableScanServerFailureDUnitTest";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS_PER_BUCKET = 10;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected final int LATEST_TIMESTAMP = 300;
  protected final int MAX_VERSIONS = 5;
  protected final int TABLE_MAX_VERSIONS = 7;
  protected final int NUM_BUCKETS = 57;
  private static final Map<String, Integer> vmToPortMap = new HashMap<>();
  private static final Map<Integer, byte[]> bucketIdToOneRowKeyMap = new HashMap<>();
  private static float EVICT_HEAP_PCT = 50.9f;

  public static float getEVICT_HEAP_PCT() {
    return EVICT_HEAP_PCT;
  }

  public void setEVICT_HEAP_PCT(float EVICT_HEAP_PCT) {
    this.EVICT_HEAP_PCT = EVICT_HEAP_PCT;
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
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

  private void createTableClient(String tName) {
    createTableClient(tName, 0);
  }

  private void createTableClient(String tName, int numRedundantCopies) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      if (colmnIndex == 0) {
        tableDescriptor =
            tableDescriptor.addColumn(COLUMN_NAME_PREFIX + colmnIndex, BasicTypes.INT);
      } else {
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
    }
    tableDescriptor.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    tableDescriptor.setTotalNumOfSplits(NUM_BUCKETS);
    tableDescriptor.setRedundantCopies(numRedundantCopies);
    Admin admin = clientCache.getAdmin();
    FTable table = admin.createFTable(tName, tableDescriptor);
    assertEquals(table.getName(), tName);
    assertNotNull(table);
  }

  private void createTableFromClient(VM vm, String tName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTableClient(tName);
        return null;
      }
    });
  }

  private void doPuts(String tName, int number) {
    FTable table = MClientCacheFactory.getAnyInstance().getFTable(tName);
    Map<Integer, Pair<byte[], byte[]>> splitsMap = MTableUtils.getUniformKeySpaceSplit(NUM_BUCKETS);

    Set<ByteArrayKey> inputStartKeys = new HashSet<>();
    Set<ByteArrayKey> inputEndKeys = new HashSet<>();

    for (int bucketId = 0; bucketId < NUM_BUCKETS; bucketId++) {
      Pair<byte[], byte[]> pair = splitsMap.get(bucketId);
      List<byte[]> keysInRange = getKeysInRange(pair.getFirst(), pair.getSecond(), number);
      bucketIdToOneRowKeyMap.put(bucketId, keysInRange.get(bucketId % number));
      for (int j = 0; j < keysInRange.size(); j++) {
        Record record = new Record();
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          if (columnIndex == 0) {
            record.add(COLUMN_NAME_PREFIX + columnIndex, bucketId);
          } else {
            record.add(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                Bytes.toBytes(VALUE_PREFIX + columnIndex));
          }
        }
        table.append(record);
      }
    }
    Assert.assertTrue(bucketIdToOneRowKeyMap.size() == NUM_BUCKETS);
  }

  private void doSingleBucketPuts(String tName, long totalRecords) {

    FTable table = MClientCacheFactory.getAnyInstance().getFTable(tName);
    int counter = 0;
    for (int rowIndex = 0; rowIndex < totalRecords; rowIndex++) {
      Record record = new Record();
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        if (colIndex == 0) {
          record.add(COLUMN_NAME_PREFIX + colIndex, counter++);
        } else {
          record.add(Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex),
              Bytes.toBytes(VALUE_PREFIX + colIndex));
        }
      }
      table.append(record);
    }
    System.out.println("TEST doSingleBucketPuts done successfully!");
  }

  private void verifyBucketActuallyMoved(
      final Map<String, ServerLocation> oldServerLocationOfMovedBucket, String tName) {
    this.vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        PartitionedRegion pr = (PartitionedRegion) cache.getRegion(tName);
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
  public void testBuckedMovedAndVerifyValueType() {
    String tName = TABLE_NAME + getTestMethodName();
    // create table from client-1
    createTableFromClient(this.client1, tName);
    /* put from client 2 */
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    FTable table = cache.getFTable(tName);
    assertNotNull(table);

    doPuts(tName, NUM_OF_ROWS_PER_BUCKET);

    int count = 0;
    int expectedTotalRows = NUM_OF_ROWS_PER_BUCKET * NUM_BUCKETS;
    Scanner scanner = table.getScanner(new Scan());
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);

    vm0.invoke(() -> {
      return checkValueType(tName);
    });

    vm1.invoke(() -> {
      return checkValueType(tName);
    });

    vm2.invoke(() -> {
      return checkValueType(tName);
    });

    // Testing with scan in streming mode
    Scan scan = new Scan();
    scan.setScanTimeout(150);
    scanner = table.getScanner(scan);
    movedRandomBuckets(table, this.vm0, this.vm1, tName);
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);

    vm0.invoke(() -> {
      return checkValueType(tName);
    });

    vm1.invoke(() -> {
      return checkValueType(tName);
    });

    vm2.invoke(() -> {
      return checkValueType(tName);
    });

    // // Testing with scan in batch mode
    scan = new Scan();
    scan.setScanTimeout(150);
    // scan.enableBatchMode();
    scanner = table.getScanner(scan);
    movedRandomBuckets(table, this.vm1, this.vm0, tName);
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);

    vm0.invoke(() -> {
      return checkValueType(tName);
    });

    vm1.invoke(() -> {
      return checkValueType(tName);
    });

    vm2.invoke(() -> {
      return checkValueType(tName);
    });

    cache.getAdmin().deleteFTable(tName);
  }

  @Test
  public void testBuckedMoved() {
    String tName = TABLE_NAME + "_testBuckedMoved";
    // create table from client-1
    createTableFromClient(this.client1, tName);
    /* put from client 2 */
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    FTable table = cache.getFTable(tName);
    assertNotNull(table);

    doPuts(tName, NUM_OF_ROWS_PER_BUCKET);

    int count = 0;
    int expectedTotalRows = NUM_OF_ROWS_PER_BUCKET * NUM_BUCKETS;
    Scanner scanner = table.getScanner(new Scan());
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);

    // Testing with scan in streming mode
    Scan scan = new Scan();
    scan.setScanTimeout(150);
    scanner = table.getScanner(scan);
    movedRandomBuckets(table, this.vm0, this.vm1, tName);
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);

    // Testing with scan in batch mode
    scan = new Scan();
    scan.setScanTimeout(150);
    // scan.enableBatchMode();
    scanner = table.getScanner(scan);
    movedRandomBuckets(table, this.vm1, this.vm0, tName);
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);

    cache.getAdmin().deleteFTable(tName);
  }

  private Boolean checkValueType(String tableName) {
    MCache anyInstance = MCacheFactory.getAnyInstance();
    Region<Object, Object> region = anyInstance.getRegion(tableName);

    Collection<Object> values = region.values();
    PartitionedRegion tableRegionPR = (PartitionedRegion) region;
    Set<Integer> allLocalBucketIds = tableRegionPR.getDataStore().getAllLocalBucketIds();
    allLocalBucketIds.forEach(bucketid -> {
      BucketRegion bucket = tableRegionPR.getDataStore().getLocalBucketById(bucketid);
      System.out.println("Operating for bucket " + bucketid + " isPrimary: "
          + bucket.getBucketAdvisor().isPrimary());
      SortedMap iMap =
          ((RowTupleConcurrentSkipListMap) bucket.getRegionMap().getInternalMap()).getInternalMap();
      Set<Map.Entry<IMKey, RegionEntry>> entries = iMap.entrySet();
      for (Map.Entry<IMKey, RegionEntry> entry : entries) {
        RegionEntry value1 = entry.getValue();
        Object value = value1._getValue();
        if (value != null) {
          if (value instanceof VMCachedDeserializable) {
            value = ((VMCachedDeserializable) value).getDeserializedForReading();
          }
          BlockValue blockValue = (BlockValue) value;
          System.out.println("FTableAppendDUnitTest.call :: " + "val class: " + value.getClass());
          assertEquals(BlockValue.class, blockValue.getClass());
        } else {
          System.out.println("FTableAppendDUnitTest.call :: " + "val class: NULL");
          fail("Value should not be null");
        }
      }
    });
    return true;
  }

  public void forceEvictionTask(String ftable) {
    /* Force eviction task */
    forceEvictiononServer(this.vm0, ftable);
    forceEvictiononServer(this.vm1, ftable);
    forceEvictiononServer(this.vm2, ftable);
  }

  /**
   * Raise a fake notification so that eviction gets started..
   */
  public static void raiseFakeNotification() {
    ((MonarchCacheImpl) MCacheFactory.getAnyInstance()).getHeapEvictor().testAbortAfterLoopCount =
        1;
    HeapMemoryMonitor.setTestDisableMemoryUpdates(true);
    System.setProperty("gemfire.memoryEventTolerance", "0");
    MCacheFactory.getAnyInstance().getResourceManager().setEvictionHeapPercentage(85);
    HeapMemoryMonitor hmm =
        ((MonarchCacheImpl) MCacheFactory.getAnyInstance()).getResourceManager().getHeapMonitor();
    hmm.setTestMaxMemoryBytes(100);
    hmm.updateStateAndSendEvent(90);
  }

  private static void forceEvictiononServer(final VM vm, final String regionName)
      throws RMIException {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        try {
          MCacheFactory.getAnyInstance().getResourceManager()
              .setEvictionHeapPercentage(getEVICT_HEAP_PCT());
          final PartitionedRegion pr =
              (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(regionName);
          assertNotNull(pr);
          raiseFakeNotification();
          /** wait for 60 seconds till all entries are evicted.. **/
          Awaitility.await().with().pollInterval(1, TimeUnit.SECONDS).atMost(60, TimeUnit.SECONDS)
              .until(() -> getTotalEntryCount(pr) == 0);
          System.out.println(
              "FTableScanServerFailureDUnitTest.run YYYYYYYYYYYYY" + getTotalEntryCount(pr));
          assertEquals("Expected no entries.", 0, getTotalEntryCount(pr));
        } finally {
          ((MonarchCacheImpl) MCacheFactory.getAnyInstance())
              .getHeapEvictor().testAbortAfterLoopCount = Integer.MAX_VALUE;
          HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
          System.clearProperty("gemfire.memoryEventTolerance");
        }
      }
    });
  }

  /**
   * Get the total count of entries in the region; aggregate entries across all buckets.
   *
   * @param pr the partitioned region
   * @return the total number of entries
   */
  private static int getTotalEntryCount(final PartitionedRegion pr) {
    return pr.getDataStore().getAllLocalBucketRegions().stream().mapToInt(BucketRegion::size).sum();
  }

  @Test
  public void testBuckedMovedWithOverflow() {
    String tName = TABLE_NAME + "_testBuckedMovedWithOverflow";
    // create table from client-1
    createTableFromClient(this.client1, tName);
    /* put from client 2 */
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    FTable table = cache.getFTable(tName);
    assertNotNull(table);
    doPuts(tName, NUM_OF_ROWS_PER_BUCKET);

    int count = 0;
    int expectedTotalRows = NUM_OF_ROWS_PER_BUCKET * NUM_BUCKETS;
    Scanner scanner = table.getScanner(new Scan());
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);

    // overflow
    forceEvictionTask(tName);

    // Testing with scan in streaming mode
    Scan scan = new Scan();
    scan.setScanTimeout(150);
    scanner = table.getScanner(scan);
    movedRandomBuckets(table, this.vm0, this.vm1, tName);
    // scanner = table.getScanner(scan);
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);

    // Testing with scan in batch mode
    scan = new Scan();
    scan.setScanTimeout(150);
    // scan.enableBatchMode();
    scanner = table.getScanner(scan);
    movedRandomBuckets(table, this.vm1, this.vm0, tName);
    // scanner = table.getScanner(scan);
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);
    cache.getAdmin().deleteFTable(tName);
  }

  private int validateScan(Scanner scanner) {
    int count = 0;
    Row result = null;
    do {
      result = scanner.next();
      if (result != null) {
        int columnIndex = 0;
        for (Cell cell : result.getCells()) {
          if (columnIndex >= NUM_OF_COLUMNS) {
            break;
          }
          assertEquals(Bytes.toString(cell.getColumnName()), COLUMN_NAME_PREFIX + columnIndex);
          if (columnIndex == 0) {
            int columnValue = (int) cell.getColumnValue();
            // System.out.println("FTableScanServerFailureDUnitTest.validateScan " + columnValue);
            // assertEquals(columnValue, columnIndex);
          } else {
            assertEquals(VALUE_PREFIX + columnIndex,
                Bytes.toString((byte[]) cell.getColumnValue()));
          }
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
    testScanServerDownWORedundancy(false, "_testScanServerDownWORedundancy", false);
  }

  @Test
  public void testScanServerDownWORedundancyWithOverFlow() {
    testScanServerDownWORedundancy(false, "_testScanServerDownWORedundancyWithOverFlow", true);
  }

  public void testScanServerDownWORedundancy(boolean batchMode, String tableName,
      boolean overFlow) {
    // create table from client-1
    String tName = TABLE_NAME + tableName;
    createTableFromClient(this.client1, tName);
    /* put from client 2 */
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    FTable table = cache.getFTable(tName);
    assertNotNull(table);
    doPuts(tName, NUM_OF_ROWS_PER_BUCKET);

    int expectedTotalRows = NUM_OF_ROWS_PER_BUCKET * NUM_BUCKETS;

    Scan scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    Scanner scanner = table.getScanner(scan);
    int count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);
    System.out.println(
        "FTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: number of rows (1): "
            + count);


    // Finding number of buckets hosted by vm0
    HashSet<Integer> bucketIds = new HashSet<>();
    for (int bucketId = 0; bucketId < NUM_BUCKETS; bucketId++) {
      bucketIds.add(bucketId);
    }

    Map<Integer, Set<ServerLocation>> bucketToServerMap =
        MTableUtils.getBucketToServerMap(tName, bucketIds, AmpoolOpType.SCAN_OP);
    int numBucketsOnVm0 = 0;
    for (Map.Entry<Integer, Set<ServerLocation>> locationEntry : bucketToServerMap.entrySet()) {
      for (ServerLocation serverLocation : locationEntry.getValue()) {
        if (serverLocation.getPort() == vmToPortMap.get("vm0")) {
          numBucketsOnVm0++;
        }
      }
    }
    System.out.println(
        "FTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: number of buckets on VM0 = "
            + numBucketsOnVm0);
    expectedTotalRows = expectedTotalRows - numBucketsOnVm0 * NUM_OF_ROWS_PER_BUCKET;
    scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    scanner = table.getScanner(scan);
    System.out.println("FTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: "
        + "Stopping server on vm0");

    stopServerOn(this.vm0);
    if (overFlow) {
      forceEvictiononServer(this.vm1, tName);
      forceEvictiononServer(this.vm2, tName);
    }
    boolean caughtException = false;

    try {
      validateScan(scanner);
    } catch (Throwable ex) {
      // This is excepted exception
      if (ex.getCause() instanceof ServerConnectivityException
          || ex.getCause() instanceof EOFException
          || ex instanceof MScanSplitUnavailableException) {
        caughtException = true;
      } else {
        throw ex;
      }
    }
    assertTrue(caughtException);

    // Scan after vm0 is down
    scanner = table.getScanner(new Scan());
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);
    System.out.println(
        "FTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: number of rows (2): "
            + count);

    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    cache.getAdmin().deleteFTable(tName);
  }

  @Test
  public void testScanServerDownWithRedundancy() throws InterruptedException {
    testScanServerDownWithRedundancy(false, "_testScanServerDownWithRedundancy", false);
  }

  @Test
  public void testScanWithRedundancy() throws InterruptedException {
    testScanWithRedundancy(false, "_testScanWithRedundancy", true);
  }

  @Test
  public void testScanServerDownWithRedundancyWithOverflow() throws InterruptedException {
    testScanServerDownWithRedundancy(false, "_testScanServerDownWithRedundancyWithOverflow", true);
  }

  @Test
  public void testScanServerDownWithRedundancyWithOverflow2() throws InterruptedException {
    testScanServerDownWithRedundancy(false, "_testScanServerDownWithRedundancyWithOverflow", true);
  }

  public void testScanServerDownWithRedundancy(boolean batchMode, String tableName,
      boolean overflow) throws InterruptedException {
    String tName = TABLE_NAME + tableName;
    // create table from client-1
    createTableClient(tName, 2);
    /* put from client 2 */
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    FTable table = cache.getFTable(tName);
    assertNotNull(table);
    doPuts(tName, NUM_OF_ROWS_PER_BUCKET);

    int expectedTotalRows = NUM_OF_ROWS_PER_BUCKET * NUM_BUCKETS;

    Scan scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    Scanner scanner = table.getScanner(scan);
    int count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);
    System.out.println(
        "FTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: number of rows (1): "
            + count);

    scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    scanner = table.getScanner(scan);
    System.out.println("FTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: "
        + "Stopping server on vm0");
    stopServerOn(this.vm0);
    if (overflow) {
      forceEvictiononServer(this.vm1, tName);
      forceEvictiononServer(this.vm2, tName);
    }
    count = validateScan(scanner);
    assertEquals(expectedTotalRows, count);
    System.out.println(
        "FTableScanServerFailureDUnitTest.testScanServerDownWORedundancy :: number of rows (2): "
            + count);

    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    Thread.sleep(10_000);
    cache.getAdmin().deleteFTable(tName);
  }

  public void testScanWithRedundancy(boolean batchMode, String tableName, boolean overflow)
      throws InterruptedException {
    String tName = TABLE_NAME + tableName;
    // create table from client-1
    createTableClient(tName, 2);
    /* put from client 2 */
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    FTable table = cache.getFTable(tName);
    assertNotNull(table);
    int rows = 885;
    doPuts(tName, rows);

    int expectedTotalRows = rows * NUM_BUCKETS;

    Scan scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    Scanner scanner = table.getScanner(scan);
    int count = validateScan(scanner);
    System.out.println(
        "FTableScanServerFailureDUnitTest.testScanWithRedundancy :: number of rows (1): " + count);
    assertEquals(expectedTotalRows, count);

    if (overflow) {
      forceEvictionTask(tName);
    }

    scan = new Scan();
    if (batchMode) {
      scan.enableBatchMode();
    }
    scanner = table.getScanner(scan);
    count = validateScan(scanner);
    System.out.println(
        "FTableScanServerFailureDUnitTest.testScanWithRedundancy :: number of rows (2): " + count);
    assertEquals(expectedTotalRows, count);

    cache.getAdmin().deleteFTable(tName);
  }

  private void movedRandomBuckets(FTable table, VM source, VM destination, String tName) {
    String[] bucketsToMov = new String[5];
    /* Get some random bucket Ids from source to make Move to another node */
    String[] bucketMovedKeys = (String[]) source.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(tName);
        Set<BucketRegion> allLocalBucketRegions =
            pr.getDataStore().getAllLocalPrimaryBucketRegions();
        int index = 0;
        for (BucketRegion BR : allLocalBucketRegions) {
          if (index >= 5) {
            break;
          }
          if (pr.getDataStore().isManagingBucket(BR.getId())) {
            System.out
                .println("FTableScanServerFailureDUnitTest.call isManagingBucket BR.getId() = "
                    + BR.getId() + " entry count = " + BR.entryCount());
            bucketsToMov[index++] = MTableUtils.getTableToBucketIdKey(tName, BR.getId());
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
        .println("FTableScanServerFailureDUnitTest.movedRandomBuckets # 227 :: Buckets to move: "
            + oldSlOfBucketsTobeMoved);

    /* Move buckets from source to destination */
    destination.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(tName);
        for (int i = 0; i < bucketsToMov.length; i++) {
          String[] keys = bucketMovedKeys[i].split(":");
          System.out.println("FTableLocationInfoDUnitTest.call:: Moving Bucket:" + keys[1]
              + " from sourceMemberId = " + sourceMemberId);
          assertTrue(pr.getDataStore().moveBucket(Integer.valueOf(keys[1]), sourceMemberId, true));
          /** wait for 20 seconds till for bucket move.. **/
          Thread.sleep(20_000);
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

    verifyBucketActuallyMoved(oldSlOfBucketsTobeMoved, tName);

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
