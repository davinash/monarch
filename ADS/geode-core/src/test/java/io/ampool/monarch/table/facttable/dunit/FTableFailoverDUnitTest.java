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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.jayway.awaitility.Awaitility;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.facttable.FTableDUnitHelper;
import io.ampool.monarch.table.facttable.FTableTestHelper;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.ftable.internal.FTableScanner;
import io.ampool.monarch.table.region.ScanContext;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.internal.cache.*;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.store.StoreHandler;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.MemoryEvent;
import org.apache.geode.internal.cache.control.ResourceListener;
import org.apache.geode.internal.cache.control.TestMemoryThresholdListener;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(FTableTest.class)
@RunWith(JUnitParamsRunner.class)
public class FTableFailoverDUnitTest extends MTableDUnitHelper {
  private static int DEFAULT_NUM_COLUMNS = 1;
  private static String DEFAULT_COL_PREFIX = "COL";
  private static String DEFAULT_COLVAL_PREFIX = "VAL";
  private final AtomicLong recordsCounter = new AtomicLong(0);
  private static float EVICT_HEAP_PCT = 50.9f;

  private static final int NUM_OF_COLUMNS = 10;
  private static final String COLUMN_NAME_PREFIX = "COL";

  private String tableName = null;

  public static float getEVICT_HEAP_PCT() {
    return EVICT_HEAP_PCT;
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    if (tableName != null) {
      deleteFTable(tableName);
      tableName = null;
    }
    closeMClientCache();
    stopServerOn(vm0);
    stopServerOn(vm1);
    super.tearDown2();
  }

  private FTable createFTable(final String ftableName, final int redundantCopies,
      final int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setRedundantCopies(redundantCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return MClientCacheFactory.getAnyInstance().getAdmin().createFTable(ftableName,
        tableDescriptor);
  }

  private void doAppend(final String fTableName, final long numRecords) {
    final FTable fTable = MClientCacheFactory.getAnyInstance().getFTable(fTableName);
    for (int i = 0; i < numRecords; i++) {
      Record record = new Record();
      for (int j = 0; j < DEFAULT_NUM_COLUMNS; j++) {
        record.add(DEFAULT_COL_PREFIX + j,
            Bytes.toBytes(DEFAULT_COLVAL_PREFIX + i + "_" + Thread.currentThread().getId()));
      }
      record.add(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, (long) i);
      fTable.append(record);
      recordsCounter.getAndIncrement();
    }
  }


  private void runScan(final FTable fTable, final long expectedRecords) {
    final Iterator<Row> iterator = fTable.getScanner(new Scan()).iterator();
    int actualRecods = 0;
    while (iterator.hasNext()) {
      final Row row = iterator.next();
      final Iterator<Cell> mCellIterator = row.getCells().iterator();
      final Cell cell = mCellIterator.next();
      // System.err.println("Column Name " + Bytes.toString(mCell.getColumnName()));
      // System.err.println("Column Value " + Bytes.toString((byte[]) mCell.getColumnValue()));
      actualRecods++;
    }
    assertEquals(expectedRecords, actualRecods);
  }

  private void deleteFTable(final String ftableName) {
    MClientCacheFactory.getAnyInstance().getAdmin().deleteFTable(ftableName);
  }

  protected int getCount(String tableName) {
    int entriesCount = 0;
    final Region<Object, Object> region = MCacheFactory.getAnyInstance().getRegion(tableName);
    assertNotNull(region);
    final Iterator<BucketRegion> allLocalPrimaryBucketRegions =
        ((PartitionedRegion) region).getDataStore().getAllLocalPrimaryBucketRegions().iterator();
    while (allLocalPrimaryBucketRegions.hasNext()) {
      final BucketRegion bucketRegion = allLocalPrimaryBucketRegions.next();
      final RowTupleConcurrentSkipListMap internalMap =
          (RowTupleConcurrentSkipListMap) bucketRegion.entries.getInternalMap();
      final Map concurrentSkipListMap = internalMap.getInternalMap();
      final Iterator<Entry> iterator = concurrentSkipListMap.entrySet().iterator();
      while (iterator.hasNext()) {
        final Entry entry = iterator.next();
        Object o = ((RegionEntry) entry.getValue())._getValue();

        if (o instanceof BlockValue) {
          final BlockValue blockValue = (BlockValue) o;
          final BlockKey blockKey = (BlockKey) ((RegionEntry) entry.getValue()).getKey();
          final Iterator objectIterator = blockValue.iterator();
          while (objectIterator.hasNext()) {
            objectIterator.next();
            entriesCount++;
          }
        } else {
          System.out.println(
              "FTableBatchAppendDUnitTest.verifyValues:: Entry value is not \"BlockValue\"");
        }
      }
      System.out.println(
          "Bucket Region Name : " + bucketRegion.getName() + "   Size: " + bucketRegion.size());
    }
    System.out.println("FTableAppendDUnitTest.verifyValues :: " + "ECount: " + entriesCount);
    return entriesCount;
  }

  private SerializableCallable resetFakeNotification = new SerializableCallable() {
    public Object call() throws Exception {
      InternalResourceManager irm =
          ((GemFireCacheImpl) MCacheFactory.getAnyInstance()).getResourceManager();
      // Reset CRITICAL_UP by informing all that heap usage is now 1 byte (0 would disable).
      irm.getHeapMonitor().updateStateAndSendEvent(50);
      Set<ResourceListener> listeners =
          irm.getResourceListeners(InternalResourceManager.ResourceType.HEAP_MEMORY);
      Iterator<ResourceListener> it = listeners.iterator();
      while (it.hasNext()) {
        ResourceListener<MemoryEvent> l = it.next();
        if (l instanceof TestMemoryThresholdListener) {
          ((TestMemoryThresholdListener) l).resetThresholdCalls();
        }
      }
      irm.setCriticalHeapPercentage(0f);
      irm.setEvictionHeapPercentage(0f);
      irm.getHeapMonitor().setTestMaxMemoryBytes(0);
      HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
      return null;
    }
  };


  /**
   * Get the total count of entries in the region; aggregate entries across all buckets.
   *
   * @param pr the partitioned region
   * @return the total number of entries
   */
  private static int getTotalEntryCount(final PartitionedRegion pr) {
    return pr.getDataStore().getAllLocalBucketRegions().stream().mapToInt(BucketRegion::size).sum();
  }


  public static void forceEvictiononServer(final VM vm, final String regionName)
      throws RMIException {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        try {
          final PartitionedRegion pr =
              (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(regionName);
          assertNotNull(pr);
          FTableTestHelper.raiseFakeNotification();
          /** wait for 60 seconds till all entries are evicted.. **/
          Awaitility.await().with().pollInterval(1, TimeUnit.SECONDS).atMost(60, TimeUnit.SECONDS)
              .until(() -> getTotalEntryCount(pr) == 0);
          System.out.println(
              "FTableScanServerFailureDUnitTest.run YYYYYYYYYYYYY" + getTotalEntryCount(pr));
          assertEquals("Expected no entries.", 0, getTotalEntryCount(pr));
        } finally {
          FTableTestHelper.revokeFakeNotification();
        }
      }
    });
  }



  private static void pauseWalMontoring(final VM vm, final String tableName) throws RMIException {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        StoreHandler.getInstance().pauseWALMonitoring(tableName, 0);
      }
    });
  }

  private static void resumeWalMontoring(final VM vm, final String tableName) throws RMIException {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        StoreHandler.getInstance().resumeWALMonitoring(tableName, 0);
      }
    });
  }

  private static void flushWAL(final VM vm, final String tableName) throws RMIException {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        StoreHandler.getInstance().flushWriteAheadLog(tableName, 0);
      }
    });
  }

  private void stopStartServers() {
    stopServerOn(vm1);
    stopServerOn(vm0);
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
  }

  public static int getInMemoryRecordsCount(final VM vm, final String tableName,
      final boolean onlyPrimary) {
    return (int) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return getInMemoryRecordsCount(tableName, onlyPrimary);
      }
    });
  }

  protected static int getInMemoryRecordsCount(final String tableName, final boolean onlyPrimary) {
    int entriesCount = 0;
    int recordsCount = 0;
    final Region<Object, Object> region = MCacheFactory.getAnyInstance().getRegion(tableName);
    assertNotNull(region);
    final Iterator<BucketRegion> bucketRegionIterator = onlyPrimary
        ? ((PartitionedRegion) region).getDataStore().getAllLocalPrimaryBucketRegions().iterator()
        : ((PartitionedRegion) region).getDataStore().getAllLocalBucketRegions().iterator();
    while (bucketRegionIterator.hasNext()) {
      final BucketRegion bucketRegion = bucketRegionIterator.next();
      final RowTupleConcurrentSkipListMap internalMap =
          (RowTupleConcurrentSkipListMap) bucketRegion.entries.getInternalMap();
      final Map concurrentSkipListMap = internalMap.getInternalMap();
      final Iterator<Entry> iterator = concurrentSkipListMap.entrySet().iterator();
      while (iterator.hasNext()) {
        final Entry entry = iterator.next();
        RegionEntry value1 = (RegionEntry) entry.getValue();
        Object value = value1._getValue();
        if (value instanceof Token) {
          continue;
        }
        entriesCount++;
        if (value instanceof VMCachedDeserializable) {
          value = ((VMCachedDeserializable) value).getDeserializedForReading();
        }
        final BlockValue blockValue = (BlockValue) value;
        final Iterator objectIterator = blockValue.iterator();
        while (objectIterator.hasNext()) {
          objectIterator.next();
          recordsCount++;
        }
      }
      System.out.println(
          "Bucket Region Name : " + bucketRegion.getName() + "   Size: " + bucketRegion.size());
    }
    System.out.println("FTableAppendDUnitTest.verifyValues :: " + "ECount: " + entriesCount);
    System.out.println("FTableAppendDUnitTest.verifyValues :: " + "RecordCount: " + recordsCount);

    return recordsCount;
  }

  public Object[] getNumRecords() {
    return new Object[][] {{"testRecovery1", 10}, {"testRecovery4", 100}, {"testRecovery7", 1000},
        {"testRecovery10", 10000}, {"testRecovery13", 100000},};
  }

  private void appendBatchesWithLargeRecords(final String ftablename, final int numRows,
      final int numBatches, String data) throws InterruptedException {

    final FTable table = MCacheFactory.getAnyInstance().getFTable(ftablename);
    for (int j = 1; j <= numBatches; j++) {
      Record[] records = new Record[numRows];
      for (int i = 0; i < numRows; i++) {
        records[i] = new Record();
        final Iterator<MColumnDescriptor> iterator =
            table.getTableDescriptor().getAllColumnDescriptors().iterator();
        while (iterator.hasNext()) {
          final MColumnDescriptor mColumnDescriptor = iterator.next();
          records[i].add(mColumnDescriptor.getColumnName(),
              Bytes.toBytes(mColumnDescriptor.getColumnNameAsString() + "_" + i + "_" + data));
        }
      }
      table.append(records);
      Thread.sleep(1000);
      System.out.println("Appended batch " + j);
    }
  }

  @Test
  @Parameters(method = "getNumRecords")
  public void testRecovery(final String fTableName, final int numRecords)
      throws InterruptedException {
    this.tableName = fTableName;
    final FTable fTable = createFTable(fTableName, 1, 1);
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords);

    VM primaryVm = null;
    VM secondaryVm = null;
    if (getInMemoryRecordsCount(vm0, fTableName, true) > 0) {
      primaryVm = vm0;
      secondaryVm = vm1;
    } else {
      primaryVm = vm1;
      secondaryVm = vm0;
    }
    System.out.println("Primary vm is " + primaryVm);
    assertEquals(numRecords, getInMemoryRecordsCount(primaryVm, fTableName, true));
    forceEvictiononServer(primaryVm, fTableName);
    Thread.sleep(15_000);
    flushWAL(primaryVm, fTableName);
    primaryVm.invoke(resetFakeNotification);
    assertEquals(0, getInMemoryRecordsCount(primaryVm, fTableName, true));
    doAppend(fTableName, numRecords);
    assertEquals(numRecords, getInMemoryRecordsCount(primaryVm, fTableName, true));
    runScan(fTable, numRecords * 2);
    assertEquals(numRecords * 2, getInMemoryRecordsCount(secondaryVm, fTableName, false));
    stopServerOn(primaryVm);
    runScan(fTable, numRecords * 2);
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords * 3);
    assertEquals(numRecords * 3, getInMemoryRecordsCount(secondaryVm, fTableName, true));
    startServerOn(primaryVm, DUnitLauncher.getLocatorString());
    Thread.sleep(15_000);
    assertEquals(numRecords * 3, getInMemoryRecordsCount(primaryVm, fTableName, false));
    runScan(fTable, numRecords * 3);
    stopServerOn(secondaryVm);
    Thread.sleep(15_000);
    assertEquals(numRecords * 3, getInMemoryRecordsCount(primaryVm, fTableName, true));
    runScan(fTable, numRecords * 3);
  }


  @Test
  public void testRecoveryWithSecondaryEviction() throws InterruptedException {
    String fTableName = "testRecoveryWithSecondaryEviction";
    final int numRecords = 200;
    this.tableName = fTableName;
    final FTable fTable = createFTable(fTableName, 1, 1);
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords);

    VM primaryVm = null;
    VM secondaryVm = null;
    if (getInMemoryRecordsCount(vm0, fTableName, true) > 0) {
      primaryVm = vm0;
      secondaryVm = vm1;
    } else {
      primaryVm = vm1;
      secondaryVm = vm0;
    }
    System.out.println("Primary vm is " + primaryVm);
    assertEquals(numRecords, getInMemoryRecordsCount(primaryVm, fTableName, true));
    assertEquals(numRecords, getInMemoryRecordsCount(secondaryVm, fTableName, false));
    Thread.sleep(15_000);
    forceEvictiononServer(secondaryVm, fTableName);
    Thread.sleep(15_000);
    flushWAL(primaryVm, fTableName);
    secondaryVm.invoke(resetFakeNotification);
    doAppend(fTableName, numRecords);
    Thread.sleep(15_000);
    stopServerOn(secondaryVm);
    Thread.sleep(15_000);
    startServerOn(secondaryVm, DUnitLauncher.getLocatorString());
    runScan(fTable, numRecords * 2);
  }

  private void asyncRestartVM(final VM vm) {
    try {
      asyncStartServerOn(vm, DUnitLauncher.getLocatorString()).join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testRecoverWithoutValues() throws InterruptedException {
    this.tableName = getTestMethodName();
    final int recordCount = 1000;
    final FTable fTable = createFTable(tableName, 2, 100);
    doAppend(tableName, recordCount);
    runScan(fTable, recordCount);

    stopServerOn(vm0);
    stopServerOn(vm1);

    vm0.invoke(() -> System.setProperty("gemfire.disk.recoverValues", "false"));
    vm1.invoke(() -> System.setProperty("gemfire.disk.recoverValues", "false"));

    System.out.println("### Restarting VMs: vm0 and vm1");
    Arrays.asList(vm0, vm1).parallelStream().forEach(this::asyncRestartVM);

    Thread.sleep(5_000);

    doAppend(tableName, recordCount);

    runScan(fTable, recordCount * 2);

    vm0.invoke(() -> System.clearProperty("gemfire.disk.recoverValues"));
    vm1.invoke(() -> System.clearProperty("gemfire.disk.recoverValues"));
  }

  @Test
  public void testPersistenceWithLargeRecords() throws InterruptedException {
    int numRows = 5;
    int numBatches = 10;
    int numSplits = 1;
    int redundancy = 0;
    stopServerOn(vm1);
    this.tableName = getTestMethodName();
    final FTable table = createFTable(tableName, redundancy, numSplits);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0);
    StringBuilder sb = new StringBuilder(2080);
    for (int i = 0; i < 2600; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(8));
    }
    String data = sb.toString();
    appendBatchesWithLargeRecords(tableName, numRows, numBatches, data);
    vm0.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        Iterator<BucketRegion> iterator =
            ((PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(tableName)).getDataStore()
                .getAllLocalPrimaryBucketRegions().iterator();
        while (iterator.hasNext()) {
          BucketRegion bucketRegion = iterator.next();
          Iterator<RegionEntry> regionEntryIterator =
              bucketRegion.entries.regionEntries().iterator();
          while (regionEntryIterator.hasNext()) {
            RegionEntry regionEntry = regionEntryIterator.next();
            BlockValue value = (BlockValue) ((VMCachedDeserializable) regionEntry._getValue())
                .getDeserializedForReading();
            System.out.println("FTableDeltaPersistenceDUnitTest.run 256 " + value);
            Iterator<Object> objectIterator = value.iterator();
            while (objectIterator.hasNext()) {
              System.out
                  .println("FTableDeltaPersistenceDUnitTest.run 261 " + objectIterator.next());
            }
          }
        }
      }
    });
    runScan(table, numRows * numBatches);

    stopServerOn(vm0);
    try {
      runScan(table, numRows * numBatches);
    } catch (ServerConnectivityException e) {
      //// expected this exception here.
    } catch (Exception e) {
      fail("Expected ServerConnectivityException.");
    }
    startServerOn(vm0, DUnitLauncher.getLocatorString());
    Thread.sleep(10_000);
    runScan(table, numRows * numBatches);
  }

  private void forceTombstomeExpiryFortest(VM[] vms) {
    for (VM vm : vms) {
      vm.invoke(new SerializableCallable<Object>() {
        @Override
        public Object call() throws Exception {
          ((GemFireCacheImpl) MCacheFactory.getAnyInstance()).getTombstoneService()
              .forceBatchExpirationForTests(1000);
          return null;
        }
      });
    }
  }

  private void verifyKeyExistOnVM(VM vm, String tableName, int expectedKeyCount) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        Set<BucketRegion> allBucketRegions =
            ((PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(tableName)).getDataStore()
                .getAllLocalBucketRegions();

        System.out.println("NNN allBucketRegions.size() = " + allBucketRegions.size());
        Iterator<BucketRegion> iterator = allBucketRegions.iterator();
        while (iterator.hasNext()) {
          BucketRegion bucketRegion = iterator.next();
          ConcurrentSkipListMap internalMap =
              (ConcurrentSkipListMap) ((RowTupleConcurrentSkipListMap) bucketRegion.getRegionMap()
                  .getInternalMap()).getInternalMap();
          System.out.println("NNN internalMap.size = " + internalMap.size());
          assertEquals(internalMap.entrySet().size(), expectedKeyCount);
          Iterator<Map.Entry> entryIterator = internalMap.entrySet().iterator();
          while (entryIterator.hasNext()) {
            Entry entry = entryIterator.next();
            System.out.println("NNN Key = " + entry.getKey());
            if (entry.getValue() instanceof VMCachedDeserializable) {
              Object deserializedForReading =
                  ((VMCachedDeserializable) entry.getValue()).getDeserializedForReading();
              System.out.println("NNN Value = " + deserializedForReading);
            } else {
              System.out.println("NNN Value = " + entry.getValue());
            }
          }
        }
      }
    });
  }

  private void verifyRowCountOnVM(VM vm, String tableName, int expectedCount) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        final MCache cache = MCacheFactory.getAnyInstance();
        final FTable table = cache.getFTable(tableName);
        Scan fScan = new Scan();
        fScan.setBucketId(0);
        final Scanner scanner = table.getScanner(fScan);
        int count = 0;
        Row res = scanner.next();
        System.out.println("FTableScanTests.runScanAtServer :: " + "scan result: " + res);
        while (res != null) {
          count++;
          res = scanner.next();
        }
        System.out.println("NNN FTable Server Scan runScanAtServer :: " + "count: " + count);
        assertEquals(expectedCount, count);
      }
    });
  }

  private int getTableRowCountUsingScan(String tableName) {

    final MClientCache cache = MClientCacheFactory.getAnyInstance();
    final FTable table = cache.getFTable(tableName);
    Scan fScan = new Scan();
    final Scanner scanner = table.getScanner(fScan);
    int count = 0;
    Row res = scanner.next();
    System.out.println("FTableScanTests.runScanAtServer :: " + "scan result: " + res);
    while (res != null) {
      count++;
      res = scanner.next();
    }
    System.out.println("NNN FTable client scan :: " + "count: " + count);
    // assertEquals(expectedCount, count);
    return count;
  }

  /**
   * Restart the servers on the specified VMs. It stops all VMs and then restarts the server on
   * respective VMs asynchronously.
   *
   * @param vms server VMs to be restarted
   */
  public void restartServers(final List<VM> vms, final String tableName, final int bucketCount) {
    for (VM vm : vms) {
      // Add a comment to this line
      stopServerOn(vm);
    }
    vms.parallelStream().forEach(vm -> {
      try {
        asyncStartServerOn(vm, DUnitLauncher.getLocatorString()).join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });

    /* give some time to sync the data (secondary) across all buckets/tiers */
    if (tableName != null) {
      try {
        Awaitility.await().with().pollInterval(5, TimeUnit.SECONDS).atMost(60, TimeUnit.SECONDS)
            .until(() -> getBucketCount(tableName, new VM[] {vm0, vm1}) == bucketCount);
      } catch (Exception e) {
        /////
        System.out.println("### Time-out (60s) occurred during wait..");
      }
      System.out.println("### BucketCount= " + getBucketCount(tableName, new VM[] {vm0, vm1}));
    }
  }


  private void waitForRecovery(final String tableName, final VM vm, boolean isPrimary) {
    if (tableName != null) {
      try {
        Awaitility.await().with().pollInterval(5, TimeUnit.SECONDS).atMost(60, TimeUnit.SECONDS)
            .until(() -> getInMemoryRecordsCount(vm, tableName, isPrimary) > 0);
      } catch (Exception e) {
        /////
        System.out.println("### Time-out (60s) occurred during wait..");
      }
      System.out.println("### Records Count= " + getInMemoryRecordsCount(vm, tableName, isPrimary));
    }
  }

  private long getBucketCount(final String name, final VM[] vms) {
    long totalCount = 0;
    for (final VM vm : vms) {
      totalCount += vm.invoke(() -> {
        long count = 0;
        final MCache cache = MCacheFactory.getAnyInstance();
        FTablePartitionedRegion region = (FTablePartitionedRegion) cache.getRegion(name);
        if (region == null) {
          return count;
        }
        for (final BucketRegion br : region.getDataStore().getAllLocalBucketRegions()) {
          if (!br.getBucketAdvisor().isHosting() && !br.getBucketAdvisor().isInitialized()) {
            continue;
          }
          count++;
        }
        return count;
      });
    }
    System.out.println("### getBucketCount.totalCount= " + totalCount);
    return totalCount;
  }

  long getTotalCount(final String name, final VM[] vms, final boolean isPrimary) {
    long totalCount = 0;
    for (final VM vm : vms) {
      totalCount += vm.invoke(() -> {
        long count = 0;
        final MCache cache = MCacheFactory.getAnyInstance();
        FTablePartitionedRegion region = (FTablePartitionedRegion) cache.getRegion(name);
        Scan scan = new Scan();
        scan.setMessageChunkSize(100);
        for (final BucketRegion br : region.getDataStore().getAllLocalBucketRegions()) {
          if (isPrimary != br.getBucketAdvisor().isPrimary()) {
            continue;
          }
          scan.setBucketId(br.getId());
          final ScanContext sc =
              new ScanContext(null, region, scan, region.getDescriptor(), null, null);
          final Iterator itr = new FTableScanner(sc,
              (RowTupleConcurrentSkipListMap) br.getRegionMap().getInternalMap());
          while (itr.hasNext()) {
            final Object next = itr.next();
            count++;
          }
        }
        return count;
      });
    }
    return totalCount;
  }

  public static void revokeEvictiononServer(final VM vm) throws RMIException {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        FTableTestHelper.revokeFakeNotification();
      }
    });
  }

  /**
   * Test-case: 1. Appned partial records (500 records) in a block. 2. delete the key on secondary
   * (Trigger eviction , tombstone expiry on secondary) 3. verify key exist on secondary or not..?
   * expectation is key should not exist in secondary. 4. Ingest 500 more records. 5. restart the
   * secondary and verify data 6. restart both servers and verify count on both primary and
   * secondary.
   *
   * @throws InterruptedException
   */
  @Test
  public void _testKeyExistOnSecondaryAfterEvictionAndRestart(final String tableName,
      final int intialRecords, final int remainingReconrds, boolean testLargeRecords)
      throws InterruptedException {


    // final int intialRecords = 500;
    if (testLargeRecords) {
      StringBuilder sb = new StringBuilder(2080);
      for (int i = 0; i < 2600; i++) {
        sb.append(RandomStringUtils.randomAlphabetic(8));
      }
      String data = sb.toString();
      // System.err.println("NNN data = " + data);
      appendBatchesWithLargeRecords(tableName, intialRecords / 10, 10, data);
    } else {
      doAppend(tableName, intialRecords);
    }

    System.out.println("NNN waiting - 1");
    Thread.sleep(5000);
    // find the primary and secondary vm for the bucket.
    VM primaryVM = null;
    VM secondaryVM = null;
    if (getInMemoryRecordsCount(vm0, tableName, true) > 0) {
      primaryVM = vm0;
      secondaryVM = vm1;
    } else {
      primaryVM = vm1;
      secondaryVM = vm0;
    }

    // verify the records in primary and secondary bucket.
    int countOnPrimary = getInMemoryRecordsCount(primaryVM, tableName, true);
    int countOnSecondary = getInMemoryRecordsCount(secondaryVM, tableName, false);
    assertEquals(countOnPrimary, countOnSecondary);

    // trigger eviction on secondary
    forceEvictiononServer(secondaryVM, tableName);
    System.out.println("NNN waiting - 2 triggered forced eviction on secondary");
    Thread.sleep(5000);


    // trigger tombstone expirary on secondary.
    forceTombstomeExpiryFortest(new VM[] {secondaryVM});
    System.out.println("NNN waiting - 3 tombstone expirary on secondary");
    Thread.sleep(5000);

    // verify whether key exist on secondary or not in memory..?
    verifyKeyExistOnVM(secondaryVM, tableName, 0);
    int inMemoryCountSecondary = getInMemoryRecordsCount(secondaryVM, tableName, false);
    long totalCountSecondary = getTotalCount(tableName, new VM[] {secondaryVM}, false);

    assertEquals(0, inMemoryCountSecondary);
    assertEquals(intialRecords, totalCountSecondary);

    revokeEvictiononServer(secondaryVM);
    Thread.sleep(5000);

    // ingest 500 more records.
    // final int remainingReconrds = 500;
    if (testLargeRecords) {
      StringBuilder sb = new StringBuilder(2080);
      for (int i = 0; i < 2600; i++) {
        sb.append(RandomStringUtils.randomAlphabetic(8));
      }
      String data = sb.toString();
      appendBatchesWithLargeRecords(tableName, remainingReconrds / 10, 10, data);
    } else {
      doAppend(tableName, remainingReconrds);
    }

    int inMemoryCountPrimary = getInMemoryRecordsCount(primaryVM, tableName, true);
    inMemoryCountSecondary = getInMemoryRecordsCount(secondaryVM, tableName, false);
    totalCountSecondary = getTotalCount(tableName, new VM[] {secondaryVM}, false);
    long totalCountPrimary = getTotalCount(tableName, new VM[] {primaryVM}, true);

    assertEquals(0, inMemoryCountSecondary);
    assertEquals(intialRecords + remainingReconrds, inMemoryCountPrimary);
    assertEquals(intialRecords + remainingReconrds, totalCountSecondary);
    assertEquals(intialRecords + remainingReconrds, totalCountPrimary);

    // restart the secondary and verify the data.
    List<VM> vms = new ArrayList<>();
    vms.add(secondaryVM);
    restartServers(vms, tableName, 2);
    vms.clear();
    waitForRecovery(tableName, secondaryVM, false);
    // verify the data on secondary vm after restart.
    System.out.println("NNN After restart - Verify key exist on secondary");
    verifyKeyExistOnVM(secondaryVM, tableName, 1);
    inMemoryCountSecondary = getInMemoryRecordsCount(secondaryVM, tableName, false);
    totalCountSecondary = getTotalCount(tableName, new VM[] {secondaryVM}, false);
    totalCountPrimary = getTotalCount(tableName, new VM[] {primaryVM}, true);
    inMemoryCountPrimary = getInMemoryRecordsCount(primaryVM, tableName, true);

    assertEquals(intialRecords + remainingReconrds, inMemoryCountSecondary);
    assertEquals(intialRecords + remainingReconrds, inMemoryCountPrimary);
    assertEquals(intialRecords + remainingReconrds, totalCountSecondary);
    assertEquals(intialRecords + remainingReconrds, totalCountPrimary);

    // restart both the servers and verify the data.
    vms = new ArrayList<>();
    vms.add(secondaryVM);
    vms.add(primaryVM);
    restartServers(vms, tableName, 2);
    vms.clear();
    waitForRecovery(tableName, primaryVM, false);
    waitForRecovery(tableName, secondaryVM, false);

    // find primary and secondary node for the bucket after restart.
    if (getInMemoryRecordsCount(vm0, tableName, true) > 0) {
      primaryVM = vm0;
      secondaryVM = vm1;
    } else {
      primaryVM = vm1;
      secondaryVM = vm0;
    }

    verifyKeyExistOnVM(primaryVM, tableName, 1);
    totalCountPrimary = getTableRowCountUsingScan(tableName);
    assertEquals(intialRecords + remainingReconrds, totalCountPrimary);

    verifyKeyExistOnVM(secondaryVM, tableName, 1);
    totalCountSecondary = getTotalCount(tableName, new VM[] {secondaryVM}, false);
    assertEquals(intialRecords + remainingReconrds, totalCountSecondary);

  }

  private void doAppendWithLargeRecords() {
    StringBuilder sb = new StringBuilder(1040);
    for (int i = 0; i < 1024; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(8));
    }
    String data = sb.toString();

    // appendBatchesWithLargeRecords();
  }


  @Test
  public void testKeyExistOnSecondaryAfterEvictionAndRestart() throws InterruptedException {
    final String tableName = getTestMethodName();

    // create table with redundancy 1 and bucket 1
    final FTable fTable = createFTable(tableName, 1, 1);
    System.out.println("NNN [ tableName => block size ] => " + "[" + fTable.getName() + " => "
        + fTable.getTableDescriptor().getBlockSize() + "]");

    _testKeyExistOnSecondaryAfterEvictionAndRestart(tableName, 500, 500, false);

    deleteFTable(tableName);
  }

  @Test
  public void testEvictionOnSecondaryWithMultipleOplogFiles() throws InterruptedException {
    String DISK_STORE_NAME = "testDiskStore1";
    String tableName = getTestMethodName();
    createDiskStoreOnserver(DISK_STORE_NAME, true, 1, new VM[] {vm0, vm1}, true);
    final FTable table = createFTable(tableName, 1, 1, DISK_STORE_NAME, false);
    assertNotNull(table);

    _testKeyExistOnSecondaryAfterEvictionAndRestart(tableName, 500, 500, true);

    deleteFTable(tableName);

  }

  /**
   * Helper functions
   */
  public static FTableDescriptor getFTableDescriptor(final int numSplits, int redundancy,
      String diskStoreName, boolean synchronous) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < NUM_OF_COLUMNS; i++) {
      tableDescriptor.addColumn(COLUMN_NAME_PREFIX + i);
    }
    tableDescriptor.setTotalNumOfSplits(numSplits);
    tableDescriptor.setRedundantCopies(redundancy);
    if (synchronous) {
      tableDescriptor.enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS);
    }
    if (diskStoreName != null) {
      tableDescriptor.setDiskStore(diskStoreName);
    }
    return tableDescriptor;
  }

  public static FTable createFTable(String ftableName, final int numSplits, int redundancy,
      String diskStoreName, boolean synchronous) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    final FTable fTable = clientCache.getAdmin().createFTable(ftableName,
        getFTableDescriptor(numSplits, redundancy, diskStoreName, synchronous));
    return fTable;
  }

  private void createDiskStore(String disk_store_name, boolean allowDeltaPersistence,
      int maxOplogSize, boolean autocompact) {
    DiskStoreAttributes dsa = new DiskStoreAttributes();
    DiskStoreFactory dsf = new DiskStoreFactoryImpl(CacheFactory.getAnyInstance(), dsa);
    dsf.setAutoCompact(autocompact);
    dsf.setMaxOplogSize(maxOplogSize);
    dsf.setEnableDeltaPersistence(allowDeltaPersistence);
    dsf.create(disk_store_name);
  }

  private void createDiskStoreOnserver(String disk_store_name, boolean allowDeltaPersistence,
      int maxOplogSize, VM[] vms, boolean autoCompact) {
    for (VM vm : vms) {
      vm.invoke(new SerializableCallable<Object>() {
        @Override
        public Object call() throws Exception {
          createDiskStore(disk_store_name, allowDeltaPersistence, maxOplogSize, autoCompact);
          return null;
        }
      });
    }
  }
}
