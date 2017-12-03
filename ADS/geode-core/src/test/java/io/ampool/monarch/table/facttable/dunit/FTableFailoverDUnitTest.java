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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.jayway.awaitility.Awaitility;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.store.StoreHandler;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.VMCachedDeserializable;
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
      // System.out.println("Column Name " + Bytes.toString(mCell.getColumnName()));
      // System.out.println("Column Value " + Bytes.toString((byte[]) mCell.getColumnValue()));
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

  private int getInMemoryRecordsCount(final VM vm, final String tableName,
      final boolean onlyPrimary) {
    return (int) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return getInMemoryRecordsCount(tableName, onlyPrimary);
      }
    });
  }

  protected int getInMemoryRecordsCount(final String tableName, final boolean onlyPrimary) {
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

  @Test
  @Parameters(method = "getNumRecords")
  public void testRecovery(final String fTableName, final int numRecords)
      throws InterruptedException {
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
    deleteFTable(fTableName);
  }
}
