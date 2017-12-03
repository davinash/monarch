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
import static org.junit.Assert.assertTrue;

import com.jayway.awaitility.Awaitility;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.IMKey;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.CompareOp;
import io.ampool.store.StoreHandler;
import io.ampool.utils.TimestampUtil;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.VersionedThinDiskLRURegionEntryHeapObjectKey;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


@Category(FTableTest.class)
public class FTableEvictionDUnitTest extends MTableDUnitHelper {
  private static int DEFAULT_NUM_COLUMNS = 1;
  private static String DEFAULT_COL_PREFIX = "COL";
  private static String DEFAULT_COLVAL_PREFIX = "VAL";
  private List<io.ampool.monarch.table.facttable.dunit.FTableServerFailureDUnitTest.AppendThread> appendThreads =
      new ArrayList<io.ampool.monarch.table.facttable.dunit.FTableServerFailureDUnitTest.AppendThread>();
  private static float EVICT_HEAP_PCT = 50.9f;

  public static float getEVICT_HEAP_PCT() {
    return EVICT_HEAP_PCT;
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
    }
  }

  private void runScan(final FTable fTable, final long expectedRecords) {
    final Iterator<Row> iterator = fTable.getScanner(new Scan()).iterator();
    int actualRecods = 0;
    long prevSeqId = 0;
    while (iterator.hasNext()) {
      final Row row = iterator.next();
      BlockKey key = new BlockKey(row.getRowId());
      long currSeqId = key.getBlockSequenceID();
      assertTrue(currSeqId == prevSeqId || currSeqId == prevSeqId + 1);
      prevSeqId = currSeqId;
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
      final Iterator<Map.Entry> iterator = concurrentSkipListMap.entrySet().iterator();
      while (iterator.hasNext()) {
        final Map.Entry entry = iterator.next();
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

  /**
   * Read the blocks which were inserted before time stop, uses both scanner and direct access to
   * the data store map
   */
  private void touchBlocks(VM vm, String fTableName, long start, long stop) {

    int numRecordsScanned = 0;
    /* Touch the records using Scan with filter */
    Scan scan = new Scan();
    scan.setFilter(new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, stop));
    FTable table = MClientCacheFactory.getAnyInstance().getFTable(fTableName);
    Iterator scanItr = table.getScanner(scan).iterator();
    while (scanItr.hasNext()) {
      scanItr.next();
      numRecordsScanned++;
    }
    System.out.println("Number of records scanned = " + numRecordsScanned);

    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        int blocksTouched = 0;
        PartitionedRegion pr =
            (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(fTableName);
        BucketRegion br = pr.getDataStore().getLocalBucketById(0);
        RowTupleConcurrentSkipListMap internalMap =
            (RowTupleConcurrentSkipListMap) br.getRegionMap().getInternalMap();
        Map realMap = internalMap.getInternalMap();

        Iterator<Map.Entry<IMKey, RegionEntry>> itr = realMap.entrySet().iterator();
        while (itr.hasNext()) {
          final Map.Entry<IMKey, RegionEntry> blockKeyRegionEntry = itr.next();
          BlockKey blockkey = (BlockKey) blockKeyRegionEntry.getKey();
          if (blockkey.getStartTimeStamp() > stop) {
            break;
          }
          /* touch */
          if (blockkey.getBlockSequenceID() == 0) {
            blockKeyRegionEntry.getValue()._getValue();
            /* This will make the test fail as the order of eviction will change */
            // blockKeyRegionEntry.getValue().setRecentlyUsed();
            blocksTouched++;
          }
        }
        System.out.println("Number of blocks touched = " + blocksTouched);
        return null;
      }
    });
  }

  private void markBlocksUnused(VM vm, String fTableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        int blocksTouched = 0;
        PartitionedRegion pr =
            (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(fTableName);
        BucketRegion br = pr.getDataStore().getLocalBucketById(0);
        RowTupleConcurrentSkipListMap internalMap =
            (RowTupleConcurrentSkipListMap) br.getRegionMap().getInternalMap();
        Map realMap = internalMap.getInternalMap();

        Iterator<Map.Entry<IMKey, RegionEntry>> itr = realMap.entrySet().iterator();
        while (itr.hasNext()) {
          final Map.Entry<IMKey, RegionEntry> blockKeyRegionEntry = itr.next();
          ((VersionedThinDiskLRURegionEntryHeapObjectKey) blockKeyRegionEntry.getValue())
              .unsetRecentlyUsed();
        }
        System.out.println("Number of blocks touched = " + blocksTouched);
        return null;
      }
    });
  }

  @Test
  public void testEvictionOrder() {
    final String fTableName = getTestMethodName();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    createClientCache();

    pauseWalMontoring(vm0, fTableName);

    final FTable fTable = createFTable(fTableName, 1, 1);
    /* Insert two blocks */
    long start = TimestampUtil.getCurrentTime();
    doAppend(fTableName, 2000);
    long stop = TimestampUtil.getCurrentTime();

    doAppend(fTableName, 2000);

    markBlocksUnused(vm0, fTableName);
    touchBlocks(vm0, fTableName, start, stop);

    runScan(fTable, 4000);

    forceEvictiononServer(vm0, fTableName);

    runScan(fTable, 4000);

    // deleteFTable(getTestMethodName());
    closeMClientCache();
    stopServerOn(vm0);
  }

}

