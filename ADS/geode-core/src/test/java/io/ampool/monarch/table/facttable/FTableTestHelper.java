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
package io.ampool.monarch.table.facttable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jayway.awaitility.Awaitility;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.PartitionResolver;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.ftable.internal.FTableScanner;
import io.ampool.monarch.table.region.ScanContext;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FTableTestHelper {

  public static FTable getFTable(String ftableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    final FTable fTable = clientCache.getAdmin().createFTable(ftableName,
            FTableDescriptorHelper.getFTableDescriptor());
    return fTable;
  }

  public static FTable getFTable(String ftableName, PartitionResolver partitionResolver) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    final FTable fTable = clientCache.getAdmin().createFTable(ftableName,
            FTableDescriptorHelper.getFTableDescriptor(partitionResolver));
    return fTable;
  }

  public static FTable getFTable(String ftableName, byte[] partitioningColumn,
                                 PartitionResolver partitionResolver) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    final FTable fTable = clientCache.getAdmin().createFTable(ftableName,
            FTableDescriptorHelper.getFTableDescriptor(partitioningColumn, partitionResolver));
    return fTable;
  }

  public static EvictionTrigger createEvictionTrigger(String regionName) {
    return new EvictionTrigger(regionName);
  }

  /**
   * Class which can be used trigger eviction
   */
  public static class EvictionTrigger extends SerializableCallable {

    private float evictionHeapPer = 1.0f;
    private String tableName;

    public EvictionTrigger(final String tableName) {
      // this.evictionHeapPer = evictionHeapPer;
      this.tableName = tableName;
    }

    public float getEvictionHeapPer() {
      return evictionHeapPer;
    }

    public String getTableName() {
      return tableName;
    }

    @Override
    public Object call() throws Exception {
      try {
        // MCacheFactory.getAnyInstance().getResourceManager().setEvictionHeapPercentage(getEvictionHeapPer());
        final PartitionedRegion pr =
                (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(getTableName());
        assertNotNull(pr);

        raiseFakeNotification();

        /** wait for 60 seconds till all entries are evicted.. **/
        Awaitility.await().with().pollInterval(1, TimeUnit.SECONDS).atMost(60, TimeUnit.SECONDS)
                .until(() -> getTotalEntryCount(pr) <= 0);

        assertTrue("Expected no entries.", getTotalEntryCount(pr) <= 0);
      } finally {
        revokeFakeNotification();
      }
      return null;
    }

  }

  /**
   * Get the total count of entries in the region; aggregate entries across all buckets.
   *
   * @param pr the partitioned region
   * @return the total number of entries
   */
  public static int getTotalEntryCount(final PartitionedRegion pr) {
    int count =
            pr.getDataStore().getAllLocalBucketRegions().stream().mapToInt(BucketRegion::size).sum();
    System.out.println("FTableTestHelper.getTotalEntryCount COUNT" + count);
    return count;
  }

  /**
   * Raise a fake notification so that eviction gets started..
   */
  public static void raiseFakeNotification() {
    RegionEvictorTask.TEST_EVICTION_BURST_PAUSE_TIME_MILLIS = 0;
    HeapMemoryMonitor.setTestDisableMemoryUpdates(true);
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(1);
    HeapMemoryMonitor hmm =
            ((MonarchCacheImpl) MCacheFactory.getAnyInstance()).getResourceManager().getHeapMonitor();
    hmm.setTestMaxMemoryBytes(2);
    hmm.updateStateAndSendEvent();
  }

  public static void revokeFakeNotification() {
    ((MonarchCacheImpl) MCacheFactory.getAnyInstance()).getHeapEvictor().testAbortAfterLoopCount =
            Integer.MAX_VALUE;
    HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
    System.clearProperty("gemfire.memoryEventTolerance");
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(-1);
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

  private static int getInMemoryRecordsCount(final String tableName, final boolean onlyPrimary) {
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
      final Iterator<Map.Entry> iterator = concurrentSkipListMap.entrySet().iterator();
      while (iterator.hasNext()) {
        final Map.Entry entry = iterator.next();
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
        System.out
                .println(Thread.currentThread() + "::FTableTestHelper.getInMemoryRecordsCount 180 "
                        + entry.getKey() + " value " + blockValue);
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

  @SuppressWarnings("unchecked")
  public long getCount(final String name, final VM[] vms, final boolean isPrimary) {
    long totalCount = 0;
    for (final VM vm : vms) {
      totalCount += vm.invoke(() -> {
        long count = 0;
        final MCache cache = MCacheFactory.getAnyInstance();
        FTablePartitionedRegion region = (FTablePartitionedRegion) cache.getRegion(name);
        Scan scan = new Scan();
        scan.setMessageChunkSize(100);
        final ScanContext sc =
                new ScanContext(null, region, scan, region.getDescriptor(), null, null);
        for (final BucketRegion br : region.getDataStore().getAllLocalBucketRegions()) {
          if (isPrimary != br.getBucketAdvisor().isPrimary()) {
            continue;
          }
          scan.setBucketId(br.getId());
          long c = 0;
          final Iterator itr = new FTableScanner(sc,
                  (RowTupleConcurrentSkipListMap) br.getRegionMap().getInternalMap());
          while (itr.hasNext()) {
            final Object next = itr.next();
            c++;
          }
          count += c;
        }
        return count;
      });
    }
    return totalCount;
  }


}
