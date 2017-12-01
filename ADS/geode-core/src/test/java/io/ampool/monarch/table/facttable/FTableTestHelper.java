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

import com.jayway.awaitility.Awaitility;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.PartitionResolver;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.test.dunit.SerializableCallable;

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
            .until(() -> getTotalEntryCount(pr) == 0);

        assertEquals("Expected no entries.", 0, getTotalEntryCount(pr));
      } finally {
        ((MonarchCacheImpl) MCacheFactory.getAnyInstance())
            .getHeapEvictor().testAbortAfterLoopCount = Integer.MAX_VALUE;
        HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
        System.clearProperty("gemfire.memoryEventTolerance");
      }
      return null;
    }

    /**
     * Get the total count of entries in the region; aggregate entries across all buckets.
     * 
     * @param pr the partitioned region
     * @return the total number of entries
     */
    private static int getTotalEntryCount(final PartitionedRegion pr) {

      System.out.println("FTableOverflowToTierTest.getTotalEntryCount COUNT" + pr.getDataStore()
          .getAllLocalBucketRegions().stream().mapToInt(BucketRegion::size).sum());
      return pr.getDataStore().getAllLocalBucketRegions().stream().mapToInt(BucketRegion::size)
          .sum();
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
  }
}
