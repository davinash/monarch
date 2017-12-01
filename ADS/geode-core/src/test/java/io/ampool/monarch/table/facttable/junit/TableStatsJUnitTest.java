/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.ampool.monarch.table.facttable.junit;

import junitparams.JUnitParamsRunner;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.management.bean.stats.MBeanStatsTestCase;
import org.apache.geode.management.internal.beans.PartitionedRegionBridge;
import org.apache.geode.management.internal.beans.RegionMBeanBridge;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Test coverage for Table Stats
 * 
 */

@Category(FTableTest.class)
@RunWith(JUnitParamsRunner.class)
public class TableStatsJUnitTest extends MBeanStatsTestCase {

  private RegionMBeanBridge bridge;

  private PartitionedRegionBridge parBridge;

  private CachePerfStats cachePerfStats;

  private PartitionedRegionStats partitionedRegionStats;

  protected void init() {
    cachePerfStats = new CachePerfStats(system);
    partitionedRegionStats = new PartitionedRegionStats(system, "/tests");
    bridge = new RegionMBeanBridge(cachePerfStats);
    parBridge = new PartitionedRegionBridge(partitionedRegionStats);
  }

  @Test
  public void testCachePerStats() throws InterruptedException {
    cachePerfStats.incEntryCount(400);
    partitionedRegionStats.incScanCount(5);
    partitionedRegionStats.incBucketCount(2);
    partitionedRegionStats.incLowRedundancyBucketCount(1);
    partitionedRegionStats.setActualRedundantCopies(2);

    partitionedRegionStats.incDataStoreEntryCount(1);
    partitionedRegionStats.incPrimaryBucketCount(10);

    sample();

    assertEquals(400, getEntryCount());
    assertEquals(2, getBucketCount());
    assertEquals(5, getScanCount());
    assertEquals(1, getNumBucketsWithoutRedundancy());
    assertEquals(2, getActualRedundancy());
    assertEquals(1, getDataStoreEntryCount());
    assertEquals(10, getPrimaryBucketCount());
  }


  private long getEntryCount() {
    return bridge.getEntryCount();
  }

  private long getScanCount() {
    return parBridge.getScanCount();
  }

  private int getActualRedundancy() {
    return parBridge.getActualRedundancy();
  }

  private int getBucketCount() {
    return parBridge.getBucketCount();
  }

  private int getNumBucketsWithoutRedundancy() {
    return parBridge.getNumBucketsWithoutRedundancy();
  }

  private int getPrimaryBucketCount() {
    return parBridge.getPrimaryBucketCount();
  }

  private int getDataStoreEntryCount() {
    return parBridge.getTotalBucketSize();
  }

}
