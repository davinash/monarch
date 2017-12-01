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
package io.ampool.tierstore;

import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.TierStoreStats;
import org.apache.geode.management.bean.stats.MBeanStatsTestCase;
import org.apache.geode.management.internal.beans.TierStoreMBeanBridge;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

/**
 */
@Category(FTableTest.class)
public class TierStoreStatsJUnitTest extends MBeanStatsTestCase {

  private TierStoreMBeanBridge tierBridge;

  private TierStoreStats tierStoreStats;

  private static long testStartTime = NanoTimer.getTime();

  public void init() {
    tierStoreStats = new TierStoreStats(system, "test");
    tierBridge = new TierStoreMBeanBridge();
    tierBridge.addTierStoreStats(tierStoreStats);
  }



  /* Test the update of the stats from the TierStoreStats as well TierStoreMBeanBridge */
  @Test
  public void testTierStatsCounters() throws InterruptedException {

    assertEquals(0, tierStoreStats.getWriters());

    tierStoreStats.puttWriters();
    assertEquals(1, tierStoreStats.getWriters());

    sample();

    assertEquals(1, getTierStoreWritersCreated());

  }

  private int getTierStoreWritersCreated() {
    return tierBridge.getTierStoreWriters();
  }

}
