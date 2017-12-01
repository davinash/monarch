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

package io.ampool.monarch.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.jayway.awaitility.Awaitility;
import io.ampool.internal.TierHelper;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.functions.TestDUnitBase;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public abstract class MTableOverflowToTierTest {
  protected static final String TABLE_NAME = "MTableOverflowToTierTest";
  protected static final String FTABLE_NAME = "FTableOverflowToTierTest";
  protected static final boolean debug = false;
  private static final TestDUnitBase testBase = new TestDUnitBase();
  protected static MClientCache clientCache;
  private static final float EVICT_HEAP_PCT = 50.9f;

  @BeforeClass
  public static void setUp() throws Exception {
    testBase.preSetUp();
    testBase.postSetUp();
    clientCache = testBase.getClientCache();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    testBase.preTearDownCacheTestCase();
  }

  /** methods to be implemented for specific type/configuration **/
  protected abstract void verifyMTableDataUsingGet(Map<Object, Object[]> data);

  protected abstract Map<Object, Object[]> putAndVerifyMTableData();

  protected abstract void createMTable();

  /** methods implemented for Ftable specific configuration **/

  protected abstract void appendFTableData();

  protected abstract void createFTable();


  /**
   * Get the total count of entries in the region; aggregate entries across all buckets.
   *
   * @param pr the partitioned region
   * @return the total number of entries
   */
  private static int getTotalEntryCount(final PartitionedRegion pr) {
    return pr.getDataStore().getAllLocalBucketRegions().stream().mapToInt(BucketRegion::size).sum();
  }

  /**
   * Task for verifying the overflowed entries to the tier i.e. from file (for test).
   */
  static final SerializableCallable GET_COUNT_FROM_OVERFLOW_TIER_MTABLE =
      new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          MCacheFactory.getAnyInstance().getResourceManager()
              .setEvictionHeapPercentage(EVICT_HEAP_PCT);
          final PartitionedRegion pr =
              (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(TABLE_NAME);
          if (debug)
            System.out.println("MTableOverflowToTierTest.call YYYYYYYYYYYYYYYY");
          assertNotNull(pr);
          int count = 0;
          /** get the total count of number of entries from overflow-tier **/
          for (final BucketRegion br : pr.getDataStore().getAllLocalBucketRegions()) {
            FileInputStream fis = new FileInputStream(TierHelper.getTierFileNameKeys(br));
            DataInputStream dis = new DataInputStream(new BufferedInputStream(fis, 32768));
            while (dis.available() > 0) {
              DataSerializer.readObject(dis);
              count++;
            }
            dis.close();
          }
          return count;
        }
      };


  /**
   * Task for verifying the overflowed entries to the tier i.e. from file (for test).
   */
  static final SerializableCallable GET_COUNT_FROM_OVERFLOW_TIER_FTABLE =
      new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          MCacheFactory.getAnyInstance().getResourceManager()
              .setEvictionHeapPercentage(EVICT_HEAP_PCT);
          final PartitionedRegion pr =
              (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(FTABLE_NAME);
          assertNotNull(pr);
          int count = 0;
          /** get the total count of number of entries from overflow-tier **/
          for (final BucketRegion br : pr.getDataStore().getAllLocalBucketRegions()) {
            FileInputStream fis = new FileInputStream(TierHelper.getTierFileNameKeys(br));
            DataInputStream dis = new DataInputStream(new BufferedInputStream(fis, 32768));
            while (dis.available() > 0) {
              DataSerializer.readObject(dis);
              count++;
            }
            dis.close();
          }
          return count;
        }
      };

  /**
   * Task for evicting all entries to the next tier on servers. It asserts that there are no entries
   * (i.e. 0) on server and all entries are evicted to the next tier.
   */
  static final SerializableRunnable FORCE_EVICTION_TASK_ON_MTABLE = new SerializableRunnable() {
    @Override
    public void run() throws Exception {
      try {
        MCacheFactory.getAnyInstance().getResourceManager()
            .setEvictionHeapPercentage(EVICT_HEAP_PCT);
        final PartitionedRegion pr =
            (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(TABLE_NAME);
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
    }
  };



  /**
   * Task for evicting all entries to the next tier on servers. It asserts that there are no entries
   * (i.e. 0) on server and all entries are evicted to the next tier.
   */
  static final SerializableRunnable FORCE_EVICTION_TASK_ON_FTABLE = new SerializableRunnable() {
    @Override
    public void run() throws Exception {
      try {
        MCacheFactory.getAnyInstance().getResourceManager()
            .setEvictionHeapPercentage(EVICT_HEAP_PCT);
        final PartitionedRegion pr =
            (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(FTABLE_NAME);
        assertNotNull(pr);
        if (debug)
          System.out.println("MTableOverflowToTierTest.run Prior Notification YYYYYYYYYYYYY"
              + getTotalEntryCount(pr));
        raiseFakeNotification();

        /** wait for 60 seconds till all entries are evicted.. **/
        Awaitility.await().with().pollInterval(1, TimeUnit.SECONDS).atMost(60, TimeUnit.SECONDS)
            .until(() -> getTotalEntryCount(pr) == 0);
        if (debug)
          System.out.println("MTableOverflowToTierTest.run YYYYYYYYYYYYY" + getTotalEntryCount(pr));
        assertEquals("Expected no entries.", 0, getTotalEntryCount(pr));
      } finally {
        ((MonarchCacheImpl) MCacheFactory.getAnyInstance())
            .getHeapEvictor().testAbortAfterLoopCount = Integer.MAX_VALUE;
        HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
        System.clearProperty("gemfire.memoryEventTolerance");
      }
    }
  };


  /**
   * Task for getting all entries on servers.
   */
  /**
   * Task for verifying the overflowed entries to the tier i.e. from file (for test).
   */
  static final SerializableCallable GET_RECORD_COUNT_FROM_MEMORY = new SerializableCallable() {
    @Override
    public Object call() throws Exception {
      MCacheFactory.getAnyInstance().getResourceManager().setEvictionHeapPercentage(EVICT_HEAP_PCT);
      final PartitionedRegion pr =
          (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(FTABLE_NAME);
      assertNotNull(pr);
      return (getTotalEntryCount(pr));
    }
  };



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

  @Test
  public void testPutAndOverflowToTier() {
    createMTable();
    Map<Object, Object[]> data = putAndVerifyMTableData();
    verifyMTableDataUsingGet(data);

    /** servers are on VMs 0, 2, and 3 **/
    testBase.getVm(0).invoke(FORCE_EVICTION_TASK_ON_MTABLE);
    testBase.getVm(2).invoke(FORCE_EVICTION_TASK_ON_MTABLE);
    testBase.getVm(3).invoke(FORCE_EVICTION_TASK_ON_MTABLE);

    /** make sure that client sees no data via scan.. **/
    int count = 0;
    for (final Row ignored : clientCache.getTable(TABLE_NAME).getScanner(new Scan())) {
      count++;
    }
    assertEquals("Expected no entries in memory.", 0, count);

    /** assert on the total count of entries from overflow-tier on each server **/
    int t0 = (int) testBase.getVm(0).invoke(GET_COUNT_FROM_OVERFLOW_TIER_MTABLE);
    int t1 = (int) testBase.getVm(2).invoke(GET_COUNT_FROM_OVERFLOW_TIER_MTABLE);
    int t2 = (int) testBase.getVm(3).invoke(GET_COUNT_FROM_OVERFLOW_TIER_MTABLE);
    if (debug) {
      System.out
          .println("MTableOverflowToTierTest.testOverflowTieronEmptyMTable t0 XXXXXXXXXXXXX " + t0);
      System.out
          .println("MTableOverflowToTierTest.testOverflowTieronEmptyMTable t1 XXXXXXXXXXXXX " + t1);
      System.out
          .println("MTableOverflowToTierTest.testOverflowTieronEmptyMTable t2 XXXXXXXXXXXXX " + t2);
    }

    int totalCount = t0 + t1 + t2;
    assertEquals("Incorrect number of entries in overflow-tier.", data.size(), totalCount);

    /** delete table **/
    clientCache.getAdmin().deleteTable(TABLE_NAME);

  }

  /**
   * Create an empty table and execute overflow post raising an forced eviction task
   */
  @Test
  public void testOverflowonEmptyFTablePostEviction() {
    createFTable();
    testBase.getVm(0).invoke(FORCE_EVICTION_TASK_ON_FTABLE);
    testBase.getVm(2).invoke(FORCE_EVICTION_TASK_ON_FTABLE);
    testBase.getVm(3).invoke(FORCE_EVICTION_TASK_ON_FTABLE);

    /** assert on the total count of entries from overflow-tier on each server **/
    int t0 = (int) testBase.getVm(0).invoke(GET_COUNT_FROM_OVERFLOW_TIER_FTABLE);
    int t1 = (int) testBase.getVm(2).invoke(GET_COUNT_FROM_OVERFLOW_TIER_FTABLE);
    int t2 = (int) testBase.getVm(3).invoke(GET_COUNT_FROM_OVERFLOW_TIER_FTABLE);
    if (debug) {
      System.out
          .println("MTableOverflowToTierTest.testOverflowTieronEmptyFTable t0 XXXXXXXXXXXXX " + t0);
      System.out
          .println("MTableOverflowToTierTest.testOverflowTieronEmptyFTable t1 XXXXXXXXXXXXX " + t1);
      System.out
          .println("MTableOverflowToTierTest.testOverflowTieronEmptyFTable t2 XXXXXXXXXXXXX " + t2);
    }
    assertEquals("Expected no entries in memory.", 0, t0 + t1 + t2);
    /** delete table **/
    clientCache.getAdmin().deleteFTable(FTABLE_NAME);

  }



  /**
   * Create an empty table and execute overflow prior raising an forced eviction task
   */
  @Test
  public void testOverflowonEmptyFTablePriortoEviction() {
    createFTable();
    /** assert on the total count of entries from overflow-tier on each server **/
    int t0 = (int) testBase.getVm(0).invoke(GET_COUNT_FROM_OVERFLOW_TIER_FTABLE);
    int t1 = (int) testBase.getVm(2).invoke(GET_COUNT_FROM_OVERFLOW_TIER_FTABLE);
    int t2 = (int) testBase.getVm(3).invoke(GET_COUNT_FROM_OVERFLOW_TIER_FTABLE);
    if (debug) {
      System.out
          .println("MTableOverflowToTierTest.testOverflowTieronEmptyFTable t0 XXXXXXXXXXXXX " + t0);
      System.out
          .println("MTableOverflowToTierTest.testOverflowTieronEmptyFTable t1 XXXXXXXXXXXXX " + t1);
      System.out
          .println("MTableOverflowToTierTest.testOverflowTieronEmptyFTable t2 XXXXXXXXXXXXX " + t2);
    }
    assertEquals("Expected no entries in memory.", 0, t0 + t1 + t2);
    /** delete table **/
    clientCache.getAdmin().deleteFTable(FTABLE_NAME);

  }


  /**
   * Create a table and populate records and execute overflow
   */
  @Test
  public void testAppendAndOverflowonFTable() {
    createFTable();
    appendFTableData();

    /** Get the count of records from in memory ftable Tier0 */
    int inmemory = (int) testBase.getVm(0).invoke(GET_RECORD_COUNT_FROM_MEMORY)
        + (int) testBase.getVm(2).invoke(GET_RECORD_COUNT_FROM_MEMORY)
        + (int) testBase.getVm(3).invoke(GET_RECORD_COUNT_FROM_MEMORY);
    assertEquals("Expected no entries in memory prior to Eviction.", inmemory, 452);
    testBase.getVm(0).invoke(FORCE_EVICTION_TASK_ON_FTABLE);
    testBase.getVm(2).invoke(FORCE_EVICTION_TASK_ON_FTABLE);
    testBase.getVm(3).invoke(FORCE_EVICTION_TASK_ON_FTABLE);

    /**
     * Get the count of records from in memory ftable Tier0 now there should be no records as they
     * have been evicted
     */
    int postEviction = (int) testBase.getVm(0).invoke(GET_RECORD_COUNT_FROM_MEMORY)
        + (int) testBase.getVm(2).invoke(GET_RECORD_COUNT_FROM_MEMORY)
        + (int) testBase.getVm(3).invoke(GET_RECORD_COUNT_FROM_MEMORY);
    assertEquals("Expected no entries in memory post Eviction.", postEviction, 0);

    /** assert on the total count of entries from overflow-tier on each server **/
    int t0 = (int) testBase.getVm(0).invoke(GET_COUNT_FROM_OVERFLOW_TIER_FTABLE);
    int t1 = (int) testBase.getVm(2).invoke(GET_COUNT_FROM_OVERFLOW_TIER_FTABLE);
    int t2 = (int) testBase.getVm(3).invoke(GET_COUNT_FROM_OVERFLOW_TIER_FTABLE);

    if (debug) {
      System.out.println(
          "MTableOverflowToTierTest.testOverflowTieronFTable XXXXXXXXXXXXXXX IN MEMORY" + inmemory);
      System.out.println(
          "MTableOverflowToTierTest.testOverflowTieronFTable XXXXXXXXXXXXXXX IN MEMORY POST EVICTION"
              + postEviction);
      System.out
          .println("MTableOverflowToTierTest.testOverflowTieronEmptyFTable t0 XXXXXXXXXXXXX " + t0);
      System.out
          .println("MTableOverflowToTierTest.testOverflowTieronEmptyFTable t1 XXXXXXXXXXXXX " + t1);
      System.out
          .println("MTableOverflowToTierTest.testOverflowTieronEmptyFTable t2 XXXXXXXXXXXXX " + t2);
    }
    assertEquals("Expected no entries in memory.", inmemory, t0 + t1 + t2);

    /** delete table **/
    clientCache.getAdmin().deleteFTable(FTABLE_NAME);


  }



  /**
   * Create a ftable and populate records int it. Test the records are correctly inserted Then
   * restart all the servers Now the inserted records should be brought back again from persistence
   * store
   */
  @Test
  public void testPersistenceOnFTableBeforeEviction() {
    createFTable();
    appendFTableData();

    /** Get the count of records from in memory ftable Tier0 */
    int inmemory = (int) testBase.getVm(0).invoke(GET_RECORD_COUNT_FROM_MEMORY)
        + (int) testBase.getVm(2).invoke(GET_RECORD_COUNT_FROM_MEMORY)
        + (int) testBase.getVm(3).invoke(GET_RECORD_COUNT_FROM_MEMORY);
    assertEquals("Expected no entries in memory prior to Eviction.", inmemory, 452);

    testBase.restart();

    inmemory = (int) testBase.getVm(0).invoke(GET_RECORD_COUNT_FROM_MEMORY)
        + (int) testBase.getVm(2).invoke(GET_RECORD_COUNT_FROM_MEMORY)
        + (int) testBase.getVm(3).invoke(GET_RECORD_COUNT_FROM_MEMORY);
    if (debug)
      System.out.println(
          "MTableOverflowToTierTest.testRestartServeronFTable XXXXXXXXXXXXXX inmemory" + inmemory);

    /** delete table **/
    clientCache.getAdmin().deleteFTable(FTABLE_NAME);
  }


  /**
   * Create a ftable and populate records int it. Test the records are correctly inserted Then
   * restart all the servers Now the inserted records should be brought back again from persistence
   * store
   */
  @Test
  public void testRestartServeronFTablePostEviction() {
    createFTable();
    appendFTableData();

    /** Get the count of records from in memory ftable Tier0 */
    int inmemory = (int) testBase.getVm(0).invoke(GET_RECORD_COUNT_FROM_MEMORY)
        + (int) testBase.getVm(2).invoke(GET_RECORD_COUNT_FROM_MEMORY)
        + (int) testBase.getVm(3).invoke(GET_RECORD_COUNT_FROM_MEMORY);
    assertEquals("Expected no entries in memory prior to Eviction.", inmemory, 452);

    testBase.restart();

    inmemory = (int) testBase.getVm(0).invoke(GET_RECORD_COUNT_FROM_MEMORY)
        + (int) testBase.getVm(2).invoke(GET_RECORD_COUNT_FROM_MEMORY)
        + (int) testBase.getVm(3).invoke(GET_RECORD_COUNT_FROM_MEMORY);
    if (debug)
      System.out.println(
          "MTableOverflowToTierTest.testRestartServeronFTable XXXXXXXXXXXXXX inmemory" + inmemory);

    /** delete table **/
    clientCache.getAdmin().deleteFTable(FTABLE_NAME);
  }


}
