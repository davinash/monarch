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

package io.ampool.monarch.table.facttable.junit;

import com.jayway.awaitility.Awaitility;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.facttable.FTableTestHelper;
import io.ampool.monarch.table.facttable.dunit.FTableOverflowToParquetTierDUnit;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.ftable.internal.FTableScanner;
import io.ampool.monarch.table.functions.TestDUnitBase;
import io.ampool.monarch.table.internal.AbstractTableDescriptor;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.region.ScanContext;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.TypeUtils;
import io.ampool.store.DefaultConstructorMissingException;
import io.ampool.store.StoreCreateException;
import io.ampool.store.StoreHandler;
import io.ampool.store.StoreHierarchy;
import io.ampool.store.StoreRecord;
import io.ampool.store.StoreUtils;
import io.ampool.tierstore.FileFormats;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreFactory;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.TierStoreReaderFactory;
import io.ampool.tierstore.TierStoreWriter;
import io.ampool.tierstore.TierStoreWriterFactory;
import io.ampool.tierstore.wal.WALRecord;
import io.ampool.tierstore.wal.WALResultScanner;
import io.ampool.tierstore.wal.WriteAheadLog;
import io.ampool.utils.ReflectionUtils;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.FTablePartitionedRegion;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.test.dunit.*;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@Category(FTableTest.class)
public abstract class FTableOverflowToTierTest extends Thread {

  protected static final String TABLE_NAME = "MTableOverflowToTierTest";
  private static final String FTABLE_NAME = "FTableOverflowToTierTest";
  protected static final boolean debug = false;
  private static final TestDUnitBase testBase = new TestDUnitBase();
  protected static MClientCache clientCache;
  private static float EVICT_HEAP_PCT = 50.9f;
  @Rule
  public TestName testName = new TestName();


  public static float getEVICT_HEAP_PCT() {
    return EVICT_HEAP_PCT;
  }

  public void setEVICT_HEAP_PCT(float EVICT_HEAP_PCT) {
    this.EVICT_HEAP_PCT = EVICT_HEAP_PCT;
  }

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

  private static final SerializableRunnable REST_HEAP_MAX_BYTES = new SerializableRunnable() {
    @Override
    public void run() throws Exception {
      final InternalResourceManager irm =
          ((MonarchCacheImpl) MCacheFactory.getAnyInstance()).getResourceManager();
      irm.getHeapMonitor().setTestMaxMemoryBytes(Runtime.getRuntime().maxMemory());
    }
  };
  private static final SerializableRunnable GC_TASK = new SerializableRunnable() {
    @Override
    public void run() throws Exception {
      System.gc();
    }
  };

  @After
  public void cleanUpMethod() {
    /** delete table **/
    clientCache.getAdmin().deleteFTable(getTableName());
    /** check the table is deleted **/
    assertEquals(null, clientCache.getFTable(getTableName()));
    for (final VM vm : testBase.getServers()) {
      /* reset the eviction-state, if any, due to the forced eviction. */
      vm.invoke(new SerializableRunnable() {
        @Override
        public void run() throws Exception {
          MCacheFactory.getAnyInstance().getResourceManager().setEvictionHeapPercentage(0);
          MCacheFactory.getAnyInstance().getResourceManager().setCriticalHeapPercentage(0);
          HeapMemoryMonitor hmm = ((MonarchCacheImpl) MCacheFactory.getAnyInstance())
              .getResourceManager().getHeapMonitor();
          hmm.setTestMaxMemoryBytes(Long.MAX_VALUE);
          hmm.updateStateAndSendEvent(0L);
        }
      });
      vm.invoke(GC_TASK);
    }
    if (getTableName().contains("MultipleStores") && tierArgs != null) {
      deRegisterStores(tierArgs);
      tierArgs = null;
    }
  }

  /**
   * Get the standardized table-name for all methods in format: <prefix>_<method_name>
   *
   * @return the table name
   */
  public String getTableName() {
    return FTABLE_NAME + "__" + testName.getMethodName();
  }

  public abstract void appendFTableDataforORCStore();

  public abstract void appendFTableforWAL();

  protected abstract void createFTableWithBlockSizeOne();

  /**
   * methods implemented for Ftable specific configuration
   **/

  protected abstract void appendFTableData();

  protected abstract void appendOneRecordinFTable();

  protected void createFTable() {
    createFTable(99);
  }

  protected abstract void createFTable(int numSplits);

  /**
   * Get the total count of entries in the region; aggregate entries across all buckets.
   *
   * @param pr the partitioned region
   * @return the total number of entries
   */
  private static int getTotalEntryCount(final PartitionedRegion pr) {
    final int count =
        pr.getDataStore().getAllLocalBucketRegions().stream().mapToInt(BucketRegion::size).sum();
    System.out.println("getTotalEntryCount::: count = " + count);
    FTableTestHelper.raiseFakeNotification();
    return count;
  }


  private static void forceEvictiononServer(final VM vm, final String regionName)
      throws RMIException {

    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        try {
          System.gc();
          MCacheFactory.getAnyInstance().getResourceManager()
              .setEvictionHeapPercentage(getEVICT_HEAP_PCT());
          final PartitionedRegion pr =
              (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(regionName);
          assertNotNull(pr);
          FTableTestHelper.raiseFakeNotification();

          /** wait for 60 seconds till all entries are evicted.. **/
          Awaitility.await().with().pollInterval(5, TimeUnit.SECONDS).atMost(220, TimeUnit.SECONDS)
              .until(() -> getTotalEntryCount(pr) == 0);
          System.out.println(
              "FTableOverflowToTierTest.run :: " + "Total Entry Count: " + getTotalEntryCount(pr));
          assertEquals("Expected no entries.", 0, getTotalEntryCount(pr));
        } finally {
          FTableTestHelper.revokeFakeNotification();
        }

      }
    });
  }


  private static int getRegionCountOnServer(final VM vm, final String regionName)
      throws RMIException {
    return (int) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        final PartitionedRegion pr =
            (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(regionName);
        assertNotNull(pr);
        return (getTotalEntryCount(pr));
      }
    });
  }


  private static int getCountFromWal(final VM vm, final String regionName) throws RMIException {
    return (int) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        int scanCount = 0;
        for (int i = 0; i < 113; i++) {

          final WALResultScanner scanner = WriteAheadLog.getInstance().getScanner(regionName, i);
          while (true) {
            WALRecord a = scanner.next();
            if (a != null) {
              BlockValue blockValue = a.getBlockValue();
              scanCount += blockValue.getCurrentIndex();
            } else {
              break;
            }
          }
        }
        if (debug) {
          System.out.println("FTableOverflowToTierTest.getCountFromWalScan Scan Count" + scanCount);
        }
        return (scanCount);
      }
    });
  }


  public void createParquetStore() {
    final String STORE_CLASS = "io.ampool.tierstore.stores.LocalTierStore";
    final String PARQUET_READER_CLASS =
        "io.ampool.tierstore.readers.parquet.TierStoreParquetReader";
    final String PARQUET_WRITER_CLASS =
        "io.ampool.tierstore.writers.parquet.TierStoreParquetWriter";

    // on each server create local tier-store with parquet file format
    for (final VM vm : testBase.getServers()) {
      vm.invoke(new SerializableRunnable() {
        @Override
        public void run() throws Exception {
          final Properties empty = new Properties();
          TierStoreReader reader = new TierStoreReaderFactory().create(PARQUET_READER_CLASS, empty);
          TierStoreWriter writer = new TierStoreWriterFactory().create(PARQUET_WRITER_CLASS, empty);
          final Properties props = new Properties();
          props.setProperty("base.dir.path", "parquet");

          TierStore store = new TierStoreFactory().create(STORE_CLASS,
              FTableOverflowToParquetTierDUnit.STORE_NAME, props, writer, reader,
              (MonarchCacheImpl) MCacheFactory.getAnyInstance());
          StoreHandler.getInstance().registerStore(FTableOverflowToParquetTierDUnit.STORE_NAME,
              store);
        }
      });
      vm.invoke(GC_TASK);
    }
  }

  /**
   * Task for getting all entries on servers.
   */
  /**
   * Task for verifying the overflowed entries to the tier i.e. from file (for test).
   */
  // static final SerializableCallable CREATE_ORC_STORE = new SerializableCallable() {
  // @Override
  // public Object call() throws Exception {
  // MCache cache = MCacheFactory.getAnyInstance();
  // StoreHierarchy sh = StoreHierarchy.getInstance();
  // // sh.removeStore("ORC");
  // // This is admin activity
  // // cache.getAdmin().setStoreHierarchy(sh);
  // // sh.addStore("LocalStore"); ORC store is already added
  // // new TierStoreFactory().create("io.ampool.store.orc.LocalNativeORCArchiveStore",
  // "LocalStore", new java.util.Properties());
  // // sh.addStore("LocalStore");
  // cache.getAdmin().setStoreHierarchy(sh);
  // new TierStoreFactory().create("io.ampool.store.orc.LocalNativeORCArchiveStore", "LocalStore",
  // new java.util.Properties());
  // return null;
  // }
  // };


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
   * Create an empty table and execute overflow post raising an forced eviction task
   */
  @Test
  public void testOverflowonEmptyFTablePostEviction() {
    createFTable();
    forceEvictionTask(getTableName());
    assertEquals("Expected no entries in memory.", 0, getCountFromScan(getTableName()));
  }

  /**
   * Create an empty table and execute overflow prior raising an forced eviction task
   */
  @Test
  public void testOverflowonEmptyFTablePriortoEviction() {
    createFTable();
    /** Get the count now from scan **/
    assertEquals("Expected no entries in memory from scan", 0, getCountFromScan(getTableName()));
    /** delete table **/
  }

  /**
   * Create a table and populate records and execute overflow Test the populated records from region
   * as well as scan.
   */
  @Test
  public void testAppendAndOverflowOnFTable() {
    createFTable();
    appendFTableData();
    /** Get the count of records from in memory ftable Tier0 by region get */
    assertEquals("Expected no entries in memory prior to Eviction.", 452,
        getinMemoryCount(getTableName()));
    /** Get the count of records from in memory ftable Tier0 via scan */
    assertEquals("Scan expected the number of entries", 452, getCountFromScan(getTableName()));
    /* Force eviction task */
    forceEvictionTask(getTableName());
    /** Get the count now from scan post eviction **/
    assertEquals("Scan expected the number of entries", 452, getCountFromScan(getTableName()));
    /** Get the count of records from in memory ftable Tier0 by region get */
    assertEquals("Expected no entries in memory post Eviction.", 0,
        getinMemoryCount(getTableName()));
  }


  /**
   * Create a table and populate records and execute overflow Test the populated records from region
   * as well as scan.
   */
  @Test
  public void testAppendAndOverflowOnFTablewithBlockSizeOne() {
    createFTableWithBlockSizeOne();
    appendOneRecordinFTable();
    /** Get the count of records from in memory ftable Tier0 by region get */
    assertEquals("Expected no entries in memory prior to Eviction.", 1,
        getinMemoryCount(getTableName()));
    /** Get the count of records from in memory ftable Tier0 via scan */
    assertEquals("Scan expected the number of entries", 1, getCountFromScan(getTableName()));
    /* Force eviction task */
    forceEvictionTask(getTableName());
    /** Get the count now from scan post eviction **/
    assertEquals("Scan expected the number of entries", 1, getCountFromScan(getTableName()));
    /** Get the count of records from in memory ftable Tier0 by region get */
    assertEquals("Expected no entries in memory post Eviction.", 0,
        getinMemoryCount(getTableName()));
  }

  /**
   * This test is failing due to complex data types used in it
   */
  // @Test
  public void testWALMonitorPauseResume() {
    createFTable(1);
    // pause the WAL monitoring
    pauseWALMonitor(getTableName(), 0);
    appendFTableDataforORCStore();

    /** Get the count of records from in memory ftable Tier0 by region get */
    // assertEquals("Expected all entries in memory prior to Eviction.", 452000,
    // getinMemoryCount(getTableName()));
    /** Get the count of records from in memory ftable Tier0 via scan */
    assertEquals("Scan expected the number of entries", 452000, getCountFromScan(getTableName()));
    /* Force eviction task */
    forceEvictionTask(getTableName());
    /** Get the count now from scan post eviction **/
    assertEquals("Scan expected the number of entries", 452000, getCountFromScan(getTableName()));
    /** Get the count of records from in memory ftable Tier0 by region get */
    assertEquals("Expected no entries in memory post Eviction.", 0,
        getinMemoryCount(getTableName()));
    /* make sure that nothing is in ORC dir */
    assertEmptyORCDIR();

    resumeWALMonitor(getTableName(), 0);
    try {
      Thread.sleep(30000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertNonEmptyORCDIR();
  }

  public void assertEmptyORCDIR() {
    assertEmptyORCDIROnServer(testBase.getVm(0));
    assertEmptyORCDIROnServer(testBase.getVm(2));
    assertEmptyORCDIROnServer(testBase.getVm(3));
  }

  public void assertEmptyORCDIROnServer(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Path orcDir = Paths.get("ORC");
        if (Files.exists(orcDir)) {
          assertEquals(0, new File("ORC").listFiles().length);
        }
        return null;
      }
    });
  }

  public void assertNonEmptyORCDIR() {
    int one = (int) assertNonEmptyORCDIROnServer(testBase.getVm(0));
    int two = (int) assertNonEmptyORCDIROnServer(testBase.getVm(2));
    int three = (int) assertNonEmptyORCDIROnServer(testBase.getVm(3));
    assertNotEquals(0, one + two + three);
  }

  public Object assertNonEmptyORCDIROnServer(VM vm) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        File dir = new File("ORC");
        return dir.exists() ? dir.listFiles().length : 0;
      }
    });
  }

  public void pauseWALMonitor(String tableName, int partitionId) {
    pauseWALMonitorOnServer(testBase.getVm(0), tableName, partitionId);
    pauseWALMonitorOnServer(testBase.getVm(2), tableName, partitionId);
    pauseWALMonitorOnServer(testBase.getVm(3), tableName, partitionId);
  }

  public void pauseWALMonitorOnServer(VM vm, String tableName, int partitionId) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        StoreHandler.getInstance().pauseWALMonitoring(tableName, partitionId);
        return null;
      }
    });
  }

  public void resumeWALMonitor(String tableName, int partitionId) {
    resumeWALMonitorOnServer(testBase.getVm(0), tableName, partitionId);
    resumeWALMonitorOnServer(testBase.getVm(2), tableName, partitionId);
    resumeWALMonitorOnServer(testBase.getVm(3), tableName, partitionId);
  }

  public void resumeWALMonitorOnServer(VM vm, String tableName, int partitionId) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        StoreHandler.getInstance().resumeWALMonitoring(tableName, partitionId);
        return null;
      }
    });
  }

  public long getinMemoryCount(String ftable) {
    FTable mTable = testBase.getClientCache().getFTable(ftable);
    Map<Integer, Set<ServerLocation>> map = new HashMap<>();
    List<MTableUtils.MSplit> splits = MTableUtils.getSplits(ftable, 113, 113, map);

    long totalCount = 0;
    for (MTableUtils.MSplit split : splits) {
      totalCount += MTableUtils.getTotalCount(mTable, split.getBuckets(), null, false);
    }
    return totalCount;
    // return (getRegionCountOnServer(testBase.getVm(0), ftable) +
    // getRegionCountOnServer(testBase.getVm(2), ftable) + getRegionCountOnServer(testBase
    // .getVm(3), ftable));
  }

  private AsyncInvocation forceEvictionAsyncTask(final VM vm, final String table) {
    return vm.invokeAsync(() -> {
      vm.invoke(new SerializableRunnable() {
        @Override
        public void run() throws Exception {
          try {
            System.gc();
            final PartitionedRegion pr =
                (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(table);
            assertNotNull(pr);
            FTableTestHelper.raiseFakeNotification();

            /** wait for 60 seconds till all entries are evicted.. **/
            Awaitility.await().with().pollInterval(5, TimeUnit.SECONDS)
                .atMost(220, TimeUnit.SECONDS).until(() -> getTotalEntryCount(pr) == 0);
            System.out.println("FTableOverflowToTierTest.run :: " + "Total Entry Count: "
                + getTotalEntryCount(pr));
            assertEquals("Expected no entries.", 0, getTotalEntryCount(pr));
          } finally {
            FTableTestHelper.revokeFakeNotification();
          }

        }
      });
    });
  }

  public void forceEvictionTask(String ftable) {
    /* Force eviction task */
    MCacheFactory.getAnyInstance().getAdmin().forceFTableEviction(ftable);
    forceEvictiononServer(testBase.getVm(0), ftable);
    forceEvictiononServer(testBase.getVm(2), ftable);
    forceEvictiononServer(testBase.getVm(3), ftable);
  }

  public int getCountFromScan(String ftable) {
    int scanCount = 0;
    // for (final MResult ignored : clientCache.getFTable(getTableName()).getScanner(new MScan())) {
    // scanCount++;
    // }

    final Iterator<Row> mResultIterator =
        clientCache.getFTable(getTableName()).getScanner(new Scan()).iterator();
    while (mResultIterator.hasNext()) {
      final Row row = mResultIterator.next();
      scanCount++;

    }
    return (scanCount);
  }

  public int getCountFromWalScan(String ftable) {
    return (getCountFromWal(testBase.getVm(0), ftable) + getCountFromWal(testBase.getVm(2), ftable)
        + getCountFromWal(testBase.getVm(3), ftable));
  }


  /**
   * Create a ftable and populate records int it. Test the records are correctly inserted Then
   * restart all the servers Now the inserted records should be brought back again from persistence
   * store
   */
  @Test
  public void testRestartServeronFTableBeforeEviction() {
    createFTable();
    appendFTableData();
    /** Get the count now from region scan **/
    assertEquals("Expected no entries in memory prior to Eviction.", 452,
        getCountFromScan(getTableName()));
    testBase.restart();

    TableDescriptor tableDescriptor = MTableUtils
        .getTableDescriptor((MonarchCacheImpl) testBase.getClientCache(), getTableName());
    /** Get the count now from region get **/
    assertEquals("Incorrect count after restart.",
        ((AbstractTableDescriptor) tableDescriptor).isDiskPersistenceEnabled() ? 452 : 0,
        getCountFromScan(getTableName()));
  }


  /**
   * Create a ftable and populate records int it. Test the records are correctly inserted Then evict
   * the entries to tier 1 Then restart all the servers Now the inserted records should be not be
   * brought back again from persistence store
   */
  @Test
  public void testRestartServeronFTablePostEviction() {
    createFTable();
    appendFTableData();
    /** Get the count now from region get **/
    assertEquals("Expected no entries in memory prior to Eviction.", 452,
        getCountFromScan(getTableName()));
    /** Evict the entries **/
    forceEvictionTask(getTableName());
    /**
     * Get the count of records from in memory ftable Tier0 now there should be no records as they
     * have been evicted
     */

    assertEquals("Expected no entries in memory prior to Eviction.", 0,
        getinMemoryCount(getTableName()));

    assertEquals("Expected no entries in memory prior to Eviction.", 0,
        getinMemoryCount(getTableName()));

    /* Now restart the servers */
    testBase.restart();
    try {
      Thread.sleep(5_000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    /**
     * Again Get the count of records from in memory ftable Tier0 now there should be no records as
     * they have been evicted
     */

    TableDescriptor tableDescriptor = MTableUtils
        .getTableDescriptor((MonarchCacheImpl) testBase.getClientCache(), getTableName());

    assertEquals("Expected no entries in memory prior to Eviction.", 0,
        getinMemoryCount(getTableName()));

    /* Now run a scan and get the total entries */
    assertEquals("Expected no entries in memory prior to Eviction.",
        ((AbstractTableDescriptor) tableDescriptor).isDiskPersistenceEnabled() ? 452 : 0,
        getCountFromScan(getTableName()));

    assertEquals("Expected no entries in memory prior to Eviction.", 0,
        getinMemoryCount(getTableName()));
    /* Now run a scan and get the total entries */
    assertEquals("Expected no entries in memory prior to Eviction.",
        ((AbstractTableDescriptor) tableDescriptor).isDiskPersistenceEnabled() ? 452 : 0,
        getCountFromScan(getTableName()));
  }


  /**
   * Create a ftable and populate records int it. Configure an ORC Store for the FTable. Then evict
   * the entries from inmemory so that they overflow to ORC store Now check the memory count and
   * then scan the ORC store and check the count
   */
  @Test
  public void testFTableAppendOnORCStore() throws StoreCreateException,
      DefaultConstructorMissingException, ClassNotFoundException, InterruptedException {
    createFTable();
    // testBase.getVm(0).invoke(CREATE_ORC_STORE);
    // testBase.getVm(2).invoke(CREATE_ORC_STORE);
    // testBase.getVm(3).invoke(CREATE_ORC_STORE);
    appendFTableDataforORCStore();
    /** Get the count now from in memory through region get **/
    assertEquals("Expected all entries in memory prior to Eviction.", 452000,
        getinMemoryCount(getTableName()));
    /** Get the count now from in memory through Scan **/
    assertEquals("Number of records in scan are  not same as expected.", 452000,
        getCountFromScan(getTableName()));
    /** Evict the entries **/
    forceEvictionTask(getTableName());
    /** Get the count now from in memory through region get **/
    assertEquals("Expected no entries in memory after Eviction.", 0,
        getinMemoryCount(getTableName()));
    /** Get the count now from in memory through Scan **/

    /**
     * Get the count now from scan with 5 retries.. sometimes the count is not correctly reported
     * immediately after eviction
     */
    int i = 0;
    int scanCount = 0;
    while (i++ < 5) {
      scanCount = getCountFromScan(getTableName());
      if (scanCount == 45200) {
        break;
      }
      Thread.sleep(2_000);
    }
    assertEquals("Total entries from scan mismatch", 452000, scanCount);
  }


  /**
   * Create a ftable and populate records int it. Configure an ORC Store for the FTable. Then evict
   * the entries from inmemory so that they overflow to ORC store Now check the memory count and
   * then scan the ORC store and check the count
   */
  @Test
  public void testOverflowOnORCStorewithRestart() throws StoreCreateException,
      DefaultConstructorMissingException, ClassNotFoundException, InterruptedException {
    getServers().forEach(vm -> vm.invoke(REST_HEAP_MAX_BYTES));
    getServers().forEach(vm -> vm.invoke(GC_TASK));
    createFTable();

    // Store store0 = (Store) testBase.getVm(0).invoke(CREATE_ORC_STORE);
    // Store store2 = (Store) testBase.getVm(2).invoke(CREATE_ORC_STORE);
    // Store store3 = (Store) testBase.getVm(3).invoke(CREATE_ORC_STORE);

    appendFTableDataforORCStore();
    /** Get the count now from scan **/
    int i = 0;
    int scanCount = 0;
    while (i++ < 2) {
      scanCount = getCountFromScan(getTableName());
      if (scanCount == 45200) {
        break;
      }
    }
    assertEquals("Expected no entries in memory prior to Eviction.", 452000, scanCount);
    /** Evict the entries **/
    forceEvictionTask(getTableName());
    /**
     * Get the count of records from in memory ftable Tier0 now there should be no records as they
     * have been evicted
     */
    assertEquals("Expected no entries in memory prior to Eviction.", 0,
        getinMemoryCount(getTableName()));

    /** wait for 60 seconds till all entries are evicted.. **/
    try {
      Thread.sleep(10_000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    /* Now run a scan and get the total entries */
    // assertEquals("Expected no entries in memory prior to Eviction.", 452000,
    // getCountFromScan(getTableName()));
    scanCount = i = 0;
    while (i++ < 2) {
      scanCount = getCountFromScan(getTableName());
      if (scanCount == 45200) {
        break;
      }
    }
    assertEquals("Expected no entries in memory after Eviction.", 452000, scanCount);
    /** Now restart the servers **/
    testBase.restart();
    Thread.sleep(10_000);
    TableDescriptor tableDescriptor = MTableUtils
        .getTableDescriptor((MonarchCacheImpl) testBase.getClientCache(), getTableName());
    /* Now run a scan and get the total entries */
    assertEquals("Incorrect scan count after restart.",
        ((AbstractTableDescriptor) tableDescriptor).isDiskPersistenceEnabled() ? 452000 : 0,
        getScanCount(getTableName(), 452000));
  }


  private static final String COLUMN_NAME_PREFIX = "column_";
  private static final byte[] VALUE_BYTES = "VALUE".getBytes();

  private void ingest(final int count, final FTable table) {
    for (int i = 0; i < count; i++) {
      Record record = new Record();
      for (int colIndex = 0; colIndex < 10; colIndex++) {
        record.add(COLUMN_NAME_PREFIX + colIndex, VALUE_BYTES);
      }
      record.add(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, (long) i);
      table.append(record);
    }
  }

  private void moveToTier(final VM vm, final String tableName) {
    /** Evict the entries **/
    forceEvictiononServer(vm, getTableName());
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        StoreHandler storeHandler = cache.getStoreHandler();
        for (int parId = 0; parId < 10; parId++) {
          storeHandler.pauseWALMonitoring(tableName, parId);
          storeHandler.flushWriteAheadLog(tableName, parId);
          storeHandler.resumeWALMonitoring(tableName, parId);
        }
      }
    });
  }

  @Test
  public void testOverflowOneServerWithRestart() throws StoreCreateException,
      DefaultConstructorMissingException, ClassNotFoundException, InterruptedException {
    getServers().forEach(vm -> vm.invoke(REST_HEAP_MAX_BYTES));
    getServers().forEach(vm -> vm.invoke(GC_TASK));
    FTableDescriptor ftd = new FTableDescriptor();
    ftd.setRedundantCopies(2);
    ftd.setBlockSize(7);
    ftd.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    Schema.Builder sb = new Schema.Builder();
    for (int i = 0; i < 10; i++) {
      sb.column(COLUMN_NAME_PREFIX + i);
    }
    ftd.setSchema(sb.build());
    final String tableName = getTableName();
    FTable table = clientCache.getAdmin().createFTable(tableName, ftd);

    final FTableDescriptor td = table.getTableDescriptor();
    final int totalCount = 113 * 4 * 10;

    System.out.println("1) testOverflowOneServerWithRestart -- Ingesting: count= " + totalCount);
    ingest(totalCount, table);

    /** Get the count now from scan **/
    assertEquals("1) Incorrect scan count.", totalCount, getScanCount(tableName, totalCount));

    System.out.println("testOverflowOneServerWithRestart -- Moving data to tier on vm2.");
    moveToTier(testBase.getVm(2), tableName);
    assertEquals("2) Incorrect scan count.", totalCount, getScanCount(tableName, totalCount));

    System.out
        .println("2) testOverflowOneServerWithRestart -- Ingesting again: count= " + totalCount);
    ingest(totalCount, table);
    assertEquals("3) Incorrect scan count.", totalCount * 2,
        getScanCount(tableName, totalCount * 2));

    Thread.sleep(5_000);
    System.out.println("testOverflowOneServerWithRestart -- Moving data to tier on vm0.");
    moveToTier(testBase.getVm(0), tableName);
    assertEquals("4) Incorrect scan count.", totalCount * 2, getScanCount(tableName, totalCount));

    System.out
        .println("3) testOverflowOneServerWithRestart -- Ingesting again: count= " + totalCount);
    ingest(totalCount, table);
    assertEquals("Before restart: Incorrect scan count.", totalCount * 3,
        getScanCount(tableName, totalCount));

    final VM[] vms = testBase.getServers().toArray(new VM[0]);
    assertEquals("Before restart: Incorrect primary count.", totalCount * 3,
        getCount(tableName, vms, true));
    assertEquals("Before restart: Incorrect secondary count.", totalCount * 3 * 2,
        getCount(tableName, vms, false));

    /** Now restart the servers **/
    testBase.restart();

    Thread.sleep(10_000);

    if (td.isDiskPersistenceEnabled()) {
      assertEquals("After restart: Incorrect scan count.", totalCount * 3,
          getScanCount(tableName, totalCount * 3));
      assertEquals("After restart: Incorrect primary count.", totalCount * 3,
          getCount(tableName, vms, true));
      assertEquals("After restart: Incorrect secondary count.", totalCount * 3 * 2,
          getCount(tableName, vms, false));
    } else {
      assertEquals("Incorrect scan count after restart.", 0, getScanCount(getTableName(), 0));
    }
  }

  @SuppressWarnings("unchecked")
  long getCount(final String name, final VM[] vms, final boolean isPrimary) {
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

  /**
   * Create a ftable and populate records int it. Overflow to WAL. Memory count should be 0 but wal
   * count should return the entries Now check the memory count and then scan the ORC store and
   * check the count
   */
  @Test
  public void testEvictToWal()
      throws StoreCreateException, DefaultConstructorMissingException, ClassNotFoundException {
    createFTable();
    appendFTableData();
    /** Get the count now from scan **/
    assertEquals("Expected no entries in memory prior to Eviction.", 452,
        getCountFromScan(getTableName()));
    /** Evict the entries **/
    forceEvictionTask(getTableName());
    /**
     * Get the count of records from in memory ftable Tier0 now there should be no records as they
     * have been evicted
     */
    assertEquals("Expected no entries in memory prior to Eviction.", 0,
        getinMemoryCount(getTableName()));
    /** Get the count now from scan **/
    assertEquals("Expected no entries in memory prior to Eviction.", 452,
        getCountFromScan(getTableName()));
    assertEquals("EvictionCount should match number of evicted entries.", 452,
        getTotalEvictedCount(getTableName()));
  }

  private long getTotalEvictedCount(final String tableName) {
    long totalCount = 0;
    for (VM vm : testBase.getServers()) {
      totalCount += vm.invoke(() -> {
        final Region<Object, Object> region = MCacheFactory.getAnyInstance().getRegion(tableName);
        assertNotNull(region);
        long count = 0;
        for (BucketRegion br : ((PartitionedRegion) region).getDataStore()
            .getAllLocalBucketRegions()) {
          count += br.getEvictions();
        }
        return count;
      });
    }
    return totalCount;
  }

  private void disableORCStore() {
    Integer[] vms = {0, 2, 3};
    for (Integer vmIdx : vms) {
      testBase.getVm(vmIdx).invoke(new SerializableCallable() {
        public Object call() {
          /* Force creation of store instance to avoid adding of ORC store later. */
          System.out.println(StoreHandler.getInstance().toString());
          StoreHierarchy.getInstance().removeStore("ORC");
          return null;
        }
      });
    }
  }

  private void enableORCStore() {
    Integer[] vms = {0, 2, 3};
    for (Integer vmIdx : vms) {
      testBase.getVm(vmIdx).invoke(new SerializableCallable() {
        public Object call() {
          /* Force creation of store instance to avoid adding of ORC store later. */
          System.out.println(StoreHandler.getInstance().toString());
          StoreHierarchy.getInstance().addStore("ORC");
          return null;
        }
      });
    }
  }

  /**
   * Create a ftable and populate records int it. Set eviction to 1%. Evict after every 1000 records
   * and check all the counters WAL memory and store
   */
  @Test
  public void testWAL4520Eviction()
      throws StoreCreateException, DefaultConstructorMissingException, ClassNotFoundException {
    // disableORCStore();
    this.setEVICT_HEAP_PCT(1);
    appendFTableforWAL();
    // enableORCStore();
  }


  public void runDeleteBucketOps(String tableName) {
    testBase.getVm(0).invoke(new SerializableCallable() {
      public Object call() throws IOException {
        TierStore store = StoreHandler.getInstance().getTierStore(tableName, 1);
        final TableDescriptor tableDescriptor =
            MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
        final StoreRecord storeRecord =
            new StoreRecord(tableDescriptor.getAllColumnDescriptors().size());
        tableDescriptor.getAllColumnDescriptors().forEach((MCD) -> {
          if (MCD.getColumnNameAsString().equals(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) {
            storeRecord.addValue(1l);
          } else {
            storeRecord.addValue(new byte[3]);
          }
        });
        for (int i = 0; i < 201; i++) {
          store.getWriter(TABLE_NAME, 0).write(new Properties(), storeRecord);
          store.getWriter(TABLE_NAME, 1).write(new Properties(), storeRecord);
          store.getWriter(TABLE_NAME, 2).write(new Properties(), storeRecord);
        }
        store.deletePartition(tableName, 1);
        store.deletePartition(tableName, 2);
        return null;
      }
    });
  }

  @Test
  public void testStoreBucketDelete() {
    // enableORCStore();
    createFTable();
    runDeleteBucketOps(getTableName());
  }

  /**
   * Make sure that the table can be re-created with same name (create after delete) without any
   * issues. Assert on the scan count across tiers.
   */
  @Test
  public void testRecreateTableWithSameName() throws InterruptedException {
    getServers().forEach(vm -> vm.invoke(REST_HEAP_MAX_BYTES));
    getServers().forEach(vm -> vm.invoke(GC_TASK));
    createFTable();
    appendFTableDataforORCStore();

    /** Evict the entries and the assert on the scan count **/
    forceEvictionTask(getTableName());
    Thread.sleep(5_000);
    assertEquals("Total entries from scan mismatch", 452000, getScanCount(getTableName(), 45200));

    clientCache.getAdmin().deleteFTable(getTableName());
    getServers().forEach(vm -> vm.invoke(REST_HEAP_MAX_BYTES));
    getServers().forEach(vm -> vm.invoke(GC_TASK));
    Thread.sleep(10_000);

    createFTable();
    assertEquals("After re-create no entries should be there.", 0, getScanCount(getTableName(), 0));
    appendFTableDataforORCStore();

    /** Evict the entries and the assert on the scan count **/
    forceEvictionTask(getTableName());
    assertEquals("Total entries from scan mismatch", 452000, getScanCount(getTableName(), 45200));
  }

  /**
   * Get the count now from scan with 2 retries.. sometimes the count is not correctly reported
   * immediately after eviction hence retry if the expected count is not returned in the first run.
   *
   * @param tableName the table name
   * @param expectedCount the expected count
   * @return the total number of rows in the table
   */
  public int getScanCount(final String tableName, final int expectedCount) {
    int i = 0;
    int scanCount = 0;
    while (i++ < 4) {
      scanCount = getCountFromScan(tableName);
      if (scanCount == expectedCount) {
        break;
      }
    }
    return scanCount;
  }

  public List<VM> getServers() {
    return testBase.getServers();
  }

  /**
   * A simple wrapper class to hold the required arguments..
   */
  private static final class TierArgs implements Serializable {
    final String prefix = "/tmp/stores/";
    final String[] names;
    final String[] locations;
    final int totalDataCopies = 2; // this includes primary and secondary buckets.
    int expectedCount = 100;
    final int totalSplitCount = 10;

    public TierArgs(final String[] tierNames) {
      this.names = tierNames;
      this.locations = new String[tierNames.length];
      for (int i = 0; i < this.names.length; i++) {
        this.locations[i] = prefix + this.names[i];
      }
    }
  }

  private TierArgs tierArgs;

  /**
   * A simple test with two tiers. Tier-1 and Tier-2 are both local-disk with ORC format but
   * configured, for testing purpose, in a hierarchical way such that data from Tier-1 moves to
   * Tier-2 after the specified expiry interval. For tests the expiry intervals are set manually
   * (via test hook) to trigger the expiry immediately.
   *
   * @throws InterruptedException in case the thread was interrupted
   * @throws IOException in case there were errors while doing I/O
   */
  @Test
  public void testMultipleStores() throws InterruptedException, IOException {
    this.tierArgs = new TierArgs(new String[] {"tier_1", "tier_2"});

    /* create the required stores on all servers and also setup the respective evictor threads */
    initializeTierStores(tierArgs);

    /* create the FTable with the provided configuration and ingest dummy data into it */
    createTableAndAppendData(tierArgs);

    /* assert that all data is in in-memory tier (tier-0) and tier-1/2 have 0 records */
    assertScanCount("BeforeEviction: Data In Tier-0", tierArgs, tierArgs.expectedCount, 0, 0);

    /* evict from in-memory tier to tier-1 and then wait */
    forceEvictionTask(getTableName());
    Thread.sleep(10_000);
    forceWalFlush(getTableName(), tierArgs);

    /* assert that total scan count is fine and all data is in tier-1 */
    assertScanCount("AfterEviction: Data In Tier-1", tierArgs, tierArgs.expectedCount,
        tierArgs.expectedCount, 0);

    /* force expiration/eviction from tier-1 to tier-2 */
    setupTierEviction(new long[] {0L, 3_000L}, tierArgs);
    Thread.sleep(10_000);

    /* assert that all data is in tier-2 */
    assertScanCount("AfterEviction: Data In Tier-2", tierArgs, tierArgs.expectedCount, 0,
        tierArgs.expectedCount);

    /* force expiration on tier-2 so that it deletes the data from that tier */
    setupTierEviction(new long[] {0L, 0L}, tierArgs);
    Thread.sleep(10_000);

    /* since data is deleted.. neither full table scan nor any tier should have any data */
    assertScanCount("AfterExpiration: Data Deleted From Tier-2", tierArgs, 0, 0, 0);
  }

  /**
   * A simple test with two tiers. Tier-1 and Tier-2 are both local-disk with ORC format but
   * configured, for testing purpose, in a hierarchical way such that data from Tier-1 moves to
   * Tier-2 after the specified expiry interval and data from Tier-2 is expired after configured
   * time.
   *
   * Reference : GEN-2003
   *
   * @throws InterruptedException in case the thread was interrupted
   * @throws IOException in case there were errors while doing I/O
   */
  @Test
  public void testMultipleStoresAfterExpiryScan() throws InterruptedException, IOException {
    TierArgs tierArgs1 = new TierArgs(new String[] {"tier_1", "tier_2"});

    /* create the required stores on all servers and also setup the respective evictor threads */
    final String STORE_CLASS = "io.ampool.tierstore.stores.LocalTierStore";
    final String ORC_READER_CLASS = "io.ampool.tierstore.readers.orc.TierStoreORCReader";
    final String ORC_WRITER_CLASS = "io.ampool.tierstore.writers.orc.TierStoreORCWriter";

    for (final String location : tierArgs1.locations) {
      new File(location).mkdirs();
    }

    getServers().forEach(vm -> vm.invoke(new SerializableRunnable("vm" + vm.getPid()) {
      @Override
      public void run() throws Exception {
        final Properties empty = new Properties();
        TierStoreReader reader = new TierStoreReaderFactory().create(ORC_READER_CLASS, empty);
        TierStoreWriter writer = new TierStoreWriterFactory().create(ORC_WRITER_CLASS, empty);
        for (int i = 0; i < tierArgs1.names.length; i++) {
          final Properties p = new Properties();
          p.setProperty("base.dir.path", tierArgs1.locations[i] + "/" + this.getName());
          TierStore ts = new TierStoreFactory().create(STORE_CLASS, tierArgs1.names[i], p, writer,
              reader, (MonarchCacheImpl) MCacheFactory.getAnyInstance());
          StoreHandler.getInstance().registerStore(tierArgs1.names[i], ts);
        }

        final Map<String, Long> TIER_EVICT_INTERVAL = new HashMap<>(tierArgs1.names.length);
        for (final String name : tierArgs1.names) {
          TIER_EVICT_INTERVAL.put(name, 3_000L);
        }

        ReflectionUtils.setStaticFieldValue(
            Class.forName("io.ampool.tierstore.internal.TierEvictorThread"), "fixedWaitTimeSecs",
            1);
        ReflectionUtils.setStaticFieldValue(
            Class.forName("io.ampool.tierstore.internal.TierEvictorThread"),
            "TEST_TIER_EVICT_INTERVAL", TIER_EVICT_INTERVAL);
        ReflectionUtils.setStaticFieldValue(
            Class.forName("io.ampool.tierstore.internal.TierEvictorThread"), "TEST_EVICT", true);
      }
    }));

    /* create the FTable with the provided configuration and ingest dummy data into it */
    FTableDescriptor ftd = new FTableDescriptor();
    ftd.setTotalNumOfSplits(tierArgs1.totalSplitCount);
    ftd.setRedundantCopies(tierArgs1.totalDataCopies - 1);
    ftd.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    TypeUtils.addColumnsFromJson(SCHEMA, ftd);

    Properties props = new Properties();
    props.setProperty(TierStoreConfiguration.TIER_TIME_TO_EXPIRE, "0.0166667");
    props.setProperty(TierStoreConfiguration.TIER_PARTITION_INTERVAL, "0.0166667");

    TierStoreConfiguration tierStoreConfiguration = new TierStoreConfiguration();
    tierStoreConfiguration.setTierProperties(props);

    LinkedHashMap<String, TierStoreConfiguration> tiers = new LinkedHashMap<>();
    for (final String name : tierArgs1.names) {
      tiers.put(name, tierStoreConfiguration);
    }
    ftd.addTierStores(tiers);
    assertEquals(tiers.size(), ftd.getTierStores().size());

    FTable table = clientCache.getAdmin().createFTable(getTableName(), ftd);

    final List<MColumnDescriptor> cds = ftd.getAllColumnDescriptors();

    // append data
    for (int i = 0; i < tierArgs1.expectedCount; i++) {
      Record record = new Record();
      for (MColumnDescriptor cd : cds) {
        record.add(cd.getColumnNameAsString(), TypeUtils.getRandomValue(cd.getColumnType()));
      }
      table.append(record);
    }

    /* assert that all data is in in-memory tier (tier-0) and tier-1/2 have 0 records */
    assertScanCount("BeforeEviction: Data In Tier-0", tierArgs1, tierArgs1.expectedCount, 0, 0);

    /* evict from in-memory tier to tier-1 and then wait */
    forceEvictionTask(getTableName());
    Thread.sleep(10_000);
    forceWalFlush(getTableName(), tierArgs1);

    /* assert that total scan count is fine and all data is in tier-1 */
    assertScanCount("AfterEviction: Data In Tier-1", tierArgs1, tierArgs1.expectedCount,
        tierArgs1.expectedCount, 0);

    /* force expiration/eviction from tier-1 to tier-2 */
    setupTierEviction(new long[] {0L, 3_000L}, tierArgs1);
    Thread.sleep(10_000);

    /* assert that all data is in tier-2 */
    assertScanCount("AfterEviction: Data In Tier-2", tierArgs1, tierArgs1.expectedCount, 0,
        tierArgs1.expectedCount);

    /* force expiration on tier-2 so that it deletes the data from that tier */
    setupTierEviction(new long[] {0L, 0L}, tierArgs1);
    Thread.sleep(10_000);

    /* since data is deleted.. neither full table scan nor any tier should have any data */
    assertScanCount("AfterExpiration: Data Deleted From Tier-2", tierArgs1, 0, 0, 0);

    String tableName = getTableName();
    List<Integer> counts = new ArrayList<>();

    // Situation similar to GEN-2003 where mash scan is called.
    // server side scan to simulate mash

    SerializableCallableIF<Integer> scanCount = () -> {
      MCache serverCache = MCacheFactory.getAnyInstance();
      FTable fTable = serverCache.getFTable(tableName);
      int scanRecCount = 0;
      final Iterator<Row> mResultIterator = fTable.getScanner(new Scan()).iterator();
      while (mResultIterator.hasNext()) {
        final Row row = mResultIterator.next();
        scanRecCount++;
      }
      return scanRecCount;
    };

    getServers().forEach(vm -> {
      Integer count = vm.invoke(scanCount);
      counts.add(count);
    });

    // aggregated count must be zero
    Integer count = counts.stream().reduce(new Integer(0), (old, current) -> old + current);
    assertEquals(new Integer(0), count);
  }

  /**
   * A simple test with five tiers. All tiers are local-disk with ORC format but configured, for
   * testing purpose, in a hierarchical way such that data from one tier moves to next after the
   * specified expiry interval. For tests the expiry intervals are set manually (via test hook) to
   * trigger the expiry immediately.
   *
   * @throws InterruptedException in case the thread was interrupted
   * @throws IOException in case there were errors while doing I/O
   */
  @Test
  public void testMultipleStores_Five() throws InterruptedException, IOException {
    final String[] tierNames = {"tier_1", "tier_2", "tier_3", "tier_4", "tier_5"};
    this.tierArgs = new TierArgs(tierNames);
    final int ec = this.tierArgs.expectedCount;
    final long[][] tierCounts = new long[][] {new long[] {0L, 0L, 0L, 0L, 0L},
        new long[] {ec, 0L, 0L, 0L, 0L}, new long[] {0L, ec, 0L, 0L, 0L},
        new long[] {0L, 0L, ec, 0L, 0L}, new long[] {0L, 0L, 0L, ec, 0L},
        new long[] {0L, 0L, 0L, 0L, ec}, new long[] {0L, 0L, 0L, 0L, 0},};

    final long et = 3_000L;
    final long[][] expiryTime = new long[][] {new long[] {0L, et, 0L, 0L, 0L},
        new long[] {0L, 0L, et, 0L, 0L}, new long[] {0L, 0L, 0L, et, 0L},
        new long[] {0L, 0L, 0L, 0L, et}, new long[] {0L, 0L, 0L, 0L, 0L},};

    /* create the required stores on all servers and also setup the respective evictor threads */
    initializeTierStores(tierArgs);

    /* create the FTable with the provided configuration and ingest dummy data into it */
    createTableAndAppendData(tierArgs);

    /* assert that all data is in in-memory tier (tier-0) and tier-1/2 have 0 records */
    assertScanCount("BeforeEviction: Data In Tier-0", tierArgs, tierCounts[0]);

    /* evict from in-memory tier to tier-1 and then wait */
    forceEvictionTask(getTableName());
    Thread.sleep(10_000);
    forceWalFlush(getTableName(), tierArgs);

    String msg;
    /* assert that total scan count is fine and all data is in respective tier */
    for (int i = 0; i < tierArgs.names.length; i++) {
      assertScanCount("AfterEviction: Data In Tier: " + tierArgs.names[i], tierArgs,
          tierCounts[i + 1]);
      /* evict data from this tier to next tier, if any, and then wait for some time */
      setupTierEviction(expiryTime[i], tierArgs);
      Thread.sleep(10_000);
    }

    tierArgs.expectedCount = 0;
    /* since data is deleted.. neither full table scan nor any tier should have any data */
    assertScanCount("AfterExpiration: Data Deleted From Last Tier.", tierArgs,
        tierCounts[tierCounts.length - 1]);
  }

  private void deRegisterStores(final TierArgs tierArgs) {
    for (VM vm : getServers()) {
      vm.invoke(new SerializableRunnable() {
        @Override
        public void run() throws Exception {
          for (final String name : tierArgs.names) {
            StoreHandler.getInstance().deRegisterStore(name);
          }
        }
      });
    }
  }

  private void forceWalFlush(final String tableName, final TierArgs tierArgs) {
    for (VM vm : getServers()) {
      vm.invoke(new SerializableRunnable() {
        @Override
        public void run() throws Exception {
          MCache cache = MCacheFactory.getAnyInstance();
          StoreHandler storeHandler = cache.getStoreHandler();
          for (int parId = 0; parId < tierArgs.totalSplitCount; parId++) {
            storeHandler.pauseWALMonitoring(tableName, parId);
            storeHandler.flushWriteAheadLog(tableName, parId);
            storeHandler.resumeWALMonitoring(tableName, parId);
          }
        }
      });
    }
  }

  public void actOnWal(final String tableName, final int bucketCount, final int action) {
    for (VM vm : getServers()) {
      vm.invoke(new SerializableRunnable() {
        @Override
        public void run() throws Exception {
          MCache cache = MCacheFactory.getAnyInstance();
          StoreHandler storeHandler = cache.getStoreHandler();
          for (int parId = 0; parId < bucketCount; parId++) {
            storeHandler.pauseWALMonitoring(tableName, parId);
          }
        }
      });
    }
  }

  /**
   * Helper method to modify the test hook for the tier-evictor threads.
   *
   * @param expiry tier expiry intervals in seconds of each tier by index
   * @param tierArgs the common tier arguments
   */
  private void setupTierEviction(final long[] expiry, final TierArgs tierArgs) {
    final Map<String, Long> TIER_EVICT_INTERVAL = new HashMap<>(tierArgs.names.length);
    for (int i = 0; i < tierArgs.names.length; i++) {
      TIER_EVICT_INTERVAL.put(tierArgs.names[i], expiry[i]);
    }
    getServers().forEach(vm -> vm.invoke(new SerializableRunnable("vm" + vm.getPid()) {
      @Override
      public void run() throws Exception {
        ReflectionUtils.setStaticFieldValue(
            Class.forName("io.ampool.tierstore.internal.TierEvictorThread"),
            "TEST_TIER_EVICT_INTERVAL", TIER_EVICT_INTERVAL);
      }
    }));
  }

  private void assertScanCount(final String msg, final TierArgs tierArgs,
      final int expectedTotalScanCount, final int expectedTier1Count, final int expectedTier2Count)
      throws IOException {
    int scanCount = getScanCount(getTableName(), expectedTotalScanCount);
    assertEquals(msg + ": Incorrect scan count across all tiers.", expectedTotalScanCount,
        scanCount);
    long tier1Count =
        getCountInTier(tierArgs.locations[0], getTableName(), tierArgs.totalDataCopies);
    assertEquals(msg + ": Incorrect scan count from tier-1.", expectedTier1Count, tier1Count);
    long tier2Count =
        getCountInTier(tierArgs.locations[1], getTableName(), tierArgs.totalDataCopies);
    assertEquals(msg + ": Incorrect scan count from tier-2.", expectedTier2Count, tier2Count);
  }

  private void assertScanCount(final String msg, final TierArgs tierArgs,
      final long[] expectedTierCount) throws IOException {
    int scanCount = getScanCount(getTableName(), tierArgs.expectedCount);
    assertEquals(msg + ": Incorrect scan count across all tiers.", tierArgs.expectedCount,
        scanCount);
    for (int i = 0; i < tierArgs.names.length; i++) {
      long tierCount =
          getCountInTier(tierArgs.locations[i], getTableName(), tierArgs.totalDataCopies);
      assertEquals(msg + ": Incorrect scan count from " + tierArgs.names[i], expectedTierCount[i],
          tierCount);
    }
  }

  private static final String SCHEMA = "{'c1':'INT','c2':'array<LONG>','c3':'STRING'}";

  /**
   * Create the FTable with the provided configuration and then insert some dummy data to it.
   *
   * @param tierArgs the common tier arguments
   */
  private void createTableAndAppendData(final TierArgs tierArgs) {
    FTableDescriptor ftd = new FTableDescriptor();
    ftd.setTotalNumOfSplits(tierArgs.totalSplitCount);
    ftd.setRedundantCopies(tierArgs.totalDataCopies - 1);
    ftd.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    TypeUtils.addColumnsFromJson(SCHEMA, ftd);

    LinkedHashMap<String, TierStoreConfiguration> tiers = new LinkedHashMap<>();
    for (final String name : tierArgs.names) {
      tiers.put(name, new TierStoreConfiguration());
    }
    ftd.addTierStores(tiers);
    assertEquals(tiers.size(), ftd.getTierStores().size());

    FTable table = clientCache.getAdmin().createFTable(getTableName(), ftd);

    final List<MColumnDescriptor> cds = ftd.getAllColumnDescriptors();

    for (int i = 0; i < tierArgs.expectedCount; i++) {
      Record record = new Record();
      for (MColumnDescriptor cd : cds) {
        record.add(cd.getColumnNameAsString(), TypeUtils.getRandomValue(cd.getColumnType()));
      }
      table.append(record);
    }
  }

  /**
   * Create and initialize the tiers; two tiers are created that are both local-disk using ORC
   * format. Both tiers store the data at independent locations in server specific directory.
   *
   * @param tierArgs the common tier arguments
   */
  private void initializeTierStores(final TierArgs tierArgs) {
    final String STORE_CLASS = "io.ampool.tierstore.stores.LocalTierStore";
    final String ORC_READER_CLASS = "io.ampool.tierstore.readers.orc.TierStoreORCReader";
    final String ORC_WRITER_CLASS = "io.ampool.tierstore.writers.orc.TierStoreORCWriter";

    for (final String location : tierArgs.locations) {
      new File(location).mkdirs();
    }

    /* use the test hook to setup dummy expiry intervals for all tiers. */
    final Map<String, Long> TIER_EVICT_INTERVAL = new HashMap<>(tierArgs.names.length);
    for (final String name : tierArgs.names) {
      TIER_EVICT_INTERVAL.put(name, 3_000L);
    }

    getServers().forEach(vm -> vm.invoke(new SerializableRunnable("vm" + vm.getPid()) {
      @Override
      public void run() throws Exception {
        final Properties empty = new Properties();
        TierStoreReader reader = new TierStoreReaderFactory().create(ORC_READER_CLASS, empty);
        TierStoreWriter writer = new TierStoreWriterFactory().create(ORC_WRITER_CLASS, empty);
        for (int i = 0; i < tierArgs.names.length; i++) {
          final Properties p = new Properties();
          p.setProperty("base.dir.path", tierArgs.locations[i] + "/" + this.getName());
          TierStore ts = new TierStoreFactory().create(STORE_CLASS, tierArgs.names[i], p, writer,
              reader, (MonarchCacheImpl) MCacheFactory.getAnyInstance());
          StoreHandler.getInstance().registerStore(tierArgs.names[i], ts);
        }

        /* enable TEST_EVICT testing hook */
        ReflectionUtils.setStaticFieldValue(
            Class.forName("io.ampool.tierstore.internal.TierEvictorThread"), "fixedWaitTimeSecs",
            1);
        ReflectionUtils.setStaticFieldValue(
            Class.forName("io.ampool.tierstore.internal.TierEvictorThread"),
            "TEST_TIER_EVICT_INTERVAL", TIER_EVICT_INTERVAL);
        ReflectionUtils.setStaticFieldValue(
            Class.forName("io.ampool.tierstore.internal.TierEvictorThread"), "TEST_EVICT", true);
      }
    }));
  }

  /**
   * Get the total count of records for the specified table from the provided location. It creates a
   * local-disk ORC reader and reads all the ORC files from the provided location. Since it reads
   * all data from the provided location it includes primary and secondary count in it; this needs
   * to be corrected factored-in for the correct single copy count.
   *
   * @param location the base location to read data from
   * @param tableName the table name
   * @param totalDataCopies the total number of data copies read; including primary and secondary
   * @return the number of records in the tier per data copy
   * @throws IOException in case there were errors reading data
   */
  private long getCountInTier(final String location, final String tableName,
      final int totalDataCopies) throws IOException {
    long totalCount = 0;
    for (VM vm : getServers()) {
      Properties props = new Properties();
      props.setProperty("base.uri", location + "/vm" + vm.getPid());
      props.setProperty("base.dir.path", location + "/vm" + vm.getPid());
      TierStore lts = null;
      final String ORC_READER_CLASS = "io.ampool.tierstore.readers.orc.TierStoreORCReader";
      final String ORC_WRITER_CLASS = "io.ampool.tierstore.writers.orc.TierStoreORCWriter";
      TierStoreReader reader = null;
      TierStoreWriter writer = null;
      try {
        final Constructor<?> c = StoreUtils
            .getTierStoreConstructor(Class.forName("io.ampool.tierstore.stores.LocalTierStore"));
        lts = (TierStore) c.newInstance("local-disk-tier");
        reader = (TierStoreReader) Class.forName(ORC_READER_CLASS).newInstance();
        writer = (TierStoreWriter) Class.forName(ORC_WRITER_CLASS).newInstance();
      } catch (ClassNotFoundException | IllegalAccessException | InstantiationException
          | DefaultConstructorMissingException | InvocationTargetException e) {
        e.printStackTrace();
        throw new IOException("Error.." + e.getMessage());
      }
      ReflectionUtils.setFieldValue(lts, "reader", reader);
      ReflectionUtils.setFieldValue(lts, "writer", writer);
      ReflectionUtils.setFieldValue(lts, "storeProperties", props);
      ReflectionUtils.setFieldValue(lts, "fileFormat", FileFormats.ORC);

      final TierStoreReader reader1 = lts.getReader(props, tableName);
      reader1.setConverterDescriptor(lts.getConverterDescriptor(tableName));
      final Iterator<StoreRecord> iterator = reader1.iterator();
      while (iterator.hasNext()) {
        StoreRecord sr = iterator.next();
        if (sr == null) {
          break;
        }
        totalCount++;
      }
    }
    return totalCount / totalDataCopies;
  }
}
