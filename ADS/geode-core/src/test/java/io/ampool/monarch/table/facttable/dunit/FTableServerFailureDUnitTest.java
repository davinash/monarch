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

import com.jayway.awaitility.Awaitility;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCache;
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
import io.ampool.monarch.table.ftable.internal.FTableScanner;
import io.ampool.monarch.table.internal.InternalTable;
import io.ampool.monarch.table.region.ScanContext;
import io.ampool.monarch.types.TypeUtils;
import org.apache.geode.internal.cache.*;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.store.StoreHandler;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Category(FTableTest.class)
public class FTableServerFailureDUnitTest extends MTableDUnitHelper {
  private static int DEFAULT_NUM_COLUMNS = 1;
  private static String DEFAULT_COL_PREFIX = "COL";
  private static String DEFAULT_COLVAL_PREFIX = "VAL";
  private final AtomicLong recordsCounter = new AtomicLong(0);
  private List<AppendThread> appendThreads = new ArrayList<AppendThread>();
  private static float EVICT_HEAP_PCT = 50.9f;
  private static MCache cacheBeforeReconnect = null;
  private static MCache cacheAfterReconnect = null;

  @Parameterized.Parameter
  public static FTableDescriptor.BlockFormat blockFormat;

  @Parameterized.Parameters(name = "BlockFormat__{0}")
  public static Collection<FTableDescriptor.BlockFormat> data() {
    return Arrays.asList(FTableDescriptor.BlockFormat.values());
  }

  /**
   * Return the method-name. In case of Parameterized tests, the parameters are appended to the
   * method-name (like testMethod[0]) and hence need to be stripped off.
   *
   * @return the test method name
   */
  public static String getMethodName() {
    return getTestMethodName().replaceAll("\\W+", "_");
  }

  public static float getEVICT_HEAP_PCT() {
    return EVICT_HEAP_PCT;
  }

  public Object startServerOnWithForceDisconnect(VM vm, final String locators) {
    System.out.println("Starting server on VM: " + vm.toString());
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return startServerWithForceDisconnect(locators);
      }
    });
  }

  public Object startServerWithForceDisconnect(final String locators) throws IOException {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
    props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
    props.setProperty(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME, "false");
    props.setProperty(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "true");
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
    MCache c = null;
    try {
      c = MCacheFactory.getAnyInstance();
      c.close();
    } catch (CacheClosedException cce) {
    }
    c = MCacheFactory.create(getSystem(props));
    CacheServer s = c.addCacheServer();
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    s.setPort(port);
    s.start();
    MCacheFactory.getAnyInstance();
    // registerFunction();
    return port;
  }


  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    startServerOnWithForceDisconnect(vm0, DUnitLauncher.getLocatorString());
    startServerOnWithForceDisconnect(vm1, DUnitLauncher.getLocatorString());
    createClientCache();
  }


  @Override
  public void tearDown2() throws Exception {
    deleteFTable(getMethodName());
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
    tableDescriptor.setBlockFormat(blockFormat);
    tableDescriptor.setBlockSize(123);
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
      // try {
      // if (i%100 == 0)
      // Thread.sleep(1_000);
      // } catch (InterruptedException e) {
      // e.printStackTrace();
      // }
      recordsCounter.getAndIncrement();
    }
  }

  private void doAppend(final String fTableName, final long numRecords, final int seed) {
    final FTable fTable = MClientCacheFactory.getAnyInstance().getFTable(fTableName);
    try {
      for (int i = seed; i < numRecords + seed; i++) {
        Record record = new Record();
        for (int j = 0; j < DEFAULT_NUM_COLUMNS; j++) {
          record.add(DEFAULT_COL_PREFIX + j,
              Bytes.toBytes(DEFAULT_COLVAL_PREFIX + i + "_" + Thread.currentThread().getId()));
        }
        fTable.append(record);
        recordsCounter.getAndIncrement();
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }

  }

  private void doParallelAppend(final String fTableName, final int numThreads) {
    for (int i = 0; i < numThreads; i++) {
      AppendThread t = new AppendThread(fTableName);
      appendThreads.add(t);
      t.start();
    }
  }

  private void stopAppendThreads() {
    appendThreads.forEach((t) -> {
      t.stopThread();
    });
    appendThreads.forEach((t) -> {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
  }


  private void runScan(final FTable fTable, final long expectedRecords) {
    final Iterator<Row> iterator = fTable.getScanner(new Scan()).iterator();
    int actualRecods = 0;
    while (iterator.hasNext()) {
      final Row row = iterator.next();
      final Iterator<Cell> mCellIterator = row.getCells().iterator();
      final Cell cell = mCellIterator.next();
      // System.out.println("; Column Value " + Bytes.toString((byte[]) cell.getColumnValue()));
      actualRecods++;
    }
    assertEquals(expectedRecords, actualRecods);
  }

  private void deleteFTable(final String ftableName) {
    try {
      MClientCacheFactory.getAnyInstance().getAdmin().deleteFTable(ftableName);
    } catch (Exception e) {
      // e.printStackTrace();
    }
  }

  class AppendThread extends Thread implements Serializable {
    private volatile boolean stopThread = false;
    private final String fTableName;

    AppendThread(final String fTable) {
      this.fTableName = fTable;
    }


    private void stopThread() {
      stopThread = true;
    }

    @Override
    public void run() {
      int seed = 0;
      while (!stopThread) {
        doAppend(fTableName, 10, seed);
        seed += 10;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
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

  private void forceDisconnect(VM vm) {
    System.out.println("Before ForcedDisconnect -> Crash a node in Distributed System");
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        cacheBeforeReconnect = MCacheFactory.getAnyInstance();
        InternalDistributedSystem distributedSystem =
            (InternalDistributedSystem) cacheBeforeReconnect.getDistributedSystem();
        assertNotNull(distributedSystem);
        MembershipManagerHelper.crashDistributedSystem(distributedSystem);
        return null;
      }
    });
  }

  private void reconnect(VM vm) {
    System.out.println("After ForcedDisconnect -> Reconnect the MCache");
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        cacheAfterReconnect = cacheBeforeReconnect.getReconnectedCache();
        return null;
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


  @Test
  public void testServerRestart() {
    final String fTableName = getMethodName();
    final FTable fTable = createFTable(fTableName, 1, 1);
    final int numRecords = 4_000;
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords);
    stopServerOn(vm1);
    runScan(fTable, numRecords);
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    runScan(fTable, numRecords);
  }

  @Test
  public void testServerRestartWithBucketsSync() {
    cacheBeforeReconnect = null;
    cacheAfterReconnect = null;
    final String fTableName = getMethodName();
    final FTable fTable = createFTable(fTableName, 1, 1);
    final int numRecords = 4_000;
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords);
    stopServerOn(vm1);
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords * 2);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    runScan(fTable, numRecords * 2);
  }


  @Test
  public void testServerRestartWithBucketsSyncWithOverflow() {
    cacheBeforeReconnect = null;
    cacheAfterReconnect = null;
    final String fTableName = getMethodName();
    final FTable fTable = createFTable(fTableName, 1, 1);
    final int numRecords = 400;
    final int expectedRecords = 800;
    final int sleepIntervalMillis = 10_000;
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords);
    forceEvictiononServer(vm0, fTableName);
    forceEvictiononServer(vm1, fTableName);
    pauseWalMontoring(vm0, fTableName);
    pauseWalMontoring(vm1, fTableName);
    flushWAL(vm0, fTableName);
    flushWAL(vm1, fTableName);
    stopServerOn(vm1);
    doAppend(fTableName, numRecords);
    runScan(fTable, expectedRecords);
    try {
      Thread.sleep(sleepIntervalMillis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    forceEvictiononServer(vm0, fTableName);
    try {
      Thread.sleep(sleepIntervalMillis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    try {
      Thread.sleep(sleepIntervalMillis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    stopServerOn(vm0);
    try {
      Thread.sleep(sleepIntervalMillis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    runScan(fTable, expectedRecords);
  }

  @Test
  public void testForcedDisconnectWithBucketsSync() {
    cacheBeforeReconnect = null;
    cacheAfterReconnect = null;
    final String fTableName = getMethodName();
    final FTable fTable = createFTable(fTableName, 1, 1);
    final int numRecords = 4_000;
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords);
    forceDisconnect(vm1);
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords * 2);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    forceEvictiononServer(vm0, fTableName);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    reconnect(this.vm1);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    runScan(fTable, numRecords * 2);
  }



  @Test
  public void testForcedDisconnectWithBucketsSyncWithOverflow() {
    cacheBeforeReconnect = null;
    cacheAfterReconnect = null;
    final String fTableName = getMethodName();
    final FTable fTable = createFTable(fTableName, 1, 1);
    final int numRecords = 4_000;
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords);
    forceEvictiononServer(vm0, fTableName);
    forceEvictiononServer(vm1, fTableName);
    pauseWalMontoring(vm0, fTableName);
    pauseWalMontoring(vm1, fTableName);
    flushWAL(vm0, fTableName);
    flushWAL(vm1, fTableName);
    forceDisconnect(vm1);
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords * 2);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    forceEvictiononServer(vm0, fTableName);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    reconnect(this.vm1);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    runScan(fTable, numRecords * 2);
  }

  @Test
  public void testWithParallelAppend() {
    final String fTableName = getMethodName();
    recordsCounter.set(0);
    final FTable fTable = createFTable(fTableName, 1, 1);
    final int blockSize = fTable.getTableDescriptor().getBlockSize();
    doParallelAppend(fTableName, 10);
    try {
      Thread.sleep(100000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    stopAppendThreads();
    final int blockCount = (int) Math.ceil(1.0 * recordsCounter.get() / blockSize);
    assertEquals("Incorrect block count.", blockCount,
        ((InternalTable) fTable).getInternalRegion().keySetOnServer().size());
    runScan(fTable, recordsCounter.get());
  }

  @Test
  public void testWithParallelAppendInIteration() {
    final int randomInt = TypeUtils.getRandomInt(5);
    for (int i = 0; i < randomInt; i++) {
      System.out.println("### Iteration= " + i);
      testWithParallelAppend();
      deleteFTable(getMethodName());
    }
  }

  @Test
  public void testWithParallelAppendWithOverflow() {
    final String fTableName = getMethodName();
    recordsCounter.set(0);
    final FTable fTable = createFTable(fTableName, 1, 1);
    doParallelAppend(fTableName, 10);
    try {
      Thread.sleep(50_000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    forceEvictiononServer(vm0, fTableName);
    forceEvictiononServer(vm1, fTableName);
    pauseWalMontoring(vm0, fTableName);
    pauseWalMontoring(vm1, fTableName);
    try {
      Thread.sleep(10_000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    flushWAL(vm0, fTableName);
    flushWAL(vm1, fTableName);
    stopAppendThreads();
    runScan(fTable, recordsCounter.get());

    long expected = recordsCounter.get();
    assertEquals("Incorrect total count on primary.", expected, getCount(fTableName, true));
    assertEquals("Incorrect total count on secondary.", expected, getCount(fTableName, false));
  }



  @Test
  public void testServerRestartWithParallelAppend() {
    final String fTableName = getMethodName();
    recordsCounter.set(0);
    final FTable fTable = createFTable(fTableName, 1, 1);
    doParallelAppend(fTableName, 10);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    stopServerOn(vm0);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());

    try {
      Thread.sleep(20000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    stopServerOn(vm0);
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());

    try {
      Thread.sleep(20000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    stopAppendThreads();
    runScan(fTable, recordsCounter.get());

    long expected = recordsCounter.get();
    assertEquals("Incorrect primary count.", expected, getCount(fTableName, true));
    assertEquals("Incorrect secondary count.", expected, getCount(fTableName, false));
  }

  @SuppressWarnings("unchecked")
  private long getCount(final String name, final boolean isPrimary) {
    long totalCount = 0;
    for (final VM vm : new VM[] {vm0, vm1}) {
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

  @Test
  public void testServerRestartWithAppend() {
    final String fTableName = getMethodName();
    final FTable fTable = createFTable(fTableName, 1, 1);
    final int numRecords = 4_000;
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords);
    stopServerOn(vm1);
    runScan(fTable, numRecords);
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords * 2);
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    runScan(fTable, numRecords * 2);
    stopServerOn(vm0);
    runScan(fTable, numRecords * 2);
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords * 3);
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    runScan(fTable, numRecords * 3);
  }

  @Test
  public void testAppendWithOverflow() throws InterruptedException {
    final String fTableName = getMethodName();
    final FTable fTable = createFTable(fTableName, 1, 1);
    final int numRecords = 4_000;
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords);
    forceEvictiononServer(vm0, fTableName);
    forceEvictiononServer(vm1, fTableName);
    pauseWalMontoring(vm0, fTableName);
    pauseWalMontoring(vm1, fTableName);
    flushWAL(vm0, fTableName);
    flushWAL(vm1, fTableName);
    Thread.sleep(10_000);
    runScan(fTable, numRecords);
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords * 2);
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords * 3);
  }

  @Test
  public void testServerRestartWithOverflow() throws InterruptedException {
    final String fTableName = getMethodName();
    final FTable fTable = createFTable(fTableName, 1, 1);
    final int numRecords = 400;
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords);
    forceEvictiononServer(vm0, fTableName);
    forceEvictiononServer(vm1, fTableName);
    pauseWalMontoring(vm0, fTableName);
    pauseWalMontoring(vm1, fTableName);
    flushWAL(vm0, fTableName);
    flushWAL(vm1, fTableName);
    stopServerOn(vm1);
    Thread.sleep(10_000);
    runScan(fTable, numRecords);
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords * 2);
    doAppend(fTableName, numRecords);
    runScan(fTable, numRecords * 3);
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    Thread.sleep(15_000);
    runScan(fTable, numRecords * 3);
    stopServerOn(vm0);
    Thread.sleep(10_000);
    runScan(fTable, numRecords * 3);
    doAppend(fTableName, numRecords);
    Thread.sleep(10_000);
    System.err.println("numRecords = " + numRecords);
    runScan(fTable, numRecords * 4);
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    Thread.sleep(15_000);
    runScan(fTable, numRecords * 4);
  }
}
