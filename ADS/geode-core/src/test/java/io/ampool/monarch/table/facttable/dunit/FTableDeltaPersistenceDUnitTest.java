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


import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.facttable.FTableDUnitHelper;
import io.ampool.monarch.table.facttable.FTableTestHelper;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.AdminImpl;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.internal.cache.*;
import org.apache.geode.test.dunit.*;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileFilter;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.ampool.monarch.table.facttable.dunit.FTableFailoverDUnitTest.forceEvictiononServer;
import static org.junit.Assert.*;

@Category(FTableTest.class)
@RunWith(JUnitParamsRunner.class)
public class FTableDeltaPersistenceDUnitTest extends MTableDUnitHelper {
  private static final int NUM_OF_COLUMNS = 10;
  private static final String COLUMN_NAME_PREFIX = "COL";
  private static String tableName;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());
    // createClientCache(vm3);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    // closeMClientCache(vm3);
    if (tableName != null) {
      deleteFTable(tableName);
    }
    closeMClientCache();
    super.tearDown2();
    tableName = null;
  }



  private void restart() throws InterruptedException {
    stopServerOn(vm0);
    stopServerOn(vm1);
    stopServerOn(vm2);
    closeMClientCache();
    Thread.sleep(10000);
    AsyncInvocation asyncInvocation = asyncStartServerOn(vm0, DUnitLauncher.getLocatorString());
    AsyncInvocation asyncInvocation1 = asyncStartServerOn(vm1, DUnitLauncher.getLocatorString());
    AsyncInvocation asyncInvocation2 = asyncStartServerOn(vm2, DUnitLauncher.getLocatorString());
    asyncInvocation.join();
    asyncInvocation1.join();
    asyncInvocation2.join();
    Thread.sleep(10000);
    createClientCache();
  }


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

  public static FTable createFTable(String ftableName, final int numSplits, int redundancy,
      boolean synchronous) {
    return createFTable(ftableName, numSplits, redundancy, null, synchronous);
  }


  public static void deleteFTable(String ftableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    if (admin.existsFTable(tableName)) {
      clientCache.getAdmin().deleteFTable(ftableName);
    }
  }

  private void appendBatches(final String ftablename, final int numRows, final int numBatches)
      throws InterruptedException {
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
              Bytes.toBytes(mColumnDescriptor.getColumnNameAsString() + i));
        }
      }
      table.append(records);
      System.out.println("Appended batch " + j);
    }
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

  private void appendBatchesWithLargeRecordsInParallel(final String ftablename, final int numRows,
      final int numBatches, String data, int nthreads) throws InterruptedException {

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

      ExecutorService executorService = Executors.newFixedThreadPool(nthreads);
      Future[] futures = new Future[nthreads];
      for (int i = 0; i < nthreads; i++) {
        futures[i] = executorService.submit(new Runnable() {
          public void run() {
            System.out.println("Asynchronous task");
            table.append(records);
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        });
      }

      for (int i = 0; i < nthreads; i++) {
        try {
          futures[i].get();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      }

      executorService.shutdown();
      System.out.println("Appended batch " + j);
    }
  }

  private void scanRecords(final String ftableName, final int expectedrecords) {
    final FTable table = MCacheFactory.getAnyInstance().getFTable(ftableName);
    Iterator<Row> iterator = table.getScanner(new Scan()).iterator();
    int recordCount = 0;
    while (iterator.hasNext()) {
      Row next = iterator.next();
      // System.out.println(next);
      recordCount++;
    }
    Assert.assertEquals(expectedrecords, recordCount);
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

  private void verifyDiskStore(String disk_store_name, int expectedFiles,
      int expectedFileSizeAfterDelta, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(new SerializableCallable<Object>() {
        @Override
        public Object call() throws Exception {
          MCache cache = MCacheFactory.getAnyInstance();
          GemFireCacheImpl gfc = (GemFireCacheImpl) cache;
          Collection<DiskStoreImpl> diskStores = gfc.listDiskStores();
          assertNotNull(diskStores);
          assertFalse(diskStores.isEmpty());
          assertTrue(diskStores.size() == 4);
          for (DiskStoreImpl diskStore : diskStores) {
            if (diskStore.getName().equals(disk_store_name)) {
              File[] diskDirs = diskStore.getDiskDirs();
              for (File diskDir : diskDirs) {
                FileFilter fileFilter = new WildcardFileFilter("BACKUPtestDiskStore*.crf");
                int length = diskDir.listFiles(fileFilter).length;
                File[] files = diskDir.listFiles(fileFilter);
                assertEquals(expectedFiles, length, 1);
                long totalSizeInMb = 0;
                for (File file : files) {
                  long sizeInBytes = file.length();
                  long sizeInMb = sizeInBytes / (1024 * 1024);
                  totalSizeInMb += sizeInMb;
                }
                assertTrue(totalSizeInMb < expectedFileSizeAfterDelta);
              }
            }
          }
          return null;
        }
      });
    }
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


  // {numbatches, numbuckets, numrows, redundancy}
  public Object[] getParams() throws NoSuchMethodException {
    return new Object[][] {{10, 1, 1001, 2}, {1, 1, 1, 0}, {10, 1, 100, 0}, {1, 1, 1000, 0},
        {10, 1, 1000, 0}, {1, 113, 113, 0}, {10, 113, 113, 0}, {1, 113, 11300, 0},
        {10, 113, 11300, 0}, {1, 1, 1, 2}, {10, 1, 100, 2}, {1, 1, 1000, 2}, {10, 1, 1000, 2},
        {1, 113, 113, 2}, {10, 113, 113, 2}, {1, 113, 11300, 2}, {10, 113, 11300, 2}};
  }



  @Test
  @Parameters(method = "getParams")
  public void testDeltaPersistence(final int numBatches, final int numSplits, final int numRows,
      final int redundancy) throws InterruptedException {
    tableName =
        getTestMethodName() + "_" + numBatches + "_" + numSplits + "_" + numRows + "_" + redundancy;
    final FTable table = createFTable(tableName, numSplits, redundancy, false);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);
    appendBatches(tableName, numRows, numBatches);
    scanRecords(tableName, numRows * numBatches);
    restart();
    scanRecords(tableName, numRows * numBatches);
    deleteFTable(tableName);
  }

  @Test
  @Parameters(method = "getParams")
  public void testDeltaWithSynchronousPersistence(final int numBatches, final int numSplits,
      final int numRows, final int redundancy) throws InterruptedException {
    tableName =
        getTestMethodName() + "_" + numBatches + "_" + numSplits + "_" + numRows + "_" + redundancy;
    final FTable table = createFTable(tableName, numSplits, redundancy, true);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);
    appendBatches(tableName, numRows, numBatches);
    scanRecords(tableName, numRows * numBatches);
    restart();
    scanRecords(tableName, numRows * numBatches);
    appendBatches(tableName, numRows, numBatches);
    scanRecords(tableName, 2 * numRows * numBatches);
    deleteFTable(tableName);
  }

  @Test
  @Parameters(method = "getParams")
  public void testDeltaWithSynchronousPersistenceWithFullAndPartialEviction(final int numBatches,
      final int numSplits, final int numRows, final int redundancy) throws InterruptedException {
    IgnoredException.addIgnoredException("java.util.concurrent.RejectedExecutionException");
    tableName =
        getTestMethodName() + "_" + numBatches + "_" + numSplits + "_" + numRows + "_" + redundancy;
    final FTable table = createFTable(tableName, numSplits, redundancy, true);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);
    appendBatches(tableName, numRows, numBatches);
    scanRecords(tableName, numRows * numBatches);
    vm0.invoke(() -> FTableTestHelper.raiseFakeNotification());
    vm1.invoke(() -> FTableTestHelper.raiseFakeNotification());
    vm2.invoke(() -> FTableTestHelper.raiseFakeNotification());
    Thread.sleep(10000);
    scanRecords(tableName, numRows * numBatches);
    restart();
    scanRecords(tableName, numRows * numBatches);
    vm0.invoke(() -> FTableTestHelper.revokeFakeNotification());
    vm1.invoke(() -> FTableTestHelper.revokeFakeNotification());
    vm2.invoke(() -> FTableTestHelper.revokeFakeNotification());
    appendBatches(tableName, numRows, numBatches);
    scanRecords(tableName, 2 * numRows * numBatches);
    vm0.invoke(() -> FTableTestHelper.raiseFakeNotification());
    vm1.invoke(() -> FTableTestHelper.raiseFakeNotification());
    vm2.invoke(() -> FTableTestHelper.raiseFakeNotification());
    Thread.sleep(10000);
    scanRecords(tableName, numRows * numBatches * 2);
    deleteFTable(tableName);
  }

  @Test
  @Parameters(method = "getParams")
  public void testDeltaWithASynchronousPersistenceWithFullAndPartialEviction(final int numBatches,
      final int numSplits, final int numRows, final int redundancy) throws InterruptedException {
    IgnoredException.addIgnoredException("java.util.concurrent.RejectedExecutionException");
    tableName =
        getTestMethodName() + "_" + numBatches + "_" + numSplits + "_" + numRows + "_" + redundancy;
    final FTable table = createFTable(tableName, numSplits, redundancy, false);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);
    appendBatches(tableName, numRows, numBatches);
    scanRecords(tableName, numRows * numBatches);
    vm0.invoke(() -> FTableTestHelper.raiseFakeNotification());
    vm1.invoke(() -> FTableTestHelper.raiseFakeNotification());
    vm2.invoke(() -> FTableTestHelper.raiseFakeNotification());
    Thread.sleep(10000);
    scanRecords(tableName, numRows * numBatches);
    restart();
    scanRecords(tableName, numRows * numBatches);
    vm0.invoke(() -> FTableTestHelper.revokeFakeNotification());
    vm1.invoke(() -> FTableTestHelper.revokeFakeNotification());
    vm2.invoke(() -> FTableTestHelper.revokeFakeNotification());
    appendBatches(tableName, numRows, numBatches);
    scanRecords(tableName, 2 * numRows * numBatches);
    vm0.invoke(() -> FTableTestHelper.raiseFakeNotification());
    vm1.invoke(() -> FTableTestHelper.raiseFakeNotification());
    vm2.invoke(() -> FTableTestHelper.raiseFakeNotification());
    Thread.sleep(10000);
    scanRecords(tableName, numRows * numBatches * 2);
    deleteFTable(tableName);
  }

  @Test
  public void testOnlineCompaction() throws InterruptedException {
    int numRows = 5;
    int numBatches = 10;
    int numSplits = 1;
    int redundancy = 0;
    String DISK_STORE_NAME = "testDiskStore";
    String tableName = getTestMethodName();
    stopServerOn(vm1);
    stopServerOn(vm2);
    createDiskStoreOnserver(DISK_STORE_NAME, true, 1, new VM[] {vm0}, true);
    final FTable table = createFTable(tableName, numSplits, redundancy, DISK_STORE_NAME, false);
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
    scanRecords(tableName, numRows * numBatches);
    System.out.println("SRI: " + Thread.currentThread().getId() + " "
        + "FTableDeltaPersistenceDUnitTest.testOnlineCompaction.241");
    Thread.sleep(10000);
    forceEvictiononServer(vm0, tableName);
    Thread.sleep(30000);
    forceTombstomeExpiryFortest(new VM[] {vm0});
    Thread.sleep(30000);
    scanRecords(tableName, numRows * numBatches);
    restart();
    Thread.sleep(10000);
    scanRecords(tableName, numRows * numBatches);
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

  @Test
  public void testCompactionAfterEviction() throws InterruptedException {
    int numRows = 5;
    int numBatches = 10;
    int numSplits = 1;
    int redundancy = 2;
    String DISK_STORE_NAME = "testDiskStore2";
    String tableName = getTestMethodName();
    createDiskStoreOnserver(DISK_STORE_NAME, true, 1, new VM[] {vm0, vm1, vm2}, true);
    final FTable table = createFTable(tableName, numSplits, redundancy, DISK_STORE_NAME, false);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);

    StringBuilder sb = new StringBuilder(2080);
    for (int i = 0; i < 2600; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(8));
    }
    String data = sb.toString();
    appendBatchesWithLargeRecords(tableName, numRows, numBatches, data);
    scanRecords(tableName, numRows * numBatches);
    assertEquals(numRows * numBatches,
        FTableFailoverDUnitTest.getInMemoryRecordsCount(vm0, tableName, false));

    verifyDiskStore(DISK_STORE_NAME, 10, 11, vm0, vm1, vm2);

    forceEvictiononServer(vm0, tableName);
    forceEvictiononServer(vm1, tableName);
    forceEvictiononServer(vm2, tableName);
    Thread.sleep(5000);
    forceTombstomeExpiryFortest(new VM[] {vm0, vm1, vm2});
    Thread.sleep(5000);
    scanRecords(tableName, numRows * numBatches);
    assertEquals(0, FTableFailoverDUnitTest.getInMemoryRecordsCount(vm0, tableName, false));

    verifyDiskStore(DISK_STORE_NAME, 1, 2, vm0, vm1, vm2);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    if (admin.existsFTable(tableName)) {
      clientCache.getAdmin().deleteFTable(tableName);
    }
  }

  @Test
  public void testCompactionAfterPartialEviction() throws InterruptedException {
    int numRows = 5;
    int numBatches = 10;
    int numSplits = 1;
    int redundancy = 2;
    String DISK_STORE_NAME = "testDiskStore3";
    String tableName = getTestMethodName();
    createDiskStoreOnserver(DISK_STORE_NAME, true, 1, new VM[] {vm0, vm1, vm2}, true);
    final FTable table = createFTable(tableName, numSplits, redundancy, DISK_STORE_NAME, false);
    assertNotNull(table);
    /*
     * FTableImpl fTable = (FTableImpl) table;
     * fTable.getInternalRegion().getAttributesMutator().getEvictionAttributesMutator().setMaximum(
     * 25);
     */
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);

    StringBuilder sb = new StringBuilder(2080);
    for (int i = 0; i < 2600; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(8));
    }
    String data = sb.toString();
    appendBatchesWithLargeRecords(tableName, numRows, numBatches, data);
    scanRecords(tableName, numRows * numBatches);
    assertEquals(numRows * numBatches,
        FTableFailoverDUnitTest.getInMemoryRecordsCount(vm0, tableName, false));
    verifyDiskStore(DISK_STORE_NAME, 10, 11, vm0, vm1, vm2);

    System.out.println("Evicting from memory to tier");
    forceEvictiononServer(vm0, tableName);
    forceEvictiononServer(vm1, tableName);
    forceEvictiononServer(vm2, tableName);
    Thread.sleep(5000);

    forceTombstomeExpiryFortest(new VM[] {vm0, vm1, vm2});
    Thread.sleep(10000);
    scanRecords(tableName, numRows * numBatches);
    assertEquals(0, FTableFailoverDUnitTest.getInMemoryRecordsCount(vm0, tableName, false));
    verifyDiskStore(DISK_STORE_NAME, 1, 2, vm0, vm1, vm2);

    appendBatchesWithLargeRecords(tableName, numRows, numBatches, data);
    scanRecords(tableName, 2 * numRows * numBatches);
    verifyDiskStore(DISK_STORE_NAME, 1, 2, vm0, vm1, vm2);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    if (admin.existsFTable(tableName)) {
      clientCache.getAdmin().deleteFTable(tableName);
    }
  }

  @Test
  public void testFTableBatchAppendInParallel() throws InterruptedException {
    int numRows = 1;
    int numBatches = 50;
    int numSplits = 1;
    int redundancy = 2;
    String DISK_STORE_NAME = "testDiskStore4";
    String tableName = getTestMethodName();
    createDiskStoreOnserver(DISK_STORE_NAME, true, 5, new VM[] {vm0, vm1, vm2}, true);
    final FTable table = createFTable(tableName, numSplits, redundancy, DISK_STORE_NAME, false);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);

    StringBuilder sb = new StringBuilder(2080);
    for (int i = 0; i < 2600; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(8));
    }
    String data = sb.toString();
    int nThreads = 5;
    appendBatchesWithLargeRecordsInParallel(tableName, numRows, numBatches, data, nThreads);
    scanRecords(tableName, nThreads * numRows * numBatches);

    Thread.sleep(20000);
    verifyDiskStore(DISK_STORE_NAME, 13, 65, vm0, vm1, vm2);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    if (admin.existsFTable(tableName)) {
      clientCache.getAdmin().deleteFTable(tableName);
    }
  }

  @Test
  public void testCompactionAfterPartialEviction2() throws InterruptedException {
    int numRows = 5;
    int numBatches = 10;
    int numSplits = 5;
    int redundancy = 2;
    String DISK_STORE_NAME = "testDiskStore5";
    String tableName = getTestMethodName() + 1;
    String tableName2 = getTestMethodName() + 2;

    createDiskStoreOnserver(DISK_STORE_NAME, true, 1, new VM[] {vm0, vm1, vm2}, true);
    final FTable table = createFTable(tableName, numSplits, redundancy, DISK_STORE_NAME, false);
    assertNotNull(table);

    final FTable table2 = createFTable(tableName2, numSplits, redundancy, DISK_STORE_NAME, false);
    assertNotNull(table2);

    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);
    FTableDUnitHelper.verifyFTableONServer(tableName2, vm0, vm1, vm2);

    StringBuilder sb = new StringBuilder(1040);
    for (int i = 0; i < 1024; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(8));
    }
    String data = sb.toString();

    appendBatchesWithLargeRecords(tableName, numRows, numBatches, data);
    scanRecords(tableName, numRows * numBatches);
    assertEquals(numRows * numBatches,
        FTableFailoverDUnitTest.getInMemoryRecordsCount(vm0, tableName, false));
    verifyDiskStore(DISK_STORE_NAME, 5, 6, vm0, vm1, vm2);

    appendBatchesWithLargeRecords(tableName2, numRows, numBatches, data);
    scanRecords(tableName2, numRows * numBatches);
    assertEquals(numRows * numBatches,
        FTableFailoverDUnitTest.getInMemoryRecordsCount(vm0, tableName2, false));
    verifyDiskStore(DISK_STORE_NAME, 10, 11, vm0, vm1, vm2);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();

    admin.forceFTableEviction(tableName2);

    Thread.sleep(5000);

    scanRecords(tableName, numRows * numBatches);
    assertEquals(numRows * numBatches,
        FTableFailoverDUnitTest.getInMemoryRecordsCount(vm0, tableName, false));


    forceTombstomeExpiryFortest(new VM[] {vm0, vm1, vm2});
    Thread.sleep(20000);

    scanRecords(tableName2, numRows * numBatches);
    assertEquals(0, FTableFailoverDUnitTest.getInMemoryRecordsCount(vm0, tableName2, false));

    verifyDiskStore(DISK_STORE_NAME, 6, 11, vm0, vm1, vm2);

    admin.forceFTableEviction(tableName);

    Thread.sleep(5000);

    assertEquals(0, FTableFailoverDUnitTest.getInMemoryRecordsCount(vm0, tableName, false));


    forceTombstomeExpiryFortest(new VM[] {vm0, vm1, vm2});
    Thread.sleep(20000);
    forceTombstomeExpiryFortest(new VM[] {vm0, vm1, vm2});
    Thread.sleep(20000);

    scanRecords(tableName, numRows * numBatches);

    verifyDiskStore(DISK_STORE_NAME, 1, 2, vm0, vm1, vm2);

    if (admin.existsFTable(tableName)) {
      clientCache.getAdmin().deleteFTable(tableName);
    }

    if (admin.existsFTable(tableName2)) {
      clientCache.getAdmin().deleteFTable(tableName2);
    }
  }

  /* Two table sharing the same file. */
  @Test
  public void testFTableOnlineCompactionWithSharedFile() throws InterruptedException {
    int numRows = 5;
    int numBatches = 10;
    int numSplits = 5;
    int redundancy = 2;
    String DISK_STORE_NAME = "testDiskStore6";
    String tableName = getTestMethodName() + 1;
    String tableName2 = getTestMethodName() + 2;
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();

    createDiskStoreOnserver(DISK_STORE_NAME, true, 5, new VM[] {vm0, vm1, vm2}, true);
    final FTable table = createFTable(tableName, numSplits, redundancy, DISK_STORE_NAME, false);
    assertNotNull(table);

    final FTable table2 = createFTable(tableName2, numSplits, redundancy, DISK_STORE_NAME, false);
    assertNotNull(table2);

    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);
    FTableDUnitHelper.verifyFTableONServer(tableName2, vm0, vm1, vm2);

    StringBuilder sb = new StringBuilder(1040);
    for (int i = 0; i < 1024; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(8));
    }
    String data = sb.toString();


    Thread t1 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          appendBatchesWithLargeRecords(tableName, numRows, numBatches, data);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    Thread t2 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          appendBatchesWithLargeRecords(tableName2, numRows, numBatches, data);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    t1.start();
    t2.start();

    t1.join();
    t2.join();

    scanRecords(tableName2, numRows * numBatches);
    scanRecords(tableName, numRows * numBatches);

    Thread.sleep(10000);
    verifyDiskStore(DISK_STORE_NAME, 2, 12, vm0, vm1, vm2);

    ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).forceFTableEviction(tableName);

    forceTombstomeExpiryFortest(new VM[] {vm0, vm1, vm2});
    Thread.sleep(20000);

    /* No files should be deleted as the data of other table is still there in the files */
    verifyDiskStore(DISK_STORE_NAME, 2, 12, vm0, vm1, vm2);

    ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).forceFTableEviction(tableName2);

    forceTombstomeExpiryFortest(new VM[] {vm0, vm1, vm2});
    Thread.sleep(20000);

    verifyDiskStore(DISK_STORE_NAME, 1, 5, vm0, vm1, vm2);
    if (admin.existsFTable(tableName)) {
      clientCache.getAdmin().deleteFTable(tableName);
    }

    if (admin.existsFTable(tableName2)) {
      clientCache.getAdmin().deleteFTable(tableName2);
    }
  }
}
