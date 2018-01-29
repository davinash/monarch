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

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.BasicTypes;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

@Category(FTableTest.class)
public class FTablePersistenceJUnitTest {
  private static Object[][] columnNameTypes = {{"ID", BasicTypes.INT}, {"NAME", BasicTypes.STRING},
      {"AGE", BasicTypes.INT}, {"SEX", BasicTypes.STRING}, {"DESIGNATION", BasicTypes.STRING}};

  private static Object[][] values =
      {{1, "Phoebe", 34, "F", "PM"}, {2, "Chandler", 33, "M", "Team Leader"},
          {3, "Rachel", 16, "F", "Software Developer"}, {4, "Ross", 27, "M", "Software Developer"},
          {5, "Monica", 38, "F", "ARCH"}, {6, "Joey", 33, "M", "ARCH"},
          {7, "Janice", 29, "M", "HR"}, {8, "Gunther", 33, "M", "CUSTOMER SUPPORT"},
          {9, "Jack", 54, "M", "GM"}, {10, "Judy", 56, "F", "CEO"}};



  private Properties createLonerProperties() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    return props;
  }

  private MCache createCache() {
    return createCache(new Properties());
  }

  private MCache createCache(Properties cacheProps) {
    Properties properties = createLonerProperties();
    properties.putAll(cacheProps);
    return new MCacheFactory(properties).create();
  }

  public static FTableDescriptor getFTableDescriptor(final int numSplits) {
    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    fTableDescriptor.setTotalNumOfSplits(numSplits);
    for (int i = 0; i < columnNameTypes.length; i++) {
      fTableDescriptor.addColumn((String) columnNameTypes[i][0],
          (BasicTypes) columnNameTypes[i][1]);
    }
    return fTableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(final int numSplits, int redundancy,
      String diskStoreName) {

    return getFTableDescriptor(numSplits, redundancy, diskStoreName, 1000);
  }

  public static FTableDescriptor getFTableDescriptor(final int numSplits, int redundancy,
      String diskStoreName, int blockSize) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < 10; i++) {
      tableDescriptor.addColumn("COL_" + i);
    }
    tableDescriptor.setTotalNumOfSplits(numSplits);
    tableDescriptor.setRedundantCopies(redundancy);
    if (diskStoreName != null) {
      tableDescriptor.setDiskStore(diskStoreName);
    }
    tableDescriptor.setBlockSize(blockSize);
    return tableDescriptor;
  }

  private void createFTable(final String tableName, final int numSplits, int redundancy,
      String disk_store_name) {
    createFTable(tableName, numSplits, redundancy, disk_store_name, 1000);
  }

  private void createFTable(final String tableName, final int numSplits, int redundancy,
      String disk_store_name, int blockSize) {
    MCache cache = MCacheFactory.getAnyInstance();
    cache.getAdmin().createFTable(tableName,
        getFTableDescriptor(numSplits, redundancy, disk_store_name, blockSize));
  }

  private void createFTable(final String tableName, final int numSplits) {
    MCacheFactory.getAnyInstance().getAdmin().createFTable(tableName,
        getFTableDescriptor(numSplits));
  }

  private void appendRecords(final String tableName) {
    FTable fTable = MCacheFactory.getAnyInstance().getFTable(tableName);
    Record record = new Record();
    Arrays.stream(values).forEach((VAL) -> {
      Iterator<Object[]> columnNames = Arrays.stream(columnNameTypes).iterator();
      Arrays.stream(VAL).forEach((V) -> record.add((String) columnNames.next()[0], V));
      fTable.append(record);
    });
  }

  private void scanRecords(final String tableName, final int expectedRecords) {
    FTable fTable = MCacheFactory.getAnyInstance().getFTable(tableName);
    final AtomicInteger counter = new AtomicInteger(0);
    fTable.getScanner(new Scan()).forEach((ROW) -> {
      // System.out.println(ROW.getCells());
      counter.incrementAndGet();
    });
    Assert.assertEquals(expectedRecords, counter.get());
  }

  private void deleteFTable(String tableName) {
    MCacheFactory.getAnyInstance().getAdmin().deleteFTable(tableName);
  }

  private void closeCache() {
    MCacheFactory.getAnyInstance().close();
  }

  private void raiseFakeNotification() {
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

  // private void verifyDeltaStats(final int expectedWrites, final int expectedReads) {
  // DiskStoreImpl diskStore = (DiskStoreImpl) MCacheFactory.getAnyInstance()
  // .findDiskStore(MTableUtils.DEFAULT_FTABLE_DISK_STORE_NAME);
  //
  // assertTrue(expectedReads > 0 ? diskStore.getStats().getDeltaReads() > 1
  // : diskStore.getStats().getDeltaReads() == 0);
  // assertTrue(expectedWrites > 0 ? diskStore.getStats().getDeltaWrites() > 1
  // : diskStore.getStats().getDeltaWrites() == 0);
  // }



  @After
  public void close() {
    MCacheFactory.getAnyInstance().close();
  }

  @Before
  public void clean() {
    clean(".");
    clean("WAL");
    clean("ORC");
  }

  public void clean(final String dirName) {
    File serverDir = new File(dirName);
    if (serverDir.isDirectory() && serverDir.exists()) {
      File[] files = serverDir.listFiles();
      for (File file : files) {
        if (file.getName().endsWith(".krf") || file.getName().endsWith(".drf")
            || file.getName().endsWith(".crf") || file.getName().endsWith(".if")
            || file.getName().endsWith(".done") || file.getName().endsWith(".inprogress")
            || file.getName().endsWith(".orc")) {
          file.delete();
        }
      }
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

  private void createDiskStore(String disk_store_name, boolean allowDeltaPersistence,
      int maxOplogSize, boolean autocompact) {
    DiskStoreFactory dsf = MCacheFactory.getAnyInstance().createDiskStoreFactory();
    dsf.setAutoCompact(autocompact);
    dsf.setMaxOplogSize(maxOplogSize);
    dsf.setEnableDeltaPersistence(allowDeltaPersistence);
    dsf.create(disk_store_name);
  }

  @Test
  public void testFTableWrite1() throws InterruptedException {
    final String tableName = "testFTableWrite1";
    createCache();
    createFTable(tableName, 1);
    appendRecords(tableName);
    Thread.sleep(10000);
    appendRecords(tableName);
    scanRecords(tableName, 20);
    Thread.sleep(10000);
    // verifyDeltaStats(1, 0);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableWrite2() throws InterruptedException {
    final String tableName = "testFTableWrite2";
    createCache();
    createFTable(tableName, 1);
    int blockSize =
        MCacheFactory.getAnyInstance().getFTable(tableName).getTableDescriptor().getBlockSize();
    for (int i = 0; i < blockSize; i += 10) {
      appendRecords(tableName);
      Thread.sleep(1000);
    }
    scanRecords(tableName, blockSize);
    // verifyDeltaStats(99, 0);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableWrite3() throws InterruptedException {
    final String tableName = "testFTableWrite3";
    createCache();
    createFTable(tableName, 1);
    int blockSize =
        MCacheFactory.getAnyInstance().getFTable(tableName).getTableDescriptor().getBlockSize();
    for (int i = 0; i < blockSize; i += 10) {
      appendRecords(tableName);
      Thread.sleep(1000);
    }
    scanRecords(tableName, blockSize);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableWrite4() throws InterruptedException {
    System.setProperty(MTableUtils.AMPL_DELTA_PERS_PROP_NAME, "false");
    final String tableName = "testFTableWrite1";
    createCache();
    createFTable(tableName, 1);
    appendRecords(tableName);
    Thread.sleep(10000);
    appendRecords(tableName);
    scanRecords(tableName, 20);
    Thread.sleep(10000);
    // verifyDeltaStats(0, 0);
    DiskStoreImpl diskStore = (DiskStoreImpl) MCacheFactory.getAnyInstance()
        .findDiskStore(MTableUtils.DEFAULT_FTABLE_DISK_STORE_NAME);
    Assert.assertFalse(diskStore.getEnableDeltaPersistence());
    deleteFTable(tableName);
    System.setProperty(MTableUtils.AMPL_DELTA_PERS_PROP_NAME, "true");
  }

  @Test
  public void testFTableRecovery1() throws InterruptedException {
    final String tableName = "testFTableRecovery1";
    createCache();
    createFTable(tableName, 1);
    appendRecords(tableName);
    Thread.sleep(10000);
    appendRecords(tableName);
    scanRecords(tableName, 20);
    closeCache();
    Thread.sleep(10000);
    createCache();
    Thread.sleep(10000);
    scanRecords(tableName, 20);
    // verifyDeltaStats(0, 1);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableRecovery2() throws InterruptedException {
    final String tableName = "testFTableRecovery2";
    createCache();
    createFTable(tableName, 1);
    appendRecords(tableName);
    Thread.sleep(10000);
    appendRecords(tableName);
    scanRecords(tableName, 20);
    closeCache();
    Thread.sleep(10000);
    createCache();
    Thread.sleep(10000);
    scanRecords(tableName, 20);
    // verifyDeltaStats(0, 1);
    appendRecords(tableName);
    scanRecords(tableName, 30);
    // verifyDeltaStats(0, 1);
    closeCache();
    Thread.sleep(10000);
    createCache();
    scanRecords(tableName, 30);
    // verifyDeltaStats(0, 1);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableRecovery3() throws InterruptedException {
    final String tableName = "testFTableRecovery2";
    createCache();
    createFTable(tableName, 1);
    appendRecords(tableName);
    Thread.sleep(10000);
    appendRecords(tableName);
    scanRecords(tableName, 20);
    closeCache();
    Thread.sleep(10000);
    createCache();
    Thread.sleep(10000);
    scanRecords(tableName, 20);
    appendRecords(tableName);
    scanRecords(tableName, 30);
    appendRecords(tableName);
    Thread.sleep(10000);
    scanRecords(tableName, 40);
    // verifyDeltaStats(1, 1);
    closeCache();
    Thread.sleep(10000);
    createCache();
    scanRecords(tableName, 40);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableRecovery4() throws InterruptedException {
    final String tableName = "testFTableRecovery1";
    createCache();
    createFTable(tableName, 1);
    appendRecords(tableName);
    Thread.sleep(10000);
    appendRecords(tableName);
    scanRecords(tableName, 20);
    closeCache();
    createCache();
    scanRecords(tableName, 20);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableRecovery5() throws InterruptedException {
    final String tableName = "testFTableRecovery5";
    createCache();
    createFTable(tableName, 1);
    int blockSize =
        MCacheFactory.getAnyInstance().getFTable(tableName).getTableDescriptor().getBlockSize();
    for (int i = 0; i < blockSize; i += 10) {
      appendRecords(tableName);
    }
    scanRecords(tableName, blockSize);
    closeCache();
    createCache();
    scanRecords(tableName, blockSize);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableRecovery6() throws InterruptedException {
    final String tableName = "testFTableRecovery6";
    createCache();
    createFTable(tableName, 1);
    int blockSize =
        MCacheFactory.getAnyInstance().getFTable(tableName).getTableDescriptor().getBlockSize();
    for (int i = 0; i < blockSize; i += 10) {
      appendRecords(tableName);
      Thread.sleep(1000);
    }
    scanRecords(tableName, blockSize);
    closeCache();
    createCache();
    scanRecords(tableName, blockSize);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableRecovery7() throws InterruptedException {
    final int numBatch = 10000;

    final String tableName = "testFTableRecovery7";
    createCache();
    createFTable(tableName, 1);
    for (int i = 0; i < numBatch; i++) {
      appendRecords(tableName);
      // Thread.sleep(100);
    }
    scanRecords(tableName, numBatch * 10);
    closeCache();
    Thread.sleep(10000);
    createCache();
    Thread.sleep(10000);
    scanRecords(tableName, numBatch * 10);
    Thread.sleep(40000);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableRecovery8() throws InterruptedException {
    final String tableName = "testFTableRecovery1";
    createCache();
    createFTable(tableName, 1);
    FTable fTable = MCacheFactory.getAnyInstance().getFTable(tableName);
    Record record = new Record();
    for (int i = 0; i < columnNameTypes.length; i++) {
      record.add((String) columnNameTypes[i][0], values[0][i]);
    }
    fTable.append(record);
    closeCache();
    createCache();
    fTable = MCacheFactory.getAnyInstance().getFTable(tableName);
    scanRecords(tableName, 1);
    fTable.append(record);
    fTable.append(record);
    fTable.append(record);
    scanRecords(tableName, 4);
    Thread.sleep(5000);
    closeCache();
    createCache();
    scanRecords(tableName, 4);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableCompaction1() throws InterruptedException {
    final String tableName = "testFTableCompaction1";
    createCache();
    createFTable(tableName, 1);
    org.apache.geode.cache.DiskStore diskStore = ((GemFireCacheImpl) MCacheFactory.getAnyInstance())
        .findDiskStore(MTableUtils.DEFAULT_FTABLE_DISK_STORE_NAME);
    appendRecords(tableName);
    diskStore.flush();
    appendRecords(tableName);
    scanRecords(tableName, 20);
    diskStore.forceCompaction();
    scanRecords(tableName, 20);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableCompaction2() throws InterruptedException {
    final String tableName = "testFTableCompaction2";
    createCache();
    createFTable(tableName, 1);
    DiskStoreImpl diskStore = (DiskStoreImpl) ((GemFireCacheImpl) MCacheFactory.getAnyInstance())
        .findDiskStore(MTableUtils.DEFAULT_FTABLE_DISK_STORE_NAME);
    appendRecords(tableName);
    diskStore.flush();
    appendRecords(tableName);
    scanRecords(tableName, 20);
    diskStore.forceCompaction();
    scanRecords(tableName, 20);
    closeCache();
    createCache();
    scanRecords(tableName, 20);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableCompaction3() throws InterruptedException {
    final String tableName = "testFTableCompaction3";
    createCache();
    createFTable(tableName, 1);
    DiskStoreImpl diskStore = (DiskStoreImpl) ((GemFireCacheImpl) MCacheFactory.getAnyInstance())
        .findDiskStore(MTableUtils.DEFAULT_FTABLE_DISK_STORE_NAME);
    for (int i = 0; i < 10; i++) {
      appendRecords(tableName);
      Thread.sleep(10000);
    }
    diskStore.flush();
    scanRecords(tableName, 100);
    diskStore.forceCompaction();
    scanRecords(tableName, 100);
    Thread.sleep(10000);
    deleteFTable(tableName);
  }


  @Test
  public void testFTableOverflow1() throws InterruptedException {
    final String tableName = "testFTableOverflow1";
    createCache();
    createFTable(tableName, 1);
    appendRecords(tableName);
    scanRecords(tableName, 10);
    raiseFakeNotification();
    Thread.sleep(10000);
    scanRecords(tableName, 10);
    closeCache();
    createCache();
    scanRecords(tableName, 10);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableOverflow2() throws InterruptedException {
    final String tableName = "testFTableOverflow1";
    createCache();
    createFTable(tableName, 1);
    appendRecords(tableName);
    scanRecords(tableName, 10);
    appendRecords(tableName);
    scanRecords(tableName, 20);
    raiseFakeNotification();
    Thread.sleep(100000);
    scanRecords(tableName, 20);
    closeCache();
    createCache();
    scanRecords(tableName, 20);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableOverflow3() throws InterruptedException {
    final String tableName = "testFTableOverflow1";
    createCache();
    createFTable(tableName, 1);
    appendRecords(tableName);
    scanRecords(tableName, 10);
    appendRecords(tableName);
    scanRecords(tableName, 20);
    raiseFakeNotification();
    appendRecords(tableName);
    Thread.sleep(100000);
    scanRecords(tableName, 30);
    closeCache();
    createCache();
    scanRecords(tableName, 30);
    deleteFTable(tableName);
  }

  @Test
  public void testFTableOverflow4() throws InterruptedException {
    final String tableName = "testFTableOverflow4";
    System.setProperty("gemfire.non-replicated-tombstone-timeout", "1");
    System.setProperty("gemfire.tombstone-scan-interval", "1");
    createCache();
    createFTable(tableName, 1);
    appendRecords(tableName);
    scanRecords(tableName, 10);
    raiseFakeNotification();
    Thread.sleep(10000);
    scanRecords(tableName, 10);
    Thread.sleep(30000);
    ((GemFireCacheImpl) MCacheFactory.getAnyInstance()).getTombstoneService()
        .forceBatchExpirationForTests(1);
    closeCache();
    createCache();
    scanRecords(tableName, 10);
    deleteFTable(tableName);
  }


  @Test
  public void testOnlineCompaction() throws InterruptedException {
    int numRows = 1;
    int numBatches = 50;
    int numSplits = 1;
    int redundancy = 0;
    String DISK_STORE_NAME = "testDiskStore";
    String tableName = "testOnlineCompaction";
    createCache();
    createDiskStore(DISK_STORE_NAME, true, 5, true);
    createFTable(tableName, numSplits, redundancy, DISK_STORE_NAME, 5);

    StringBuilder sb = new StringBuilder(2080);
    for (int i = 0; i < 2600; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(8));
    }
    String data = sb.toString();
    appendBatchesWithLargeRecords(tableName, numRows, numBatches, data);
    scanRecords(tableName, numRows * numBatches);
    System.out.println("SRI: " + Thread.currentThread().getId() + " "
        + "FTableDeltaPersistenceDUnitTest.testOnlineCompaction.241");
    Thread.sleep(10000);
    raiseFakeNotification();
    Thread.sleep(20000);
    ((GemFireCacheImpl) MCacheFactory.getAnyInstance()).getTombstoneService()
        .forceBatchExpirationForTests(100);
    Thread.sleep(20_000);
    scanRecords(tableName, numRows * numBatches);
    closeCache();
    Thread.sleep(10000);
    createCache();
    scanRecords(tableName, numRows * numBatches);
  }

  @Test
  public void testOnlineCompactionRecordEviction() throws InterruptedException {
    int numRows = 5;
    int numBatches = 10;
    int numSplits = 1;
    int redundancy = 0;
    String DISK_STORE_NAME = "testDiskStore";
    String tableName = "testOnlineCompactionFileDeletion";
    createCache();
    createDiskStore(DISK_STORE_NAME, true, 3, true);
    createFTable(tableName, numSplits, redundancy, DISK_STORE_NAME);
    StringBuilder sb = new StringBuilder(2080);
    for (int i = 0; i < 2600; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(8));
    }
    String data = sb.toString();
    // Append
    appendBatchesWithLargeRecords(tableName, numRows, numBatches, data);
    scanRecords(tableName, numRows * numBatches);
    Thread.sleep(5000);
    verifyStoreDir(5, DISK_STORE_NAME);

    // trigger eviction
    raiseFakeNotification();

    // wait for evication to finish
    Thread.sleep(10000);
    ((GemFireCacheImpl) MCacheFactory.getAnyInstance()).getTombstoneService()
        .forceBatchExpirationForTests(100);
    // let the tombstones go away
    Thread.sleep(20000);
    scanRecords(tableName, numRows * numBatches);
    verifyStoreDir(1, DISK_STORE_NAME);
  }


  @Test
  public void testOnlineCompactionRecordEviction2() throws InterruptedException {
    int numRows = 5;
    int numBatches = 10;
    int numSplits = 1;
    int redundancy = 0;
    String DISK_STORE_NAME = "testDiskStore";
    String tableName = "testOnlineCompactionRecordEviction2";
    createCache();
    createDiskStore(DISK_STORE_NAME, true, 3, true);
    createFTable(tableName, numSplits, redundancy, DISK_STORE_NAME);
    StringBuilder sb = new StringBuilder(2080);
    for (int i = 0; i < 2600; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(8));
    }
    String data = sb.toString();
    // Append
    appendBatchesWithLargeRecords(tableName, numRows, numBatches, data);
    scanRecords(tableName, numRows * numBatches);
    Thread.sleep(5000);
    verifyStoreDir(5, DISK_STORE_NAME);

    // trigger eviction
    raiseFakeNotification();

    // wait for evication to finish
    Thread.sleep(10000);
    ((GemFireCacheImpl) MCacheFactory.getAnyInstance()).getTombstoneService()
        .forceBatchExpirationForTests(100);
    // let the tombstones go away

    Thread.sleep(20000);
    scanRecords(tableName, numRows * numBatches);
    verifyStoreDir(1, DISK_STORE_NAME);

    appendBatchesWithLargeRecords(tableName, numRows, numBatches, data);
    scanRecords(tableName, numRows * numBatches * 2);

    // verifyStoreDir(10, DISK_STORE_NAME);

    // trigger eviction
    raiseFakeNotification();

    // wait for evication to finish
    Thread.sleep(10000);
    ((GemFireCacheImpl) MCacheFactory.getAnyInstance()).getTombstoneService()
        .forceBatchExpirationForTests(100);
    // let the tombstones go away
    Thread.sleep(20000);
    scanRecords(tableName, numRows * numBatches * 2);

    // TODO: stop eviction trigger, else the server will
    // keep evicting and will only add TOMBSTONES to the
    // last file which are small in size and the file will never get full
    verifyStoreDir(1, DISK_STORE_NAME);
  }

  private void verifyStoreDir(int expNumCrfs, String diskStoreName) {
    File storeDir = new File(".");
    File[] entries = storeDir.listFiles();
    int actualCount = 0;
    for (File file : entries) {
      if (file.getName().endsWith(".crf")
          && file.getName().startsWith("BACKUP" + diskStoreName + "_")) {
        actualCount++;
      }
    }
    Assert.assertEquals(expNumCrfs, actualCount);
  }


  @Test
  public void testRecovery5() throws InterruptedException {
    int numRows = 5;
    int numBatches = 10;
    int numSplits = 1;
    int redundancy = 0;
    String DISK_STORE_NAME = "testDiskStore";
    String tableName = "testRecovery5";
    createCache();
    createDiskStore(DISK_STORE_NAME, true, 1, true);
    createFTable(tableName, numSplits, redundancy, DISK_STORE_NAME);
    FTableDescriptor tableDescriptor =
        MCacheFactory.getAnyInstance().getFTable(tableName).getTableDescriptor();
    Assert.assertEquals(tableDescriptor.getDiskStore(), DISK_STORE_NAME);
    Assert.assertEquals(tableDescriptor.getDiskStoreAttributes().getEnableDeltaPersistence(), true);
    Assert.assertEquals(tableDescriptor.getDiskStoreAttributes().getMaxOplogSizeInBytes(), 1);
    StringBuilder sb = new StringBuilder(2080);
    for (int i = 0; i < 2600; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(8));
    }
    String data = sb.toString();
    appendBatchesWithLargeRecords(tableName, numRows, numBatches, data);
    scanRecords(tableName, numRows * numBatches);
    Thread.sleep(10000);

    closeCache();
    createCache();

    // wait for recovery
    Thread.sleep(60000);
    tableDescriptor = MCacheFactory.getAnyInstance().getFTable(tableName).getTableDescriptor();
    Assert.assertEquals(tableDescriptor.getDiskStore(), DISK_STORE_NAME);
    Assert.assertEquals(tableDescriptor.getDiskStoreAttributes().getEnableDeltaPersistence(), true);
    Assert.assertEquals(tableDescriptor.getDiskStoreAttributes().getMaxOplogSizeInBytes(), 1);

    scanRecords(tableName, numRows * numBatches);
    appendBatchesWithLargeRecords(tableName, numRows, numBatches, data);
    scanRecords(tableName, numRows * numBatches * 2);
  }


  @Test
  public void testDefaultDiskStore() {
    final String tableName = "testDefaultDiskStore";
    createCache();
    createFTable(tableName, 1);
    FTableDescriptor tableDescriptor =
        MCacheFactory.getAnyInstance().getFTable(tableName).getTableDescriptor();
    Assert.assertEquals(tableDescriptor.getDiskStore(), MTableUtils.DEFAULT_FTABLE_DISK_STORE_NAME);
    Assert.assertEquals(tableDescriptor.getDiskStoreAttributes(), null);
    Assert.assertEquals(
        ((DiskStoreImpl) MCacheFactory.getAnyInstance()
            .findDiskStore(MTableUtils.DEFAULT_FTABLE_DISK_STORE_NAME)).getEnableDeltaPersistence(),
        true);
    appendRecords(tableName);
    scanRecords(tableName, 10);
    closeCache();
    createCache();
    tableDescriptor = MCacheFactory.getAnyInstance().getFTable(tableName).getTableDescriptor();
    Assert.assertEquals(tableDescriptor.getDiskStore(), MTableUtils.DEFAULT_FTABLE_DISK_STORE_NAME);
    Assert.assertEquals(tableDescriptor.getDiskStoreAttributes(), null);
    Assert.assertEquals(
        ((DiskStoreImpl) MCacheFactory.getAnyInstance()
            .findDiskStore(MTableUtils.DEFAULT_FTABLE_DISK_STORE_NAME)).getEnableDeltaPersistence(),
        true);
    scanRecords(tableName, 10);
    appendRecords(tableName);
    scanRecords(tableName, 20);
    deleteFTable(tableName);
  }

  @Test
  public void testRecoveryWithPartialRecords() throws InterruptedException {
    //
    // oplog1: <------
    // -- key1: first record |
    // -- key1: delta record |
    // oplog2: |-----Files will be deleted when key1 is deleted/evicted
    // -- key1: delta record |
    // -- key1: delta record <-----|
    // oplog3:
    // -- key1: delta record <----- No block value will be present for this entry during recovery
    // -- key2: first record
    // -- key2: delta record
    // Evict/delete key1
    // close cache
    // create cache and recover

    int numRows = 1;
    int numBatches = 20;
    int numSplits = 1;
    int redundancy = 0;
    String DISK_STORE_NAME = "testDiskStoretestRecoveryWithPartialRecords";
    String tableName = "testRecoveryWithPartialRecords";
    createCache();

    createDiskStore(DISK_STORE_NAME, true, 1, true);
    createFTable(tableName, numSplits, redundancy, DISK_STORE_NAME, 5);
    StringBuilder sb = new StringBuilder(2080);
    for (int i = 0; i < 2600; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(8));
    }
    String data = sb.toString();
    // Create 10 blocks
    appendBatchesWithLargeRecords(tableName, numRows, numBatches, data);
    scanRecords(tableName, numRows * numBatches);
    Thread.sleep(10000);

    /* Delete first record */
    deleteFirstRecordOfTable(tableName);

    Thread.sleep(10000);
    ((GemFireCacheImpl) MCacheFactory.getAnyInstance()).getTombstoneService()
        .forceBatchExpirationForTests(100);
    // let the tombstones go away
    Thread.sleep(20000);

    closeCache();
    createCache();

    // wait for recovery
    Thread.sleep(60000);
    scanRecords(tableName, (numRows * numBatches) - 5);
  }

  private void deleteFirstRecordOfTable(String tableName) {
    Cache cache = CacheFactory.getAnyInstance();
    BucketRegion br =
        ((PartitionedRegion) cache.getRegion(tableName)).getDataStore().getLocalBucketById(0);
    ConcurrentSkipListMap<IMKey, RegionEntry> map =
        (ConcurrentSkipListMap<IMKey, RegionEntry>) ((RowTupleConcurrentSkipListMap) br
            .getRegionMap().getInternalMap()).getInternalMap();
    Iterator<Map.Entry<IMKey, RegionEntry>> itr = map.entrySet().iterator();
    IMKey key = itr.next().getKey();
    if (key instanceof BlockKey) {
      cache.getRegion(tableName).remove(key);
    }
  }

}
