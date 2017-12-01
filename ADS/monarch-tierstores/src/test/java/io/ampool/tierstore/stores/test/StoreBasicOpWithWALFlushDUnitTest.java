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

package io.ampool.tierstore.stores.test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.store.DefaultConstructorMissingException;
import io.ampool.store.IStoreResultScanner;
import io.ampool.store.StoreCreateException;
import io.ampool.store.StoreHandler;
import io.ampool.store.StoreRecord;
import io.ampool.store.StoreScan;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.stores.store.StoreDunitHelper;
import io.ampool.tierstore.wal.WALRecord;
import io.ampool.tierstore.wal.WALResultScanner;
import io.ampool.tierstore.wal.WriteAheadLog;
import junit.framework.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(FTableTest.class)
public class StoreBasicOpWithWALFlushDUnitTest extends StoreDunitHelper {

  public static final int NUM_BUCKETS = 113;

  public StoreBasicOpWithWALFlushDUnitTest() {
    super();
  }

  Host host = Host.getHost(0);
  private VM vm0 = host.getVM(0);
  private VM vm1 = host.getVM(1);
  private VM vm2 = host.getVM(2);
  private VM client1 = host.getVM(3);

  private final int NUM_OF_COLUMNS = 1;
  private final String TABLE_NAME = "StoreBasicOpWithWALFlushDUnitTest";
  private final String KEY_PREFIX = "KEY";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 100;
  private final String COLUMN_NAME_PREFIX = "COLUMN";
  private final byte[] byteData = new byte[] {0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 8};

  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);

    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            MCacheFactory.getAnyInstance().close();
            return null;
          }
        }));

    super.tearDown2();
  }

  private void createTable()
      throws ClassNotFoundException, DefaultConstructorMissingException, StoreCreateException {

    MCache cache = MCacheFactory.getAnyInstance();
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    Admin admin = cache.getAdmin();
    FTable table = admin.createFTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  private void appendRecordsToStoreHandler(int numRecords) {
    MCache cache = MCacheFactory.getAnyInstance();
    StoreHandler storeHandler = cache.getStoreHandler();
    assertNotNull(storeHandler);
    BlockValue bv = new BlockValue(1000);
    bv.checkAndAddRecord(byteData);

    for (int parId = 0; parId < NUM_BUCKETS; parId++) {
      for (int recIndex = 0; recIndex < numRecords; recIndex++) {
        try {
          storeHandler.append(TABLE_NAME, parId, new BlockKey(System.currentTimeMillis()), bv);
          System.out.println("TEST APPEND storeHandler.append append Done, itr = " + parId
              + " RecordIndex = " + recIndex);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private void scanStoreHandler(int numRecords) { // scan from storeHandler
    MCache cache = MCacheFactory.getAnyInstance();
    StoreHandler storeHandler = cache.getStoreHandler();
    for (int i = 0; i < NUM_BUCKETS; i++) {
      WALResultScanner scanner = storeHandler.getWALScanner(TABLE_NAME, i);
      WALRecord record = null;
      int count = 0;
      do {
        record = scanner.next();
        System.out.println("TEST SCAN storeHandler.getScanner.next done, iter = " + i);
        // assertNotNull(record);
        count++;
      } while (record != null);
      /// count - 1 as last null iteration will increase the count by 1
      assertEquals(numRecords, count - 1);
      // System.out.println("NNNN TEST Scan count = "+ count + " And numRecords = "+ numRecords);
    }
  }

  private void scanStoreHandlerAfterWALFlush(int numRecords) {
    MCache cache = MCacheFactory.getAnyInstance();
    StoreHandler storeHandler = cache.getStoreHandler();
    for (int i = 0; i < NUM_BUCKETS; i++) {
      // verify that no WAL specific disk files exist after flush WAL
      String[] walFiles =
          WriteAheadLog.getInstance().getAllFilesForTableWithBucketId(TABLE_NAME, i);
      assertEquals(walFiles.length, 0);

      IStoreResultScanner scanner = storeHandler.getStoreScanner(TABLE_NAME, i, null);
      StoreRecord record = null;
      Iterator itr = scanner.iterator();
      int count = 0;
      while (itr.hasNext()) {
        record = scanner.next();
        assertNotNull(record);
        count++;
      }
      // System.out.println("NNNN TEST AFTER FLUSH Scan count = " + count + " And numRecords = " +
      // numRecords);
      Assert.assertEquals(count, numRecords);
    }
  }

  private void invokWALFlush() { // scan from storeHandler
    MCache cache = MCacheFactory.getAnyInstance();
    StoreHandler storeHandler = cache.getStoreHandler();
    for (int parId = 0; parId < NUM_BUCKETS; parId++) {
      storeHandler.pauseWALMonitoring(TABLE_NAME, parId);
      storeHandler.flushWriteAheadLog(TABLE_NAME, parId);
      // System.out.println("TEST FLUSH storeHandler.flushWriteAheadLog iter = " + parId);
    }
    System.out.println("TEST WAL Flush done");
  }

  private byte[] incrementByteArray(byte[] arr) {
    int a = Bytes.toInt(arr);
    a++;
    return Bytes.toBytes(a);
  }

  private void createAndAppendRecordsToLocalStore() throws StoreCreateException,
      DefaultConstructorMissingException, ClassNotFoundException, IOException {
    TierStore localStore = StoreHandler.getInstance().getTierStore(TABLE_NAME, 1);
    TableDescriptor tableDescriptor = MCacheFactory.getAnyInstance().getTableDescriptor(TABLE_NAME);
    int numRecords = 5001;

    StoreRecord[] storeRecords = new StoreRecord[numRecords];
    for (int i = 0; i < storeRecords.length; i++) {
      final byte[] data1 = incrementByteArray(byteData);
      StoreRecord storeRecord = new StoreRecord(tableDescriptor.getAllColumnDescriptors().size());
      tableDescriptor.getAllColumnDescriptors().forEach((MCD) -> storeRecord.addValue(byteData));
      storeRecords[i] = storeRecord;
    }

    int append = localStore.getWriter(TABLE_NAME, 0).write(new Properties(), storeRecords);
    assertEquals(storeRecords.length, append);

    int count = 0;

    TierStoreReader localStoreReader = localStore.getReader(TABLE_NAME, 0);
    localStoreReader.setFilter(new StoreScan(new Scan()));
    Iterator<StoreRecord> iterator = localStoreReader.iterator();
    assertNotNull(iterator);
    byte[] data1 = byteData;
    StoreRecord result = null;

    while (iterator.hasNext()) {
      result = iterator.next();
      if (result != null) {
        assertEquals(count, result.getTimeStamp());
        assertEquals(0, Bytes.compareTo(data1, (byte[]) result.getValues()[0]));
        count++;
      }
      data1 = incrementByteArray(data1);
    }

    assertEquals(10, count);

    for (int i = 1; i < 10; i++) {
      append = localStore.getWriter(TABLE_NAME, i).write(new Properties(), storeRecords);
      assertEquals(storeRecords.length, append);
    }

    for (int i = 1; i < 10; i++) {
      count = 0;
      localStoreReader = localStore.getReader(TABLE_NAME, i);
      localStoreReader.setFilter(new StoreScan(new Scan()));
      iterator = localStoreReader.iterator();
      assertNotNull(iterator);
      data1 = byteData;
      result = null;

      while (iterator.hasNext()) {
        result = iterator.next();
        if (result != null) {
          assertEquals(count, result.getTimeStamp());
          assertEquals(0, Bytes.compareTo(data1, (byte[]) result.getValues()[0]));
          count++;
        }
        data1 = incrementByteArray(data1);
      }

      assertEquals(numRecords - 1, count);
    }

    append = localStore.getWriter(TABLE_NAME, 11).write(new Properties(), storeRecords);
    assertEquals(storeRecords.length, append);

    count = 0;
    localStoreReader = localStore.getReader(TABLE_NAME, 11);
    localStoreReader.setFilter(new StoreScan(new Scan()));
    iterator = localStoreReader.iterator();
    assertNotNull(iterator);
    data1 = byteData;
    result = null;

    while (iterator.hasNext()) {
      result = iterator.next();
      if (result != null) {
        assertEquals(count, result.getTimeStamp());
        assertEquals(0, Bytes.compareTo(data1, (byte[]) result.getValues()[0]));
        count++;
      }
      data1 = incrementByteArray(data1);
    }

    localStore.deleteTable(TABLE_NAME);
  }

  private void doCreateTable(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable();
        return null;
      }
    });
  }

  private void doAppendRecordsToStoreHandler(VM vm, int numRecords) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        appendRecordsToStoreHandler(numRecords);
        return null;
      }
    });
  }

  private void doScanStoreHandler(VM vm, int numRecords) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        scanStoreHandler(numRecords);
        return null;
      }
    });
  }

  private void doScanStoreHandlerAfterWALFlush(VM vm, int numRecords) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        scanStoreHandlerAfterWALFlush(numRecords);
        return null;
      }
    });
  }

  private void doFlushWAL(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        invokWALFlush();
        return null;
      }
    });
  }

  private void doPauseWAL(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        StoreHandler storeHandler = cache.getStoreHandler();
        for (int parId = 0; parId < NUM_BUCKETS; parId++) {
          storeHandler.pauseWALMonitoring(TABLE_NAME, parId);
        }
        return null;
      }
    });
  }

  private void doCreateAndAppendRecordsToLocalStore(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createAndAppendRecordsToLocalStore();
        return null;
      }
    });
  }

  @Test
  public void testWALFlushWithAllInProgressFiles() {
    testBasicOpWithWALFlush(WriteAheadLog.WAL_FILE_RECORD_LIMIT / 2);
  }

  @Test
  public void testWALFlushWithInProgressAndDoneFiles() {
    testBasicOpWithWALFlush(WriteAheadLog.WAL_FILE_RECORD_LIMIT + 5);
  }

  /**
   * Test-case explained : All WAL files are in-progress. There are total 3 servers, vm0, vm1, vm2
   * with each having 113 buckets/partitions. Append records in each bucket of all the servers such
   * a way that all WAL files are in-progress state. do a scan and fetch the records, this will be
   * scan on a wal file. invoke a WAL flush for a given table and each partitionID using
   * storeHandler.flushWriteAheadLog(TN, PID) Run a store scanner to verify that WAL records are
   * pushed to store and WAL files are getting deleted after a flush.
   */
  private void testBasicOpWithWALFlush(int numRecords) {
    doCreateTable(client1);
    doPauseWAL(vm0);
    doPauseWAL(vm1);
    doPauseWAL(vm2);

    System.out.println("------------APPEND Records---------------");
    doAppendRecordsToStoreHandler(vm0, numRecords);
    doAppendRecordsToStoreHandler(vm1, numRecords);
    doAppendRecordsToStoreHandler(vm2, numRecords);
    System.out.println("------------SCAN Records---------------");
    doScanStoreHandler(vm0, numRecords);
    doScanStoreHandler(vm1, numRecords);
    doScanStoreHandler(vm2, numRecords);
    System.out.println("------------FLUSH WAL---------------");
    doFlushWAL(vm0);
    doFlushWAL(vm1);
    doFlushWAL(vm2);
    System.out.println("------------SCAN Records---------------");
    doScanStoreHandlerAfterWALFlush(vm0, numRecords);
    doScanStoreHandlerAfterWALFlush(vm1, numRecords);
    doScanStoreHandlerAfterWALFlush(vm2, numRecords);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  private void spawnNThreadsDoAppend(VM vm, int feederThreads) {
    vm.invoke(new FeederSerializableCallable(TABLE_NAME));
  }

  private AsyncInvocation spawnNThreadsDoScan(VM vm, int feederThreads) {
    return vm.invokeAsync(new ScannerSerializable(TABLE_NAME));
  }

  private void stopFeederThread(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        // Give you set of Threads
        Set<Thread> setOfThread = Thread.getAllStackTraces().keySet();

        // Iterate over set to find yours
        for (Thread thread : setOfThread) {
          if ("ftable_feeder".equalsIgnoreCase(thread.getName())) {
            System.out.println("TEST StopFeeder interrupting thread name = " + thread.getName());
            thread.interrupt();
          }
        }
        return null;
      }
    });
  }

  public void WaitForScannerThreadToDone(final AsyncInvocation[] ais) throws InterruptedException {
    for (final AsyncInvocation ai : ais) {
      ai.join();
    }
  }

  public void doStopFeederThreadsOnAllServers( /* List feederThreads */) {
    stopFeederThread(this.vm0);
    stopFeederThread(this.vm1);
    stopFeederThread(this.vm2);
  }

  public AsyncInvocation[] doParallelScanOnAllServers(int numThreads) {
    /* Spawn a specified thread and do append */
    return new AsyncInvocation[] {spawnNThreadsDoScan(this.vm0, numThreads),
        spawnNThreadsDoScan(this.vm1, numThreads), spawnNThreadsDoScan(this.vm2, numThreads)};
  }

  public void doAppendRecordsOnAllServers(int numThreads) {
    /* Spawn a thread and do append */
    spawnNThreadsDoAppend(this.vm0, numThreads);
    spawnNThreadsDoAppend(this.vm1, numThreads);
    spawnNThreadsDoAppend(this.vm2, numThreads);
  }

  @Test
  public void testWALFlushWithParallelOps() throws InterruptedException {
    doTestWALFlushWithOpsInParallel();
  }

  private void doTestWALFlushWithOpsInParallel() throws InterruptedException {
    // Span feeder clients that do continuous ingestions
    // per server feeder threads.
    doCreateTable(client1);

    int numFeederThreads = 1;

    // Do append some initial records
    doAppendRecordsToStoreHandler(vm0, 100);
    doAppendRecordsToStoreHandler(vm1, 100);
    doAppendRecordsToStoreHandler(vm2, 100);

    // spawn a append thread on all server VM and do random append
    doAppendRecordsOnAllServers(numFeederThreads);

    // spawn a scanner thread on all server VM and do bucket scan
    AsyncInvocation[] ais = doParallelScanOnAllServers(numFeederThreads);
    Thread.sleep(5000);
    System.out.println(" ----- do flush to WAL ------- ");
    doFlushWAL(vm0);
    doFlushWAL(vm1);
    doFlushWAL(vm2);
    System.out.println(" ----- waiting for scanner -------");
    WaitForScannerThreadToDone(ais);
    doStopFeederThreadsOnAllServers();
    System.out.println("NNN TEST completed");
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);

  }
}


class FeederSerializableCallable extends SerializableCallable {

  public static boolean feederCompleted = false;

  private String tableName;

  public FeederSerializableCallable(String name) {
    tableName = name;
  }

  @Override
  public Object call() throws Exception {
    // Do a append here
    MCache cache = MCacheFactory.getAnyInstance();
    StoreHandler storeHandler = cache.getStoreHandler();
    assertNotNull(storeHandler);

    FeederThread ft = new FeederThread(tableName);
    Thread feedar = new Thread(ft);
    feedar.setName("ftable_feeder");
    feedar.start();
    return null;
  }

  public static boolean isFeederCompleted() {
    Set<Thread> setOfThread = Thread.getAllStackTraces().keySet();
    // Iterate over set to find yours
    for (Thread thread : setOfThread) {
      if ("ftable_feeder".equalsIgnoreCase(thread.getName())) {
        System.out.println("TEST StopFeeder interrupting thread name = " + thread.getName());
        if (!thread.isAlive()) {
          return true;
        }
      }
    }
    return false;
  }
}


class FeederThread implements Runnable {
  private volatile boolean shutdown;
  private final byte[] byteData = new byte[] {12, 45, 65, 66, 67, 75, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  private String tableName;

  public FeederThread(String tName) {
    shutdown = false;
    tableName = tName;
  }

  @Override
  public void run() {
    // Do a append here
    MCache cache = MCacheFactory.getAnyInstance();
    StoreHandler storeHandler = cache.getStoreHandler();
    assertNotNull(storeHandler);

    // Now pick a random bucket out of total bucket and do ingestion
    Random r = new Random();
    do {
      try {
        int bucketId = r.nextInt(113);

        try {
          storeHandler.append(tableName, bucketId, new BlockKey(System.currentTimeMillis()), null);
          // System.out.println("TEST FeederThread.run Random append on bID = "+ bucketId);
        } catch (IOException e) {
          e.printStackTrace();
        }
        Thread.sleep(10);
      } catch (InterruptedException e) {
        // Allow thread to shutdown
        shutdown = true;
      }
    } while (!shutdown);

    System.out.println("NNNN TEST FeederThread.run Append stopped ");
  }

  public void shutdown() {
    shutdown = true;
  }
}


class ScannerSerializable extends SerializableRunnable {

  public boolean isCompleted = false;
  String tableName;
  boolean checkScannerStatus = false;
  private final String TABLE_NAME = "StoreBasicOpWithWALFlushDUnitTest";

  public ScannerSerializable(String name) {
    tableName = name;
    checkScannerStatus = false;
  }

  public ScannerSerializable(String name, boolean checkStatus) {
    tableName = name;
    checkScannerStatus = checkStatus;
  }

  @Override
  public void run() throws Exception {
    MCache cache = MCacheFactory.getAnyInstance();
    StoreHandler storeHandler = cache.getStoreHandler();
    Random r = new Random();
    int bucketCounter = 0;
    while (bucketCounter < 113) {
      bucketCounter++;
      int bucketId = r.nextInt(113);
      final WALResultScanner walScanner = storeHandler.getWALScanner(tableName, bucketId);
      WALRecord record = null;
      int count = 0;
      do {
        try {
          record = walScanner.next();
          // System.out.println("TEST SCAN doContinuousScan done, BID = " + bucketId + " Count = " +
          // count);
          assertNotNull(record);
          count++;
          Thread.sleep(10);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } while (count < 50);
      System.out.println("TEST TotalBucket scanned = " + bucketCounter);
    }
  }

  public static void checkAndWait() {}
}
