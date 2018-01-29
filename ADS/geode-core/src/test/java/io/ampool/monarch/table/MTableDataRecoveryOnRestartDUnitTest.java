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

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.RowKeyDoesNotExistException;
import io.ampool.monarch.table.internal.MTableUtils;
import junit.framework.TestCase;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.MTableRangePartitionResolver;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 *
 * Dunit test to verify data recovery usecase after all servers in a cluster are down.
 */

@Category(MonarchTest.class)
public class MTableDataRecoveryOnRestartDUnitTest extends MTableDUnitHelper {
  public MTableDataRecoveryOnRestartDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  private List<VM> allServerVms = null;

  private final int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "MTableDiskPersistDUnitTest";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 1000;
  private final String COLUMN_NAME_PREFIX = "COLUMN";

  // new tables
  private final String TABLE_NAME_1 = "MTableDiskPersistDUnitTest_1";
  private final String TABLE_NAME_2 = "MTableDiskPersistDUnitTest_2";
  private final String TABLE_NAME_3 = "MTableDiskPersistDUnitTest_3";
  private final String TABLE_NAME_4 = "MTableDiskPersistDUnitTest_4";

  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    allServerVms = new ArrayList<>(Arrays.asList(this.vm0, this.vm1, this.vm2));
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    // createClientCache(this.client1);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    // closeMClientCache(client1);

    allServerVms.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    }));

    super.tearDown2();
  }

  private List<byte[]> doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);

    List<byte[]> allKeys = new ArrayList<>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> KEY_LIST.forEach((KEY) -> {
      allKeys.add(KEY);
    }));

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    return allKeys;
  }

  private List<byte[]> getKeys(boolean doPuts) {
    MTable table_1 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_1);
    MTable table_3 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_3);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table_1.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);

    List<byte[]> allKeys = new ArrayList<>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> KEY_LIST.forEach((KEY) -> {
      allKeys.add(KEY);
    }));

    assertEquals((table_1.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS),
        allKeys.size());

    if (doPuts) {
      allKeys.forEach((K) -> {
        Put record = new Put(K);
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex));
        }
        table_1.put(record);
        table_3.put(record);
      });
    }

    return allKeys;
  }

  protected void createTable() {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(3);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    // tableDescriptor.setTotalNumOfSplits(10);

    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME + "-1", tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  protected void createTables() {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    // create ordered_table-1
    {
      MTableDescriptor tableDescriptor = new MTableDescriptor();
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.setRedundantCopies(2);
      tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
      // tableDescriptor.setTotalNumOfSplits(10);

      Admin admin = clientCache.getAdmin();
      MTable table = admin.createMTable(TABLE_NAME_1, tableDescriptor);
      assertEquals(table.getName(), TABLE_NAME_1);
      assertNotNull(table);
    }

    // create ordered_table-2
    {
      MTableDescriptor tableDescriptor = new MTableDescriptor();
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.setRedundantCopies(2);
      tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
      // tableDescriptor.setTotalNumOfSplits(10);

      Admin admin = clientCache.getAdmin();
      MTable table = admin.createMTable(TABLE_NAME_2, tableDescriptor);
      assertEquals(table.getName(), TABLE_NAME_2);
      assertNotNull(table);
    }

    // create Unordered_table-1
    {
      MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.setRedundantCopies(2);
      tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
      // tableDescriptor.setTotalNumOfSplits(10);

      Admin admin = clientCache.getAdmin();
      MTable table = admin.createMTable(TABLE_NAME_3, tableDescriptor);
      assertEquals(table.getName(), TABLE_NAME_3);
      assertNotNull(table);
    }

    // create Unordered_table-1
    {
      MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.setRedundantCopies(2);
      tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
      // tableDescriptor.setTotalNumOfSplits(10);

      Admin admin = clientCache.getAdmin();
      MTable table = admin.createMTable(TABLE_NAME_4, tableDescriptor);
      assertEquals(table.getName(), TABLE_NAME_4);
      assertNotNull(table);
    }

  }

  private void doGets(List<byte[]> keys) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    keys.forEach((K) -> {
      Get get = new Get(K);
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        // DEBUG_STATEMENTS
        /*
         * System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
         * System.out.println("actualColumnName   => " + Arrays.toString(cell.getColumnName()));
         */

        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out
              .println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
        // DEBUG_STATEMENTS
        /*
         * System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
         * System.out.println("actualValue    => " + Arrays.toString((byte[])
         * cell.getColumnValue()));
         */

        if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
          System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
          System.out.println(
              "actualValue    => " + Arrays.toString((byte[]) row.get(i).getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex++;
      }
    });
  }

  private void stopAllCacheServers() {
    allServerVms.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        Iterator iter = MCacheFactory.getAnyInstance().getCacheServers().iterator();
        if (iter.hasNext()) {
          CacheServer server = (CacheServer) iter.next();
          server.stop();
        }
        cache.close();
        return null;
      }
    }));
  }

  private void startAllServerAsync(final String locatorString) {
    AsyncInvocation vm0Task = vm0.invokeAsync(new SerializableCallable() {
      @Override
      public Object call() throws Exception {

        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system_after_restart.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locatorString);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");

        MCache cache = null;
        try {
          cache = MCacheFactory.getAnyInstance();
          cache.close();
        } catch (CacheClosedException cce) {
        }
        cache = MCacheFactory.create(getSystem(props));
        CacheServer s = cache.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        System.out.println("MTableDiskPersistDUnitTest.call.VM0.PORT = " + port);
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        return null;
      }
    });


    AsyncInvocation vm1Task = vm1.invokeAsync(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system_after_restart.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locatorString);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
        MCache cache = null;
        try {
          cache = MCacheFactory.getAnyInstance();
          cache.close();
        } catch (CacheClosedException cce) {
        }
        cache = MCacheFactory.create(getSystem(props));
        CacheServer s = cache.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        System.out.println("MTableDiskPersistDUnitTest.call.VM1.PORT = " + port);
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        return null;
      }
    });

    AsyncInvocation vm2Task = vm2.invokeAsync(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system_after_restart.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locatorString);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
        MCache cache = null;
        try {
          cache = MCacheFactory.getAnyInstance();
          cache.close();
        } catch (CacheClosedException cce) {
        }
        cache = MCacheFactory.create(getSystem(props));
        CacheServer s = cache.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        System.out.println("MTableDiskPersistDUnitTest.call.VM2.PORT = " + port);
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        return null;
      }
    });


    try {
      vm0Task.join();
      vm1Task.join();
      vm2Task.join();

    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void countPrimaryBuckets(final String state) {
    allServerVms.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        Region r = cache.getRegion(TABLE_NAME);
        System.out.println("MTableDiskPersistDUnitTest.call.R.TYPE " + r.getClass().getName());
        PartitionedRegion pr = (PartitionedRegion) cache.getRegion(TABLE_NAME);
        if (pr == null) {
          System.out.println("MTableDiskPersistDUnitTest.call.PR.NULL");
        }
        assertNotNull(pr);
        System.out.println("MTableDiskPersistDUnitTest.call " + pr);

        RegionAttributes<?, ?> rattr = pr.getAttributes();
        PartitionResolver<?, ?> presolver = rattr.getPartitionAttributes().getPartitionResolver();
        assertTrue(presolver instanceof MTableRangePartitionResolver);


        System.out.println(state + " NUMBER OF PRIMARY BUCKETS  => "
            + pr.getDataStore().getAllLocalPrimaryBucketRegions().size());
        System.out.println(state + " TOTAL NUMBER OF BUCKETS => " + pr.getTotalNumberOfBuckets());


        /*
         * final Region<?, ?> metaTableBktIdToSLocRegion =
         * cache.getRegion(MTableUtils.getMetaRegionBktIdToSLocName()); Set<?> keySetOnServer =
         * metaTableBktIdToSLocRegion.keySet(); System.out.println(state +
         * " BUCKET ID TO SERVER LOCATION SIZE => " + keySetOnServer.size());
         */

        return null;
      }
    }));
  }

  private void restart() {
    closeMClientCache();
    stopAllCacheServers();

    startAllServerAsync(DUnitLauncher.getLocatorString());
    createClientCache();

  }

  private void doSelectedGets(List<byte[]> listOfKeys, int count, final String when) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int keyIndex = 0; keyIndex < count; keyIndex++) {
      Get get = new Get(listOfKeys.get(keyIndex));
      for (int colIdx = 0; colIdx < 5; colIdx++) {
        get.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colIdx));
      }
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS - 5, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (Cell cell : row) {
        Assert.assertNotEquals(5, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        // System.out.println("actualColumnName => " + Arrays.toString(cell.getColumnName()));

        if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
          System.out.println(
              "[ " + when + " ] expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out.println(
              "[ " + when + " ] actualColumnName   => " + Arrays.toString(cell.getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
        // System.out.println("actualValue => " + Arrays.toString((byte[]) cell.getColumnValue()));

        if (!Bytes.equals(exptectedValue, (byte[]) cell.getColumnValue())) {
          System.out
              .println("[ " + when + " ] exptectedValue => " + Arrays.toString(exptectedValue));
          System.out.println("[ " + when + " ] actualValue    => "
              + Arrays.toString((byte[]) cell.getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex++;
      }
    }
  }

  // Data recovery test with multithreaded random ops.
  public void populateRowData(Put put, MTable table) {

    for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
      put.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
          Bytes.toBytes(VALUE_PREFIX + "-updated-" + columnIndex));
    }
  }

  private class PutWorker implements Callable<Integer> {
    private int totalRequests;
    private List<byte[]> keys;

    public PutWorker(int numTimes, List<byte[]> keys) {
      this.totalRequests = numTimes;
      this.keys = keys;
    }

    @Override
    public Integer call() throws Exception {
      int counter = 0;
      while (counter < totalRequests) {
        int indexRange = (keys.size() - 1 - 0) + 1 + 0;
        int index = new Random().nextInt(keys.size());
        Put put = new Put(keys.get(index));

        // table-1
        {
          MTable table_1 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_1);
          populateRowData(put, table_1);
          table_1.put(put);
          Get get = new Get(keys.get(index));
          Row result = table_1.get(get);
        }

        // table-2
        {
          MTable table_2 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_2);
          populateRowData(put, table_2);
          table_2.put(put);
          Get get = new Get(keys.get(index));
          Row result = table_2.get(get);
        }

        // table-3
        {
          MTable table_3 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_3);
          populateRowData(put, table_3);
          table_3.put(put);
          Get get = new Get(keys.get(index));
          Row result = table_3.get(get);
        }

        // table-4
        {
          MTable table_4 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_4);
          populateRowData(put, table_4);
          table_4.put(put);
          Get get = new Get(keys.get(index));
          Row result = table_4.get(get);
        }

        counter++;
      }
      return counter;
    }
  }

  private class GetWorker implements Callable<Integer> {

    private int totalRequests;
    private List<byte[]> keys;

    public GetWorker(int numTimes, List<byte[]> keys) {
      this.totalRequests = numTimes;
      this.keys = keys;
    }

    @Override
    public Integer call() throws Exception {
      int counter = 0;
      while (counter < totalRequests) {
        int indexRange = (keys.size() - 1 - 0) + 1 + 0;
        int index = new Random().nextInt(keys.size());
        Put put = new Put(keys.get(index));

        // Put-Get ops on table-1
        {
          MTable table_1 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_1);
          populateRowData(put, table_1);
          table_1.put(put);
          Get get = new Get(keys.get(index));
          Row result = table_1.get(get);
        }

        // Put-Get ops on table-2
        {
          MTable table_2 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_2);
          populateRowData(put, table_2);
          table_2.put(put);
          Get get = new Get(keys.get(index));
          Row result = table_2.get(get);
        }

        // Put-Get ops on table-3
        {
          MTable table_3 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_3);
          populateRowData(put, table_3);
          table_3.put(put);
          Get get = new Get(keys.get(index));
          Row result = table_3.get(get);
        }

        // Put-Get ops on table-4
        {
          MTable table_4 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_4);
          populateRowData(put, table_4);
          table_4.put(put);
          Get get = new Get(keys.get(index));
          Row result = table_4.get(get);
        }

        counter++;
      }
      return counter;
    }
  }

  private class DestroyWorker implements Callable<Integer> {

    private int totalRequests;
    private List<byte[]> keys;

    public DestroyWorker(int numTimes, List<byte[]> keys) {
      this.totalRequests = numTimes;
      this.keys = keys;
    }

    @Override
    public Integer call() throws Exception {
      int counter = 0;
      // Set<Integer> deletedKeys = new HashSet<>();
      List<byte[]> deletedKeys = new ArrayList<>();

      while (counter < totalRequests) {
        int indexRange = (keys.size() - 1 - 0) + 1 + 0;
        int index = new Random().nextInt(keys.size());

        deletedKeys.add(keys.get(index));
        Delete delete = new Delete(keys.get(index));

        // Delete op on table-1
        try {
          {
            MTable table_1 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_1);
            table_1.delete(delete);
          }

          // Delete op on table-2
          {
            MTable table_2 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_2);
            table_2.delete(delete);
          }

          // Delete op on table-3
          {
            MTable table_3 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_3);
            table_3.delete(delete);
          }

          // Delete op on table-4
          {
            MTable table_4 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_4);
            table_4.delete(delete);
          }

          // System.out.println("DestroyWorker.call delete key");

        } catch (RowKeyDoesNotExistException re) {
          logger.error("NNN DestroyWorker.call cought RowKeyDoesNotExistException");
        } catch (Exception e) {
          logger.error("Unexpected table.delete Error cought : " + e.getMessage());
        } finally {
          counter++;
        }
      }

      // DEBUG_STATEMENT
      // writeKeysToFile(deletedKeys, "deletedKeys.txt-"+Thread.currentThread().getId());
      return deletedKeys.size();
    }
  }

  private void doOpsInMultipleThreads(List<byte[]> keys)
      throws ExecutionException, InterruptedException {
    List<Future<Integer>> getList = new ArrayList<Future<Integer>>();
    List<Future<Integer>> putList = new ArrayList<Future<Integer>>();
    List<Future<Integer>> destroyList = new ArrayList<Future<Integer>>();

    ExecutorService es = Executors.newFixedThreadPool(8);

    int numTimes = 50000;
    int numTasks = 12;
    // create tasks
    for (int i = 1; i <= numTasks; i++) {
      if (i % 3 == 0) {
        putList.add(es.submit(new PutWorker(numTimes, keys)));
      } else if (i % 2 == 0) {
        getList.add(es.submit(new GetWorker(numTimes, keys)));
      } else {
        destroyList.add(es.submit(new DestroyWorker(numTimes, keys)));
      }
    }


    for (Future<Integer> result1 : putList) {
      logger.info("ExecutorTest.main Put => #times = " + result1.get());
    }

    for (Future<Integer> result2 : getList) {
      logger.info("ExecutorTest.main Get => #times = " + result2.get());
    }

    for (Future<Integer> result3 : destroyList) {
      logger.info("NNN ExecutorTest.main Destroyed Keys size => #times = " + result3.get());
    }

    System.out.println("ExecutorTest.main : Done with waiting!");

    es.shutdown();

    while (!es.isTerminated()) {
    }
    System.out.println("Finished all threads");
  }

  public void verifyBucketCount(MTable mTable) {
    Map<Integer, Pair<ServerLocation, Long>> primaryMap = new HashMap<>(10);
    Map<Integer, Set<Pair<ServerLocation, Long>>> secondaryMap = new HashMap<>(10);
    MTableUtils.getLocationAndCount(mTable, primaryMap, secondaryMap);

    long totalCount = MTableUtils.getTotalCount(mTable, null, null, true);
    long primaryCount = primaryMap.values().stream().mapToLong(Pair::getSecond).sum();
    long secondaryCount = secondaryMap.values().stream()
        .mapToLong(x -> x.stream().mapToLong(Pair::getSecond).sum()).sum();

    System.out.println("NNN verifyBucketCount Table ==> " + mTable.getName() + " totalCount= "
        + totalCount + " primaryCount = " + primaryCount + " secCount = " + secondaryCount);
    TestCase.assertEquals(totalCount, primaryCount);
    // Since we have redundant copies set to 2, we will have double the totalcount
    TestCase.assertEquals(totalCount,
        secondaryCount / mTable.getTableDescriptor().getRedundantCopies());
  }

  @Test
  public void testDataRecoveryWithMultiThreadedOps() throws InterruptedException {
    createTables();

    // counters
    int preCount_1 = 0;
    int postCount_1 = 0;
    int preCount_2 = 0;
    int postCount_2 = 0;
    int preCount_3 = 0;
    int postCount_3 = 0;
    int preCount_4 = 0;
    int postCount_4 = 0;

    List<byte[]> listOfKeys = getKeys(true);
    System.out.println("NNNN start keys.size = " + listOfKeys.size());

    // DEBUG_STATEMENTS
    // countPrimaryBuckets("[ BEFORE RESTART ]");
    // writeKeysToFile(listOfKeys, "insertedKeys.txt");

    // start threads doing random ops
    try {
      doOpsInMultipleThreads(listOfKeys);
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // wait for sometime so cluster gets into stable state.
    Thread.sleep(1000);

    List<byte[]> preScannedKeys_1 = new ArrayList<>();
    List<byte[]> preScannedKeys_2 = new ArrayList<>();
    List<byte[]> preScannedKeys_3 = new ArrayList<>();
    List<byte[]> preScannedKeys_4 = new ArrayList<>();

    List<byte[]> postScannedKeys_1 = new ArrayList<>();
    List<byte[]> postScannedKeys_2 = new ArrayList<>();
    List<byte[]> postScannedKeys_3 = new ArrayList<>();
    List<byte[]> postScannedKeys_4 = new ArrayList<>();

    // Get the table count for table_1
    {
      MTable table_1 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_1);
      Scanner preScanner =
          table_1.getScanner(new Scan()/* .addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0)) */);
      Row preRecord = preScanner.next();
      preCount_1 = 0;

      while (preRecord != null) {
        preScannedKeys_1.add(preRecord.getRowId());

        // DEBUG_STATEMENTS
        /*
         * System.out.println("Row Id -> " + Arrays.toString(preRecord.getRowId()) + " , " +
         * Arrays.toString(preRecord.getCells().get(0).getColumnName()) + " , " +
         * Arrays.toString((byte[]) preRecord.getCells().get(0).getColumnValue()));
         */

        preRecord = preScanner.next();
        preCount_1++;
      }
      System.out.println("NNNN Table-1 preCount = " + preCount_1);
      preScanner.close();

      // DEBUG_STATEMENTS
      // write keys found in scan result to file.
      // writeKeysToFile(preScannedKeys_1, "preScan1.txt");

      verifyBucketCount(table_1);
    }

    // Get the table count for table_2
    {
      MTable table_2 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_2);
      Scanner preScanner = table_2.getScanner(new Scan());
      Row preRecord = preScanner.next();
      preCount_2 = 0;

      while (preRecord != null) {
        preScannedKeys_2.add(preRecord.getRowId());
        // DEBUG_STATEMENTS
        /*
         * System.out.println("Row Id -> " + Arrays.toString(preRecord.getRowId()) + " , " +
         * Arrays.toString(preRecord.getCells().get(0).getColumnName()) + " , " +
         * Arrays.toString((byte[]) preRecord.getCells().get(0).getColumnValue()));
         */

        preRecord = preScanner.next();
        preCount_2++;
      }
      System.out.println("NNNN Table-2 preCount = " + preCount_2);
      preScanner.close();

      // DEBUG_STATEMENTS
      // write keys found in scan result to file.
      // writeKeysToFile(preScannedKeys_2, "preScan2.txt");

      verifyBucketCount(table_2);
    }

    // Get the table count for table_3
    {
      MTable table_3 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_3);
      Scanner preScanner = table_3.getScanner(new Scan());
      Row preRecord = preScanner.next();
      preCount_3 = 0;

      while (preRecord != null) {
        preScannedKeys_3.add(preRecord.getRowId());
        // DEBUG_STATEMENTS
        /*
         * System.out.println("Row Id -> " + Arrays.toString(preRecord.getRowId()) + " , " +
         * Arrays.toString(preRecord.getCells().get(0).getColumnName()) + " , " +
         * Arrays.toString((byte[]) preRecord.getCells().get(0).getColumnValue()));
         */

        preRecord = preScanner.next();
        preCount_3++;
      }
      System.out.println("NNNN Table-3 preCount = " + preCount_3);
      preScanner.close();
      // DEBUG_STATEMENTS
      // write keys found in scan result to file.
      // writeKeysToFile(preScannedKeys_3, "preScan3.txt");

      verifyBucketCount(table_3);
    }

    // Get the table count for table_4
    {
      MTable table_4 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_4);
      Scanner preScanner = table_4.getScanner(new Scan());
      Row preRecord = preScanner.next();
      preCount_4 = 0;
      // List<byte[]> scannedKeys = new ArrayList<>();
      while (preRecord != null) {
        preScannedKeys_4.add(preRecord.getRowId());
        // DEBUG_STATEMENTS
        /*
         * System.out.println("Row Id -> " + Arrays.toString(preRecord.getRowId()) + " , " +
         * Arrays.toString(preRecord.getCells().get(0).getColumnName()) + " , " +
         * Arrays.toString((byte[]) preRecord.getCells().get(0).getColumnValue()));
         */

        preRecord = preScanner.next();
        preCount_4++;
      }
      System.out.println("NNNN Table-4 preCount = " + preCount_4);
      preScanner.close();

      // DEBUG_STATEMENTS
      // write keys found in scan result to file.
      // writeKeysToFile(preScannedKeys_4, "preScan4.txt");

      verifyBucketCount(table_4);
    }

    System.out.println("----------------------- RESTARTING SERVERS ----------------------------");
    restart();
    System.out.println("----------------------- RESTARTING SERVERS DONE -----------------------");

    // DEBUG_STATEMENTS
    // countPrimaryBuckets("[ AFTER RESTART ]");
    // doGets(listOfKeys);
    // doSelectedGets(listOfKeys, 1, "AFTER RESTART ");
    // post restart table_1

    Thread.sleep(60000);
    {
      MTable postTable_1 = MClientCacheFactory.getAnyInstance().getMTable(TABLE_NAME_1);
      assertNotNull(postTable_1);

      Scanner postScanner =
          postTable_1.getScanner(new Scan()/* .addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0)) */);
      Row postRecord = postScanner.next();
      postCount_1 = 0;

      while (postRecord != null) {
        postScannedKeys_1.add(postRecord.getRowId());
        // DEBUG_STATEMENTS
        /*
         * System.out.println("Row Id -> " + Arrays.toString(postRecord.getRowId()) + " , " +
         * Arrays.toString(postRecord.getCells().get(0).getColumnName()) + " , " +
         * Arrays.toString((byte[]) postRecord.getCells().get(0).getColumnValue()));
         */

        postRecord = postScanner.next();
        postCount_1++;
      }

      System.out.println("NNNN postCount = " + postCount_1);
      postScanner.close();

      // DEBUG_STATEMENTS
      // write keys found in scan result to file.
      // writeKeysToFile(postScannedKeys_1, "postScan1.txt");


      System.out.println("NNNN preScannedKeys_1.size = " + preScannedKeys_1.size());

      // DEBUG_STATEMENTS
      // writeKeysToFile(postScannedKeys_1, "t1_afterRec_extraKeys.txt");

      // verify the count
      assertEquals(preCount_1, postCount_1);

      verifyBucketCount(postTable_1);
    }

    // post restart table_2
    {
      MTable postTable_2 = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME_2);
      assertNotNull(postTable_2);

      Scanner postScanner =
          postTable_2.getScanner(new Scan()/* .addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0)) */);
      Row postRecord = postScanner.next();
      postCount_2 = 0;

      while (postRecord != null) {
        postScannedKeys_2.add(postRecord.getRowId());
        // DEBUG_STATEMENTS
        /*
         * System.out.println("Row Id -> " + Arrays.toString(postRecord.getRowId()) + " , " +
         * Arrays.toString(postRecord.getCells().get(0).getColumnName()) + " , " +
         * Arrays.toString((byte[]) postRecord.getCells().get(0).getColumnValue()));
         */

        postRecord = postScanner.next();
        postCount_2++;
      }

      System.out.println("NNNN postCount = " + postCount_2);
      postScanner.close();

      // DEBUG_STATEMENTS
      // write keys found in scan result to file.
      // writeKeysToFile(postScannedKeys_2, "postScan2.txt");
      // writeKeysToFile(postScannedKeys_2, "t2_afterRec_extraKeys.txt");

      // verify the count
      assertEquals(preCount_2, postCount_2);

      verifyBucketCount(postTable_2);
    }

    // post restart table_3
    {
      MTable postTable_3 = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME_3);
      assertNotNull(postTable_3);

      Scanner postScanner =
          postTable_3.getScanner(new Scan()/* .addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0)) */);
      Row postRecord = postScanner.next();
      postCount_3 = 0;

      while (postRecord != null) {
        postScannedKeys_3.add(postRecord.getRowId());
        // DEBUG_STATEMENTS
        /*
         * System.out.println("Row Id -> " + Arrays.toString(postRecord.getRowId()) + " , " +
         * Arrays.toString(postRecord.getCells().get(0).getColumnName()) + " , " +
         * Arrays.toString((byte[]) postRecord.getCells().get(0).getColumnValue()));
         */

        postRecord = postScanner.next();
        postCount_3++;
      }

      System.out.println("NNNN postCount = " + postCount_3);
      postScanner.close();
      // DEBUG_STATEMENTS
      // write keys found in scan result to file.
      // writeKeysToFile(postScannedKeys_3, "postScan3.txt");


      // verify the count
      assertEquals(preCount_3, postCount_3);

      verifyBucketCount(postTable_3);
    }

    // post restart table_4
    {
      MTable postTable_4 = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME_4);
      assertNotNull(postTable_4);

      Scanner postScanner =
          postTable_4.getScanner(new Scan()/* .addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0)) */);
      Row postRecord = postScanner.next();
      postCount_4 = 0;
      while (postRecord != null) {
        postScannedKeys_4.add(postRecord.getRowId());
        // DEBUG_STATEMENTS
        /*
         * System.out.println("Row Id -> " + Arrays.toString(postRecord.getRowId()) + " , " +
         * Arrays.toString(postRecord.getCells().get(0).getColumnName()) + " , " +
         * Arrays.toString((byte[]) postRecord.getCells().get(0).getColumnValue()));
         */

        postRecord = postScanner.next();
        postCount_4++;
      }

      System.out.println("NNNN postCount = " + postCount_4);
      postScanner.close();

      // DEBUG_STATEMENTS
      // write keys found in scan result to file.
      // writeKeysToFile(postScannedKeys_4, "postScan4.txt");
      // postScannedKeys_4.removeAll(preScannedKeys_4);
      // writeKeysToFile(postScannedKeys_4, "t4_afterRec_extraKeys.txt");

      // verify the count
      assertEquals(preCount_4, postCount_4);

      verifyBucketCount(postTable_4);
    }

    preScannedKeys_1.clear();
    postScannedKeys_1.clear();
    preScannedKeys_2.clear();
    postScannedKeys_2.clear();
    preScannedKeys_3.clear();
    postScannedKeys_3.clear();
    preScannedKeys_4.clear();
    postScannedKeys_4.clear();
  }

  private void writeKeysToFile(List<byte[]> keys, String fileName) {
    File file = new File(fileName);

    FileWriter fr = null;
    BufferedWriter bw = null;

    try {
      fr = new FileWriter(file);
      bw = new BufferedWriter(fr);
      for (int i = 0; i < keys.size(); i++) {
        bw.write(Arrays.toString(keys.get(i)));
        bw.write("\n");
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        bw.close();
        fr.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    System.out.println("NNNN writeKeysToFile [ " + fileName + " => total count=" + keys.size()
        + "] successfully done!");
  }

}
