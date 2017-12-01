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

import org.apache.geode.cache.*;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.geode.internal.cache.MTableRangePartitionResolver;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.*;

@Category(MonarchTest.class)
public class MTableDiskPersistDUnitTest extends MTableDUnitHelper {
  public MTableDiskPersistDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  private List<VM> allServerVms = null;

  private final int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "MTableDiskPersistDUnitTest";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 1;
  private final String COLUMN_NAME_PREFIX = "COLUMN";

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

  protected void createTable() {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    // tableDescriptor.setTotalNumOfSplits(10);

    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
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

  @Test
  public void testDiskPersistenance() {
    createTable();
    List<byte[]> listOfKeys = doPuts();
    doGets(listOfKeys);
    doSelectedGets(listOfKeys, 1, "BEFORE RESTART ");

    countPrimaryBuckets("[ BEFORE RESTART ]");
    System.out.println("----------------------- RESTARTING SERVERS ----------------------------");
    restart();
    System.out.println("----------------------- RESTARTING SERVERS DONE -----------------------");
    countPrimaryBuckets("[ AFTER RESTART ]");

    doGets(listOfKeys);
    doSelectedGets(listOfKeys, 1, "AFTER RESTART ");

    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    Scanner scanner = table.getScanner(new Scan().addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0)));
    Row record = scanner.next();
    while (record != null) {
      System.out.println("Row Id -> " + Arrays.toString(record.getRowId()) + " , "
          + Arrays.toString(record.getCells().get(0).getColumnName()) + " , "
          + Arrays.toString((byte[]) record.getCells().get(0).getColumnValue()));

      record = scanner.next();
    }
    scanner.close();

  }
}
