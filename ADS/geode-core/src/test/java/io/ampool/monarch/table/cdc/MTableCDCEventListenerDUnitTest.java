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
package io.ampool.monarch.table.cdc;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.CDCConfig;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MEventOperation;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.client.coprocessor.AggregationClient;

import org.apache.geode.cache.CacheClosedException;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

@Category(MonarchTest.class)
public class MTableCDCEventListenerDUnitTest extends MTableDUnitHelper {

  private final int NUM_OF_COLUMNS = 10;
  private final String COLUMN_NAME_PREFIX = "COLUMN";
  private final String TABLE_NAME = "MTableCDCEventListenerDUnitTest";
  private final int NUM_OF_ROWS = 1;
  private final String VALUE_PREFIX = "VALUE";
  private final String VALUE_PREFIX_UPDATE = "VALUE-UPDATE";

  private final String CDC_EVENT_STATS_REGION = "CDC_STATS_REGION";
  // private final String CDC_EVENT_REGION = "CDC_EVENT_REGION";


  Host host = Host.getHost(0);
  private VM vm0 = host.getVM(0);
  private VM vm1 = host.getVM(1);
  private VM vm2 = host.getVM(2);
  private VM client1 = host.getVM(3);


  private Object startServerOnCDC(VM vm, final String locators) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
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

        if (c.getRegion(CDC_EVENT_STATS_REGION) == null) {
          RegionFactory<String, Integer> rf;
          rf = c.createRegionFactory(RegionShortcut.REPLICATE);
          rf.create(CDC_EVENT_STATS_REGION);
        }
        return port;
      }
    });
  }

  private void createClientCacheCDC() {
    String testName = getTestMethodName();
    String logFileName = testName + "-client.log";

    MClientCache clientCache = new MClientCacheFactory().set("log-file", logFileName)
        .addPoolLocator("127.0.0.1", getLocatorPort()).create();

    ClientRegionFactory<String, Integer> crf =
        ClientCacheFactory.getAnyInstance().createClientRegionFactory(ClientRegionShortcut.PROXY);
    crf.create(CDC_EVENT_STATS_REGION);

  }

  public void createClientCacheCDC(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        createClientCacheCDC();
        return null;
      }
    });
  }

  private void closeMClientCacheCDC() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    if (clientCache != null) {
      clientCache.close();
      clientCache = null;
    }
  }

  private void closeMClientCacheCDC(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        closeMClientCache();
        return null;
      }
    });
  }

  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();
    startServerOnCDC(this.vm0, DUnitLauncher.getLocatorString());
    startServerOnCDC(this.vm1, DUnitLauncher.getLocatorString());
    startServerOnCDC(this.vm2, DUnitLauncher.getLocatorString());
    createClientCacheCDC(this.client1);
    createClientCacheCDC();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCacheCDC();
    closeMClientCacheCDC(client1);

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


  public MTableCDCEventListenerDUnitTest() {
    super();
  }

  private MTable createTable(final boolean isOrdered, final boolean isCDCPersistenance) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    if (!isOrdered) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    }
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    Admin admin = clientCache.getAdmin();

    CDCConfig cdcConfig = tableDescriptor.createCDCConfig();
    cdcConfig.setPersistent(isCDCPersistenance);

    tableDescriptor.addCDCStream("MTableCDCEventListenerDUnitTestCDC",
        "io.ampool.monarch.table.cdc.CDCEventListener", cdcConfig);

    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);

    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
    return table;
  }

  private List<byte[]> doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);

    List<byte[]> allKeys = new ArrayList<byte[]>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> {
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

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


  @Test
  public void testCDCEventsReceivedOrderedTable() throws InterruptedException {
    doCDCEventsReceived(true, false);

  }

  @Test
  public void testCDCEventsReceivedOrderedTableWithPersistent() throws InterruptedException {
    doCDCEventsReceived(true, true);
  }

  @Test
  public void testCDCEventsReceivedUnOrderedTable() throws InterruptedException {
    doCDCEventsReceived(false, false);

  }

  @Test
  public void testCDCEventsReceivedUnOrderedTableWithPersistent() throws InterruptedException {
    doCDCEventsReceived(false, true);
  }

  private void doCDCEventsReceived(final boolean isOrdered, final boolean isCDCPersistenance)
      throws InterruptedException {

    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            assertNotNull(MCacheFactory.getAnyInstance().getRegion(CDC_EVENT_STATS_REGION));
            return null;
          }
        }));

    MTable table = createTable(isOrdered, isCDCPersistenance);
    List<byte[]> actualKeys = doPuts();
    // Verify from the client that number of rows are equal to puts list size.
    assertEquals(113, new AggregationClient().rowCount(TABLE_NAME, new Scan()));

    Region<Object, Object> cdcEventBBRegion =
        ClientCacheFactory.getAnyInstance().getRegion(CDC_EVENT_STATS_REGION);
    assertNotNull(cdcEventBBRegion);

    verifyPutEvents(actualKeys, cdcEventBBRegion);
    verifyUpdateEvent(table, actualKeys, cdcEventBBRegion);
    VerifyDeleteEvent(table, actualKeys, cdcEventBBRegion);
    VerifyDeleteEventWithColumns(table, actualKeys, cdcEventBBRegion);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  private void verifyPutEvents(List<byte[]> actualKeys, Region<Object, Object> cdcEventBBRegion)
      throws InterruptedException {
    int numOfEventReceived = 0;
    int possibleDuplicates = 0;

    Set<byte[]> receivedKeys = new TreeSet<>(Bytes.BYTES_COMPARATOR);

    Set<Object> allKeysReceived = cdcEventBBRegion.keySetOnServer();

    while (allKeysReceived.size() != 113) {
      System.out.println("MTableCDCEventListenerDUnitTest.verifyPutEvents. Waiting for Events ... "
          + allKeysReceived.size());
      Thread.sleep(1500);
      allKeysReceived = cdcEventBBRegion.keySetOnServer();
    }

    for (Object key : allKeysReceived) {
      EventData ed = (EventData) cdcEventBBRegion.get(key);
      numOfEventReceived++;
      if (ed.possibleDuplicate) {
        possibleDuplicates++;
      }
      receivedKeys.add(ed.rowKey);
      // Assert the operation.
      assertEquals(MEventOperation.CREATE, ed.operation);

      assertNotNull(ed.row);
      List<Cell> cellsE = ed.row.getCells();
      Get get = new Get(ed.rowKey);
      List<Cell> cellsT =
          MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME).get(get).getCells();
      // Verify that the columns received in the events and columns in the table
      // for this row in the MTable is same.
      assertEquals(cellsE.size(), cellsT.size() - 1);

      for (int colPos = 0; colPos < cellsE.size() - 1; colPos++) {
        Cell cellE = cellsE.get(colPos); // Cell from MEvent
        Cell cellT = cellsT.get(colPos); // Cell from MTable

        if (Bytes.compareTo(cellE.getColumnName(), cellT.getColumnName()) != 0) {
          fail(" Mismatch in Column Names");
        }

        if (Bytes.compareTo((byte[]) cellE.getColumnValue(),
            (byte[]) cellT.getColumnValue()) != 0) {
          fail(" Mismatch in Column Values");
        }

        if (cellE.getColumnType() != cellT.getColumnType()) {
          fail(" Mismatch in Column Type");
        }

      }

    }
    assertEquals(113, (numOfEventReceived - possibleDuplicates));

    // Verify all the keys.
    int keyVerified = 0;
    for (byte[] key : actualKeys) {
      assertTrue(receivedKeys.contains(key));
      keyVerified++;
    }
    assertEquals(113, keyVerified);
  }

  private void verifyUpdateEvent(MTable table, List<byte[]> actualKeys,
      Region<Object, Object> cdcEventBBRegion) throws InterruptedException {
    Set<Object> allKeysReceived;
    int numOfEventReceived;
    int possibleDuplicates;// VERIFY THE UPDATES BEHAVIOUR
    cdcEventBBRegion.clear();
    assertEquals(0, cdcEventBBRegion.keySetOnServer().size());

    for (int i = 0; i < 5; i++) {
      Put record = new Put(actualKeys.get(i));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX_UPDATE + columnIndex));
      }
      table.put(record);
    }

    // Verify from the client that number of rows are equal to puts list size.
    assertEquals(113, new AggregationClient().rowCount(TABLE_NAME, new Scan()));

    allKeysReceived = cdcEventBBRegion.keySetOnServer();

    while (allKeysReceived.size() != 5) {
      System.out
          .println("MTableCDCEventListenerDUnitTest.verifyUpdateEvent. Waiting for Evnets ...");
      Thread.sleep(1500);
      allKeysReceived = cdcEventBBRegion.keySetOnServer();
    }

    Set<byte[]> receivedKeys = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    receivedKeys.clear();
    numOfEventReceived = 0;
    possibleDuplicates = 0;

    for (Object key : allKeysReceived) {
      EventData ed = (EventData) cdcEventBBRegion.get(key);
      numOfEventReceived++;
      if (ed.possibleDuplicate) {
        possibleDuplicates++;
      }
      receivedKeys.add(ed.rowKey);
      // Assert the operation.
      assertEquals(MEventOperation.UPDATE, ed.operation);
      assertNotNull(ed.row);
      List<Cell> cellsE = ed.row.getCells();
      Get get = new Get(ed.rowKey);
      List<Cell> cellsT =
          MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME).get(get).getCells();
      // Verify that the columns received in the events and columns in the table
      // for this row in the MTable is same.
      assertEquals(cellsE.size(), cellsT.size() - 1);
      for (int colPos = 0; colPos < cellsE.size() - 1; colPos++) {
        Cell cellE = cellsE.get(colPos); // Cell from MEvent
        Cell cellT = cellsT.get(colPos); // Cell from MTable

        if (Bytes.compareTo(cellE.getColumnName(), cellT.getColumnName()) != 0) {
          fail(" Mismatch in Column Names");
        }
        if (Bytes.compareTo((byte[]) cellE.getColumnValue(),
            (byte[]) cellT.getColumnValue()) != 0) {
          fail(" Mismatch in Column Values");
        }
        if (cellE.getColumnType() != cellT.getColumnType()) {
          fail(" Mismatch in Column Type");
        }
      }
    }
    assertEquals(5, (numOfEventReceived - possibleDuplicates));
  }

  private void VerifyDeleteEvent(MTable table, List<byte[]> actualKeys,
      Region<Object, Object> cdcEventBBRegion) throws InterruptedException {
    Set<Object> allKeysReceived;
    int numOfEventReceived;
    int possibleDuplicates;
    cdcEventBBRegion.clear();
    assertEquals(0, cdcEventBBRegion.keySetOnServer().size());

    for (int i = 0; i < 5; i++) {
      Delete record = new Delete(actualKeys.get(i));
      table.delete(record);
    }

    // Verify from the client that number of rows are equal to puts list size.
    assertEquals(113 - 5, new AggregationClient().rowCount(TABLE_NAME, new Scan()));
    allKeysReceived = cdcEventBBRegion.keySetOnServer();
    while (allKeysReceived.size() != 5) {
      System.out
          .println("MTableCDCEventListenerDUnitTest.VerifyDeleteEvent. Waiting for Evnets ...");
      Thread.sleep(1500);
      allKeysReceived = cdcEventBBRegion.keySetOnServer();
    }
    Set<byte[]> receivedKeys = new TreeSet<>(Bytes.BYTES_COMPARATOR);

    receivedKeys.clear();
    numOfEventReceived = 0;
    possibleDuplicates = 0;

    for (Object key : allKeysReceived) {
      EventData ed = (EventData) cdcEventBBRegion.get(key);
      numOfEventReceived++;
      if (ed.possibleDuplicate) {
        possibleDuplicates++;
      }
      receivedKeys.add(ed.rowKey);
      // This key should be delete from the MTable verify the same here.
      Get get = new Get(ed.rowKey);
      List<Cell> cellsT =
          MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME).get(get).getCells();
      assertTrue(cellsT.isEmpty());
      // Assert the operation.
      assertEquals(MEventOperation.DELETE, ed.operation);
    }
    assertEquals(5, (numOfEventReceived - possibleDuplicates));
  }

  private void VerifyDeleteEventWithColumns(MTable table, List<byte[]> actualKeys,
      Region<Object, Object> cdcEventBBRegion) throws InterruptedException {
    Set<Object> allKeysReceived;
    int numOfEventReceived;
    int possibleDuplicates;
    cdcEventBBRegion.clear();
    assertEquals(0, cdcEventBBRegion.keySetOnServer().size());

    for (int i = 10; i < 15; i++) {
      Delete record = new Delete(actualKeys.get(i));
      record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
      record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 2));
      record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 3));
      table.delete(record);
    }

    // Verify from the client that number of rows are equal to puts list size.
    assertEquals(113 - 5, new AggregationClient().rowCount(TABLE_NAME, new Scan()));

    allKeysReceived = cdcEventBBRegion.keySetOnServer();

    while (allKeysReceived.size() != 5) {
      System.out.println(
          "MTableCDCEventListenerDUnitTest.VerifyDeleteEventWithColumns. Waiting for Evnets ...");
      Thread.sleep(1500);
      allKeysReceived = cdcEventBBRegion.keySetOnServer();
    }

    Set<byte[]> receivedKeys = new TreeSet<>(Bytes.BYTES_COMPARATOR);

    receivedKeys.clear();
    numOfEventReceived = 0;
    possibleDuplicates = 0;

    for (Object key : allKeysReceived) {
      EventData ed = (EventData) cdcEventBBRegion.get(key);
      numOfEventReceived++;
      if (ed.possibleDuplicate) {
        possibleDuplicates++;
      }
      receivedKeys.add(ed.rowKey);
      // Assert the operation.
      assertEquals(MEventOperation.UPDATE, ed.operation);

      assertNotNull(ed.row);
      List<Cell> cellsE = ed.row.getCells();
      Get get = new Get(ed.rowKey);
      List<Cell> cellsT =
          MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME).get(get).getCells();
      // Verify that the columns received in the events and columns in the table
      // for this row in the MTable is same.
      assertEquals(10, cellsT.size() - 1);

      Map<byte[], Cell> colNameToMCellMapE = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (int k = 0; k < cellsE.size() - 1; k++) {
        if (cellsE.get(k).getColumnValue() != null) {
          colNameToMCellMapE.put(cellsE.get(k).getColumnName(), cellsE.get(k));
        }
      }

      Map<byte[], Cell> colNameToMCellMapT = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (int k = 0; k < cellsT.size() - 1; k++) {
        if (cellsT.get(k).getColumnValue() != null) {
          colNameToMCellMapT.put(cellsT.get(k).getColumnName(), cellsT.get(k));
        }
      }

      for (Map.Entry<byte[], Cell> cellEntry : colNameToMCellMapE.entrySet()) {
        assertTrue(colNameToMCellMapT.containsKey(cellEntry.getKey()));
        Cell cellE = cellEntry.getValue(); // Cell from MEvent
        Cell cellT = colNameToMCellMapT.get(cellEntry.getKey());
        if (Bytes.compareTo(cellE.getColumnName(), cellT.getColumnName()) != 0) {
          fail(" Mismatch in Column Names");
        }
        if (Bytes.compareTo((byte[]) cellE.getColumnValue(),
            (byte[]) cellT.getColumnValue()) != 0) {
          System.out.println("Event  Column -> " + new String(cellE.getColumnName()) + " -> "
              + new String((byte[]) cellE.getColumnValue()));
          System.out.println("MTable Column -> " + new String(cellT.getColumnName()) + " -> "
              + new String((byte[]) cellT.getColumnValue()));
          fail(" Mismatch in Column Values");
        }
        if (cellE.getColumnType() != cellT.getColumnType()) {
          fail(" Mismatch in Column Type");
        }
      }
    }
    assertEquals(5, (numOfEventReceived - possibleDuplicates));
  }
}

