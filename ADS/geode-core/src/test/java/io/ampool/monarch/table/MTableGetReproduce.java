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

import org.apache.geode.cache.CacheClosedException;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.ByteArrayKey;
import org.junit.Test;

import static org.junit.Assert.*;
import java.util.*;

public class MTableGetReproduce extends MTableDUnitHelper {

  public static String COLUMNNAME_PREFIX = "COL", VALUE_PREFIX = "VAL";
  public static final int NUM_COLUMNS = 5;
  public static String TABLENAME = "testTable";

  private Host host = null;
  protected VM vm0 = null;
  protected VM vm1 = null;
  protected VM vm2 = null;
  protected VM client1 = null;
  private List<VM> allVMList = null;

  private static final String diskStoreName = "MTableDiskStore";


  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    client1 = host.getVM(3);
    allVMList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));

    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());

    createClientCache(client1);
    createClientCache();
  }

  private void closeMCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        cache.close();
        return null;
      }
    });
  }

  private void closeMCacheAll() {
    closeMCache(this.vm0);
    closeMCache(this.vm1);
    closeMCache(this.vm2);
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(this.client1);
    closeMCacheAll();
    super.tearDown2();
  }


  public MTableGetReproduce() {
    super();
  }

  private static Map<Integer, MTableDescriptor> mTableDescriptors = new HashMap<>();

  // private static final String diskStoreName = "MTableDiskStore";

  static {

    // Set #1 MTable with default splits i.e. 113

    mTableDescriptors.put(1, createMTableDescriptor()); // ORDERED - NO PERSISTENCE - 1 MAXVERSION
    mTableDescriptors.put(2, createMTableDescriptor(MTableType.UNORDERED)); // UNORDERED -
                                                                            // NOPERSISTENCE - 1
                                                                            // MAXVERSION
    mTableDescriptors.put(3, createMTableDescriptor().setMaxVersions(5)); // ORDERED - NOPERSISTENCE
                                                                          // - 5 MAXVERSION
    mTableDescriptors.put(4,
        createMTableDescriptor().enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)); // ORDERED -
                                                                                       // DISKPERSISTENCE
                                                                                       // - 1
                                                                                       // MAXVERSION
    mTableDescriptors.put(5, createMTableDescriptor(MTableType.UNORDERED)
        .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)); // UNORDERED - NOPERSISTENCE - 1
                                                               // MAXVERSION
    mTableDescriptors.put(6, createMTableDescriptor());// .setRedundantCopies(3)); // ORDERED -
                                                       // NOPERSISTENCE - 1 MAXVERSION - 3 COPIES
    mTableDescriptors.put(7, createMTableDescriptor(MTableType.UNORDERED));
    // .setRedundantCopies(3)); // UNORDERED - NOPERSISTENCE - 1 MAXVERSION - 3 COPIES
    mTableDescriptors.put(8, createMTableDescriptor(MTableType.UNORDERED).setMaxVersions(5));
    // UNORDERED - NOPERSISTENCE - 5 MAXVERSION
  }

  public MTable createTable(String tableName, MTableDescriptor tableDescriptor) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createTable(tableName, tableDescriptor);
    assertEquals(mtable.getName(), tableName);
    return mtable;
  }


  @Test
  public void testGetAll() {

    String[] keys = {"005", "006", "007", "008", "009"};
    Long timestamp = 123456798l;

    for (Map.Entry<Integer, MTableDescriptor> integerMTableDescriptorEntry : mTableDescriptors
        .entrySet()) {
      // TABLENAME = baseTableName + integerMTableDescriptorEntry.getKey();
      closeMClientCache();
      createClientCache();
      MTableDescriptor mTableDescriptor = integerMTableDescriptorEntry.getValue();
      System.out.println("MTableDUnitConfigFramework.frameworkRunner :: "
          + "===========================================================================");
      System.out
          .println("MTableDUnitConfigFramework.frameworkRunner ::     Running Configuration id: "
              + integerMTableDescriptorEntry.getKey() + " of total " + mTableDescriptors.size());
      System.out.println("MTableDUnitConfigFramework.runAllConfigs :: " + "MTable Configuration::\n"
          + mTableDescriptor);
      System.out.println("MTableDUnitConfigFramework.frameworkRunner :: "
          + "===========================================================================");
      MTable table = createTable(TABLENAME, mTableDescriptor);
      assertNotNull(table);
      System.out
          .println("MTableDUnitConfigFramework.frameworkRunner :: " + "Running user test method");
      runMethod(table, keys, timestamp);
      System.out.println("MTableDUnitConfigFramework.frameworkRunner :: " + "Restarting...");
      // Restart
      restartTestFramework();
      table = getTableFromClientCache();
      assertNotNull(table);
      System.out.println("MTableDUnitConfigFramework.frameworkRunner :: "
          + "Running user test method after restart");
      runMethodAfterRestart(table, keys, timestamp);
      deleteTable(TABLENAME);
      System.out.println("MTableDUnitConfigFramework.frameworkRunner :: "
          + "===========================================================================");
    }


  }

  public void deleteTable(String tableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    admin.deleteTable(tableName);
    deleteDiskStore(tableName);
  }

  private void deleteDiskStore(String tableName) {
    allVMList.forEach((V) -> V.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DiskStore diskStore = MCacheFactory.getAnyInstance().findDiskStore(tableName + "-ds");
        if (diskStore != null) {
          System.out.println(
              "MTableDUnitConfigFramework.call :: " + "Deleting disk store: " + tableName + "-ds");
          // truncateStoreFiles(tableName + "-ds");
          diskStore.destroy();
        }
        return null;
      }
    }));
  }


  public void restartTestFramework() {
    stopAllServers();
    startAllServersAsync();
  }

  protected void stopAllServers() {
    try {
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
    } catch (Exception ex) {
      fail(ex.getMessage());
    }
  }

  public Object startServerOn(VM vm, final String locators) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
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
        MCacheFactory.getAnyInstance().createDiskStoreFactory().create(diskStoreName);
        MCacheFactory.getAnyInstance();
        // registerFunction();
        return port;
      }
    });
  }

  public AsyncInvocation asyncStartServerOn(VM vm, final String locators) {
    return vm.invokeAsync(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
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
        MCacheFactory.getAnyInstance().createDiskStoreFactory().create(diskStoreName);
        MCacheFactory.getAnyInstance();
        // registerFunction();
        return port;
      }
    });
  }

  protected void startAllServersAsync() {
    AsyncInvocation asyncInvocation1 =
        (AsyncInvocation) asyncStartServerOn(vm0, DUnitLauncher.getLocatorString());
    AsyncInvocation asyncInvocation2 =
        (AsyncInvocation) asyncStartServerOn(vm1, DUnitLauncher.getLocatorString());
    AsyncInvocation asyncInvocation3 =
        (AsyncInvocation) asyncStartServerOn(vm2, DUnitLauncher.getLocatorString());
    try {
      asyncInvocation1.join();
      asyncInvocation2.join();
      asyncInvocation3.join();
      createClientCache(this.client1);
      createClientCache();
    } catch (InterruptedException e) {
      fail(e.getMessage());
    }
  }

  protected static MTable getTableFromClientCache() {
    return MClientCacheFactory.getAnyInstance().getTable(TABLENAME);
  }

  public static MTableDescriptor createMTableDescriptor() {
    return createMTableDescriptor(MTableType.ORDERED_VERSIONED);
  }

  public static MTableDescriptor createMTableDescriptor(MTableType tableType) {
    MTableDescriptor mTableDescriptor = new MTableDescriptor(tableType);
    for (int i = 0; i < NUM_COLUMNS; i++) {
      mTableDescriptor.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + i));
    }
    return mTableDescriptor;
  }

  public void runMethod(MTable table, String[] keys, Long timestamp) {
    System.out.println("MTableAllGetPutDunit.run :: " + "Do Put");
    doPutForBatchGet(table, keys, timestamp);
    System.out.println("MTableAllGetPutDunit.run :: " + "Do Verify");
    verifyGetAll(table, keys, timestamp);
    // System.out.println("MTableAllGetPutDunit.run :: " + "Do verify from client1");
    // verifyGetAllFromClient(client1, table, keys, timestamp);
    System.out.println("MTableAllGetPutDunit.run :: " + "Done");
  }

  public void runMethodAfterRestart(MTable table, String[] keys, Long timestamp) {
    if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
      System.out.println("MTableAllGetPutDunit.run :: " + "Do Verify");
      verifyGetAll(table, keys, timestamp);
      // System.out.println("MTableAllGetPutDunit.run :: " + "Do verify from client1");
      // verifyGetAllFromClient(client1, table, keys, timestamp);
      System.out.println("MTableAllGetPutDunit.run :: " + "Done");
    }
  }

  private void doPutForBatchGet(MTable table, String[] keys, Long timestamp) {
    List<Put> putList = new ArrayList<>();
    Put put = null;
    for (int j = 0; j < keys.length; j++) {
      put = new Put(keys[j]);
      for (int i = 0; i < NUM_COLUMNS; i++) {
        put.addColumn(COLUMNNAME_PREFIX + i, Bytes.toBytes(keys[j] + VALUE_PREFIX + i));
      }
      // if (j % 2 == 0 && getMTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
      // Not setting for j =1 and 3
      // put.setTimeStamp(timestamp + j);
      // }
      // table.put(put);
      putList.add(put);
    }
    table.put(putList);
  }

  private void verifyGetAll(MTable table, String[] keys, Long timestamp) {
    // 0 : timestamp
    // 2 : timestamp + 2
    // 4 : timestamp + 4

    // Simple GetAll
    List<Get> getList = new ArrayList<>();
    Get get = null;
    for (String key : keys) {
      get = new Get(key);

      System.out.println("MTableAllGetPutDunit.verifyGetAll :: " + "Verifying key: " + key + "("
          + Arrays.toString(Bytes.toBytes(key)) + ") using simple get");
      // Verifying key using simple get
      Row result = table.get(get);
      assertNotNull(result);
      // Matching rowid
      assertEquals(new ByteArrayKey(Bytes.toBytes(key)), new ByteArrayKey(result.getRowId()));
      assertNotNull(result.getRowTimeStamp());
      List<Cell> cells = result.getCells();
      assertNotNull(cells);
      assertEquals(NUM_COLUMNS, cells.size());
      for (int j = 0; j < cells.size(); j++) {
        Cell cell = cells.get(j);
        assertNotNull(cell);
        assertEquals(new ByteArrayKey(cell.getColumnName()),
            new ByteArrayKey(Bytes.toBytes(COLUMNNAME_PREFIX + j)));
        assertEquals(new ByteArrayKey((byte[]) cell.getColumnValue()),
            new ByteArrayKey(Bytes.toBytes(key + VALUE_PREFIX + j)));
      }

      getList.add(get);
    }
    Row[] results = table.get(getList);
    assertNotNull(results);
    assertTrue(results.length > 0);
    for (int i = 0; i < results.length; i++) {
      assertNotNull(results[i]);
      // Matching rowid
      assertEquals(new ByteArrayKey(Bytes.toBytes(keys[i])),
          new ByteArrayKey(results[i].getRowId()));
      assertNotNull(results[i].getRowTimeStamp());
      // if (i % 2 == 0 && table.getTableDescriptor().getTableType() ==
      // MTableType.ORDERED_VERSIONED) {
      // assertEquals((Long) (timestamp + i), results[i].getRowTimeStamp());
      // }
      List<Cell> cells = results[i].getCells();
      assertNotNull(cells);
      assertEquals(NUM_COLUMNS, cells.size());
      for (int j = 0; j < cells.size(); j++) {
        Cell cell = cells.get(j);
        assertNotNull(cell);
        assertEquals(new ByteArrayKey(cell.getColumnName()),
            new ByteArrayKey(Bytes.toBytes(COLUMNNAME_PREFIX + j)));
        assertEquals(new ByteArrayKey((byte[]) cell.getColumnValue()),
            new ByteArrayKey(Bytes.toBytes(keys[i] + VALUE_PREFIX + j)));
      }
    }
  }
}
