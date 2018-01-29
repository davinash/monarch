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

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;

@Category(FTableTest.class)
public class FTableAdminAPIDUnitTest extends MTableDUnitHelper {

  private volatile Exception exceptionThread1 = null;
  private volatile Throwable exceptionThread2 = null;

  private static final int NUM_OF_COLUMNS = 10;
  private static final int INTERNAL_NUM_OF_COLUMNS = 1;
  private static final int NUM_OF_SPLITS = 113;
  private static final String COLUMN_NAME_PREFIX = "COL";
  // private static final String TABLE_NAME = "MTableRaceCondition";

  public FTableAdminAPIDUnitTest() {}

  private Host host = null;
  private VM vm0 = null;
  private VM vm1 = null;
  private VM vm2 = null;
  private VM vm3 = null;
  final int numOfEntries = 1000;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());
    createClientCache(vm3);
    createClientCache();
  }

  /**
   * Verify if fTableExist api is working
   */
  @Test
  public void testFTableExistsAPI() {
    String tableName = getTestMethodName();
    final FTable table = FTableAdminAPIDUnitTest.createFTable(tableName,
        getFTableDescriptor(FTableAdminAPIDUnitTest.NUM_OF_SPLITS, 0));
    assertNotNull(table);
    vm0.invoke(() -> {
      FTableAdminAPIDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAdminAPIDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm1.invoke(() -> {
      FTableAdminAPIDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAdminAPIDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm2.invoke(() -> {
      FTableAdminAPIDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAdminAPIDUnitTest.NUM_OF_SPLITS, 0));
    });

    final Admin admin = getmClientCache().getAdmin();
    assertNotNull(admin);
    assertTrue(admin.existsFTable(tableName));
    assertTrue(deleteFTable(tableName));
  }

  /**
   * Verify if listFTables api is working
   */
  @Test
  public void testlistFTablesAPI() {
    String tableName = getTestMethodName();
    final FTableDescriptor tabledescriptor =
        getFTableDescriptor(FTableAdminAPIDUnitTest.NUM_OF_SPLITS, 0);
    final FTable table = FTableAdminAPIDUnitTest.createFTable(tableName, tabledescriptor);
    assertNotNull(table);
    vm0.invoke(() -> {
      FTableAdminAPIDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAdminAPIDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm1.invoke(() -> {
      FTableAdminAPIDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAdminAPIDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm2.invoke(() -> {
      FTableAdminAPIDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAdminAPIDUnitTest.NUM_OF_SPLITS, 0));
    });

    final Admin admin = getmClientCache().getAdmin();
    assertNotNull(admin);
    final Map<String, FTableDescriptor> tables = admin.listFTables();
    assertNotNull(tables);
    assertTrue(tables.size() > 0);
    for (Map.Entry<String, FTableDescriptor> entry : tables.entrySet()) {
      assertNotNull(entry.getKey());
      assertEquals(tableName, entry.getKey());
      final FTableDescriptor tabDesc = entry.getValue();
      final List<MColumnDescriptor> colDesc = tabDesc.getAllColumnDescriptors();
      assertEquals(NUM_OF_COLUMNS + INTERNAL_NUM_OF_COLUMNS, colDesc.size());
      int colIndex = 0;
      for (int j = 0; j < NUM_OF_COLUMNS && j < colDesc.size(); j++) {
        assertEquals((COLUMN_NAME_PREFIX + colIndex), colDesc.get(j).getColumnNameAsString());
        colIndex++;
      }
      assertEquals(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
          colDesc.get(colIndex).getColumnNameAsString());
    }
    assertTrue(deleteFTable(tableName));
  }

  /**
   * Verify if listFTableNames api is working
   */
  @Test
  public void testlistFTableNamesAPI() {
    String tableName = getTestMethodName();
    final FTableDescriptor tabledescriptor =
        getFTableDescriptor(FTableAdminAPIDUnitTest.NUM_OF_SPLITS, 0);
    final FTable table = FTableAdminAPIDUnitTest.createFTable(tableName, tabledescriptor);
    assertNotNull(table);
    vm0.invoke(() -> {
      FTableAdminAPIDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAdminAPIDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm1.invoke(() -> {
      FTableAdminAPIDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAdminAPIDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm2.invoke(() -> {
      FTableAdminAPIDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAdminAPIDUnitTest.NUM_OF_SPLITS, 0));
    });

    final Admin admin = getmClientCache().getAdmin();
    assertNotNull(admin);
    final String[] fTableNames = admin.listFTableNames();
    assertNotNull(fTableNames);
    assertTrue(fTableNames.length > 0);
    for (int i = 0; i < fTableNames.length; i++) {
      assertEquals(tableName, fTableNames[i]);
    }
    assertTrue(deleteFTable(tableName));
  }

  @Test
  public void testlistFtableAndMTableMixed() {
    String tableName = getTestMethodName();
    createMTableAndFTable(tableName);
    verifylistTablesOnClient(tableName);
    verifylistTablesOnServers(tableName);
  }

  private void verifylistTablesOnServers(String tableName) {
    for (VM vm : new VM[] {vm0, vm1, vm2}) {
      vm.invoke(() -> {
        verifylistTablesOnServer(tableName);
      });
    }
  }

  private void verifylistTablesOnClient(String tableName) {
    Map<String, FTableDescriptor> ftables =
        MClientCacheFactory.getAnyInstance().getAdmin().listFTables();
    assertEquals(1, ftables.size());
    assertNotNull(ftables.get(tableName + "_FTABLE"));

    Map<String, MTableDescriptor> mtables =
        MClientCacheFactory.getAnyInstance().getAdmin().listMTables();
    assertEquals(1, mtables.size());
    assertNotNull(mtables.get(tableName + "_MTABLE"));
  }

  private void verifylistTablesOnServer(String tableName) {
    Map<String, FTableDescriptor> ftables = MCacheFactory.getAnyInstance().getAdmin().listFTables();
    assertEquals(1, ftables.size());
    assertNotNull(ftables.get(tableName + "_FTABLE"));

    Map<String, MTableDescriptor> mtables = MCacheFactory.getAnyInstance().getAdmin().listMTables();
    assertEquals(1, mtables.size());
    assertNotNull(mtables.get(tableName + "_MTABLE"));
  }


  private void createMTableAndFTable(String tableName) {
    MTableDescriptor mtd = new MTableDescriptor();
    mtd.addColumn(new byte[] {1, 2, 3});
    MClientCacheFactory.getAnyInstance().getAdmin().createMTable(tableName + "_MTABLE", mtd);

    FTableDescriptor ftd = new FTableDescriptor();
    ftd.addColumn(new byte[] {1, 2, 3});
    MClientCacheFactory.getAnyInstance().getAdmin().createFTable(tableName + "_FTABLE", ftd);
  }

  protected void stopServer(VM vm) {
    System.out.println("CreateMTableDUnitTest.stopServer :: " + "Stopping server....");
    try {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          MCacheFactory.getAnyInstance().close();
          return null;
        }
      });
    } catch (Exception ex) {
      fail(ex.getMessage());
    }
  }

  private Thread invokeInThreadOnVM(final VM vm3, final SerializableCallable function) {
    System.out.println("CreateMTableDUnitTest.invokeInThreadOnVM :: "
        + "Creating thread to invoke mtable creation...");
    final Thread thread = new Thread() {
      public void run() {
        System.out.println("CreateMTableDUnitTest.run :: " + "Inside thread ");
        try {
          vm3.invoke(() -> {
            System.out.println("CreateMTableDUnitTest.run :: " + "Calling create table function");
            function.call();
          });
        } catch (Throwable e) {
          exceptionThread2 = e;
          System.out
              .println("CreateMTableDUnitTest.run :: " + "Exception from thread: " + e.getClass());
          e.printStackTrace();
        }
      }
    };
    thread.start();
    return thread;
  }

  private static void verifyTableOnServer(final String tableName,
      final FTableDescriptor tableDescriptor) {
    final MCache serverCache = MCacheFactory.getAnyInstance();
    assertNotNull(serverCache);
    FTable mtable = null;
    int retries = 0;
    do {
      mtable = serverCache.getFTable(tableName);
      if (retries > 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      retries++;
      // if (retries > 1) {
      // System.out.println("CreateMTableDUnitTest.verifyTableOnServer :: " + "Attempting to fetch
      // table... Attempt " + retries);
      // }
    } while (mtable == null && retries < 500);
    assertNotNull(mtable);
    final Region<Object, Object> mregion = ((ProxyFTableRegion) mtable).getTableRegion();
    String path = mregion.getFullPath();
    assertTrue(path.contains(tableName));
    // To verify disk persistence
    assertEquals(tableDescriptor.isDiskPersistenceEnabled(),
        mregion.getAttributes().getDataPolicy().withPersistence());
  }


  private static void verifyTableNotExistOnServer(final String tableName,
      final MTableDescriptor tableDescriptor) {
    final MCache serverCache = MCacheFactory.getAnyInstance();
    assertNotNull(serverCache);
    FTable mtable = null;
    int retries = 0;
    do {
      mtable = serverCache.getFTable(tableName);
      if (retries > 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      retries++;
    } while (mtable == null && retries < 500);
    if (retries >= 500) {
      // this means after 500 retries also I am not able to find out the region on server
      // System.out.println("Mtable is : "+mtable);
      assertNull(mtable);
    }
  }

  private static FTable createFTable(final String tableName,
      final FTableDescriptor tableDescriptor) {
    // System.out.println("CreateMTableDUnitTest.createFTable :: " + "Creating mtable:---- " +
    // tableDescriptor);
    int numberOfKeysPerBucket = 20;
    int splits = 10;
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    // System.out.println("CreateMTableDUnitTest.createFTable :: " + "isCacheClosed: " +
    // clientCache.isClosed());
    // MTableDescriptor tableDescriptor = getFTableDescriptor(splits);

    FTable mtable = null;
    try {
      mtable = clientCache.getAdmin().createFTable(tableName, tableDescriptor);
    } catch (Exception e) {
      System.out.println("CreateMTableDUnitTest.createFTable :: " + "Throwing from test");
      throw e;
    }
    System.out.println("CreateMTableDUnitTest.createFTable :: " + "mtable is " + mtable);
    return mtable;

  }


  private static boolean deleteFTable(final String tableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    try {
      clientCache.getAdmin().deleteFTable(tableName);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private static FTableDescriptor getFTableDescriptor(final int splits, int descriptorIndex) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    if (descriptorIndex == 0) {
      tableDescriptor.setTotalNumOfSplits(splits);
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        // sets a partitioningColumn
        if (colmnIndex == 0) {
          tableDescriptor.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
        }
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.setRedundantCopies(1);
    } else {
      tableDescriptor.setTotalNumOfSplits(splits);
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        // sets a partitioningColumn
        if (colmnIndex == 0) {
          tableDescriptor.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
        }
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
      tableDescriptor.setRedundantCopies(2);
    }
    return tableDescriptor;
  }


  @Override
  public void tearDown2() throws Exception {
    // closeMClientCache(CreateMTableDUnitTest.vm0);
    // closeMClientCache(CreateMTableDUnitTest.vm1);
    // closeMClientCache(CreateMTableDUnitTest.vm2);
    closeMClientCache(vm3);
    closeMClientCache();
    // closeAllMCaches();
    super.tearDown2();
  }
}


