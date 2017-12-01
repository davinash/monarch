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

import io.ampool.internal.RegionDataOrder;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MTableExistsException;
import io.ampool.monarch.table.exceptions.MTableNotExistsException;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(MonarchTest.class)
public class MTableOperationsDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();
  VM server1, server2, server3;
  private List<VM> allServerVms = null;

  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    server1 = vm0;
    server2 = vm1;
    server3 = vm2;
    allServerVms = new ArrayList<>(Arrays.asList(this.server1, this.server2, this.server3));
    startServerOn(this.server1, DUnitLauncher.getLocatorString());
    startServerOn(this.server2, DUnitLauncher.getLocatorString());
    startServerOn(this.server3, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);

    allServerVms.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    }));

    super.tearDown2();
  }

  public MTableOperationsDUnitTest() {
    super();
  }

  private void createTablesOn(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        for (int i = 0; i <= 10; i++) {

          MTableDescriptor tableDescriptor =
              new MTableDescriptor().addColumn(Bytes.toBytes("Column1"))
                  .addColumn(Bytes.toBytes("Column2")).addColumn(Bytes.toBytes("Column3"));
          tableDescriptor.setRedundantCopies(1);
          Admin admin = clientCache.getAdmin();
          MTable table = admin.createTable("EmployeeTable" + i, tableDescriptor);

          assertNotNull(table);
        }
        return null;
      }
    });
  }

  private void createTablesWithSameNameOn(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        for (int i = 0; i <= 10; i++) {
          MTableDescriptor tableDescriptor = new MTableDescriptor();
          tableDescriptor.addColumn(Bytes.toBytes("Column1")).addColumn(Bytes.toBytes("Column2"))
              .addColumn(Bytes.toBytes("Column3")).addColumn(Bytes.toBytes("Column4"));
          tableDescriptor.setRedundantCopies(1);
          Exception e = null;
          try {
            Admin admin = clientCache.getAdmin();
            MTable table = admin.createTable("EmployeeTable" + i, tableDescriptor);
          } catch (MTableExistsException mee) {
            e = mee;
          }
          assertTrue(e instanceof MTableExistsException);
        }
        return null;
      }
    });
  }

  private void verifyTableCanBeAccessFromOtherClient() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    for (int i = 0; i <= 10; i++) {
      MTable mtable = clientCache.getTable("EmployeeTable" + i);
      assertNotNull(mtable);
    }
  }

  private void verifyInternalRegionsCreatedOn(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        for (int i = 0; i <= 10; i++) {
          String tableName = "EmployeeTable" + i;
          Region r = cache.getRegion(tableName);
          assertNotNull(r);
          RegionAttributes ra = r.getAttributes();
          assertEquals(RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED,
              ((AmpoolTableRegionAttributes) ra.getCustomAttributes()).getRegionDataOrder());
          // assertEquals(RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED, ra.getRegionDataOrder());
          assertEquals(ra.getDataPolicy(), DataPolicy.PARTITION);
          assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
        }
        return null;
      }
    });
  }

  private void doMultipleConnectionsFromSameClient(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        Exception e = null;
        try {
          MClientCache clientCache =
              new MClientCacheFactory().set("log-file", "/tmp/MTableClient.log")
                  .addPoolLocator("127.0.0.1", getLocatorPort()).create();

        } catch (IllegalStateException mcaee) {
          e = mcaee;
        }
        assertTrue(e instanceof IllegalStateException);
        return null;
      }
    });
  }

  private void createTableOn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        assertNotNull(clientCache);
        MTableDescriptor tableDescriptor = new MTableDescriptor();
        tableDescriptor.addColumn(Bytes.toBytes("Column1")).addColumn(Bytes.toBytes("Column2"))
            .addColumn(Bytes.toBytes("Column3")).addColumn(Bytes.toBytes("Column4"));

        tableDescriptor.setRedundantCopies(1);

        Admin admin = clientCache.getAdmin();
        admin.createTable("TableToDelete", tableDescriptor);

        return null;
      }
    });
  }

  private void getTableFrom() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    MTable table = clientCache.getTable("TableToDelete");
    assertNotNull(table);
  }

  private MTable getTableFrom(String tableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    MTable table = clientCache.getTable(tableName);
    assertNotNull(table);
    return table;
  }

  private void deleteTableOn(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        assertNotNull(clientCache);
        String tableName = "TableToDelete";
        MTable table = clientCache.getTable(tableName);
        assertNotNull(table);

        clientCache.getAdmin().deleteTable(tableName);
        table = clientCache.getTable("TableToDelete");
        assertNull(table);
        return null;
      }
    });
  }

  private void getDeletedTableFrom() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    Exception expectedException = null;

    MTable table = clientCache.getTable("TableToDelete");
    assertNull(table);
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
    AsyncInvocation vm0Task = server1.invokeAsync(new SerializableCallable() {
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
        return null;
      }
    });


    AsyncInvocation vm1Task = server2.invokeAsync(new SerializableCallable() {
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
        return null;
      }
    });

    AsyncInvocation vm2Task = server3.invokeAsync(new SerializableCallable() {
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

  @Test
  public void testDeleteTableAndAccessFromAnotherClient() {
    createTableOn(this.client1);
    getTableFrom();
    deleteTableOn(this.client1);
    getDeletedTableFrom();
  }

  private void restart() {
    closeMClientCache();
    stopAllCacheServers();

    startAllServerAsync(DUnitLauncher.getLocatorString());
    createClientCache();

  }

  public void createTableWithNoColumnsOn(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        assertNotNull(clientCache);
        MTableDescriptor tableDescriptor = new MTableDescriptor();
        tableDescriptor.setRedundantCopies(1);
        Exception expectedException = null;
        try {
          Admin admin = clientCache.getAdmin();
          admin.createTable("TABLEWITHNOCOLUMNTS", tableDescriptor);
        } catch (IllegalArgumentException e) {
          expectedException = e;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);

        return null;
      }
    });
  }

  public void createTableWithLegalAndIllegalNames(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        assertNotNull(clientCache);
        MTableDescriptor tableDescriptor = new MTableDescriptor();
        tableDescriptor.addColumn(Bytes.toBytes("DUMMY_COLUMN"));
        // tableDescriptor.setRedundantCopies(1);
        Exception expectedException = null;
        Admin admin = clientCache.getAdmin();

        try {
          admin.createTable("", tableDescriptor);
        } catch (IllegalArgumentException ia) {
          expectedException = ia;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);

        expectedException = null;
        try {
          admin.createTable(null, tableDescriptor);
        } catch (IllegalArgumentException ia) {
          expectedException = ia;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);

        expectedException = null;
        try {
          admin.createTable("abc/abc", tableDescriptor);
        } catch (IllegalArgumentException ia) {
          expectedException = ia;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);

        expectedException = null;
        try {
          admin.createTable("abc1234abc", tableDescriptor);
        } catch (IllegalArgumentException ia) {
          expectedException = ia;
        }
        assertNull(expectedException);

        expectedException = null;
        try {
          admin.createTable("abc_abc", tableDescriptor);
        } catch (IllegalArgumentException ia) {
          expectedException = ia;
        }
        assertNull(expectedException);

        expectedException = null;
        try {
          admin.createTable("abc.abc", tableDescriptor);
        } catch (IllegalArgumentException ia) {
          expectedException = ia;
        }
        assertNull(expectedException);

        expectedException = null;
        try {
          admin.createTable("ABCD", tableDescriptor);
        } catch (IllegalArgumentException ia) {
          expectedException = ia;
        }
        assertNull(clientCache.getTable("abcd"));
        assertNull(expectedException);

        expectedException = null;
        try {
          admin.createTable("abcd", tableDescriptor);
        } catch (IllegalArgumentException ia) {
          expectedException = ia;
        }
        assertNotNull(clientCache.getTable("abcd"));
        assertNull(expectedException);

        expectedException = null;
        try {
          admin.createTable("__abcd", tableDescriptor);
        } catch (IllegalArgumentException ia) {
          expectedException = ia;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);

        expectedException = null;
        try {
          admin.createTable("a%bc$d", tableDescriptor);
        } catch (IllegalArgumentException ia) {
          expectedException = ia;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);

        expectedException = null;
        try {
          admin.createTable(" ", tableDescriptor);
        } catch (IllegalArgumentException ia) {
          expectedException = ia;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);

        expectedException = null;
        try {
          admin.createTable("_", tableDescriptor);
        } catch (IllegalArgumentException ia) {
          expectedException = ia;
        }
        assertNull(expectedException);

        return null;
      }
    });
  }

  private void createTableWithVaryingParams(VM vm) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(Bytes.toBytes("DUMMY_COLUMN"));
    // tableDescriptor.setRedundantCopies(1);
    Exception expectedException = null;
    Admin admin = clientCache.getAdmin();
    final String TABLE_NAME_PREFIX = "createTableWithVaryingParams";

    // Null params
    try {
      admin.createTable("Table1", null);
    } catch (IllegalArgumentException ia) {
      expectedException = ia;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    // Empty MTableDescriptor
    expectedException = null;
    try {
      admin.createTable("Table1", new MTableDescriptor());
    } catch (IllegalArgumentException ia) {
      expectedException = ia;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    // Positive case
    expectedException = null;
    try {
      admin.createTable("Table1", new MTableDescriptor().addColumn(Bytes.toBytes("column1")));
    } catch (IllegalArgumentException ia) {
      expectedException = ia;
    }
    assertNull(expectedException);

    // UseTable false
    expectedException = null;
    try {
      MTableDescriptor desc = new MTableDescriptor();
      desc.setUserTable(false);
      desc.addColumn(Bytes.toBytes("column1"));
      admin.createTable("Table2", desc);
      getTableFrom("Table2");
    } catch (IllegalArgumentException ia) {
      expectedException = ia;
    }
    assertNull(expectedException);

    // UseTable false
    expectedException = null;
    try {
      MTableDescriptor desc = new MTableDescriptor();
      desc.setUserTable(true);
      desc.addColumn(Bytes.toBytes("column1"));
      admin.createTable("Table3", desc);
      getTableFrom("Table3");
    } catch (IllegalArgumentException ia) {
      expectedException = ia;
    }
    assertNull(expectedException);

    // Test with zero no of splits
    expectedException = null;
    try {
      MTableDescriptor desc = new MTableDescriptor();
      desc.addColumn(Bytes.toBytes("column1"));
      desc.setTotalNumOfSplits(0);
      admin.createTable("Table4", desc);
      getTableFrom("Table4");
    } catch (IllegalArgumentException ia) {
      expectedException = ia;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    // Test with 100 initial splits
    expectedException = null;
    try {
      MTableDescriptor desc = new MTableDescriptor();
      desc.addColumn(Bytes.toBytes("column1"));
      desc.setTotalNumOfSplits(100);
      admin.createTable("Table4", desc);
      getTableFrom("Table4");
    } catch (IllegalArgumentException ia) {
      expectedException = ia;
    }
    assertNull(expectedException);

    // Test with 10000 initial splits
    // expectedException = null;
    // try {
    // MTableDescriptor desc = new MTableDescriptor();
    // desc.addColumn(Bytes.toBytes("column1"));
    // desc.setTotalNumOfSplits(1000);
    // admin.createTable("Table5", desc);
    // getTableFrom("Table5");
    // } catch (IllegalArgumentException ia) {
    // expectedException = ia;
    // }
    // assertNull(expectedException);

    // Test default no of splits
    expectedException = null;
    try {
      MTableDescriptor desc = new MTableDescriptor();
      desc.addColumn(Bytes.toBytes("column1"));
      admin.createTable("Table6", desc);
      getTableFrom("Table6");
    } catch (IllegalArgumentException ia) {
      expectedException = ia;
    }
    assertNull(expectedException);
    MTableDescriptor table6Desc = clientCache.getMTableDescriptor("Table6");
    assertEquals(113, table6Desc.getTotalNumOfSplits());
    assertEquals(MTableType.ORDERED_VERSIONED, tableDescriptor.getTableType());

  }

  private void verifyTableExists() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    for (int i = 0; i <= 10; i++) {
      assertTrue(clientCache.getAdmin().tableExists("EmployeeTable" + i));
    }

    for (int i = 20; i <= 30; i++) {
      assertFalse(clientCache.getAdmin().tableExists("EmployeeTable" + i));
    }
  }

  private void verifyTableExistsOn(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        verifyTableExists();
        return null;
      }
    });
  }

  /*
   * Test Description 1. Create Connection 2. Try creation another connection from the same client.
   */
  @Test
  public void testMultipleConnectionsFromSameClient() {
    doMultipleConnectionsFromSameClient(this.client1);
  }

  @Test
  public void testCreateFromOneClientGetFromAnother() {
    createTablesOn(this.client1);
    verifyTableCanBeAccessFromOtherClient();

    verifyInternalRegionsCreatedOn(server1);
    verifyInternalRegionsCreatedOn(server2);
    verifyInternalRegionsCreatedOn(server3);
  }

  @Test
  public void testCreateTableWithSameName() {
    createTablesOn(this.client1);
    createTablesWithSameNameOn(this.client1);
  }

  @Test
  public void testCreateTableWithNoColumns() {
    createTableWithNoColumnsOn(this.client1);
  }

  /**
   * Tests MTable creation with valid & invalid table names
   *
   */
  @Test
  public void testCreateTableWithLegalAndIllegalNames() {
    createTableWithLegalAndIllegalNames(this.client1);
  }

  /**
   * Tests MTable creation parameter validations
   */
  @Test
  public void testCreateTableParams() {
    createTableWithVaryingParams(this.client1);
  }

  /**
   * Tests persistence of MTable descriptor values in META region
   *
   */
  @Test
  public void testVerifyMTableDescriptorParamInMetaRegion() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    Admin admin = clientCache.getAdmin();

    // Test table descriptor default values of all optional params
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(Bytes.toBytes("DUMMY_COLUMN"));

    String tableName = "Table1";
    try {
      admin.createTable(tableName, tableDescriptor);
    } catch (Throwable throwable) {
      fail();
    }
    restart();
    clientCache = MClientCacheFactory.getAnyInstance();
    admin = clientCache.getAdmin();
    assertNotNull(clientCache);

    MTableDescriptor mTableDescriptor = clientCache.getMTableDescriptor(tableName);
    assertNotNull(mTableDescriptor);
    assertEquals(2, mTableDescriptor.getAllColumnDescriptors().size());
    assertEquals("DUMMY_COLUMN",
        mTableDescriptor.getAllColumnDescriptors().get(0).getColumnNameAsString());
    assertEquals(1, mTableDescriptor.getMaxVersions());
    assertEquals(2, mTableDescriptor.getColumnDescriptorsMap().size());
    assertEquals(2, mTableDescriptor.getColumnsByName().size());
    assertTrue(mTableDescriptor.getColumnsByName()
        .containsKey(new ByteArrayKey(Bytes.toBytes("DUMMY_COLUMN"))));
    assertEquals(0, mTableDescriptor.getCoprocessorList().size());

    // TODO Bug MTable descriptor returns default disk store name even if disk persistence is
    // disabled
    assertEquals(false, mTableDescriptor.isDiskPersistenceEnabled());
    // assertNull(mTableDescriptor.getDiskStore());

    // TODO MTable descriptor returns ASYNCHRONOUS disk store policy even if persistence is disabled
    // assertNull(mTableDescriptor.getDiskWritePolicy());
    assertEquals(MEvictionPolicy.OVERFLOW_TO_DISK, mTableDescriptor.getEvictionPolicy());
    assertNotNull(mTableDescriptor.getKeySpace());
    assertEquals(113, mTableDescriptor.getKeySpace().size());
    assertEquals(2, mTableDescriptor.getNumOfColumns());
    assertEquals(0, mTableDescriptor.getRedundantCopies());

    // TODO incomplete javadoc for mTableDescriptor.getSchemaVersion()
    assertEquals(1, mTableDescriptor.getSchemaVersion());
    assertNull(mTableDescriptor.getStartRangeKey());
    assertNull(mTableDescriptor.getStopRangeKey());
    assertEquals(MTableType.ORDERED_VERSIONED, mTableDescriptor.getTableType());
    assertEquals(113, mTableDescriptor.getTotalNumOfSplits());
    assertTrue(mTableDescriptor.getUserTable());

    // Test table descriptor default values of all optional params for UNORDERED table
    tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    tableDescriptor.addColumn(Bytes.toBytes("DUMMY_COLUMN"));
    tableName = "Table2";
    try {
      admin.createTable(tableName, tableDescriptor);
    } catch (Throwable throwable) {
      throwable.printStackTrace();
      fail();
    }

    restart();
    clientCache = MClientCacheFactory.getAnyInstance();
    admin = clientCache.getAdmin();

    mTableDescriptor = clientCache.getMTableDescriptor(tableName);

    assertEquals(2, mTableDescriptor.getAllColumnDescriptors().size());
    assertEquals("DUMMY_COLUMN",
        mTableDescriptor.getAllColumnDescriptors().get(0).getColumnNameAsString());
    assertEquals(1, mTableDescriptor.getMaxVersions());
    assertEquals(2, mTableDescriptor.getColumnDescriptorsMap().size());
    assertEquals(2, mTableDescriptor.getColumnsByName().size());
    assertTrue(mTableDescriptor.getColumnsByName()
        .containsKey(new ByteArrayKey(Bytes.toBytes("DUMMY_COLUMN"))));
    assertEquals(0, mTableDescriptor.getCoprocessorList().size());
    assertEquals(false, mTableDescriptor.isDiskPersistenceEnabled());
    assertEquals(MEvictionPolicy.OVERFLOW_TO_DISK, mTableDescriptor.getEvictionPolicy());
    assertNotNull(mTableDescriptor.getKeySpace());
    assertEquals(113, mTableDescriptor.getKeySpace().size());
    assertEquals(2, mTableDescriptor.getNumOfColumns());
    assertEquals(0, mTableDescriptor.getRedundantCopies());
    assertEquals(1, mTableDescriptor.getSchemaVersion());
    assertNull(mTableDescriptor.getStartRangeKey());
    assertNull(mTableDescriptor.getStopRangeKey());
    assertEquals(MTableType.UNORDERED, mTableDescriptor.getTableType());
    assertEquals(113, mTableDescriptor.getTotalNumOfSplits());
    assertTrue(mTableDescriptor.getUserTable());

    // Test table descriptor default values of all optional params for disk persistence enabled
    tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(Bytes.toBytes("DUMMY_COLUMN"));
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    tableName = "Table3";
    try {
      admin.createTable(tableName, tableDescriptor);
    } catch (Throwable throwable) {
      fail();
    }

    mTableDescriptor = clientCache.getMTableDescriptor(tableName);

    assertEquals(2, mTableDescriptor.getAllColumnDescriptors().size());
    assertEquals("DUMMY_COLUMN",
        mTableDescriptor.getAllColumnDescriptors().get(0).getColumnNameAsString());
    assertEquals(1, mTableDescriptor.getMaxVersions());
    assertEquals(2, mTableDescriptor.getColumnDescriptorsMap().size());
    assertEquals(2, mTableDescriptor.getColumnsByName().size());
    assertTrue(mTableDescriptor.getColumnsByName()
        .containsKey(new ByteArrayKey(Bytes.toBytes("DUMMY_COLUMN"))));
    assertEquals(0, mTableDescriptor.getCoprocessorList().size());

    assertEquals(true, mTableDescriptor.isDiskPersistenceEnabled());
    assertEquals(MDiskWritePolicy.ASYNCHRONOUS, mTableDescriptor.getDiskWritePolicy());
    assertEquals(MEvictionPolicy.OVERFLOW_TO_DISK, mTableDescriptor.getEvictionPolicy());
    assertNotNull(mTableDescriptor.getKeySpace());
    assertEquals(113, mTableDescriptor.getKeySpace().size());
    assertEquals(2, mTableDescriptor.getNumOfColumns());
    assertEquals(0, mTableDescriptor.getRedundantCopies());
    assertEquals(1, mTableDescriptor.getSchemaVersion());
    assertNull(mTableDescriptor.getStartRangeKey());
    assertNull(mTableDescriptor.getStopRangeKey());
    assertEquals(MTableType.ORDERED_VERSIONED, mTableDescriptor.getTableType());
    assertEquals(113, mTableDescriptor.getTotalNumOfSplits());
    assertTrue(mTableDescriptor.getUserTable());

  }

  @Test
  public void testTableExists() {
    createTablesOn(this.client1);
    verifyTableExistsOn(this.client1);

    /* call tableExists on non created Table */
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);

    assertFalse(clientCache.getAdmin().tableExists("TABLE_NOT_CREATED"));

    assertTrue(clientCache.getAdmin().tableExists("EmployeeTable" + 5));

  }

  @Test
  public void testDeleteNoExists() {
    Exception e = null;
    try {
      MClientCacheFactory.getAnyInstance().getAdmin().deleteTable("NOT_CREATED");
    } catch (MTableNotExistsException mnee) {
      e = mnee;
    }
    assertTrue(e instanceof MTableNotExistsException);
  }

  @Test
  public void testTableNameWithPeridos() {
    MTable table = MClientCacheFactory.getAnyInstance().getAdmin().createTable(
        "TABLE.WITH.PERIODS.CHARACTER", new MTableDescriptor().addColumn(Bytes.toBytes("COLUMN1")));
    assertNotNull(table);

    /* Try getting the same table */
    this.client1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table =
            MClientCacheFactory.getAnyInstance().getTable("TABLE.WITH.PERIODS.CHARACTER");
        assertNotNull(table);
        return null;
      }
    });

    /* create table from Server VM API */
    this.server1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = null;
        try {
          table = MCacheFactory.getAnyInstance().getAdmin().createTable(
              "TABLE.WITH.PERIODS.CHARACTER.1",
              new MTableDescriptor().addColumn(Bytes.toBytes("COLUMN1")));
        } catch (MTableExistsException e) {
          table = MCacheFactory.getAnyInstance().getTable("TABLE.WITH.PERIODS.CHARACTER.1");
        }
        assertNotNull(table);
        return null;
      }
    });

    /* Access the same table from another vm using server API */
    this.server2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MCacheFactory.getAnyInstance().getTable("TABLE.WITH.PERIODS.CHARACTER.1");
        assertNotNull(table);
        return null;
      }
    });


  }

}
