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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.store.StoreHandler;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.internal.DefaultStore;

@Category(FTableTest.class)
public class DefaultStoreDUnitTest extends MTableDUnitHelper {


  private static final int NUM_OF_COLUMNS = 10;
  private static final int NUM_OF_SPLITS = 113;
  private static final String COLUMN_NAME_PREFIX = "COL";

  public DefaultStoreDUnitTest() {

  }

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

  @Test
  public void testDefaultStoreCreation() {
    String tableName = getTestMethodName();

    // verify that default store is created
    vm0.invoke(() -> {
      verifyDefaultStore(tableName);
    });

    vm1.invoke(() -> {
      verifyDefaultStore(tableName);
    });

    vm2.invoke(() -> {
      verifyDefaultStore(tableName);
    });

  }

  @Test
  public void testDefaultStoreAttachedToFTable() {
    String tableName = getTestMethodName();
    FTableDescriptor fTableDescriptor = getFTableDescriptor(DefaultStoreDUnitTest.NUM_OF_SPLITS, 0);
    final FTable table = DefaultStoreDUnitTest.createFTable(tableName, fTableDescriptor);
    assertNotNull(table);
    FTableDescriptor tableDescriptor = table.getTableDescriptor();
    // as we have not provided the tier stores
    assertEquals(0, fTableDescriptor.getTierStores().size());

    // as it has to be added while creating table
    assertEquals(1, tableDescriptor.getTierStores().size());
    assertTrue(tableDescriptor.getTierStores().containsKey(DefaultStore.STORE_NAME));


    vm0.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm0");
      DefaultStoreDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm0");
    });
    vm1.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm1");
      DefaultStoreDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm1");
    });
    vm2.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm2");
      DefaultStoreDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm2");
    });
  }

  @Test
  public void testDefaultStoreNotAttachedToFTable() {
    String tableName = getTestMethodName();
    FTableDescriptor fTableDescriptor = getFTableDescriptor(DefaultStoreDUnitTest.NUM_OF_SPLITS, 0);
    fTableDescriptor.setEvictionPolicy(MEvictionPolicy.NO_ACTION);
    final FTable table = DefaultStoreDUnitTest.createFTable(tableName, fTableDescriptor);
    assertNotNull(table);
    FTableDescriptor tableDescriptor = table.getTableDescriptor();
    // as we have not provided the tier stores
    assertEquals(0, fTableDescriptor.getTierStores().size());

    // as it has to be added while creating table
    assertEquals(0, tableDescriptor.getTierStores().size());


    vm0.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm0");
      DefaultStoreDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm0");
    });
    vm1.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm1");
      DefaultStoreDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm1");
    });
    vm2.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm2");
      DefaultStoreDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm2");
    });
  }

  private void verifyDefaultStore(final String tableName) {
    Region<String, Map<String, Object>> storeMetaRegion =
        MCacheFactory.getAnyInstance().getStoreMetaRegion();
    final Set<Entry<String, Map<String, Object>>> storeEntries = storeMetaRegion.entrySet();
    assertEquals(1, storeEntries.size());
    final Iterator<Entry<String, Map<String, Object>>> storeItr = storeEntries.iterator();
    while (storeItr.hasNext()) {
      Entry<String, Map<String, Object>> mapEntry = storeItr.next();
      assertEquals(DefaultStore.STORE_NAME, mapEntry.getKey());
    }

    Map<String, TierStore> initializedStores = StoreHandler.getInstance().getTierStores();
    assertEquals(1, initializedStores.size());

    Iterator<Entry<String, TierStore>> entryIterator = initializedStores.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Entry<String, TierStore> storeEntry = entryIterator.next();
      assertEquals(DefaultStore.STORE_NAME, storeEntry.getKey());
      assertEquals(DefaultStore.STORE_CLASS, storeEntry.getValue().getClass().getCanonicalName());

      assertEquals(DefaultStore.ORC_READER_CLASS,
          storeEntry.getValue().getReader(tableName, 1).getClass().getCanonicalName());
      assertEquals(DefaultStore.ORC_WRITER_CLASS,
          storeEntry.getValue().getWriter(tableName, 1).getClass().getCanonicalName());
    }
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
    if (tableDescriptor.getEvictionPolicy() == MEvictionPolicy.OVERFLOW_TO_TIER) {
      FTableDescriptor tableDescriptor1 =
          MCacheFactory.getAnyInstance().getFTableDescriptor(tableName);
      assertEquals(1, tableDescriptor1.getTierStores().size());
      assertTrue(tableDescriptor1.getTierStores().containsKey(DefaultStore.STORE_NAME));
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


