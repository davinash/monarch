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

import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import io.ampool.monarch.table.ftable.exceptions.FTableExistsException;
import io.ampool.monarch.table.ftable.internal.FTableImpl;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
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
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MTableExistsException;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.ftable.exceptions.TierStoreNotAvailableException;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;

@Category(FTableTest.class)
public class CreateFactTableDUnitTest extends MTableDUnitHelper {

  private volatile Exception exceptionThread1 = null;
  private volatile Throwable exceptionThread2 = null;

  private static final int NUM_OF_COLUMNS = 10;
  private static final int NUM_OF_SPLITS = 113;
  private static final String COLUMN_NAME_PREFIX = "COL";
  // private static final String TABLE_NAME = "MTableRaceCondition";

  public CreateFactTableDUnitTest() {

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

  /**
   * Normal table creation positive test case
   */
  @Test
  public void testFTableCreation() {
    String tableName = getTestMethodName();
    FTableDescriptor fTableDescriptor =
            getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 0);
    final FTable table = CreateFactTableDUnitTest.createFTable(tableName, fTableDescriptor);
    assertNotNull(table);
    vm0.invoke(() -> {
      System.out
              .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm0");
      CreateFactTableDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm0");
    });
    vm1.invoke(() -> {
      System.out
              .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm1");
      CreateFactTableDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm1");
    });
    vm2.invoke(() -> {
      System.out
              .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm2");
      CreateFactTableDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm2");
    });
  }

  /**
   * Ftable creation with new store api
   */
  @Test
  public void testFTableCreationWithStores() {
    String tableName = getTestMethodName();
    FTableDescriptor fTableDescriptor =
            getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 0);

    final FTable table = CreateFactTableDUnitTest.createFTable(tableName, fTableDescriptor);
    assertNotNull(table);
    vm0.invoke(() -> {
      System.out
              .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm0");
      CreateFactTableDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm0");
    });
    vm1.invoke(() -> {
      System.out
              .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm1");
      CreateFactTableDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm1");
    });
    vm2.invoke(() -> {
      System.out
              .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm2");
      CreateFactTableDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm2");
    });
  }

  /**
   * Ftable creation with new store api
   */
  @Test
  public void testFTableCreationWithStoresNonAvailableStore() {
    IgnoredException.addIgnoredException(TierStoreNotAvailableException.class.getName());
    String tableName = getTestMethodName();
    FTableDescriptor fTableDescriptor =
            getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 0);
    // add stores
    LinkedHashMap<String, TierStoreConfiguration> tiers = new LinkedHashMap<>();
    tiers.put("abc", new TierStoreConfiguration());
    fTableDescriptor.addTierStores(tiers);
    Exception exception = null;
    try {
      final FTable table = CreateFactTableDUnitTest.createFTable(tableName, fTableDescriptor);
      assertNull(table);
    } catch (TierStoreNotAvailableException ex) {
      exception = ex;
    }
    if (exception == null) {
      fail("Expecting TierStoreNotAvailableException");
    }
    IgnoredException.removeAllExpectedExceptions();
  }


  /**
   * Normal table creation positive test case
   */
  @Test
  public void testFTableCreationAndGetNameFromTableDescriptor() {
    String tableName = getTestMethodName();
    FTableDescriptor fTableDescriptor =
            getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 0);
    final FTable table = CreateFactTableDUnitTest.createFTable(tableName, fTableDescriptor);

    System.out
            .println("CreateFactTableDUnitTest.testFTableCreationAndGetNameFromTableDescriptor :: "
                    + "table: " + table);
    assertNotNull(table);
    vm0.invoke(() -> {
      System.out
              .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm0");
      CreateFactTableDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm0");
    });
    vm1.invoke(() -> {
      System.out
              .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm1");
      CreateFactTableDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm1");
    });
    vm2.invoke(() -> {
      System.out
              .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm2");
      CreateFactTableDUnitTest.verifyTableOnServer(tableName, fTableDescriptor);
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm2");
    });
    assertNotNull(table.getTableDescriptor().getTableName());
    assertEquals(tableName, table.getTableDescriptor().getTableName());
  }


  /**
   * Positive case for table creation Two tables with different name are created though different
   * thread
   */
  @Test
  public void testTwoFTableCreationThreaded() throws InterruptedException {
    String tableName = getTestMethodName();
    final Thread thread = invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        FTableDescriptor fTableDescriptor =
                getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 0);
        FTable table = CreateFactTableDUnitTest.createFTable(tableName + "1", fTableDescriptor);
        assertNotNull(table);
        return true;
      }
    });

    final Thread thread2 = invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        FTableDescriptor fTableDescriptor =
                getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 0);
        CreateFactTableDUnitTest.createFTable(tableName + "2", fTableDescriptor);
        return true;
      }
    });

    // verify tables created
    vm0.invoke(() -> {
      CreateFactTableDUnitTest.verifyTableOnServer(tableName + "1",
              getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 0));
      CreateFactTableDUnitTest.verifyTableOnServer(tableName + "2",
              getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm1.invoke(() -> {
      CreateFactTableDUnitTest.verifyTableOnServer(tableName + "1",
              getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 0));
      CreateFactTableDUnitTest.verifyTableOnServer(tableName + "2",
              getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm2.invoke(() -> {
      CreateFactTableDUnitTest.verifyTableOnServer(tableName + "1",
              getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 0));
      CreateFactTableDUnitTest.verifyTableOnServer(tableName + "2",
              getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 0));
    });
    // Joining thread
    thread.join();
    thread2.join();
  }

  private static final AtomicBoolean executeThread2 = new AtomicBoolean(false);

  /**
   * Positive case for table creation Two tables with different name are created though different
   * thread
   */
  @Test
  public void testTwoFTableWithSameNameCreationThreaded() throws InterruptedException {
    IgnoredException.addIgnoredException(FTableExistsException.class.getName());
    String tableName = getTestMethodName();
    final Thread thread = invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          FTableDescriptor fTableDescriptor =
                  getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 0);
          FTable table = CreateFactTableDUnitTest.createFTable(tableName, fTableDescriptor);
          assertNotNull(table);
          executeThread2.set(true);
        } catch (Exception e) {
          fail("No exception expected but got: " + e.getMessage());
        }
        return true;
      }
    });

    // This should fail with MTableExistException
    final Thread thread2 = invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          FTableDescriptor fTableDescriptor =
                  getFTableDescriptor(CreateFactTableDUnitTest.NUM_OF_SPLITS, 1);
          /* wait till previous thread completes the table creation */
          while (!executeThread2.get()) {
            Thread.sleep(100);
          }
          FTable table = CreateFactTableDUnitTest.createFTable(tableName, fTableDescriptor);
          assertNull(table);
        } catch (Exception e) {
          throw e;
        }
        return true;
      }
    });

    thread.join();
    thread2.join();
    System.out.println("CreateMTableDUnitTest.testTwoFTableWithSameNameCreationThreaded :: " + ""
            + exceptionThread2.getCause());
    assertNotNull(exceptionThread2);
    // IMP: using getCause since exception from invoke method on VM is wrapping it as RMIException
    assertTrue(exceptionThread2.getCause() instanceof FTableExistsException);

    // TODO what thread will be succeeded is unpredictable, so how to check it
    // 1. Get tabledescriptor from the meta region that should be present on each of the server
    final FTableDescriptor desc =
            MClientCacheFactory.getAnyInstance().getFTableDescriptor(tableName);
    // verify tables created
    vm0.invoke(() -> {
      CreateFactTableDUnitTest.verifyTableOnServer(tableName, desc);
    });
    vm1.invoke(() -> {
      CreateFactTableDUnitTest.verifyTableOnServer(tableName, desc);
    });
    vm2.invoke(() -> {
      CreateFactTableDUnitTest.verifyTableOnServer(tableName, desc);
    });
    MClientCacheFactory.getAnyInstance().getAdmin().deleteFTable(tableName);
    IgnoredException.removeAllExpectedExceptions();
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
    final Region<Object, Object> mregion = ((FTableImpl) mtable).getTableRegion();
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


