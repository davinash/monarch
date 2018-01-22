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

import io.ampool.monarch.table.internal.MTableImpl;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MBaseRegionObserver;
import io.ampool.monarch.table.exceptions.MTableExistsException;

@Category(MonarchTest.class)
public class CreateMTableDUnitTest extends MTableDUnitHelper {

  private volatile Exception exceptionThread1 = null;
  private volatile Throwable exceptionThread2 = null;

  private static final int NUM_OF_COLUMNS = 10;
  private static final int NUM_OF_SPLITS = 113;
  private static final String COLUMN_NAME_PREFIX = "COL";
  private static final String TABLE_NAME = "MTableRaceCondition";

  public CreateMTableDUnitTest() {
    super();
  }

  private static Host host = null;
  private static VM vm0 = null;
  private static VM vm1 = null;
  private static VM vm2 = null;
  private static VM vm3 = null;
  final int numOfEntries = 3;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    startServerOn(CreateMTableDUnitTest.vm0, DUnitLauncher.getLocatorString());
    startServerOn(CreateMTableDUnitTest.vm1, DUnitLauncher.getLocatorString());
    startServerOn(CreateMTableDUnitTest.vm2, DUnitLauncher.getLocatorString());
    createClientCache(CreateMTableDUnitTest.vm3);
    createClientCache();
  }

  /**
   * Normal table creation positive test case
   */
  @Test
  public void testMTableCreation() {
    final MTable table = CreateMTableDUnitTest.createMTable(TABLE_NAME,
        getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
    assertNotNull(table);
    vm0.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm0");
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME,
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm0");
    });
    vm1.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm1");
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME,
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm1");
    });
    vm2.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm2");
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME,
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm2");
    });
  }

  /**
   * Positive case for table creation Two tables with different name are created though different
   * thread
   */
  @Test
  public void testTwoMTableCreationThreaded() throws InterruptedException {
    final Thread thread = invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        MTable table = CreateMTableDUnitTest.createMTable(TABLE_NAME + "1",
            getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
        assertNotNull(table);
        return true;
      }
    });

    final Thread thread2 = invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        CreateMTableDUnitTest.createMTable(TABLE_NAME + "2",
            getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
        return true;
      }
    });

    // verify tables created
    vm0.invoke(() -> {
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME + "1",
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME + "2",
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm1.invoke(() -> {
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME + "1",
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME + "2",
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm2.invoke(() -> {
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME + "1",
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME + "2",
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
    });
    // Joining thread
    thread.join();
    thread2.join();
  }

  /**
   * Positive case for table creation Two tables with different name are created though different
   * thread
   */
  @Test
  public void testTwoMTableWithSameNameCreationThreaded() throws InterruptedException {
    IgnoredException.addIgnoredException(MTableExistsException.class.getName());
    final Thread thread = invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        MTable table = CreateMTableDUnitTest.createMTable(TABLE_NAME,
            getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
        return true;
      }
    });

    // This should fail with MTableExistException
    final Thread thread2 = invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          MTable table = CreateMTableDUnitTest.createMTable(TABLE_NAME,
              getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 1));
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
        + exceptionThread2);
    assertNotNull(exceptionThread2);
    // IMP: using getCause since exception from invoke method on VM is wrapping it as RMIException
    assertTrue(exceptionThread2.getCause() instanceof MTableExistsException);

    // TODO what thread will be succeeded is unpredictable, so how to check it
    // 1. Get tabledescriptor from the meta region that should be present on each of the server
    final MTableDescriptor desc =
        MClientCacheFactory.getAnyInstance().getMTableDescriptor(TABLE_NAME);
    // verify tables created
    vm0.invoke(() -> {
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME, desc);
    });
    vm1.invoke(() -> {
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME, desc);
    });
    vm2.invoke(() -> {
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME, desc);
    });
    IgnoredException.removeAllExpectedExceptions();
  }

  /**
   * Create table and fail one server in between. The things should be roll backed
   *
   * This has been alternately tested by approximately closing server But since no way to reporduce
   * through DUnit disabling it
   *
   * TODO Yet to complete Difficulty is picking right point to close server so as function execution
   * is failed when it is trying to create region. There is no way other than adding test code into
   * the production one
   *
   * @throws InterruptedException
   */
  @Ignore
  @Test
  public void __testMTableCreationAfterOneServerFailed() throws InterruptedException {
    final Thread thread = invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          MTable table = CreateMTableDUnitTest.createMTable(TABLE_NAME,
              getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
          assertNull(table);
        } catch (Exception e) {
          throw e;
        }
        return true;
      }
    });
    stopServer(vm1);
    thread.join();
    System.out.println("CreateMTableDUnitTest.testMTableCreationAfterOneServerFailed :: "
        + "Exception: " + exceptionThread2);
    assertNotNull(exceptionThread2);
    System.out.println("CreateMTableDUnitTest.testMTableCreationAfterOneServerFailed :: "
        + "Exception: " + exceptionThread2.getCause());
    // vm1.invoke(() -> {
    // System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying
    // vm0");
    // CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME,
    // getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
    // System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm0");
    // });
    // System.out.println("--------------------------------------------------------------------------------------------------------------------");
    vm0.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm0");
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME,
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm0");
    });
    vm2.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm2");
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME,
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm2");
    });
    System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Joined...........");
  }

  /**
   *
   * Create Mtable should be failed some where in between that should lead to roll back. At the end
   * there shouldn't be any region for table on any server
   *
   * It is tested by throwing exception from production code But this is not possible to keep so
   * disabling this test.
   *
   * TODO Yet to complete, not sure how to inject error in table creation Difficulty is picking
   * right point to close server so as function execution is failed when it is trying to create
   * region. There is no way other than adding test code into the production one
   *
   * @throws InterruptedException
   */
  @Ignore
  @Test
  public void __testMTableCreationRollBackScenario() throws InterruptedException {
    final Thread thread = invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        CreateMTableDUnitTest.createMTable(TABLE_NAME,
            getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
        return true;
      }
    });

    // CreateMTableDUnitTest.createMTable(TABLE_NAME,
    // getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
    // System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "After starting
    // thread");
    //
    // invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
    // @Override public Boolean call() throws Exception {
    // System.out.println("CreateMTableDUnitTest.call :: " + "Inside call method vm3 t2");
    // CreateMTableDUnitTest.createMTable(TABLE_NAME,
    // getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
    // System.out.println("CreateMTableDUnitTest.call :: " + "Created with persistence");
    // return true;
    // }
    // });
    // Thread.sleep(5000l);
    // stopServer(vm1);
    // vm1.bounce();


    vm1.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm0");
      CreateMTableDUnitTest.verifyTableNotExistOnServer(TABLE_NAME,
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm0");
    });
    System.out.println(
        "--------------------------------------------------------------------------------------------------------------------");
    vm0.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm0");
      CreateMTableDUnitTest.verifyTableNotExistOnServer(TABLE_NAME,
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm0");
    });
    vm2.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm2");
      CreateMTableDUnitTest.verifyTableNotExistOnServer(TABLE_NAME,
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm2");
    });
    // System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Done with
    // verifying, waiting to join");
    // try {
    // System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "isThreadRunning:
    // "+thread.isAlive()+ " isIntr:"+thread.isInterrupted());
    // thread.join();
    // }
    // catch (InterruptedException e) {
    // e.printStackTrace();
    // }
    thread.join();
    System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Joined...........");
  }

  /**
   * Random code...
   */
  @Ignore
  @Test
  public void _____testMTableCreation() {
    // final Thread thread = invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
    // @Override public Boolean call() throws Exception {
    // System.out.println("CreateMTableDUnitTest.call :: " + "Inside call method vm3 t1");
    // CreateMTableDUnitTest.createMTable(TABLE_NAME,
    // getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
    // System.out.println("CreateMTableDUnitTest.call :: " + "Created with-out persistence");
    // return true;
    // }
    // });
    final MTable table = CreateMTableDUnitTest.createMTable(TABLE_NAME,
        getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
    assertNotNull(table);
    // System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "After starting
    // thread");
    //
    // invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
    // @Override public Boolean call() throws Exception {
    // System.out.println("CreateMTableDUnitTest.call :: " + "Inside call method vm3 t2");
    // CreateMTableDUnitTest.createMTable(TABLE_NAME,
    // getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
    // System.out.println("CreateMTableDUnitTest.call :: " + "Created with persistence");
    // return true;
    // }
    // });
    vm0.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm0");
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME,
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm0");
    });
    vm1.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm1");
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME,
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm1");
    });
    vm2.invoke(() -> {
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "beforeVerifying vm2");
      CreateMTableDUnitTest.verifyTableOnServer(TABLE_NAME,
          getmTableDescriptor(CreateMTableDUnitTest.NUM_OF_SPLITS, 0));
      System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on vm2");
    });
    // System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Done with
    // verifying, waiting to join");
    // try {
    // System.out.println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "isThreadRunning:
    // "+thread.isAlive()+ " isIntr:"+thread.isInterrupted());
    // thread.join();
    // }
    // catch (InterruptedException e) {
    // e.printStackTrace();
    // }
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
      final MTableDescriptor tableDescriptor) {
    final MCache serverCache = MCacheFactory.getAnyInstance();
    assertNotNull(serverCache);
    MTable mtable = null;
    int retries = 0;
    do {
      mtable = serverCache.getTable(tableName);
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
    final Region<Object, Object> mregion = ((MTableImpl) mtable).getTableRegion();
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
    MTable mtable = null;
    int retries = 0;
    do {
      mtable = serverCache.getTable(tableName);
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

  private static MTable createMTable(final String tableName,
      final MTableDescriptor tableDescriptor) {
    // System.out.println("CreateMTableDUnitTest.createMTable :: " + "Creating mtable:---- " +
    // tableDescriptor);
    int numberOfKeysPerBucket = 20;
    int splits = 10;
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    // System.out.println("CreateMTableDUnitTest.createMTable :: " + "isCacheClosed: " +
    // clientCache.isClosed());
    // MTableDescriptor tableDescriptor = getmTableDescriptor(splits);

    MTable mtable = null;
    try {
      mtable = clientCache.getAdmin().createTable(tableName, tableDescriptor);
    } catch (Exception e) {
      System.out.println("CreateMTableDUnitTest.createMTable :: " + "Throwing from test");
      throw e;
    }
    System.out.println("CreateMTableDUnitTest.createMTable :: " + "mtable is " + mtable);
    return mtable;

  }

  private static MTableDescriptor getmTableDescriptor(final int splits, int descriptorIndex) {
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    if (descriptorIndex == 0) {

      // tableDescriptor.addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRegionObserver1");
      tableDescriptor
          .addCoprocessor("io.ampool.monarch.table.CreateMTableDUnitTest$CreateMTableObserver");
      tableDescriptor.setTotalNumOfSplits(splits);
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.setRedundantCopies(1);
    } else {
      tableDescriptor
          .addCoprocessor("io.ampool.monarch.table.CreateMTableDUnitTest$CreateMTableObserver");
      tableDescriptor.setTotalNumOfSplits(splits);
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS);
      tableDescriptor.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_DISK);
      tableDescriptor.setRedundantCopies(2);
    }
    return tableDescriptor;
  }


  @Override
  public void tearDown2() throws Exception {
    // closeMClientCache(CreateMTableDUnitTest.vm0);
    // closeMClientCache(CreateMTableDUnitTest.vm1);
    // closeMClientCache(CreateMTableDUnitTest.vm2);
    closeMClientCache(CreateMTableDUnitTest.vm3);
    closeMClientCache();
    // closeAllMCaches();
    super.tearDown2();
  }

  public static class CreateMTableObserver extends MBaseRegionObserver {

  }
}


