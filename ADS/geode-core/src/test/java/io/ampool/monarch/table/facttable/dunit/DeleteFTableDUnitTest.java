package io.ampool.monarch.table.facttable.dunit;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.geode.test.junit.categories.FTableTest;

import io.ampool.tierstore.wal.WriteAheadLog;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
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

@Category(FTableTest.class)
public class DeleteFTableDUnitTest extends MTableDUnitHelper {

  private volatile Exception exceptionThread1 = null;
  private volatile Throwable exceptionThread2 = null;

  private static final int NUM_OF_COLUMNS = 10;
  private static final int NUM_OF_SPLITS = 113;
  private static final String COLUMN_NAME_PREFIX = "COL";
  // private static final String TABLE_NAME = "MTableRaceCondition";

  public DeleteFTableDUnitTest() {

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

  private void verifyTableCreate(final String tableName) {
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2)).forEach(vm -> {
      vm.invoke(() -> {
        DeleteFTableDUnitTest.verifyTableOnServer(tableName,
            getFTableDescriptor(DeleteFTableDUnitTest.NUM_OF_SPLITS, 0));
      });
    });
  }

  private void verifyTableDelete(final String tableName) {
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2)).forEach(vm -> {
      vm.invoke(() -> {
        assertNull(MCacheFactory.getAnyInstance().getFTable(tableName));
        // TODO verify WAL and Store artifacts gets deleted
        assertTrue(WriteAheadLog.getInstance().getAllFilesForTable(tableName) == null
            || 0 == WriteAheadLog.getInstance().getAllFilesForTable(tableName).length);
      });
    });
  }


  /**
   * Normal table creation positive test case
   */
  @Test
  public void testFTableDelete() {
    String tableName = getTestMethodName();
    final FTable table = DeleteFTableDUnitTest.createFTable(tableName,
        getFTableDescriptor(DeleteFTableDUnitTest.NUM_OF_SPLITS, 0));
    assertNotNull(table);
    verifyTableCreate(tableName);
    DeleteFTableDUnitTest.deleteFTable(tableName);
    assertNull(MCacheFactory.getAnyInstance().getFTable(tableName));
    verifyTableDelete(tableName);

  }


  /**
   * Normal table deletion positive test case
   */
  @Test
  public void testFTableCreationAndGetNameFromTableDescriptor() {
    String tableName = getTestMethodName();
    final FTable table = DeleteFTableDUnitTest.createFTable(tableName,
        getFTableDescriptor(DeleteFTableDUnitTest.NUM_OF_SPLITS, 0));
    assertNotNull(table);
    assertNotNull(table.getTableDescriptor().getTableName());
    assertEquals(tableName, table.getTableDescriptor().getTableName());
    verifyTableCreate(tableName);
    DeleteFTableDUnitTest.deleteFTable(tableName);
    assertNull(MCacheFactory.getAnyInstance().getFTable(tableName));
    verifyTableDelete(tableName);
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
        FTable table = DeleteFTableDUnitTest.createFTable(tableName + "1",
            getFTableDescriptor(DeleteFTableDUnitTest.NUM_OF_SPLITS, 0));
        assertNotNull(table);
        return true;
      }
    });

    final Thread thread2 = invokeInThreadOnVM(vm3, new SerializableCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        DeleteFTableDUnitTest.createFTable(tableName + "2",
            getFTableDescriptor(DeleteFTableDUnitTest.NUM_OF_SPLITS, 0));
        return true;
      }
    });

    verifyTableCreate(tableName + "1");
    verifyTableCreate(tableName + "2");
    // Joining thread
    thread.join();
    thread2.join();

    DeleteFTableDUnitTest.deleteFTable(tableName + "1");
    assertNull(MCacheFactory.getAnyInstance().getFTable(tableName + "1"));
    verifyTableDelete(tableName + "1");

    DeleteFTableDUnitTest.deleteFTable(tableName + "2");
    assertNull(MCacheFactory.getAnyInstance().getFTable(tableName + "2"));
    verifyTableDelete(tableName + "2");
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
    } while (mtable == null && retries < 500);
    assertNotNull(mtable);
    final Region<Object, Object> mregion = ((ProxyFTableRegion) mtable).getTableRegion();
    String path = mregion.getFullPath();
    assertTrue(path.contains(tableName));
    // To verify disk persistence
    assertEquals(tableDescriptor.isDiskPersistenceEnabled(),
        mregion.getAttributes().getDataPolicy().withPersistence());
  }

  private static void deleteFTable(final String tableName) {
    MClientCacheFactory.getAnyInstance().getAdmin().deleteFTable(tableName);
  }

  private static FTable createFTable(final String tableName,
      final FTableDescriptor tableDescriptor) {
    int numberOfKeysPerBucket = 20;
    int splits = 10;
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

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


