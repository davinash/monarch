package io.ampool.monarch.table;

import org.apache.geode.cache.*;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.MTableUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Test test create Mtable api and verify region attributes & properties
 * 
 *
 */
@Category(MonarchTest.class)
public class MTableDiskPersistanceTest extends MTableDUnitHelper {
  public MTableDiskPersistanceTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  private VM mTableClient1 = null;

  protected final int NUM_OF_COLUMNS = 10;
  protected final String EMPLOYEE_TABLE_DEFAULT = "EmployeeTableDefault";
  protected final String EMPLOYEE_TABLE_PERSISTENT = "EmployeeTablePersistent";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 10;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected final int LATEST_TIMESTAMP = 300;
  protected final int MAX_VERSIONS = 5;
  protected final int TABLE_MAX_VERSIONS = 7;

  protected final int MTABLE_REGION_DEFAULT_PARTITION_REDUDANT_COPIES = 1;
  final int numOfEntries = 100;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    mTableClient1 = client1;
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.mTableClient1);

    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(mTableClient1);
    super.tearDown2();
  }

  protected void createTable(String tableName, boolean persistentEnabled) {

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
            .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
    tableDescriptor.setRedundantCopies(1);
    if (persistentEnabled) {
      tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    }

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createTable(tableName, tableDescriptor);
    assertEquals(mtable.getName(), tableName);
  }

  protected void createTableOn(VM vm, String tableName, boolean persistentEnabled) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(tableName, persistentEnabled);
        return null;
      }
    });
  }

  private void verifyRegionAttributes(VM client, String tableName, boolean persistentEnabled) {
    SerializableRunnable verifyRegionAttr =
            new SerializableRunnable("Verify MTable server default properties") {
              private static final long serialVersionUID = 1L;

              public void run() throws CacheException {
                MCache cache = MCacheFactory.getAnyInstance();
                Region region = cache.getRegion(tableName);
                assertEquals(true, region.getAttributes().getDataPolicy().withPartitioning());
                assertEquals(MTABLE_REGION_DEFAULT_PARTITION_REDUDANT_COPIES,
                        region.getAttributes().getPartitionAttributes().getRedundantCopies());
                assertEquals(persistentEnabled,
                        region.getAttributes().getDataPolicy().withPersistence());
                if (!persistentEnabled) {
                  assertNull(region.getAttributes().getDiskStoreName());
                  assertNotNull(cache.findDiskStore(MTableUtils.DEFAULT_MTABLE_DISK_STORE_NAME));
                } else {
                  assertEquals(region.getAttributes().getDataPolicy(), DataPolicy.PERSISTENT_PARTITION);
                  assertNotNull(region.getAttributes().getDiskStoreName());
                  assertNotNull(cache.findDiskStore(MTableUtils.DEFAULT_MTABLE_DISK_STORE_NAME));
                  int filesCount = 0;
                  for (File file : cache.findDiskStore(MTableUtils.DEFAULT_MTABLE_DISK_STORE_NAME)
                          .getDiskDirs()) {
                    List<File> files = (List<File>) FileUtils.listFiles(file, TrueFileFilter.INSTANCE,
                            TrueFileFilter.INSTANCE);
                    for (File file2 : files) {
                      filesCount++;
                    }
                  }

                  assertTrue(filesCount > 2);
                }
                assertEquals(persistentEnabled,
                        region.getAttributes().getDataPolicy().withPersistence());
                File diskDir = getDiskDir();
                assertNotNull(getDiskDir());
              }
            };
    client.invoke(verifyRegionAttr);
  }


  private void createTableFromClient(VM vm, String tableName, boolean persistenTable) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          createTable(tableName, persistenTable);
        } catch (CacheClosedException cce) {
        }

        return null;
      }
    });

  }

  private void doPutGetOperationFromClient(VM vm, String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {

          MTable mtable = MClientCacheFactory.getAnyInstance().getTable(tableName);

          for (int i = 0; i < numOfEntries; i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("User" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }

          // gets
          for (int i = 0; i < numOfEntries; i++) {

            String key1 = "RowKey" + i;
            Put myput = new Put(Bytes.toBytes(key1));
            myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("User" + i));
            myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            Get myget = new Get(Bytes.toBytes("RowKey" + i));
            Row result = mtable.get(myget);
            assertFalse(result.isEmpty());

            List<Cell> row = result.getCells();

            Iterator<MColumnDescriptor> iteratorColumnDescriptor =
                    mtable.getTableDescriptor().getAllColumnDescriptors().iterator();

            for (int k = 0; k < row.size() - 1; k++) {
              byte[] expectedColumnName = iteratorColumnDescriptor.next().getColumnName();
              byte[] expectedColumnValue =
                      (byte[]) myput.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));
              if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
                Assert.fail("Invalid Values for Column Name");
              }
              if (!Bytes.equals(expectedColumnValue, (byte[]) row.get(k).getColumnValue())) {
                System.out
                        .println("expectedColumnValue =>  " + Arrays.toString(expectedColumnValue));
                System.out.println("actuaColumnValue    =>  "
                        + Arrays.toString((byte[]) row.get(k).getColumnValue()));
                Assert.fail("Invalid Values for Column Value");
              }
            }
          }

          // Add additional 10 records to perform delete
          for (int i = numOfEntries; i < (numOfEntries + 10); i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("User" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }

          for (int i = numOfEntries; i < (numOfEntries + 10); i++) {
            String key1 = "RowKey" + i;
            Delete mdelete = new Delete(Bytes.toBytes(key1));
            mtable.delete(mdelete);
          }
        } catch (CacheClosedException cce) {
        }

        return null;
      }
    });
  }

  private void checkTableExists(VM vm, String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable mtable = MClientCacheFactory.getAnyInstance().getTable(tableName);
        assertNotNull(mtable);
        assertEquals(tableName, mtable.getName());
        return null;
      }
    });
  }

  private void doGetFromClient(String tableName, final boolean order) {
    try {

      MTable mtable = MClientCacheFactory.getAnyInstance().getTable(tableName);
      // gets
      for (int i = 0; i < numOfEntries; i++) {

        String key1 = "RowKey" + i;
        Put myput = new Put(Bytes.toBytes(key1));
        myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("User" + i));
        myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
        myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
        myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

        Get myget = new Get(Bytes.toBytes("RowKey" + i));
        Row result = mtable.get(myget);
        assertFalse(result.isEmpty());

        List<Cell> row = result.getCells();

        Iterator<MColumnDescriptor> iteratorColumnDescriptor =
                mtable.getTableDescriptor().getAllColumnDescriptors().iterator();

        for (int k = 0; k < row.size() - 1; k++) {
          byte[] expectedColumnName = iteratorColumnDescriptor.next().getColumnName();
          byte[] expectedColumnValue =
                  (byte[]) myput.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));
          if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
            Assert.fail("Invalid Values for Column Name");
          }
          if (!Bytes.equals(expectedColumnValue, (byte[]) row.get(k).getColumnValue())) {
            System.out.println("expectedColumnValue =>  " + Arrays.toString(expectedColumnValue));
            System.out.println(
                    "actuaColumnValue    =>  " + Arrays.toString((byte[]) row.get(k).getColumnValue()));
            Assert.fail("Invalid Values for Column Value");
          }
        }
      }

    } catch (CacheClosedException cce) {
    }
  }

  private void doGetFromClient(VM vm, String tableName, final boolean order) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {

          MTable mtable = MClientCacheFactory.getAnyInstance().getTable(tableName);
          // gets
          for (int i = 0; i < numOfEntries; i++) {

            String key1 = "RowKey" + i;
            Put myput = new Put(Bytes.toBytes(key1));
            myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("User" + i));
            myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            Get myget = new Get(Bytes.toBytes("RowKey" + i));
            Row result = mtable.get(myget);
            assertFalse(result.isEmpty());

            List<Cell> row = result.getCells();

            Iterator<MColumnDescriptor> iteratorColumnDescriptor =
                    mtable.getTableDescriptor().getAllColumnDescriptors().iterator();

            for (Cell cell : row) {
              byte[] expectedColumnName = iteratorColumnDescriptor.next().getColumnName();
              byte[] expectedColumnValue =
                      (byte[]) myput.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));
              if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
                Assert.fail("Invalid Values for Column Name");
              }
              if (!Bytes.equals(expectedColumnValue, (byte[]) cell.getColumnValue())) {
                System.out
                        .println("expectedColumnValue =>  " + Arrays.toString(expectedColumnValue));
                System.out.println(
                        "actuaColumnValue    =>  " + Arrays.toString((byte[]) cell.getColumnValue()));
                Assert.fail("Invalid Values for Column Value");
              }
            }
          }

        } catch (CacheClosedException cce) {
        }

        return null;
      }
    });
  }

  private void verifySizeOfRegionOnServer(VM vm, String tableName) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        try {
          MCache c = MCacheFactory.getAnyInstance();
          Region r = c.getRegion(tableName);
          assertNotNull(r);
          assertEquals("Region Size MisMatch", numOfEntries, r.size());

        } catch (CacheClosedException cce) {
        }
        return null;
      }
    });

  }

  @Test
  public void testDefaultTableProperties() {
    final String TABLE_NAME_NON_PERSISTENT = "MTABLE_NON_PERSISTENT";
    final String TABLE_NAME_PERSISTENT = "MTABLE_PERSISTENT";
    createTableOn(this.mTableClient1, TABLE_NAME_NON_PERSISTENT, false);
    createTableOn(this.mTableClient1, TABLE_NAME_PERSISTENT, true);
    MTable mtable1 = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME_NON_PERSISTENT);
    MTable mtable2 = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME_PERSISTENT);

    assertNotNull(mtable1);
    assertNotNull(mtable2);

    verifyRegionAttributes(this.vm0, TABLE_NAME_NON_PERSISTENT, false);
    verifyRegionAttributes(this.vm1, TABLE_NAME_NON_PERSISTENT, false);
    verifyRegionAttributes(this.vm2, TABLE_NAME_NON_PERSISTENT, false);

    verifyRegionAttributes(this.vm0, TABLE_NAME_PERSISTENT, true);
    verifyRegionAttributes(this.vm1, TABLE_NAME_PERSISTENT, true);
    verifyRegionAttributes(this.vm2, TABLE_NAME_PERSISTENT, true);

  }

  @Test
  public void testPersistanceInDefaultTable() throws InterruptedException {
    createTableFromClient(this.mTableClient1, EMPLOYEE_TABLE_DEFAULT, true);
    doPutGetOperationFromClient(this.mTableClient1, EMPLOYEE_TABLE_DEFAULT);
    checkTableExists(mTableClient1, EMPLOYEE_TABLE_DEFAULT);
    doGetFromClient(EMPLOYEE_TABLE_DEFAULT, true);
    verifySizeOfRegionOnServer(this.vm0, EMPLOYEE_TABLE_DEFAULT);
    verifySizeOfRegionOnServer(this.vm1, EMPLOYEE_TABLE_DEFAULT);
    verifySizeOfRegionOnServer(this.vm2, EMPLOYEE_TABLE_DEFAULT);

    // Do not restart all cache server as Mtable Meta regions are not
    // persistent & replicated
    stopServerOn(this.vm0);
    stopServerOn(this.vm1);
    stopServerOn(this.vm2);

    // wait for 10 seconds
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    asyncStartServerOn(this.vm0, DUnitLauncher.getLocatorString());
    asyncStartServerOn(this.vm1, DUnitLauncher.getLocatorString());
    asyncStartServerOn(this.vm2, DUnitLauncher.getLocatorString());
    Thread.sleep(5000);
    checkTableExists(mTableClient1, EMPLOYEE_TABLE_DEFAULT);
    doGetFromClient(EMPLOYEE_TABLE_DEFAULT, true);
  }

  @Test
  public void testDiskPersistanceInTable() throws InterruptedException {
    createTableFromClient(this.mTableClient1, EMPLOYEE_TABLE_PERSISTENT, true);
    doPutGetOperationFromClient(this.mTableClient1, EMPLOYEE_TABLE_PERSISTENT);
    checkTableExists(mTableClient1, EMPLOYEE_TABLE_PERSISTENT);
    doGetFromClient(EMPLOYEE_TABLE_PERSISTENT, true);
    verifySizeOfRegionOnServer(this.vm0, EMPLOYEE_TABLE_PERSISTENT);
    verifySizeOfRegionOnServer(this.vm1, EMPLOYEE_TABLE_PERSISTENT);
    verifySizeOfRegionOnServer(this.vm2, EMPLOYEE_TABLE_PERSISTENT);

    // Do not restart all cache server as Mtable Meta regions are not
    // persistent & replicated
    stopServerOn(this.vm0);
    stopServerOn(this.vm1);
    stopServerOn(this.vm2);
    // wait for 10 seconds
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    asyncStartServerOn(this.vm0, DUnitLauncher.getLocatorString());
    asyncStartServerOn(this.vm1, DUnitLauncher.getLocatorString());
    asyncStartServerOn(this.vm2, DUnitLauncher.getLocatorString());
    Thread.sleep(5000);
    verifySizeOfRegionOnServer(this.vm0, EMPLOYEE_TABLE_PERSISTENT);
    verifySizeOfRegionOnServer(this.vm1, EMPLOYEE_TABLE_PERSISTENT);
    verifySizeOfRegionOnServer(this.vm2, EMPLOYEE_TABLE_PERSISTENT);

    closeMClientCache(mTableClient1);
    createClientCache(mTableClient1);
    checkTableExists(mTableClient1, EMPLOYEE_TABLE_PERSISTENT);
    closeMClientCache();
    createClientCache();
    doGetFromClient(EMPLOYEE_TABLE_PERSISTENT, true);
  }
}
