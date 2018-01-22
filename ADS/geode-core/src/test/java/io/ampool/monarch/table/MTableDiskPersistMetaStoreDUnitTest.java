package io.ampool.monarch.table;

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.MTableUtils;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.*;

@Category(MonarchTest.class)
public class MTableDiskPersistMetaStoreDUnitTest extends MTableDUnitHelper {
  public MTableDiskPersistMetaStoreDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  private final int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "MTableDiskPersistMetaStoreDUnitTest";
  private final String KEY_PREFIX = "KEY";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 10;
  private final String COLUMN_NAME_PREFIX = "COLUMN";
  private final int LATEST_TIMESTAMP = 300;
  private final int MAX_VERSIONS = 1;
  private final int TABLE_MAX_VERSIONS = 7;


  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();

  }

  @Override
  public void tearDown2() throws Exception {
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

    super.tearDown2();
  }

  private void createTable(String tableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1).setMaxVersions(1);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    MTable table = clientCache.getAdmin().createTable(tableName, tableDescriptor);
    assertEquals(table.getName(), tableName);
    assertNotNull(table);
  }

  private void populateSomeData(String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    assertNotNull(table);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                Bytes.toBytes(tableName + VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }
  }

  private void verifyDataAfterRestart(String tableName) {

    Region actualTableRegion = MCacheFactory.getAnyInstance().getRegion(tableName);
    assertEquals(MTableUtils.DEFAULT_MTABLE_DISK_STORE_NAME,
            actualTableRegion.getAttributes().getDiskStoreName());
    assertEquals(DataPolicy.PERSISTENT_PARTITION,
            actualTableRegion.getAttributes().getDataPolicy());

    assertNotNull(actualTableRegion);
    assertEquals(NUM_OF_ROWS, actualTableRegion.size());

    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();

      /* Verify the row Id features */
      if (!Bytes.equals(result.getRowId(), Bytes.toBytes(KEY_PREFIX + rowIndex))) {
        System.out.println(
                "expectedColumnName => " + Arrays.toString(Bytes.toBytes(KEY_PREFIX + rowIndex)));
        System.out.println("actualColumnName   => " + Arrays.toString(result.getRowId()));
        Assert.fail("Invalid Row Id");
      }

      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(tableName + VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out
                  .println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
          System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
          System.out.println(
                  "actualValue    => " + Arrays.toString((byte[]) row.get(i).getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex++;
      }
    }

  }


  private void verifyTablesAfterRestart() {
    MCache mCache = MCacheFactory.getAnyInstance();
    assertNotNull(mCache);
    Region<Object, Object> metaRegion = mCache.getRegion(MTableUtils.AMPL_META_REGION_NAME);
    assertNotNull(metaRegion);

    Iterator iterator = metaRegion.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, MTableDescriptor> record =
              (Map.Entry<String, MTableDescriptor>) iterator.next();
      if (record == null) {
        continue;
      }
      String tableName = record.getKey();

      Region<Object, Object> actualTableRegion = mCache.getRegion(tableName);
      assertNotNull(actualTableRegion);

      assertEquals(MTableUtils.DEFAULT_MTABLE_DISK_STORE_NAME,
              actualTableRegion.getAttributes().getDiskStoreName());
      assertEquals(DataPolicy.PERSISTENT_PARTITION,
              actualTableRegion.getAttributes().getDataPolicy());
      assertEquals(NUM_OF_ROWS, actualTableRegion.size());

      MTableDescriptor tableDescriptor = record.getValue();
      assertNotNull(tableDescriptor);
      assertEquals(NUM_OF_COLUMNS + 1, tableDescriptor.getNumOfColumns());

      assertTrue(tableDescriptor.isDiskPersistenceEnabled());

      List<MColumnDescriptor> allColumnDescriptors = tableDescriptor.getAllColumnDescriptors();
      assertNotNull(allColumnDescriptors);
      int i = 0;
      for (int j = 0; j < allColumnDescriptors.size() - 1; j++) {
        assertEquals(COLUMN_NAME_PREFIX + i, allColumnDescriptors.get(i).getColumnNameAsString());
        i++;
      }
      assertEquals(MTableUtils.KEY_COLUMN_NAME,
              allColumnDescriptors.get(allColumnDescriptors.size() - 1).getColumnNameAsString());
      verifyDataAfterRestart(tableName);
    }
  }

  private void startAllServers() {
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
    createClientCache();
  }

  private void addInitialData() {
    for (int i = 0; i < 10; i++) {
      createTable("TableName" + i);
      populateSomeData("TableName" + i);
    }
  }

  private void closeAllMCache() {
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
            .forEach((VM) -> VM.invoke(new SerializableCallable() {
              @Override
              public Object call() throws Exception {
                MCacheFactory.getAnyInstance().close();
                return null;
              }
            }));
  }

  private void startAllServersAsync() {
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
    } catch (InterruptedException e) {
      fail(e.getMessage());
    }
  }

  private void verfyAllData() {
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
            .forEach((VM) -> VM.invoke(new SerializableCallable() {
              @Override
              public Object call() throws Exception {
                verifyTablesAfterRestart();
                return null;
              }
            }));
  }

  @Test
  public void testCreateTable() throws InterruptedException {
    startAllServers();
    addInitialData();
    closeAllMCache();
    startAllServersAsync();
    verfyAllData();
  }
}
