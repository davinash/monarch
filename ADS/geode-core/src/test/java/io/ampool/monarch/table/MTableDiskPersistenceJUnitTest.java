package io.ampool.monarch.table;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.geode.cache.*;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.exceptions.TableInvalidConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(MonarchTest.class)
public class MTableDiskPersistenceJUnitTest {

  private MCache createCache() {
    return (new MCacheFactory(this.createLonerProperties())).create();
  }

  private Properties createLonerProperties() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    return props;
  }

  private MTable createTable(String tableName, String colName) {
    MCache amplServerCache = this.createCache();
    DiskStoreFactory diskStoreFactory = amplServerCache.createDiskStoreFactory();
    diskStoreFactory.create("MTableJUnitDiskStore");
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    mTableDescriptor.addColumn(Bytes.toBytes(colName));
    mTableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    mTableDescriptor.setDiskStore("MTableJUnitDiskStore");
    MTable mTable = amplServerCache.getAdmin().createTable(tableName, mTableDescriptor);
    Assert.assertEquals(mTable.getTableDescriptor().getTableType(), MTableType.ORDERED_VERSIONED);
    Assert.assertEquals(DataPolicy.PERSISTENT_PARTITION,
        amplServerCache.getRegion(tableName).getAttributes().getDataPolicy());
    Assert.assertNotNull(
        amplServerCache.getRegion(tableName).getAttributes().getPartitionAttributes());
    Assert.assertEquals(0L, (long) mTable.getTableDescriptor().getRedundantCopies());
    Assert.assertEquals(0L, (long) amplServerCache.getRegion(tableName).getAttributes()
        .getPartitionAttributes().getRedundantCopies());
    return mTable;
  }

  private void doPuts(MTable mTable, String colName) {
    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn(Bytes.toBytes(colName), Bytes.toBytes(i + 1000));
      mTable.put(put);
    }
  }

  private void verifyGets(MTable mTable, String colName) {
    for (int i = 0; i < 10; i++) {
      Get get = new Get(Bytes.toBytes(i));
      Row row = mTable.get(get);
      Assert.assertNotNull(row);
      Assert.assertEquals(Bytes.toInt(row.getRowId()), i);
      Assert.assertNotNull(row.getCells());
      Assert.assertEquals(2, row.getCells().size());
      Assert.assertEquals(colName, Bytes.toString(row.getCells().get(0).getColumnName()));
      Assert.assertEquals(i + 1000, Bytes.toInt((byte[]) row.getCells().get(0).getColumnValue()));
    }
  }

  private void closeCache() {
    MCache mCache = MCacheFactory.getAnyInstance();
    mCache.close();
  }

  private void deleteTable(String tableName) {
    MCache mCache = MCacheFactory.getAnyInstance();
    mCache.getAdmin().deleteTable(tableName);
    mCache.close();
  }


  @Test
  public void testPersistenceTable() {
    String tableName = "testPersistenceTable";
    String colName = "testcol";
    MTable mTable = createTable(tableName, colName);
    doPuts(mTable, colName);
    verifyGets(mTable, colName);
    closeCache();

    MCache amplServerCache = this.createCache();
    // DiskStoreFactory diskStoreFactory = amplServerCache.createDiskStoreFactory();
    // diskStoreFactory.create("MTableJUnitDiskStore");
    mTable = MCacheFactory.getAnyInstance().getTable(tableName);
    verifyGets(mTable, colName);
    deleteTable(tableName);
  }

  @Test
  public void testPersistenceTableWithScan() {
    String tableName = "testPersistenceTableWithScan";
    String colName = "testcol";
    MTable mTable = createTable(tableName, colName);
    doPuts(mTable, colName);
    verifyGets(mTable, colName);
    Iterator<Row> iterator = mTable.getScanner(new Scan()).iterator();
    while (iterator.hasNext()) {
      Row result = iterator.next();
      System.out.println(Bytes.toInt(result.getRowId()));
      System.out.println(Bytes.toString(result.getCells().get(0).getColumnName()));
      System.out.println(Bytes.toInt((byte[]) result.getCells().get(0).getColumnValue()));
    }
    closeCache();
    MCache geodeCache = this.createCache();
    DiskStoreFactory diskStoreFactory = geodeCache.createDiskStoreFactory();
    diskStoreFactory.create("MTableJUnitDiskStore");
    mTable = MCacheFactory.getAnyInstance().getTable(tableName);
    verifyGets(mTable, colName);
    iterator = mTable.getScanner(new Scan()).iterator();
    while (iterator.hasNext()) {
      Row result = iterator.next();
      System.out.println(Bytes.toInt(result.getRowId()));
      System.out.println(Bytes.toString(result.getCells().get(0).getColumnName()));
      System.out.println(Bytes.toInt((byte[]) result.getCells().get(0).getColumnValue()));
    }
    deleteTable(tableName);
  }

  @Test
  public void testDeleteGEN889() {
    MCache geodeCache = this.createCache();
    String COLUMN_PREFIX = "COL", VALUE_PREFIX = "VAL";
    String[] keys_version = {"015", "016", "017", "018", "019", "020"};
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    for (int i = 0; i < 5; i++) {
      mTableDescriptor.addColumn(Bytes.toBytes(COLUMN_PREFIX + i));
    }
    mTableDescriptor.setMaxVersions(5);
    MCache cache = MCacheFactory.getAnyInstance();
    MTable mtable = cache.getAdmin().createTable("EmployeeTable", mTableDescriptor);


    int maxVersions = mtable.getTableDescriptor().getMaxVersions();
    Exception expectedException = null;
    // Populate a row with all columns with timestamp and test.
    // Test rows with columns populating all versions until maxVersions, delete a column with
    // specific version.
    // Test asserts all columns less than equal to that version are deleted.
    if (maxVersions > 1) {
      for (int i = 0; i < maxVersions; i++) {
        Put put = new Put(keys_version[5]);
        for (int columnIndex = 0; columnIndex < 5; columnIndex++) {
          put.addColumn(Bytes.toBytes(COLUMN_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex));
        }
        put.setTimeStamp(i + 1);
        try {
          mtable.put(put);
        } catch (Exception ex) {
          expectedException = ex;
          Assert.fail("Should not get exception here.");
        }
      }
    }
    Delete delete1 = new Delete(keys_version[5]);
    delete1.addColumn(Bytes.toBytes(COLUMN_PREFIX + 3));
    delete1.setTimestamp(3);
    mtable.delete(delete1);

    for (int i = 0; i < maxVersions; i++) {
      Get get = new Get(keys_version[5]);
      get.setTimeStamp(i + 1);
      try {
        Row result1 = mtable.get(get);
        Assert.assertFalse(result1.isEmpty());
        int columnIndex = 0;
        List<Cell> row = result1.getCells();
        for (int k = 0; k < row.size() - 1; k++) {
          byte[] expectedColumnName = Bytes.toBytes(COLUMN_PREFIX + columnIndex);
          byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
          if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
            System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
            System.out
                .println("actualColumnName   => " + Arrays.toString(row.get(k).getColumnName()));
            Assert.fail("Invalid Values for Column Name");
          }
          if (i <= 2) {
            if (columnIndex == 3) {
              Assert.assertEquals(row.get(k).getColumnValue(), null);
            }
          } else {
            if (!Bytes.equals(exptectedValue, (byte[]) row.get(k).getColumnValue())) {
              System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
              System.out.println(
                  "actualValue    => " + Arrays.toString((byte[]) row.get(k).getColumnValue()));
              Assert.fail("Invalid Values for Column Value");
            }
          }
          columnIndex++;
        }
      } catch (Exception ex) {
        expectedException = ex;
        if (mtable.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
          Assert.assertTrue(expectedException instanceof TableInvalidConfiguration);
        } else {
          Assert.fail("Should not get exception here.");
        }
      }
    }

    MTable table2 = cache.getTable("EmployeeTable");

    for (int i = 0; i < maxVersions; i++) {
      Get get = new Get(keys_version[5]);
      get.setTimeStamp(i + 1);
      try {
        Row result1 = mtable.get(get);
        Assert.assertFalse(result1.isEmpty());
        int columnIndex = 0;
        List<Cell> row = result1.getCells();
        for (int k = 0; k < row.size() - 1; k++) {
          byte[] expectedColumnName = Bytes.toBytes(COLUMN_PREFIX + columnIndex);
          byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
          if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
            System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
            System.out
                .println("actualColumnName   => " + Arrays.toString(row.get(k).getColumnName()));
            Assert.fail("Invalid Values for Column Name");
          }
          if (i <= 2) {
            if (columnIndex == 3) {
              Assert.assertEquals(row.get(k).getColumnValue(), null);
            }
          } else {
            if (!Bytes.equals(exptectedValue, (byte[]) row.get(k).getColumnValue())) {
              System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
              System.out.println(
                  "actualValue    => " + Arrays.toString((byte[]) row.get(k).getColumnValue()));
              Assert.fail("Invalid Values for Column Value");
            }
          }
          columnIndex++;
        }
      } catch (Exception ex) {
        expectedException = ex;
        if (mtable.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
          Assert.assertTrue(expectedException instanceof TableInvalidConfiguration);
        } else {
          Assert.fail("Should not get exception here.");
        }
      }
    }
    geodeCache.close();
  }

  @Test
  public void testCreateRegionTime() {
    MCache geodeCache = this.createCache();
    final String REGION_NAME = "Region_Name";
    RegionFactory<Object, Object> rf =
        geodeCache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT_OVERFLOW);
    rf.setDiskSynchronous(true);
    rf.setEvictionAttributes(
        EvictionAttributes.createLIFOEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    Region<Object, Object> region = rf.create(REGION_NAME + "_" + 0);

    int[][] multi = new int[][] {{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0}};

    region.put("Test", multi);
    geodeCache.close();
  }


}
