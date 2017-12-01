package io.ampool.monarch.table.loader;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.internal.AMPLJUnit4CacheTestCase;
import io.ampool.monarch.types.BasicTypes;

import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(MonarchTest.class)
public class MTableCacheLoaderJUnitTest extends AMPLJUnit4CacheTestCase {
  private final String ColumnPrefix = "ColumnFromDB-";
  private final int NumOfColumns = 10;
  private final int numOfRows = 10;

  private MCache createCache() {
    return (new MCacheFactory(this.createLonerProperties())).create();
  }

  private Properties createLonerProperties() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    return props;
  }

  @Ignore("Changes reverted while doing extended tables from regions")
  @Test
  public void testHBTDWithCacheLoader() {
    String tableName = getTestMethodName();
    MCache geodeCache = this.createCache();
    MCacheFactory.getAnyInstance();

    MTable fedTable = createTableWithCacheLoader(MTableType.UNORDERED,
        "io.ampool.monarch.table.loader.MultiTableColumnLoader", tableName);
    verifyUsingGetOp(fedTable);
    MCacheFactory.getAnyInstance().getAdmin().deleteMTable(tableName);
  }

  private MTable createTableWithCacheLoader(MTableType tableType, final String classLoader,
      String tableName) {
    MTableDescriptor descriptor = new MTableDescriptor(tableType);
    for (int i = 0; i < NumOfColumns; i++) {
      descriptor.addColumn(ColumnPrefix + i, BasicTypes.INT);
    }
    descriptor.setCacheLoaderClassName(classLoader);
    return MCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, descriptor);
  }

  private void verifyUsingGetOp(MTable fedTable) {
    for (int key = 0; key < numOfRows; key++) {
      Row row = fedTable.get(new Get(Bytes.toBytes(key)));
      assertFalse(row.isEmpty());
      List<Cell> cells = row.getCells();
      for (int colIdx = 0; colIdx < cells.size() - 1; colIdx++) {
        Cell cell = cells.get(colIdx);
        assertEquals(0,
            Bytes.compareTo(cell.getColumnName(), Bytes.toBytes(ColumnPrefix + colIdx)));
        assertEquals(key * colIdx, cell.getColumnValue());
      }
    }
  }

  @Ignore("Changes reverted while doing extended tables from regions")
  @Test
  public void testHBTDWithCacheLoaderPartialColumn() {
    MCache geodeCache = this.createCache();
    MCacheFactory.getAnyInstance();
    final String tableName = getTestMethodName();

    MTable fedTable = createTableWithCacheLoader(MTableType.UNORDERED,
        "io.ampool.monarch.table.loader.MultiTableColumnLoader", tableName);

    for (int key = 0; key < numOfRows; key++) {
      Get get = new Get(Bytes.toBytes(key));
      get.addColumn(ColumnPrefix + 0);
      get.addColumn(ColumnPrefix + 1);
      get.addColumn(ColumnPrefix + 2);
      get.addColumn(ColumnPrefix + 3);
      get.addColumn(ColumnPrefix + 4);

      Row row = fedTable.get(get);
      assertFalse(row.isEmpty());
      List<Cell> cells = row.getCells();
      for (int colIdx = 0; colIdx < cells.size(); colIdx++) {
        Cell cell = cells.get(colIdx);
        assertEquals(0,
            Bytes.compareTo(cell.getColumnName(), Bytes.toBytes(ColumnPrefix + colIdx)));
        assertEquals(key * colIdx, cell.getColumnValue());
      }
    }

    for (int key = 0; key < numOfRows; key++) {
      Get get = new Get(Bytes.toBytes(key));
      get.addColumn(ColumnPrefix + 5);
      get.addColumn(ColumnPrefix + 6);
      get.addColumn(ColumnPrefix + 7);
      get.addColumn(ColumnPrefix + 8);
      get.addColumn(ColumnPrefix + 9);
      Row row = fedTable.get(get);
      assertFalse(row.isEmpty());
      List<Cell> cells = row.getCells();
      int temColIdx = 0;
      for (int colIdx = 5; colIdx <= 9; colIdx++) {
        Cell cell = cells.get(temColIdx++);
        assertEquals(0,
            Bytes.compareTo(cell.getColumnName(), Bytes.toBytes(ColumnPrefix + colIdx)));
        assertEquals(key * colIdx, cell.getColumnValue());
      }
    }

    MCacheFactory.getAnyInstance().getAdmin().deleteMTable(tableName);
  }

}

