package io.ampool.monarch.table.coprocessor;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.loader.MultiTableColumnLoader;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.cache.*;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(MonarchTest.class)
public class MTablePreGetJUnitTest {
  private final String ColumnFromDB = "ColumnFromDB-";
  private final int NumOfColumns = 10;
  private final String tableName = "FederatedTable";
  private final int numOfRows = 10;

  private MCache createCache() {
    return new MCacheFactory(createLonerProperties()).create();
  }

  private Properties createLonerProperties() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    return props;
  }

  private MTable createTableWithCoProcessor() {
    MTableDescriptor descriptor = new MTableDescriptor(MTableType.UNORDERED);
    for (int i = 0; i < NumOfColumns; i++) {
      descriptor.addColumn(ColumnFromDB + i, BasicTypes.INT);
    }
    descriptor.addCoprocessor("io.ampool.monarch.table.coprocessor.CVCollectorObserver");
    return MCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, descriptor);
  }


  // @Test
  // public void testMTablePreGetObserverPeerToPeerMode() {
  // createCache();
  // MCacheFactory.getAnyInstance();
  // MTable fedTable = createTableWithCoProcessor();
  // for (int key = 0; key < numOfRows; key++) {
  // Row row = fedTable.get(new Get(Bytes.toBytes(key)));
  // assertFalse(row.isEmpty());
  // List<Cell> cells = row.getCells();
  // for (int colIdx = 0; colIdx < cells.size() - 1; colIdx++) {
  // Cell cell = cells.get(colIdx);
  // assertEquals(0,
  // Bytes.compareTo(cell.getColumnName(), Bytes.toBytes(ColumnFromDB + colIdx)));
  // assertEquals(key * colIdx, cell.getColumnValue());
  // }
  // }
  // // for (int key = 0; key < numOfRows; key++) {
  // // Row row = fedTable.get(new Get(Bytes.toBytes(key)));
  // // assertFalse(row.isEmpty());
  // // List<Cell> cells = row.getCells();
  // // for (int colIdx = 0; colIdx < cells.size() - 1; colIdx++) {
  // // Cell cell = cells.get(colIdx);
  // // assertEquals(0, Bytes.compareTo(cell.getColumnName(), Bytes.toBytes(ColumnFromDB +
  // colIdx)));
  // // assertEquals(key * colIdx, cell.getColumnValue());
  // // }
  // // }
  // }


  private MTable createTableWithCacheLoader() {
    MTableDescriptor descriptor = new MTableDescriptor(MTableType.UNORDERED);
    for (int i = 0; i < NumOfColumns; i++) {
      descriptor.addColumn(ColumnFromDB + i, BasicTypes.INT);
    }
    descriptor.setCacheLoaderClassName("io.ampool.monarch.table.loader.MultiTableColumnLoader");
    return MCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, descriptor);
  }

  @Ignore("Changes reverted while doing extended tables from regions")
  @Test
  public void testMTableWithCacheLoader() {
    createCache();
    MCacheFactory.getAnyInstance();
    MTable fedTable = createTableWithCacheLoader();
    for (int key = 0; key < numOfRows; key++) {
      Row row = fedTable.get(new Get(Bytes.toBytes(key)));
      assertFalse(row.isEmpty());
      List<Cell> cells = row.getCells();
      for (int colIdx = 0; colIdx < cells.size() - 1; colIdx++) {
        Cell cell = cells.get(colIdx);
        assertEquals(0,
            Bytes.compareTo(cell.getColumnName(), Bytes.toBytes(ColumnFromDB + colIdx)));
        assertEquals(key * colIdx, cell.getColumnValue());
      }
    }
    for (int key = 0; key < numOfRows; key++) {
      Row row = fedTable.get(new Get(Bytes.toBytes(key)));
      assertFalse(row.isEmpty());
      List<Cell> cells = row.getCells();
      for (int colIdx = 0; colIdx < cells.size() - 1; colIdx++) {
        Cell cell = cells.get(colIdx);
        assertEquals(0,
            Bytes.compareTo(cell.getColumnName(), Bytes.toBytes(ColumnFromDB + colIdx)));
        assertEquals(key * colIdx, cell.getColumnValue());
      }
    }
  }
}


