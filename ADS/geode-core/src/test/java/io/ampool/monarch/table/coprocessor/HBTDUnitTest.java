package io.ampool.monarch.table.coprocessor;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(MonarchTest.class)
public class HBTDUnitTest extends MTableDUnitHelper {
  private final String ColumnPrefix = "ColumnFromDB-";
  private final int NumOfColumns = 10;
  private final String tableName = "FederatedTable";
  private final int numOfRows = 10;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
    createClientCache();
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

  private MTable createTableWithCoProcessor() {
    MTableDescriptor descriptor = new MTableDescriptor(MTableType.UNORDERED);
    for (int i = 0; i < NumOfColumns; i++) {
      descriptor.addColumn(ColumnPrefix + i, BasicTypes.INT);
    }
    descriptor.addCoprocessor("io.ampool.monarch.table.coprocessor.CVCollectorObserver");
    return MClientCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, descriptor);

  }

  @Test
  public void testHBTDWithObserver() {
    MTable fedTable = createTableWithCoProcessor();
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

  private MTable createTableWithCacheLoader() {
    MTableDescriptor descriptor = new MTableDescriptor(MTableType.UNORDERED);
    for (int i = 0; i < NumOfColumns; i++) {
      descriptor.addColumn(ColumnPrefix + i, BasicTypes.INT);
    }
    descriptor.setCacheLoaderClassName("io.ampool.monarch.table.loader.MultiTableColumnLoader");
    return MClientCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, descriptor);

  }

  @Ignore("Changes reverted while doing extended tables from regions")
  @Test
  public void testHBTDWithCacheLoader() {
    MTable fedTable = createTableWithCacheLoader();
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


  private MTable createTableWithCacheLoaderWithException() {
    MTableDescriptor descriptor = new MTableDescriptor(MTableType.UNORDERED);
    for (int i = 0; i < NumOfColumns; i++) {
      descriptor.addColumn(ColumnPrefix + i, BasicTypes.INT);
    }
    descriptor
        .setCacheLoaderClassName("io.ampool.monarch.table.loader.MultiTableColumnLoaderException");
    return MClientCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, descriptor);

  }

  @Test
  public void testHBTDWithCacheLoaderThrowingException() {
    MTable fedTable = createTableWithCacheLoaderWithException();
    for (int key = 0; key < numOfRows; key++) {
      Row row = fedTable.get(new Get(Bytes.toBytes(key)));
      assertTrue(row.isEmpty());
      // List<Cell> cells = row.getCells();
      // for (int colIdx = 0; colIdx < cells.size() - 1; colIdx++) {
      // Cell cell = cells.get(colIdx);
      // assertEquals(0,
      // Bytes.compareTo(cell.getColumnName(), Bytes.toBytes(ColumnPrefix + colIdx)));
      // assertEquals(key * colIdx, cell.getColumnValue());
      // }
    }
    // for (int key = 0; key < numOfRows; key++) {
    // Row row = fedTable.get(new Get(Bytes.toBytes(key)));
    // assertFalse(row.isEmpty());
    // List<Cell> cells = row.getCells();
    // for (int colIdx = 0; colIdx < cells.size() - 1; colIdx++) {
    // Cell cell = cells.get(colIdx);
    // assertEquals(0,
    // Bytes.compareTo(cell.getColumnName(), Bytes.toBytes(ColumnPrefix + colIdx)));
    // assertEquals(key * colIdx, cell.getColumnValue());
    // }
    // }
  }

  @Test
  public void testHBTDWithCacheLoaderReturningNull() {
    MTable fedTable = createTableWithCacheLoaderWithException();
    for (int key = 0; key < numOfRows; key++) {
      Row row = fedTable.get(new Get(Bytes.toBytes(key)));
      assertTrue(row.isEmpty());
      // List<Cell> cells = row.getCells();
      // for (int colIdx = 0; colIdx < cells.size() - 1; colIdx++) {
      // Cell cell = cells.get(colIdx);
      // assertEquals(0,
      // Bytes.compareTo(cell.getColumnName(), Bytes.toBytes(ColumnPrefix + colIdx)));
      // assertEquals(key * colIdx, cell.getColumnValue());
      // }
    }
    // for (int key = 0; key < numOfRows; key++) {
    // Row row = fedTable.get(new Get(Bytes.toBytes(key)));
    // assertFalse(row.isEmpty());
    // List<Cell> cells = row.getCells();
    // for (int colIdx = 0; colIdx < cells.size() - 1; colIdx++) {
    // Cell cell = cells.get(colIdx);
    // assertEquals(0,
    // Bytes.compareTo(cell.getColumnName(), Bytes.toBytes(ColumnPrefix + colIdx)));
    // assertEquals(key * colIdx, cell.getColumnValue());
    // }
    // }
  }
}
