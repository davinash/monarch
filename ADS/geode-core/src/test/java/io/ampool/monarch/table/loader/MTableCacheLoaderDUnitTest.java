package io.ampool.monarch.table.loader;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.types.BasicTypes;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class MTableCacheLoaderDUnitTest extends MTableDUnitHelper {

  private final String ColumnPrefix = "ColumnFromDB-";
  private final int NumOfColumns = 10;
  private final String tableName = "FederatedTable";
  private final int numOfRows = 10;

  /**
   * Test Case: Create Empty Table with MCache Loader attached. Directly do a get on a key which
   * does not exists Expectation: Loader should get invoked and populate the MTable Row.
   * 
   * @param tableType
   */
  @Ignore("Changes reverted while doing extended tables from regions")
  @Test
  @Parameters(method = "tableType")
  public void testMTableWithCacheLoader(MTableType tableType) {
    MTable fedTable = createTableWithCacheLoader(tableType,
        "io.ampool.monarch.table.loader.MultiTableColumnLoader");

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
    MClientCacheFactory.getAnyInstance().getAdmin().deleteMTable(tableName);
  }

  /**
   * Test Case: Create Empty Table with 10 rows and Loader Attached. Do Get on the first 5 columns
   * Expectation : Loader should get invoked and populate the first five rows. Do Get for the same
   * key with remaining 5 columns Expectation : Loader should get invoked and poulate remaining five
   * columns.
   * 
   * @param tableType
   */
  @Ignore("Changes reverted while doing extended tables from regions")
  @Test
  @Parameters(method = "tableType")
  public void testMTableWithCacheLoaderPartialColumn(MTableType tableType) {
    MTable fedTable = createTableWithCacheLoader(tableType,
        "io.ampool.monarch.table.loader.MultiTableColumnLoader");

    // DO Get on first 5 columns for non-existing keys
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

    // DO Get on remaining 5 columns for non-existing keys
    for (int key = 0; key < numOfRows; key++) {
      Get get1 = new Get(Bytes.toBytes(key));
      get1.addColumn(ColumnPrefix + 5);
      get1.addColumn(ColumnPrefix + 6);
      get1.addColumn(ColumnPrefix + 7);
      get1.addColumn(ColumnPrefix + 8);
      get1.addColumn(ColumnPrefix + 9);
      Row row = fedTable.get(get1);
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

    // Do normal get and verify all the information.
    for (int key = 0; key < numOfRows; key++) {
      Get get = new Get(Bytes.toBytes(key));
      Row row = fedTable.get(get);
      assertFalse(row.isEmpty());
      List<Cell> cells = row.getCells();
      for (int colIdx = 0; colIdx < cells.size() - 1; colIdx++) {
        Cell cell = cells.get(colIdx);
        assertEquals(0,
            Bytes.compareTo(cell.getColumnName(), Bytes.toBytes(ColumnPrefix + colIdx)));
        assertEquals(key * colIdx, cell.getColumnValue());
      }
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteMTable(tableName);
  }

  @Test
  @Parameters(method = "tableType")
  public void testMTableWithCacheLoaderThrowingException(MTableType tableType) {
    MTable fedTable = createTableWithCacheLoader(tableType,
        "io.ampool.monarch.table.loader.MultiTableColumnLoaderException");
    for (int key = 0; key < numOfRows; key++) {
      Row row = fedTable.get(new Get(Bytes.toBytes(key)));
      assertTrue(row.isEmpty());
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteMTable(tableName);
  }

  @Test
  @Parameters(method = "tableType")
  public void testMTableWithCacheLoaderReturningNull(MTableType tableType) {
    MTable fedTable = createTableWithCacheLoader(tableType,
        "io.ampool.monarch.table.loader.MultiTableColumnLoaderReturningNull");
    for (int key = 0; key < numOfRows; key++) {
      Row row = fedTable.get(new Get(Bytes.toBytes(key)));
      assertTrue(row.isEmpty());
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteMTable(tableName);
  }

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

  private MTable createTableWithCacheLoader(MTableType tableType, final String classLoader) {
    MTableDescriptor descriptor = new MTableDescriptor(tableType);
    for (int i = 0; i < NumOfColumns; i++) {
      descriptor.addColumn(ColumnPrefix + i, BasicTypes.INT);
    }
    descriptor.setRedundantCopies(2);
    descriptor.setCacheLoaderClassName(classLoader);
    return MClientCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, descriptor);

  }

  private MTableType[] tableType() {
    return new MTableType[] {MTableType.ORDERED_VERSIONED, MTableType.UNORDERED};
  }

}
