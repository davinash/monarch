package io.ampool.monarch.table;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.CellRef;
import io.ampool.monarch.table.internal.MResultParser;
import io.ampool.monarch.table.internal.SingleVersionRow;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Tests Batch Get Operation using row-tuple
 **/

@Category(MonarchTest.class)
public class MTableBatchPutGetDUnitTest extends MTableDUnitHelper {
  public MTableBatchPutGetDUnitTest() {
    super();
  }

  private static final Logger logger = LogService.getLogger();

  public final int NUM_OF_COLUMNS = 10;
  public final String TABLE_NAME = "MTableBatchPutGetDUnitTest";
  private final String KEY_PREFIX = "KEY";
  public final String COLUMN_NAME_PREFIX = "COLUMN";
  private final int NUM_OF_ROWS = 10;
  private final String VALUE_PREFIX = "VALUE";
  private final int[] columnIds = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  List<VM> servers = new ArrayList<>();

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());

    servers.add(vm0);
    servers.add(vm1);
    servers.add(vm2);

    createClientCache(this.client1);

    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);
    super.tearDown2();
  }

  public void createTable(final boolean order, int maxVersions) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (order == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    System.out
        .println("MTableBatchPutGetDUnitTest.createTable :: 78 No of Versions " + maxVersions);
    tableDescriptor.setMaxVersions(maxVersions);

    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(TABLE_NAME, table.getName());
  }

  private void createTableOn(VM vm, final boolean order, int maxVersions) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(order, maxVersions);
        return null;
      }
    });
  }

  class PutterThread implements Runnable {
    private Thread thread;
    private String threadName;
    private MTable table;

    public PutterThread(String threadName, MTable table) {
      this.threadName = threadName;
      this.table = table;
      thread = new Thread(this, threadName);
    }

    @Override
    public void run() {
      int numRows = 1000;
      List<Put> puts = new ArrayList<>();
      for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
        Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex));
        }
        puts.add(record);
      }
      Exception e = null;
      try {
        table.put(puts);
      } catch (Exception ce) {
        e = ce;
      }
      assertNull(e);
    }

    public void start() {
      thread.start();
    }

    public void waitTillDone() {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void doPuts(int numThreads) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    List<PutterThread> l = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      l.add(new PutterThread("Thread" + i, table));
    }
    for (PutterThread pt : l) {
      pt.start();
    }
    for (PutterThread pt : l) {
      pt.waitTillDone();
    }
  }

  private void doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    List<Put> puts = new ArrayList<>();
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      puts.add(record);
    }
    table.put(puts);
  }

  private void doVersionedPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    int maxVersions = table.getTableDescriptor().getMaxVersions();
    List<Put> puts = new ArrayList<>();
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      for (int versionIndex = 1; versionIndex <= maxVersions; versionIndex++) {
        Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex + versionIndex));
        }
        puts.add(record);
      }
    }
    assertEquals(NUM_OF_ROWS * maxVersions, puts.size());
    table.put(puts);

  }

  private void doVersionedPuts(VM vm) {
    vm.invoke(() -> {
      MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
      int maxVersions = table.getTableDescriptor().getMaxVersions();
      List<Put> puts = new ArrayList<>();
      for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
        for (int versionIndex = 1; versionIndex <= maxVersions; versionIndex++) {
          Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
          for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
            record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                Bytes.toBytes(VALUE_PREFIX + columnIndex + versionIndex));
          }
          puts.add(record);
        }
      }
      assertEquals(NUM_OF_ROWS * maxVersions, puts.size());
      table.put(puts);
    });
  }

  private void doBatchPut(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPuts();
        return null;
      }
    });
  }

  private void doBatchPut(VM vm, int numThreads) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPuts(numThreads);
        return null;
      }
    });
  }

  /**
   * TEST DESCRIPTION 1. Create a Table with 10 columns 2. Insert around 50 rows in the table. 3.
   * Perform batch get ALL THE COLUMNS 4. Verify the Result.
   */


  private void batchPutGetRows(final boolean order, int maxVersions) {
    createTableOn(this.client1, order, maxVersions);
    doBatchPut(this.client1);
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable table = clientCache.getTable(TABLE_NAME);

    /* Bulk GET - Test a zero size input */
    List<Get> getListNull = null;
    Exception expectedException = null;
    try {
      table.get(getListNull);
    } catch (IllegalArgumentException e) {
      expectedException = e;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    /* Bulk PUT - Test a zero size input */
    List<Put> nullPutList = null;
    expectedException = null;
    try {
      table.put(nullPutList);
    } catch (IllegalArgumentException e) {
      expectedException = e;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    List<Get> getList = new ArrayList<>();

    /* Bulk GET - Test a zero size input */
    expectedException = null;
    try {
      table.get(getList);
    } catch (IllegalArgumentException e) {
      expectedException = e;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    /* Bulk PUT - Test a zero size input */
    expectedException = null;
    List<Put> emptyPuts = new ArrayList<>();
    try {
      table.put(emptyPuts);
    } catch (IllegalArgumentException e) {
      expectedException = e;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      getList.add(new Get(Bytes.toBytes(KEY_PREFIX + rowIndex)));
    }

    Row[] batchGetResult = table.get(getList);
    assertNotNull(batchGetResult);

    // verify result size
    assertResults(NUM_OF_ROWS, Arrays.asList(batchGetResult));

    getList.clear();
    /* Try getting keys which are alternate */
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex = rowIndex + 2) {
      getList.add(new Get(Bytes.toBytes(KEY_PREFIX + rowIndex)));
    }
    batchGetResult = table.get(getList);
    assertNotNull(batchGetResult);

    for (Row result : batchGetResult) {
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      assertEquals(NUM_OF_COLUMNS + 1, row.size());
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(NUM_OF_COLUMNS, columnIndex);

        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          System.out.println("Expected Column Name " + Bytes.toString(expectedColumnName));
          System.out.println("Actual   Column Name " + Bytes.toString(row.get(i).getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
          System.out.println("Expected Column Value " + Bytes.toString(exptectedValue));
          System.out.println(
              "Actual   Column Value " + Bytes.toString((byte[]) row.get(i).getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex = columnIndex + 1;
      }
    }

    getList.clear();
    /* try adding keys which does not exists */
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      getList.add(new Get(Bytes.toBytes(KEY_PREFIX + rowIndex + rowIndex)));
    }
    batchGetResult = table.get(getList);
    assertNotNull(batchGetResult);

    /*
     * Result should be null for the keys as those keys does not exists
     */
    for (Row result : batchGetResult) {
      assertNotNull(result);
      assertTrue(result.isEmpty());
    }
    clientCache.getAdmin().deleteTable(TABLE_NAME);

  }

  private void batchPutGetRowsVersioned(final boolean order, int maxVersions,
      boolean executePutsOnServer) throws Exception {
    createTableOn(this.client1, order, maxVersions);
    if (!executePutsOnServer) {
      doVersionedPuts();
      verifyValues(maxVersions);
    } else {
      doVersionedPuts(vm0);
      verifyValues(maxVersions);

      doVersionedPuts(vm1);
      verifyValues(maxVersions);

      doVersionedPuts(vm2);
      verifyValues(maxVersions);
    }

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  private void verifyValues(int maxVersions) throws Exception {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable table = clientCache.getTable(TABLE_NAME);

    List<Get> getList = new ArrayList<>();
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      get.setMaxVersionsToFetch(maxVersions);
      getList.add(get);
    }

    Row[] batchGetResult = table.get(getList);
    assertNotNull(batchGetResult);

    assertEquals(NUM_OF_ROWS, batchGetResult.length);
    // verify result size
    // assertResults(NUM_OF_ROWS, Arrays.asList(batchGetResult));

    for (Row result : batchGetResult) {
      Map<Long, SingleVersionRow> allVersions = result.getAllVersions();
      assertEquals(maxVersions, allVersions.size());

      // System.out.println("-------------------------------------------------------------------------");
      // allVersions.values().forEach(value -> {
      // List<String>
      // colValues =
      // value.getCells().stream().map(cell -> Bytes.toString((byte[]) cell.getColumnValue()))
      // .collect(
      // Collectors.toList());
      // String join = String.join(" | ", colValues);
      // System.out.println(join);
      // });
      // System.out.println("-------------------------------------------------------------------------");

      final AtomicInteger versionIndex = new AtomicInteger(maxVersions);
      allVersions.values().forEach(value -> {
        List<Cell> cells = value.getCells();
        assertEquals(NUM_OF_COLUMNS + 1, cells.size());
        int columnIndex = 0;
        for (int i = 0; i < cells.size() - 1; i++) {
          Assert.assertNotEquals(NUM_OF_COLUMNS, columnIndex);
          System.out
              .println("MTableBatchPutGetDUnitTest.batchPutGetRowsVersioned :: 358 version index "
                  + versionIndex.get());
          byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
          byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex + versionIndex.get());

          if (!Bytes.equals(expectedColumnName, cells.get(i).getColumnName())) {
            System.out.println("Expected Column Name " + Bytes.toString(expectedColumnName));
            System.out
                .println("Actual   Column Name " + Bytes.toString(cells.get(i).getColumnName()));
            Assert.fail("Invalid Values for Column Name");
          }
          if (!Bytes.equals(exptectedValue, (byte[]) cells.get(i).getColumnValue())) {
            System.out.println("Expected Column Value " + Bytes.toString(exptectedValue));
            System.out.println(
                "Actual   Column Value " + Bytes.toString((byte[]) cells.get(i).getColumnValue()));
            Assert.fail("Invalid Values for Column Value");
          }
          columnIndex = columnIndex + 1;
        }
        versionIndex.decrementAndGet();
      });


    }

    getList.clear();
    /* try adding keys which does not exists */
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      getList.add(new Get(Bytes.toBytes(KEY_PREFIX + rowIndex + rowIndex)));
    }
    batchGetResult = table.get(getList);
    assertNotNull(batchGetResult);

    /*
     * Result should be null for the keys as those keys does not exists
     */
    for (Row result : batchGetResult) {
      assertNotNull(result);
      assertTrue(result.isEmpty());
    }
    clientCache.getAdmin().truncateMTable(TABLE_NAME);
  }

  @Test
  public void testBatchPutGetRows() {
    batchPutGetRows(true, 1);
    batchPutGetRows(false, 1);
  }

  @Test
  public void testBatchPutGetRowsWithSameKeys() throws Exception {
    batchPutGetRowsVersioned(true, 3, false);
    batchPutGetRowsVersioned(false, 3, false);
  }

  @Test
  public void testBatchPutGetRowsWithSameKeysPutFromServer() throws Exception {
    batchPutGetRowsVersioned(true, 3, true);
    batchPutGetRowsVersioned(false, 3, true);
  }

  @Test
  public void testBatchPutMultiThread() {
    createTableOn(this.client1, true, 1);
    doBatchPut(this.client1, 10);
  }

  @Test
  public void testBatchPutWithMaxVersionEqualsToOne() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setMaxVersions(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);

    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(TABLE_NAME, table.getName());

    List<Put> puts = new ArrayList<>();
    for (int rowIndex = 0; rowIndex < 1; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      puts.add(record);
    }
    table.put(puts);
    puts.clear();

    for (int rowIndex = 0; rowIndex < 1; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex + "00"));
      }
      puts.add(record);
    }
    table.put(puts);

    Get getRecord = new Get(Bytes.toBytes(KEY_PREFIX + 0));
    Row result = table.get(getRecord);
    assertNotNull(result);

    int columnIndex = 0;
    List<Cell> row = result.getCells();
    assertEquals(NUM_OF_COLUMNS + 1, row.size());
    for (int i = 0; i < row.size() - 1; i++) {
      Assert.assertNotEquals(NUM_OF_COLUMNS, columnIndex);

      byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
      byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex + "00");

      if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
        System.out.println("Expected Column Name " + Bytes.toString(expectedColumnName));
        System.out.println("Actual   Column Name " + Bytes.toString(row.get(i).getColumnName()));
        Assert.fail("Invalid Values for Column Name");
      }
      if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
        System.out.println("Expected Column Value " + Bytes.toString(exptectedValue));
        System.out.println(
            "Actual   Column Value " + Bytes.toString((byte[]) row.get(i).getColumnValue()));
        Assert.fail("Invalid Values for Column Value");
      }
      columnIndex = columnIndex + 1;
    }
  }

  /** ---- Tests for batchGet (using MBatchGet) ---- **/
  /**
   * helper methods for asserting on results..
   **/
  private void assertResults(int numberOfRows, List<Row> results) {
    assertResults(numberOfRows, results, new int[0]);
  }

  /**
   * Helper method to assert on the results..
   * 
   * @param numberOfRows the number of rows
   * @param results results retrieved
   * @param columnIds an array of column-ids fetched
   */
  private void assertResults(int numberOfRows, List<Row> results, final int[] columnIds) {
    assertEquals(numberOfRows, results.size());
    for (Row result : results) {
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        byte[] columnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] columnValueBytes = MResultParser.EMPTY_BYTES;
        if (columnIds.length == 0 || Arrays.binarySearch(columnIds, columnIndex) >= 0) {
          columnValueBytes = Bytes.toBytes(VALUE_PREFIX + columnIndex);
        }
        assertArrayEquals("Column-id: " + columnIndex, columnName, row.get(i).getColumnName());
        assertArrayEquals("Column-id: " + columnIndex, columnValueBytes,
            ((CellRef) row.get(i)).getValueArrayCopy());
        columnIndex = columnIndex + 1;
      }
    }
  }

}
