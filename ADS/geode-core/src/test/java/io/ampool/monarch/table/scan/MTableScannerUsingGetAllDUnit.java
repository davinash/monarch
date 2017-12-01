package io.ampool.monarch.table.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MConfiguration;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableColumnType;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MScanFailedException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.DataTypeFactory;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.utils.ReflectionUtils;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class MTableScannerUsingGetAllDUnit extends MTableDUnitHelper {
  public MTableScannerUsingGetAllDUnit() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();
  Host host = Host.getHost(0);
  private VM vm0 = host.getVM(0);
  private VM vm1 = host.getVM(1);
  private VM vm2 = host.getVM(2);
  private VM client1 = host.getVM(3);

  private final int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "MTableScannerUsingGetAllDUnit";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 20;
  private final String COLUMN_NAME_PREFIX = "COLUMN";

  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();
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

  private List<byte[]> doPuts(final String tableName, final int maxVersions) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);

    List<byte[]> allKeys = new ArrayList<>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> KEY_LIST.forEach((KEY) -> {
      allKeys.add(KEY);
    }));

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), allKeys.size());

    final List<Put> puts = new ArrayList<>(NUM_OF_ROWS);
    for (int i = 0; i < maxVersions; i++) {
      final int versionId = i;
      puts.clear();
      allKeys.forEach((K) -> {
        Put record = new Put(K);
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + versionId + columnIndex));
        }
        puts.add(record);
      });
      table.put(puts);
    }

    return allKeys;
  }

  private void createTable(final String tableName, final MTableType tableType,
      final int maxVersions) {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor(tableType);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setMaxVersions(maxVersions);
    tableDescriptor.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(tableName, tableDescriptor);
    assertEquals(table.getName(), tableName);
    assertNotNull(table);
  }

  private void doScan(List<byte[]> listOfKeys, Boolean returnKeys, final int maxVersions)
      throws IOException {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);
    Scan scan = new Scan();
    scan.setClientQueueSize(10);
    if (returnKeys != null) {
      scan.setReturnKeysFlag(returnKeys);
    }
    Scanner scanner = table.getScanner(scan);
    Row currentRow = scanner.next();

    int rowIndex = 0;

    Collections.sort(listOfKeys, Bytes.BYTES_COMPARATOR);

    while (currentRow != null) {
      byte[] key = currentRow.getRowId();
      List<Cell> cells = currentRow.getCells();
      // System.out.println("--------------- " + Arrays.toString(currentRow.getRowId()) + "
      // -------------------");
      if (returnKeys == null || returnKeys == true) {
        // default is to return keys
        assertNotNull(key);
        System.out.println(" Expected Key -> " + Arrays.toString(listOfKeys.get(rowIndex)));
        System.out.println(" Actual   Key -> " + Arrays.toString(key));
        if (Bytes.compareTo(listOfKeys.get(rowIndex), key) != 0) {
          System.out.println(" Expected Key -> " + Arrays.toString(listOfKeys.get(rowIndex)));
          System.out.println(" Actual   Key -> " + Arrays.toString(key));
          Assert.fail("Order Mismatch from the Scanner");
        }
      }
      // else {
      // assertNull(key); // keys were explicitly not requested
      // }
      int columnCount = 0;
      for (int k = 0; k < cells.size() - 1; k++) {
        if (maxVersions > 1) {
          byte[] expectedValue = Bytes.toBytes(VALUE_PREFIX + (maxVersions - 1) + columnCount);
          System.out.println("MTableScannerUsingGetAllDUnit.doScan :: " + "Expected Val: "
              + (VALUE_PREFIX + (maxVersions - 1) + columnCount));
          System.out.println("MTableScannerUsingGetAllDUnit.doScan :: " + "Actual   Val: "
              + Bytes.toString(((byte[]) cells.get(k).getColumnValue())));
          assertTrue(Arrays.equals(expectedValue, ((byte[]) cells.get(k).getColumnValue())));
        }
        columnCount++;
        // System.out.println("COLUMN NAMe -> " + Arrays.toString(cell.getColumnName()));
        // System.out.println("COLUMN VALUE -> " + Arrays.toString((byte[])cell.getColumnValue()));
      }
      assertEquals(NUM_OF_COLUMNS, columnCount);
      rowIndex++;
      currentRow = scanner.next();
    }
    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), rowIndex);
    scanner.close();
  }

  // Test
  // 3 Servers
  // Redundancy equal to 2
  // Do Scan
  // Stop Server 0
  // Do Scan, Expectation Scan should pass
  // Stop Server 1
  // Do Scan, Expectation Scan should pass
  // Start Server 0
  // Do Scan, Expectation Scan should pass
  // Start Server 1
  // Do Scan, Expectation Scan should pass


  @Test
  public void testScannerWithStartStopServers() throws IOException {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(2);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);

    List<byte[]> listOfKeys = doPuts(TABLE_NAME, 1);
    // normal scan
    doScan(listOfKeys, null, 1);
    // stop Server 0
    stopServerOn(vm0);
    doScan(listOfKeys, null, 1);
    // stop Server 1
    stopServerOn(vm1);
    doScan(listOfKeys, null, 1);

    startServerOn(vm0, DUnitLauncher.getLocatorString());
    doScan(listOfKeys, null, 1);

    startServerOn(vm1, DUnitLauncher.getLocatorString());
    doScan(listOfKeys, null, 1);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testSimpleScannerUsingGetAll() throws IOException {
    createTable(TABLE_NAME, MTableType.ORDERED_VERSIONED, 1);
    List<byte[]> listOfKeys = doPuts(TABLE_NAME, 1);
    doScan(listOfKeys, null, 1);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testSimpleScannerUsingGetAllMultiVersionedTable() throws IOException {
    createTable(TABLE_NAME, MTableType.ORDERED_VERSIONED, 5);
    List<byte[]> listOfKeys = doPuts(TABLE_NAME, 5);
    doScan(listOfKeys, null, 5);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testSimpleScannerUsingGetAllKeys() throws IOException {
    createTable(TABLE_NAME, MTableType.ORDERED_VERSIONED, 1);
    List<byte[]> listOfKeys = doPuts(TABLE_NAME, 1);
    doScan(listOfKeys, true, 1);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testSimpleScannerUsingGetAllNoKeys() throws IOException {
    createTable(TABLE_NAME, MTableType.ORDERED_VERSIONED, 1);
    List<byte[]> listOfKeys = doPuts(TABLE_NAME, 1);
    doScan(listOfKeys, false, 1);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  public void doScannerUsingGetAll_UnOrdered(Boolean returnKeys) {
    System.out.println("MTableScannerUsingGetAllTest.testScannerUsingGetAll_UnOrdered");
    /** create UNORDERED table and put some data in it.. **/
    final String tableName = TABLE_NAME + "_UNORDERED";
    createTable(tableName, MTableType.UNORDERED, 1);
    final int expectedSize = doPuts(tableName, 1).size();

    MClientCache cache = MClientCacheFactory.getAnyInstance();
    MTable table = cache.getTable(tableName);

    int totalCount = 0;
    final int iniBucket = 0;
    final int maxBucket = 113;
    final int bucketBatchSize = 10;
    for (int i = iniBucket; i < maxBucket; i += bucketBatchSize) {
      Scan scan = new Scan();
      Set<Integer> set =
          IntStream.range(i, i + bucketBatchSize).mapToObj(e -> e).collect(Collectors.toSet());
      scan.setBucketIds(set);
      scan.setMessageChunkSize(1);
      scan.setClientQueueSize(10);
      if (returnKeys != null) {
        scan.setReturnKeysFlag(returnKeys);
      }
      Scanner scanner = table.getScanner(scan);
      long count = 0;
      for (Row result : scanner) {
        byte[] key = result.getRowId();
        if (returnKeys == null || returnKeys == true) {
          // default is to return keys
          assertNotNull(key);
        }
        // else {
        // assertNull(key); // keys were explicitly not requested
        // }
        count++;
      }
      totalCount += count;
    }
    System.out.println(expectedSize + " :: " + totalCount);
    assertEquals(expectedSize, totalCount);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  @Test
  public void testScannerUsingGetAll_UnOrdered() {
    // default
    doScannerUsingGetAll_UnOrdered(null);
  }

  @Test
  public void testScannerUsingGetAll_UnOrdered_keys() {
    // explicit request for keys
    doScannerUsingGetAll_UnOrdered(true);
  }

  @Test
  public void testScannerUsingGetAll_UnOrdered_nokeys() {
    // explicit request for no keys
    doScannerUsingGetAll_UnOrdered(false);
  }

  @Test
  public void testScanner_UnOrderedWithPredicates() {
    System.out.println("MTableScannerUsingGetAllTest.testScanner_UnOrderedWithPredicates");
    /** create UNORDERED table and put some data in it.. **/
    final String tableName = TABLE_NAME + "_UNORDERED_1";
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    /** create table with following schema **/
    final Object[][] columns = new Object[][] {{"c_1", BasicTypes.LONG}, {"c_2", BasicTypes.STRING},
        {"c_3", BasicTypes.INT}};
    final Function<Object, Object> F1 = (Function<Object, Object> & Serializable) e -> e;
    final Map<String, Function<Object, Object>> CONVERTERS =
        new HashMap<String, Function<Object, Object>>(3) {
          {
            put(BasicTypes.LONG.name(), F1);
            put(BasicTypes.STRING.name(), F1);
            put(BasicTypes.INT.name(), F1);
          }
        };

    for (final Object[] column : columns) {
      DataType type = DataTypeFactory.getTypeFromString(column[1].toString(),
          Collections.emptyMap(), CONVERTERS, CONVERTERS);
      td = td.addColumn(column[0].toString(), new MTableColumnType(type));
    }
    td.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(tableName, td);

    /** insert following data in above created table **/
    final Object[][] data = new Object[][] {{1L, "String_1", 11}, {2L, "String_2", 22},
        {3L, "String_3", 33}, {4L, "String_4", 44}, {5L, "String_5", 55},};
    for (int i = 0; i < data.length; i++) {
      Put put = new Put("row_" + i);
      for (int j = 0; j < data[i].length; j++) {
        put.addColumn(columns[j][0].toString(), data[i][j]);
      }
      table.put(put);
    }

    /** execute scan with predicate (single at the moment) and assert as required **/
    final Object[][] expectedObjects = new Object[][] {{data.length, null},
        {4, new SingleColumnValueFilter((String) columns[0][0], CompareOp.GREATER_OR_EQUAL, 2L)},
        {1, new SingleColumnValueFilter((String) columns[1][0], CompareOp.REGEX, ".*5$")},
        {0, new SingleColumnValueFilter((String) columns[1][0], CompareOp.REGEX, ".*ABC.*")},
        {4, new SingleColumnValueFilter((String) columns[1][0], CompareOp.NOT_EQUAL, "String_2")},
        {2, new SingleColumnValueFilter((String) columns[2][0], CompareOp.LESS, 33)},
        {0, new SingleColumnValueFilter((String) columns[2][0], CompareOp.GREATER_OR_EQUAL, 555)},
        {0, new SingleColumnValueFilter((String) columns[0][0], CompareOp.LESS_OR_EQUAL, 0)},};

    for (Object[] expectedObject : expectedObjects) {
      assertScanWithPredicates(table, (int) expectedObject[0],
          (SingleColumnValueFilter) expectedObject[1]);
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  /**
   * Test to make sure that range cannot be specified for un-ordered tables.
   */
  @Test
  public void testScanner_UnOrderedWithRange() {
    System.out.println("MTableScannerUsingGetAllTest.testScanner_UnOrderedWithRange");
    /** create UNORDERED table.. **/
    final String tableName = TABLE_NAME + "_UNORDERED_2";
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    td.addColumn("c_1", new MTableColumnType(BasicTypes.LONG));
    MTable table = clientCache.getAdmin().createTable(tableName, td);

    /** execute scan with selected columns and assert as required **/
    Scan scan = new Scan();
    try {
      table.getScanner(scan.setStartRow(new byte[] {0, 1}));
      fail("Should not provide startRow for UnOrdered table.");
    } catch (IllegalArgumentException e) {
      assertEquals("Key range should not be provided for UnOrdered scan.", e.getLocalizedMessage());
    }

    try {
      scan.setStartRow(null);
      table.getScanner(scan.setStopRow(new byte[] {0, 1}));
      fail("Should not provide stopRow for UnOrdered table.");
    } catch (IllegalArgumentException e) {
      assertEquals("Key range should not be provided for UnOrdered scan.", e.getLocalizedMessage());
    }
  }

  /**
   * Test for un-ordered scan with selected columns.
   */
  @Test
  public void testScanner_UnOrderedWithSelectedColumns() {
    System.out.println("MTableScannerUsingGetAllTest.testScanner_UnOrderedWithSelectedColumns");
    /** create UNORDERED table and put some data in it.. **/
    final String tableName = TABLE_NAME + "_UNORDERED_2";
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    /** create table with following schema **/
    final Object[][] columns = new Object[][] {{"c_1", BasicTypes.LONG}, {"c_2", BasicTypes.STRING},
        {"c_3", BasicTypes.INT}};
    for (final Object[] column : columns) {
      td = td.addColumn(column[0].toString(), new MTableColumnType(column[1].toString()));
    }
    td.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(tableName, td);

    /** insert following data in above created table **/
    final Object[][] data = new Object[][] {{null, "String_1", 11}, {2L, "String_2", 22},
        {3L, "String_3", 33}, {4L, "String_4", 44}, {5L, "String_5", 55},};
    for (int i = 0; i < data.length; i++) {
      Put put = new Put("row_" + i);
      for (int j = 0; j < data[i].length; j++) {
        put.addColumn(columns[j][0].toString(), data[i][j]);
      }
      table.put(put);
    }

    /** execute scan with selected columns and assert as required **/
    Scan scan = new Scan();
    scan.setBucketIds(IntStream.range(0, 113).mapToObj(e -> e).collect(Collectors.toSet()));
    scan.setClientQueueSize(2);
    scan.setColumns(Arrays.asList(0, 1, 2));
    scan.setFilter(
        new SingleColumnValueFilter((String) columns[1][0], CompareOp.EQUAL, "String_1"));
    Scanner scanner = table.getScanner(scan);
    int actualCount = 0;
    final int expectedCount = 1;
    for (final Row result : scanner) {
      assertEquals(data[actualCount][0], result.getCells().get(0).getColumnValue());
      assertEquals(data[actualCount][1], result.getCells().get(1).getColumnValue());
      assertEquals(data[actualCount][2], result.getCells().get(2).getColumnValue());
      actualCount++;
    }
    System.out.println(expectedCount + " :: " + actualCount);
    assertEquals(expectedCount, actualCount);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  @Test
  public void testScanner_UnOrderedWithSelectedColumns_1() {
    System.out.println("MTableScannerUsingGetAllTest.testScanner_UnOrderedWithSelectedColumns_1");
    /* create UNORDERED table and put some data in it.. */
    final String tableName = TABLE_NAME + "_UNORDERED_3";
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    /* create table with following schema */
    final Object[][] columns = new Object[][] {{"c_1", BasicTypes.LONG}, {"c_2", BasicTypes.STRING},
        {"c_3", BasicTypes.INT}};
    for (final Object[] column : columns) {
      td = td.addColumn(column[0].toString(), new MTableColumnType(column[1].toString()));
    }
    td.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(tableName, td);

    /* insert following data in above created table */
    final Object[][] data = new Object[][] {{null, "String_1", 11}, {2L, "String_2", 22},
        {3L, "String_3", 33}, {4L, "String_4", 44}, {5L, "String_5", 55},};
    for (int i = 0; i < data.length; i++) {
      Put put = new Put("row_" + i);
      for (int j = 0; j < data[i].length; j++) {
        put.addColumn(columns[j][0].toString(), data[i][j]);
      }
      table.put(put);
    }

    /* execute scan with selected columns and assert as required */
    Scan scan = new Scan();
    scan.setColumns(Collections.singletonList(1));
    Scanner scanner = table.getScanner(scan);
    int actualCount = 0;
    final int expectedCount = data.length;
    for (final Row result : scanner) {
      assertEquals(data[actualCount][1], result.getCells().get(0).getColumnValue());
      actualCount++;
    }
    System.out.println(expectedCount + " :: " + actualCount);
    assertEquals(expectedCount, actualCount);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  @Test
  public void testScanner_UnOrderedWithSelectedColumnsAndFilter() {
    System.out
        .println("MTableScannerUsingGetAllTest.testScanner_UnOrderedWithSelectedColumnsAndFilter");
    /* create UNORDERED table and put some data in it.. */
    final String tableName = TABLE_NAME + "_UNORDERED_4";
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    /* create table with following schema */
    final Object[][] columns = new Object[][] {{"c_1", BasicTypes.LONG}, {"c_2", BasicTypes.STRING},
        {"c_3", BasicTypes.INT}};
    for (final Object[] column : columns) {
      td = td.addColumn(column[0].toString(), new MTableColumnType(column[1].toString()));
    }
    td.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(tableName, td);

    /* insert following data in above created table */
    final Object[][] data = new Object[][] {{null, "String_1", 11}, {2L, "String_2", 22},
        {3L, "String_3", 33}, {4L, "String_4", 44}, {5L, "String_5", 55},};
    for (int i = 0; i < data.length; i++) {
      Put put = new Put("row_" + i);
      for (int j = 0; j < data[i].length; j++) {
        put.addColumn(columns[j][0].toString(), data[i][j]);
      }
      table.put(put);
    }

    /* execute scan with selected columns and assert as required */
    Scan scan = new Scan();
    scan.setClientQueueSize(2);
    scan.setColumns(Arrays.asList(1));
    scan.setFilter(
        new SingleColumnValueFilter((String) columns[1][0], CompareOp.EQUAL, "String_1"));
    Scanner scanner = table.getScanner(scan);
    int actualCount = 0;
    final int expectedCount = 1;
    for (final Row result : scanner) {
      assertEquals(data[actualCount][1], result.getCells().get(0).getColumnValue());
      actualCount++;
    }
    System.out.println(expectedCount + " :: " + actualCount);
    assertEquals(expectedCount, actualCount);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  /**
   * Helper method to assert on the results provided by a scan. Make sure that the required number
   * of results are returned by the scanner.
   *
   * @param table the table on which scan is to be performed
   * @param expectedCount the expected count
   * @param predicate the predicates to be executed
   */
  private void assertScanWithPredicates(final MTable table, final int expectedCount,
      final Filter predicate) {
    Scan scan = new Scan();
    scan.setBucketIds(IntStream.range(0, 113).mapToObj(e -> e).collect(Collectors.toSet()));
    scan.setMessageChunkSize(1);
    scan.setClientQueueSize(2);
    if (predicate != null)
      scan.setFilter(predicate);
    Scanner scanner = table.getScanner(scan);
    System.out.println("Predicates= " + predicate);
    long actualCount = 0;
    for (final Row result : scanner) {
      actualCount++;
      assertTrue(result.getCells().size() > 0);
    }
    System.out.println(expectedCount + " :: " + actualCount);
    assertEquals(expectedCount, actualCount);
  }

  @Test
  public void testScannerUnOrdered_WithSplits() {
    System.out.println("MTableScannerUsingGetAllTest.testScannerUnOrdered_WithSplits");
    /** create UNORDERED table and put some data in it.. **/
    final String tableName = TABLE_NAME + "_UNORDERED_T";
    createTable(tableName, MTableType.UNORDERED, 1);
    final int expectedSize = doPuts(tableName, 1).size();

    MClientCache cache = MClientCacheFactory.getAnyInstance();
    MTable table = cache.getTable(tableName);

    assertCountForSplits(expectedSize, table, 1, 113);
    assertCountForSplits(expectedSize, table, 6, 113);
    assertCountForSplits(expectedSize, table, 13, 113);
    assertCountForSplits(expectedSize, table, 113, 113);
    try {
      MTableUtils.getSplits(table.getName(), 114, 113, null);
      fail("Should raise exception.");
    } catch (IllegalArgumentException e) {
      assertTrue(
          e.getMessage().contains("Number of splits cannot be greater than number of buckets"));
    }
  }

  /**
   * Test for single value in a bucket with multiple buckets..
   */
  @Test
  public void testScannerUnOrdered_WithSplits_1() {
    System.out.println("MTableScannerUsingGetAllTest.testScannerUnOrdered_WithSplits_1");
    /** create UNORDERED table and put some data in it.. **/
    final String tableName = TABLE_NAME + "_UNORDERED_T1";

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    /*
     * MTableDescriptor tableDescriptor = new MTableDescriptor() {{
     * setTableType(MTableType.UNORDERED); addColumn("C1", MBasicObjectType.STRING);
     * setRedundantCopies(1); }};
     */

    MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    tableDescriptor.addColumn("C1", BasicTypes.STRING);
    tableDescriptor.setRedundantCopies(1);

    MTable table = clientCache.getAdmin().createTable(tableName, tableDescriptor);
    int MAX_SIZE = 6;
    for (int i = 0; i < MAX_SIZE; i++) {
      Put put = new Put("0-" + i);
      put.addColumn("C1", "Value-" + i);
      table.put(put);
    }
    assertCountForSplits(MAX_SIZE, table, 1, 113);
  }

  private void assertCountForSplits(final int expectedSize, final MTable table, final int numSplits,
      final int maxBucket) {
    int totalCount = 0;
    Map<Integer, Set<ServerLocation>> bucketMap = new HashMap<>(numSplits);
    List<MTableUtils.MSplit> splits =
        MTableUtils.getSplits(table.getName(), numSplits, maxBucket, bucketMap);

    Map<Integer, Set<ServerLocation>> primaryBucketMap = new LinkedHashMap<>(numSplits);
    bucketMap.entrySet().forEach(entry -> {
      primaryBucketMap.put(entry.getKey(),
          entry.getValue() == null || entry.getValue().isEmpty() ? null : entry.getValue());
    });

    /** assert that minimum number of splits are created **/
    assertTrue(splits.size() >= numSplits);

    for (MTableUtils.MSplit split : splits) {
      Scan scan = new Scan();
      scan.setBucketIds(split.getBuckets());
      scan.setClientQueueSize(10);
      scan.setBucketToServerMap(primaryBucketMap);
      Scanner scanner = table.getScanner(scan);
      long count = 0;
      for (Row ignored : scanner) {
        count++;
      }
      totalCount += count;
    }
    System.out.println(expectedSize + " :: " + totalCount);
    assertEquals(expectedSize, totalCount);
  }

  /**
   * Test validates that the scan reports correct results when meta-region is used as PROXY from one
   * client and CACHING_PROXY from other. The client with CACHING_PROXY has a stale table
   * (descriptor) instance and throws RegionDestroyedException. After cleaning it up (delete) the
   * table can no more be found in client with CACHING_PROXY meta-region.
   *
   * Here local client is used as PROXY and client1 as CACHING_PROXY.
   */
  @Test
  public void testScanOnDeletedTable() {
    System.out.println("MTableScannerUsingGetAllTest.testScanOnDeletedTable");
    /** create UNORDERED table and put some data in it.. **/
    final String tableName = TABLE_NAME + "_UNORDERED_DELETE";
    createTable(tableName, MTableType.UNORDERED, 1);
    final MClientCache cc = MClientCacheFactory.getAnyInstance();
    final String locators = "localhost[" + getLocatorPort() + "]";

    assertScanWithPredicates(cc.getMTable(tableName), 0, null);
    client1.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        MCacheFactory.getAnyInstance().close();
        /** create the client cache with meta-region caching enabled and assert on scan count **/

        MConfiguration conf = MConfiguration.create();
        conf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, "127.0.0.1");
        conf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, getLocatorPort());
        conf.set(Constants.MClientCacheconfig.ENABLE_META_REGION_CACHING, "true");
        final MClientCache cc = new MClientCacheFactory().create(conf);
        assertScanWithPredicates(cc.getMTable(tableName), 0, null);
      }
    });

    /**
     * do puts from on client and then assert on count from two clients.. one with PROXY and other
     * CACHING_PROXY.
     */
    final int expectedSize = doPuts(tableName, 1).size();

    assertScanWithPredicates(cc.getMTable(tableName), expectedSize, null);
    client1.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        assertScanWithPredicates(MClientCacheFactory.getAnyInstance().getMTable(tableName),
            expectedSize, null);
      }
    });

    cc.getAdmin().deleteMTable(tableName);

    /** assert on the count after deleting.. **/
    assertNull("Table should not exist.", cc.getMTable(tableName));

    client1.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        final MClientCache cc = MClientCacheFactory.getAnyInstance();
        try {
          assertScanWithPredicates(cc.getMTable(tableName), 0, null);
          fail("Expected RegionDestroyedException.");
        } catch (MScanFailedException e) {
          assertTrue("Incorrect root cause detected.",
              e.getRootCause() instanceof RegionDestroyedException);
          cc.getAdmin().deleteMTable(tableName);
        }
        assertNull("Table should not exist.", cc.getMTable(tableName));
      }
    });
  }

  /**
   * A simple test for scanner with slow consumer. The scenario is: data-fetch-thread is not able to
   * add the RowEndMarker successfully; it gets timed-out. Assert that the scan can be successfully
   * completed even in such cases.
   */
  @Test
  public void testScannerWithSlowConsumer() {
    final String tableName = TABLE_NAME + "_WITH_SLOW_CONSUMER";
    createTable(tableName, MTableType.ORDERED_VERSIONED, 1);
    final int expectedSize = doPuts(tableName, 1).size();
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);

    /* scan the table but wait for 10ms, each, when retrieving the last 200 rows */
    Iterator<Row> iterator = table.getScanner(new Scan()).iterator();
    int count = 0;
    while (iterator.hasNext()) {
      Row row = iterator.next();
      if (row == null) {
        break;
      }
      count++;
      if ((expectedSize - count) <= 200) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          fail("No failure expected.");
        }
      }
    }
    assertEquals("Incorrect number of records from scan.", expectedSize, count);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  /**
   * A simple test to assert that the data-fetch-thread is interrupted when executed the scan with
   * "isInterruptOnTimeout" enabled.
   *
   * @throws InterruptedException if the running thread is interrupted
   */
  @Test
  public void testScannerWithTimeOut() throws InterruptedException {
    final Admin admin = MClientCacheFactory.getAnyInstance().getAdmin();
    final String tableName = TABLE_NAME + "_WITH_TIME_OUT";
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    td.setTotalNumOfSplits(1);
    final String[] columns = IntStream.range(0, NUM_OF_COLUMNS)
        .mapToObj(i -> COLUMN_NAME_PREFIX + i).toArray(String[]::new);
    td.setSchema(new Schema(columns));
    td.setRedundantCopies(0);
    final MTable table = admin.createMTable(tableName, td);

    /* insert records */
    final int expectedSize = 2_000;
    final Put record = new Put("key_");
    for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
      record.addColumn(COLUMN_NAME_PREFIX + columnIndex, Bytes.toBytes(VALUE_PREFIX + columnIndex));
    }
    for (int i = 0; i < expectedSize; i++) {
      record.setRowKey(("key_" + i).getBytes());
      table.put(record);
    }

    final Scan scan = new Scan();
    scan.setMessageChunkSize(2);
    scan.setInterruptOnTimeout(true);
    final Scanner scanner = table.getScanner(scan);

    /* assert that data-fetch-thread is running */
    final Thread dft = (Thread) ReflectionUtils.getFieldValue(scanner, "dataFetchThread");
    assertTrue("DataFetchThread must be running.", dft.isAlive());

    /* consume only 5 records from 200 and still everything should be normal after timeout. */
    Iterator<Row> iterator = scanner.iterator();
    int count = 0;
    while (iterator.hasNext()) {
      final Row ignored = iterator.next();
      if (count == 5) {
        break;
      }
      count++;
    }
    admin.deleteMTable(tableName);

    /* wait till timeout interval and then assert on the required. */
    Thread.sleep((scan.getScanTimeout() + 1) * 1_000);
    assertEquals("Incorrect number of records from scan.", 5, count);
    assertEquals("DataFetchThread must have exited.", false, dft.isAlive());
  }
}
