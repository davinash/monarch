/*
 * Copyright (c) 2017 Ampool, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */
package io.ampool.monarch.table.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableRegion;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.coprocessor.MCoprocessorContext;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.coprocessor.internal.MCoprocessorContextImpl;
import io.ampool.monarch.table.filter.KeyOnlyFilter;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Category(MonarchTest.class)
public class MTableServerScanDUnitTest extends MTableDUnitHelper {
  public MTableServerScanDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();
  private List<VM> allVMList = null;

  public final static int NUM_OF_COLUMNS = 10;
  public final String TABLE_NAME = "MTableServerScanDUnitTest";
  public final String KEY_PREFIX = "KEY";
  public final static String VALUE_PREFIX = "VALUE";
  public final static int NUM_OF_ROWS = 10;
  public final static String COLUMN_NAME_PREFIX = "COLUMN";
  public final int LATEST_TIMESTAMP = 300;
  public final int MAX_VERSIONS = 5;
  public final int TABLE_MAX_VERSIONS = 7;


  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    allVMList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
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

  protected void createTable() {
    createTable(-1);
  }

  protected MTable createTable(int splits) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    if (splits != -1) {
      tableDescriptor.setTotalNumOfSplits(splits);
    }
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setMaxVersions(4);
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
    return table;
  }

  public void createTable(final boolean ordered, final String coProcessClass) {
    MCache cache = MCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    Admin admin = cache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  protected void createTableOn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable();
        return null;
      }
    });
  }

  private void createTableOn(VM vm, final boolean ordered, final String coProcessClass) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(ordered, coProcessClass);
        return null;
      }
    });
  }


  private void createScanwithNullObjectOn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        MTable table = cache.getTable(TABLE_NAME);
        assertNotNull(table);
        Exception expectedException = null;
        Scanner rs = null;
        try {
          rs = table.getScanner(null);
        } catch (Exception e) {
          expectedException = e;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);
        return null;
      }
    });
  }

  /* Scan with null object should not be allowd */
  @Test
  public void testScanCreateWithNullObject() {

    createTableOn(this.vm0, true,
        "io.ampool.monarch.table.scan.MTableServerScanDUnitTest$ExecuteScannerFn");

    createScanwithNullObjectOn(this.vm0);
    createScanwithNullObjectOn(this.vm1);
    createScanwithNullObjectOn(this.vm2);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  private Map<Integer, List<byte[]>> doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);

    List<byte[]> allKeys = new ArrayList<byte[]>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> {
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), allKeys.size());

    for (byte[] K : allKeys) {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }

    return keysForAllBuckets;
  }


  public static class ExecuteScannerFn extends MCoprocessor {
    public final String TABLE_NAME = "MTableServerScanDUnitTest";

    public ExecuteScannerFn() {}

    public Object verifyScan(MCoprocessorContext context) {

      MTableRegion tableRegion = context.getMTableRegion();

      Map<Integer, List<byte[]>> bucketWiseKeys =
          (Map<Integer, List<byte[]>>) context.getRequest().getArguments();
      assertNotNull(bucketWiseKeys);

      RegionFunctionContextImpl rfci = ((MCoprocessorContextImpl) context).getNativeContext();
      assertNotNull(rfci);

      int targetBucketId = rfci.getTargetBucketId();

      List<byte[]> keysExpected = bucketWiseKeys.get(targetBucketId);
      assertNotNull(keysExpected);
      keysExpected.sort(Bytes.BYTES_COMPARATOR);

      verifyScannerWithNullStartNullStop(tableRegion, keysExpected);
      verifyScannerWithNullStartNullStopWithFilter(tableRegion, keysExpected);
      verifyScannerWithStartKeyAndNullStopKey(tableRegion, keysExpected);
      verifyScannerWithStartKeyNullAndStopKey(tableRegion, keysExpected);
      verifyScannerWithStartKeyAndStopKey(tableRegion, keysExpected);
      verifyScannerWithSmallStartKeyThenStopKey(tableRegion, keysExpected);
      verifyScannerWithSpecifiedColumns(tableRegion);
      verifyScannerWithIterator(tableRegion, keysExpected);
      verifyScannerWithIteratorReverse(tableRegion, keysExpected);
      verifyScannerWithSpecifiedColumnsReverse(tableRegion);
      verifyScannerWithNullStartNullStopReverse(tableRegion, keysExpected);
      verifyScannerWithStartKeyAndNullStopKeyReverse(tableRegion, keysExpected);
      verifyScannerWithStartKeyNullAndStopKeyReverse(tableRegion, keysExpected);
      verifyScannerWithStartKeyAndStopKeyReverse(tableRegion, keysExpected);
      // context.getResultSender().lastResult(null);
      return null;
    }
  }

  public static void verifyScannerWithSpecifiedColumns(MTableRegion tableRegion) {
    Scan scan = new Scan();
    try {
      // Getting only half of the columns
      int numColumnsToScan = NUM_OF_COLUMNS / 2;
      for (int columnIndex = 0; columnIndex < numColumnsToScan; columnIndex++) {
        scan = scan.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex));
      }
      Scanner scanner = tableRegion.getScanner(scan);
      assertNotNull(scanner);
      Row currentRow = scanner.next();
      assertNotNull(currentRow);
      while (currentRow != null) {
        List<Cell> cells = currentRow.getCells();
        assertTrue("Expected number of columns: " + numColumnsToScan + ", but Got: " + cells.size(),
            cells.size() == numColumnsToScan);
        // Expecting the columns to be sorted by columnname
        for (int columnIndex = 0; columnIndex < cells.size(); columnIndex++) {
          byte[] actualColumnName = cells.get(columnIndex).getColumnName();
          byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
          assertTrue(
              "Expected column name: " + expectedColumnName + ", But Got :" + actualColumnName,
              Arrays.equals(expectedColumnName, actualColumnName));
        }
        currentRow = scanner.next();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Scanner Failed");
    }
  }

  public static void verifyScannerWithSpecifiedColumnsReverse(MTableRegion tableRegion) {
    Scan scan = new Scan();
    try {
      // Getting only half of the columns
      int numColumnsToScan = NUM_OF_COLUMNS / 2;
      for (int columnIndex = 0; columnIndex < numColumnsToScan; columnIndex++) {
        scan = scan.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex));
      }
      Scanner scanner = tableRegion.getScanner(scan);
      assertNotNull(scanner);
      Row currentRow = scanner.next();
      assertNotNull(currentRow);
      while (currentRow != null) {
        List<Cell> cells = currentRow.getCells();
        assertTrue("Expected number of columns: " + numColumnsToScan + ", but Got: " + cells.size(),
            cells.size() == numColumnsToScan);
        // Expecting the columns to be sorted by columnname
        for (int columnIndex = 0; columnIndex < cells.size(); columnIndex++) {
          byte[] actualColumnName = cells.get(columnIndex).getColumnName();
          byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
          assertTrue(
              "Expected column name: " + expectedColumnName + ", But Got :" + actualColumnName,
              Arrays.equals(expectedColumnName, actualColumnName));
        }
        currentRow = scanner.next();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Scanner Failed");
    }
  }

  public static void verifyScannerWithSmallStartKeyThenStopKey(MTableRegion tableRegion,
      List<byte[]> keysExpected) {

    Exception e = null;
    try {
      Scan scan = new Scan().setStartRow(keysExpected.get(keysExpected.size() - 1))
          .setStopRow(keysExpected.get(0));
      Scanner scanner = tableRegion.getScanner(scan);

    } catch (IllegalArgumentException ex) {
      e = ex;
    } catch (Exception e1) {
      e1.printStackTrace();
    }
    assertTrue(e instanceof IllegalArgumentException);

  }

  public static void verifyScannerWithStartKeyAndStopKey(MTableRegion tableRegion,
      List<byte[]> keysExpected) {

    Scan scan = new Scan().setStartRow(keysExpected.get(0))
        .setStopRow(keysExpected.get(keysExpected.size() - 1));
    int rowCounter = 0;
    try {
      Scanner scanner = tableRegion.getScanner(scan);
      assertNotNull(scanner);
      Row currentRow = scanner.next();
      assertNotNull(currentRow);
      while (currentRow != null) {
        MTableServerScanDUnitTest.verifyRowKey(currentRow, keysExpected.get(rowCounter++));
        MTableServerScanDUnitTest.verifyScannerReturnedRow(currentRow);
        currentRow = scanner.next();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Scanner Failed");
    }
    assertEquals(MTableServerScanDUnitTest.NUM_OF_ROWS - 1, rowCounter);
  }

  public static void verifyScannerWithStartKeyAndStopKeyReverse(MTableRegion tableRegion,
      List<byte[]> keysExpected) {
    Scan scan = new Scan().setStartRow(keysExpected.get(keysExpected.size() - 1))
        .setStopRow(keysExpected.get(0));
    // Setting scanner to be reverse
    scan = scan.setReversed(true);
    int rowCounter = 0;
    int index = keysExpected.size();
    try {
      Scanner scanner = tableRegion.getScanner(scan);
      assertNotNull(scanner);
      Row currentRow = scanner.next();
      assertNotNull(currentRow);
      while (currentRow != null) {
        MTableServerScanDUnitTest.verifyRowKey(currentRow, keysExpected.get(--index));
        MTableServerScanDUnitTest.verifyScannerReturnedRow(currentRow);
        currentRow = scanner.next();
        rowCounter++;
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Scanner Failed");
    }
    assertEquals("Expected " + 1 + ", Got:" + index, index, 1);
    assertEquals("Expected " + (MTableServerScanDUnitTest.NUM_OF_ROWS - 1) + ", Got:" + rowCounter,
        MTableServerScanDUnitTest.NUM_OF_ROWS - 1, rowCounter);
  }

  public static void verifyScannerWithStartKeyNullAndStopKey(MTableRegion tableRegion,
      List<byte[]> keysExpected) {

    int index = keysExpected.size() / 2;
    byte[] stopRow = keysExpected.get(index);
    Scan scan = new Scan().setStopRow(stopRow);
    int rowCounter = 0;
    try {
      Scanner scanner = tableRegion.getScanner(scan);
      assertNotNull(scanner);
      Row currentRow = scanner.next();
      assertNotNull(currentRow);
      while (currentRow != null) {
        MTableServerScanDUnitTest.verifyRowKey(currentRow, keysExpected.get(rowCounter++));
        MTableServerScanDUnitTest.verifyScannerReturnedRow(currentRow);
        currentRow = scanner.next();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Scanner Failed");
    }
    assertEquals(MTableServerScanDUnitTest.NUM_OF_ROWS / 2, rowCounter);
  }

  public static void verifyScannerWithStartKeyNullAndStopKeyReverse(MTableRegion tableRegion,
      List<byte[]> keysExpected) {
    int stopKeyIndex = keysExpected.size() / 2;
    Scan scan = new Scan().setStopRow(keysExpected.get(stopKeyIndex));
    // Setting scanner to be reverse
    scan = scan.setReversed(true);
    int rowCounter = 0;
    int index = keysExpected.size();
    try {
      Scanner scanner = tableRegion.getScanner(scan);
      assertNotNull(scanner);
      Row currentRow = scanner.next();
      assertNotNull(currentRow);
      while (currentRow != null) {
        MTableServerScanDUnitTest.verifyRowKey(currentRow, keysExpected.get(--index));
        MTableServerScanDUnitTest.verifyScannerReturnedRow(currentRow);
        currentRow = scanner.next();
        rowCounter++;
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Scanner Failed");
    }
    assertEquals("Expected " + (stopKeyIndex + 1) + ", Got:" + index, index, stopKeyIndex + 1);
    assertEquals(
        "Expected " + ((MTableServerScanDUnitTest.NUM_OF_ROWS / 2) - 1) + ", Got:" + rowCounter,
        ((MTableServerScanDUnitTest.NUM_OF_ROWS / 2) - 1), rowCounter);
  }

  public static void verifyScannerWithStartKeyAndNullStopKey(MTableRegion tableRegion,
      List<byte[]> keysExpected) {

    int index = keysExpected.size() / 2;
    Scan scan = new Scan().setStartRow(keysExpected.get(index));
    int rowCounter = 0;
    try {
      Scanner scanner = tableRegion.getScanner(scan);
      assertNotNull(scanner);
      Row currentRow = scanner.next();
      assertNotNull(currentRow);
      while (currentRow != null) {
        MTableServerScanDUnitTest.verifyRowKey(currentRow, keysExpected.get(index++));
        MTableServerScanDUnitTest.verifyScannerReturnedRow(currentRow);
        currentRow = scanner.next();
        rowCounter++;
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Scanner Failed");
    }
    assertEquals(MTableServerScanDUnitTest.NUM_OF_ROWS / 2, rowCounter);
  }

  public static void verifyScannerWithStartKeyAndNullStopKeyReverse(MTableRegion tableRegion,
      List<byte[]> keysExpected) {

    int startkeyIndex = keysExpected.size() / 2;
    Scan scan = new Scan().setStartRow(keysExpected.get(startkeyIndex));
    // Setting scanner to be reverse
    scan = scan.setReversed(true);
    int rowCounter = 0;
    int index = startkeyIndex;
    try {
      Scanner scanner = tableRegion.getScanner(scan);
      assertNotNull(scanner);
      Row currentRow = scanner.next();
      assertNotNull(currentRow);
      while (currentRow != null) {
        byte[] expectedRowkey = keysExpected.get(index--);
        MTableServerScanDUnitTest.verifyRowKey(currentRow, expectedRowkey);
        MTableServerScanDUnitTest.verifyScannerReturnedRow(currentRow);
        currentRow = scanner.next();
        rowCounter++;
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Scanner Failed");
    }
    assertEquals("Expected " + -1 + ", Got:" + index, -1, index);
    assertEquals(
        "Expected " + ((MTableServerScanDUnitTest.NUM_OF_ROWS / 2) + 1) + ", Got:" + rowCounter,
        ((MTableServerScanDUnitTest.NUM_OF_ROWS / 2) + 1), rowCounter);
  }

  public static void verifyScannerWithNullStartNullStop(MTableRegion tableRegion,
      List<byte[]> keysExpected) {
    Scan scan = new Scan();
    int rowCounter = 0;
    try {
      Scanner scanner = tableRegion.getScanner(scan);
      assertNotNull(scanner);
      Row currentRow = scanner.next();
      assertNotNull(currentRow);
      while (currentRow != null) {
        MTableServerScanDUnitTest.verifyRowKey(currentRow, keysExpected.get(rowCounter++));
        MTableServerScanDUnitTest.verifyScannerReturnedRow(currentRow);
        currentRow = scanner.next();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Scanner Failed");
    }
    assertEquals(MTableServerScanDUnitTest.NUM_OF_ROWS, rowCounter);
  }

  public static void verifyScannerWithNullStartNullStopWithFilter(MTableRegion tableRegion,
      List<byte[]> keysExpected) {
    Scan scan = new Scan();
    scan.setFilter(new KeyOnlyFilter());
    int rowCounter = 0;
    try {
      Scanner scanner = tableRegion.getScanner(scan);
      assertNotNull(scanner);
      Row currentRow = scanner.next();
      assertNotNull(currentRow);
      while (currentRow != null) {
        MTableServerScanDUnitTest.verifyRowKey(currentRow, keysExpected.get(rowCounter++));
        currentRow = scanner.next();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Scanner Failed");
    }
    assertEquals(MTableServerScanDUnitTest.NUM_OF_ROWS, rowCounter);
  }

  public static void verifyScannerWithNullStartNullStopReverse(MTableRegion tableRegion,
      List<byte[]> keysExpected) {
    // Expecting keysExpected list to be sorted in ascending
    Scan scan = new Scan();
    // Setting scanner to be reverse
    scan = scan.setReversed(true);
    int rowCounter = keysExpected.size();
    try {
      Scanner scanner = tableRegion.getScanner(scan);
      assertNotNull(scanner);
      Row currentRow = scanner.next();
      assertNotNull(currentRow);
      while (currentRow != null) {
        MTableServerScanDUnitTest.verifyRowKey(currentRow, keysExpected.get(--rowCounter));
        MTableServerScanDUnitTest.verifyScannerReturnedRow(currentRow);
        currentRow = scanner.next();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Scanner Failed");
    }
    assertEquals(rowCounter, 0);
  }

  public static void verifyScannerWithIterator(MTableRegion tableRegion,
      List<byte[]> keysExpected) {
    Scan scan = new Scan();

    int rowCounter = 0;
    try {
      Scanner scanner = tableRegion.getScanner(scan);
      assertNotNull(scanner);
      Iterator<Row> mResultIterator = scanner.iterator();
      assertNotNull(mResultIterator);
      Row currentRow = mResultIterator.next();
      assertNotNull(currentRow);
      while (currentRow != null) {
        MTableServerScanDUnitTest.verifyRowKey(currentRow, keysExpected.get(rowCounter++));
        MTableServerScanDUnitTest.verifyScannerReturnedRow(currentRow);
        currentRow = mResultIterator.next();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Scanner Failed");
    }
    assertEquals(MTableServerScanDUnitTest.NUM_OF_ROWS, rowCounter);
  }

  public static void verifyScannerWithIteratorReverse(MTableRegion tableRegion,
      List<byte[]> keysExpected) {
    Scan scan = new Scan();

    int rowCounter = 0;
    try {
      Scanner scanner = tableRegion.getScanner(scan);
      assertNotNull(scanner);
      Iterator<Row> mResultIterator = scanner.iterator();
      assertNotNull(mResultIterator);
      Row currentRow = mResultIterator.next();
      assertNotNull(currentRow);
      while (currentRow != null) {
        MTableServerScanDUnitTest.verifyRowKey(currentRow, keysExpected.get(rowCounter++));
        MTableServerScanDUnitTest.verifyScannerReturnedRow(currentRow);
        currentRow = mResultIterator.next();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Scanner Failed");
    }
    assertEquals(MTableServerScanDUnitTest.NUM_OF_ROWS, rowCounter);
  }

  public static void verifyRowKey(Row currentRow, byte[] expectedRowKey) {

    if (!Bytes.equals(expectedRowKey, currentRow.getRowId())) {
      System.out.println("Expected Row Key => " + Arrays.toString(expectedRowKey));
      System.out.println("Actual Row Key   => " + Arrays.toString(currentRow.getRowId()));
      Assert.fail("Invalid Values for Row Key");
    }
  }

  public static void verifyScannerReturnedRow(Row currentRow) {
    assertNotNull(currentRow);
    assertEquals(MTableServerScanDUnitTest.NUM_OF_COLUMNS, currentRow.size());
    assertFalse(currentRow.isEmpty());

    int columnIndex = 0;
    List<Cell> row = currentRow.getCells();

    for (Cell cell : row) {
      Assert.assertNotEquals(MTableServerScanDUnitTest.NUM_OF_COLUMNS, columnIndex);

      byte[] expectedColumnName =
          Bytes.toBytes(MTableServerScanDUnitTest.COLUMN_NAME_PREFIX + columnIndex);
      byte[] exptectedValue = Bytes.toBytes(MTableServerScanDUnitTest.VALUE_PREFIX + columnIndex);

      if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
        System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
        System.out.println("actualColumnName   => " + Arrays.toString(cell.getColumnName()));
        Assert.fail("Invalid Values for Column Name");
      }
      if (!Bytes.equals(exptectedValue, (byte[]) cell.getColumnValue())) {
        System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
        System.out.println("actualValue    => " + Arrays.toString((byte[]) cell.getColumnValue()));
        Assert.fail("Invalid Values for Column Value");
      }
      columnIndex++;
    }
  }


  @Test
  public void testScannerUsingMCoprocessor() {
    createTableOn(this.vm0, true,
        "io.ampool.monarch.table.scan.MTableServerScanDUnitTest$ExecuteScannerFn");
    Map<Integer, List<byte[]>> uniformKeys = doPuts();
    // Get the table from
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    MExecutionRequest execution = new MExecutionRequest();
    execution.setArguments(uniformKeys);

    for (int i = 0; i < table.getTableDescriptor().getTotalNumOfSplits(); i++) {
      List<Object> resultCollector = table.coprocessorService(
          "io.ampool.monarch.table.scan.MTableServerScanDUnitTest$ExecuteScannerFn", "verifyScan",
          uniformKeys.get(i).get(0), execution);
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);

  }


  public static class ExecuteScannerOnEmptyTable extends MCoprocessor {
    public final String TABLE_NAME = "MTableServerScanDUnitTest";

    public ExecuteScannerOnEmptyTable() {}

    public Object scanEmptyTable(MCoprocessorContext context) {
      MTableRegion tableRegion = context.getMTableRegion();

      Map<Integer, List<byte[]>> bucketWiseKeys =
          (Map<Integer, List<byte[]>>) context.getRequest().getArguments();
      assertNotNull(bucketWiseKeys);

      RegionFunctionContextImpl rfci = ((MCoprocessorContextImpl) context).getNativeContext();
      assertNotNull(rfci);

      int targetBucketId = rfci.getTargetBucketId();

      List<byte[]> keysExpected = bucketWiseKeys.get(targetBucketId);

      try {
        Scanner scanner = tableRegion.getScanner(new Scan());
      } catch (Exception e) {
        e.printStackTrace();
      }

      // context.getResultSender().lastResult(null);
      return null;
    }


    @Override
    public String getId() {
      return this.getClass().getName();
    }
  }

  @Test
  public void testScannerOnEmptyTable() {
    createTableOn(this.vm0, true,
        "io.ampool.monarch.table.scan.MTableServerScanDUnitTest$ExecuteScannerOnEmptyTable");
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    MExecutionRequest execution = new MExecutionRequest();
    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), 10);
    execution.setArguments(keysForAllBuckets);

    for (int i = 0; i < table.getTableDescriptor().getTotalNumOfSplits(); i++) {
      Exception e = null;
      List<Object> resultCollector = table.coprocessorService(
          "io.ampool.monarch.table.scan.MTableServerScanDUnitTest$ExecuteScannerOnEmptyTable",
          "scanEmptyTable", keysForAllBuckets.get(i).get(0), execution);
      assertTrue(resultCollector.isEmpty());
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  private boolean runScanOnServer(VM vm) {
    boolean found = (boolean) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        MTable table = cache.getTable(TABLE_NAME);
        Scan scan = new Scan();
        scan.setStartRow(null);
        scan.setStopRow(null);
        Scanner scanner = table.getScanner(scan);
        int i = 0;
        boolean done = false;
        while (true) {
          Row result = scanner.next();
          if (result == null) {
            break;
          }
          assertFalse(result.isEmpty());
          List<Cell> cells = result.getCells();
          int j = 0;
          for (int k = 0; k < cells.size() - 1; k++) {
            byte[] value = (byte[]) cells.get(k).getColumnValue();
            done = true;
            assertEquals(0,
                Bytes.compareTo(value, Bytes.toBytes(COLUMN_NAME_PREFIX + j + "version3")));
            j++;
          }
          i++;
        }
        return done;
      }
    });
    return found;
  }

  /**
   * Verify that the latest version is returned in scan.
   */
  @Test
  public void testServerScannerRowVersion() {
    MTable table = createTable(3);
    Put put = new Put("1");
    for (int i = 0; i < NUM_OF_COLUMNS; i++) {
      put.addColumn(COLUMN_NAME_PREFIX + i, Bytes.toBytes(COLUMN_NAME_PREFIX + i + "version1"));
    }
    table.put(put);
    put = new Put("1");
    for (int i = 0; i < NUM_OF_COLUMNS; i++) {
      put.addColumn(COLUMN_NAME_PREFIX + i, Bytes.toBytes(COLUMN_NAME_PREFIX + i + "version2"));
    }
    table.put(put);

    put = new Put("1");
    for (int i = 0; i < NUM_OF_COLUMNS; i++) {
      put.addColumn(COLUMN_NAME_PREFIX + i, Bytes.toBytes(COLUMN_NAME_PREFIX + i + "version3"));
    }
    table.put(put);

    boolean found0 = runScanOnServer(vm0);
    boolean found1 = runScanOnServer(vm1);
    boolean found2 = runScanOnServer(vm2);
    assertTrue(found0 | found1 | found2);
  }

  private MTable createTableForServerScanner(final MClientCache clientCache,
      final boolean isUnOrder) {

    MTableDescriptor tableDescriptor =
        new MTableDescriptor(isUnOrder ? MTableType.UNORDERED : MTableType.ORDERED_VERSIONED);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createMTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
    return table;
  }

  @Test
  public void testServerScanner() {
    final MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable table = createTableForServerScanner(clientCache, true);

    for (int i = 0; i < 1000; i++) {
      Put record = new Put("Key-" + i);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }

    int clientScanCounter = 0;
    for (Row result : table.getScanner(new Scan())) {
      clientScanCounter++;
    }
    assertEquals(1000, clientScanCounter);

    int count = (int) vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        MTable mTable = cache.getMTable(TABLE_NAME);
        Iterator<Row> iterator = mTable.getScanner(new Scan()).iterator();
        int counter = 0;
        while (iterator.hasNext()) {
          counter++;
          iterator.next();
        }
        return counter;
      }
    });

    count += (int) vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        MTable mTable = cache.getMTable(TABLE_NAME);
        Iterator<Row> iterator = mTable.getScanner(new Scan()).iterator();
        int counter = 0;
        while (iterator.hasNext()) {
          counter++;
          iterator.next();
        }
        return counter;
      }
    });


    count += (int) vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        MTable mTable = cache.getMTable(TABLE_NAME);
        Iterator<Row> iterator = mTable.getScanner(new Scan()).iterator();
        int counter = 0;
        while (iterator.hasNext()) {
          counter++;
          iterator.next();
        }
        return counter;
      }
    });
    assertEquals(1000, count);

    clientCache.getAdmin().deleteMTable(table.getName());

  }
}
