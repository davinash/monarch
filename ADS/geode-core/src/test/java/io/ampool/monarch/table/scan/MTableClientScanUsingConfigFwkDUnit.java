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

import io.ampool.monarch.table.Scanner;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;

import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MTableClientScanUsingConfigFwkDUnit extends MTableDUnitConfigFramework {

  private static Map<String, List<String>> tableData = new HashMap<>();

  static {
    tableData.put("005",
        new ArrayList<String>(Arrays.asList("VAL00", "VAL01", "VAL02", "VAL03", "VAL04")));
    tableData.put("006",
        new ArrayList<String>(Arrays.asList("VAL10", "VAL11", "VAL12", "VAL13", "VAL14")));
    tableData.put("007",
        new ArrayList<String>(Arrays.asList("VAL20", "VAL21", "VAL22", "VAL23", "VAL24")));
    tableData.put("008",
        new ArrayList<String>(Arrays.asList("VAL30", "VAL31", "VAL32", "VAL33", "VAL34")));
    tableData.put("009",
        new ArrayList<String>(Arrays.asList("VAL40", "VAL41", "VAL42", "VAL43", "VAL44")));
  }

  public MTableClientScanUsingConfigFwkDUnit() {
    super();
  }

  /**
   * Scan API test coverage, Do scan ops across two clients (from same client that did puts as well
   * as different client). This covers before and after restart ampool servers using config
   * framework. Following tests are covered, doScanExceptionsTest, tests exceptions doBatchScanTest,
   * tests scan in batch mode doDefaultScanTest, tests default (streaming mode) scan
   * doScanWithStartStopKeysTest, tests scan with setting start and stop keys.
   */
  @Test
  public void testSimplePutScan() {
    /*
     * String[] keys = { "000", "001", "002", "003", "004", "005", "006", "007", "008", "009",
     * "010", "011", "012", "013", "014", "015", "016", "017", "018", "019", "020", "021", "022",
     * "023", "024", "025", "026", "027", "028", "029", "030", "031", "032", "033", "034", "035",
     * "036", "037", "038", "039", "040", "041", "042", "043", "044", "045", "046", "047", "048",
     * "049", "050", "051", "052", "053", "054", "055", "056", "057", "058", "059", "060", "061",
     * "062", "063", "064", "065", "066", "067", "068", "069", "070", "071", "072", "073", "074",
     * "075", "076", "077", "078", "079", "080", "081", "082", "083", "084", "085", "086", "087",
     * "088", "089", "090", "091", "092", "093", "094", "095", "096", "097", "098", "099" };
     */
    String[] keys = {"005", "006", "007", "008", "009"};
    Long timestamp = 123456798l;

    runAllConfigs(new FrameworkRunnable() {
      @Override
      public void run() {
        MTable table = getTable();
        doPut(table, keys, timestamp);
        System.out.println("NNN Test.run ");

        System.out.println("TEST.run SCAN SAME_CLIENT");

        verifyScan(table, keys, timestamp);

        System.out.println("TEST.run SCAN DIFFERENT_CLIENT");
        doVerifyScanFromClient(client1, keys, timestamp);
      }

      @Override
      public void runAfterRestart() {

        MTable table = getTable();
        assertNotNull(table);
        if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
          verifyScan(table, keys, timestamp);

          doVerifyScanFromClient(client1, keys, timestamp);
        }
      }
    });
  }

  private void doPut(MTable table, String[] keys, Long timestamp) {

    for (int rowInd = 0; rowInd < keys.length; rowInd++) {
      Put put = new Put(Bytes.toBytes(keys[rowInd]));

      for (int colInd = 0; colInd < NUM_COLUMNS; colInd++) {
        put.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + colInd),
            Bytes.toBytes(VALUE_PREFIX + rowInd + colInd));
      }
      table.put(put);
    }
  }

  private void doVerifyScanFromClient(VM vm, String[] keys, Long timestamp) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyScan(getTableFromClientCache(), keys, timestamp);
        return null;
      }
    });
  }

  /**
   * Test Scan exceptions. MScan.setBatchSize(int batchSize) should throws
   * java.lang.IllegalArgumentException for negative batch-size Mscan.setClientQueueSize(int
   * clientQueueSize) should throws java.lang.IllegalArgumentException for negative queue-size
   * MResultScanner.nextBatch() without scan.enableBatchMode() should throw
   * java.lang.IllegalStateException
   * 
   * @param table
   * @param keys
   * @param timestamp
   */
  private void doScanExceptionsTest(MTable table, String[] keys, Long timestamp) {
    Exception expectedException = null;
    try {
      Scan scan = new Scan();
      scan.enableBatchMode();
      scan.setBatchSize(-100);
    } catch (IllegalArgumentException iae) {
      expectedException = iae;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    expectedException = null;
    // scan.setClientQueueSize(500);
    Scanner scanner = table.getScanner(new Scan());
    try {
      Row[] results = scanner.nextBatch();
    } catch (IllegalStateException ise) {
      expectedException = ise;
    }

    assertTrue(expectedException instanceof IllegalStateException);
    scanner.close();
  }

  // Test: Test scan in batch mode.
  private void doBatchScanTest(MTable table, String[] keys, Long timestamp) {
    Scan scan = new Scan();
    scan.enableBatchMode();
    scan.setBatchSize(1000);
    scan.setClientQueueSize(10);

    verifyScanResult(table, keys, timestamp, scan);
  }

  // Test default (streaming mode) scan.
  private void doDefaultScanTest(MTable table, String[] keys, Long timestamp) {
    // Test scan (default) in stream mode
    Scan scan = new Scan();
    verifyScanResult(table, keys, timestamp, scan);
  }

  private void verifyScanResult(MTable table, String[] keys, Long timestamp, Scan scan) {
    Scanner scanner = table.getScanner(scan);

    assertNotNull(scanner);

    Row row = scanner.next();

    assertNotNull(row);

    int noOfRecordsInScan = 0;
    byte[] pastRowKey = new byte[0];
    int rowIndex = 0;

    assertTrue("Result is empty", !row.isEmpty());

    while (row != null) {
      // System.out.println("Test.verifyScan-2");
      // check columns and rowkey value is right
      byte[] rowKey = row.getRowId();
      // System.out.println("NNNN verifyScan :: " + "Row REcvd: " + Arrays.toString(rowKey));
      // check if this row is greter than pervious
      // ignore for first row key
      assertTrue(rowKey.length > 0);

      // TEST:: ORDERED_TABLE, New row key should always be greater than the lastRowKey
      if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED
          && rowIndex > 0) {
        assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
      }

      // check row key value
      String rowKeyExpected = null;
      if (table.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
        // UNORDERED_TABLE
        // check whether received rowKey is contained in IP set of keys and find its index
        boolean isKeyFound = false;
        int keyIndex = -1;
        for (int i = 0; i < keys.length; i++) {
          if (Bytes.compareTo(Bytes.toBytes(keys[i]), rowKey) == 0) {
            isKeyFound = true;
            keyIndex = i;
            break;
          }
        }
        assertTrue(isKeyFound);

      } else {
        // ORDERED_TABLE case
        rowKeyExpected = keys[rowIndex];
        // System.out.println("XXX TEST.Scan :: " + "-------------------------SCAN
        // ROW-------------------------");
        // System.out.println("XXX TEST.Scan :: " + "Row expected: " + rowKeyExpected);
        // System.out.println("XXX TEST.Scan :: " + "Row received: " + Arrays.toString(rowKey));
        assertEquals(rowKeyExpected, Bytes.toString(rowKey));
      }

      // Test- Assert if number of cols are equal
      List<Cell> cells = row.getCells();
      // System.out.println("XXX TEST.scan Total-Columns = " + cells.size());
      assertEquals(NUM_COLUMNS + 1, cells.size());

      for (int columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex++) {
        Cell cell = cells.get(columnIndex);
        byte[] colNameRec = cell.getColumnName();
        byte[] colNameExp = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);

        // Verify ColumnNames for the Row
        // System.out.println("XXX TEST.Scan :: " + "ColumnName expected: " +
        // Arrays.toString(colNameExp));
        // System.out.println("XXX TEST.Scan :: " + "ColumnName received: " +
        // Arrays.toString(colNameRec));
        assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));

        //// Verify Column-Values for the Row
        byte[] colValRec = (byte[]) cell.getColumnValue();
        byte[] colValExp = null;
        if (table.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
          colValExp = Bytes.toBytes(tableData.get(Bytes.toString(rowKey)).get(columnIndex));
        } else {
          colValExp = Bytes.toBytes(VALUE_PREFIX + rowIndex + columnIndex);
        }
        // System.out.println("XXX TEST.Scan :: " + "ColumnValue expected: " +
        // Arrays.toString(colValExp));
        // System.out.println("XXX TEST.Scan :: " + "ColumnValue received: " +
        // Arrays.toString(colValRec));
        assertEquals(0, Bytes.compareTo(colValExp, colValRec));

      }
      pastRowKey = rowKey;
      noOfRecordsInScan++;
      rowIndex++;
      row = scanner.next();
      if (keys.length != noOfRecordsInScan) {
        assertTrue("Result is empty", !row.isEmpty());
      }
    }

    assertEquals(keys.length, noOfRecordsInScan);

    scanner.close();
  }

  /**
   * Test default scan with startKey and stopKey set. Not applicable for UNORDERED, so check for
   * IllegalArgumentException
   */
  private void doScanWithStartStopKeysTest(MTable table, String[] keys, Long timestamp) {
    try {
      Scan scan = new Scan();
      scan.setStartRow(Bytes.toBytes("000"));
      scan.setStopRow(Bytes.toBytes("111"));

      verifyScanResult(table, keys, timestamp, scan);
    } catch (IllegalArgumentException iae) {
      assertEquals(table.getTableDescriptor().getTableType(), MTableType.UNORDERED);
    }
  }

  private void doScanTestInCoprocessor(MTable table, String[] keys, Long timestamp) {
    String SCAN_COPROCESSOR_CLASS = "io.ampool.monarch.table.scan.MTableScanCoprocessor";

    // String[] keys1 = {"005", "006", "007", "008", "009"};
    // String[] keys2 = {"000", "001", "002", "003", "004"};
    // String[] keys3 = {"021", "022", "023", "024", "025"};

    // endpoint co-processor execution is supported for ORDERED table
    if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
      {
        // execute chechExceptions method of endpoint co-processor.
        Map<Integer, List<Object>> collector = table.coprocessorService(SCAN_COPROCESSOR_CLASS,
            "chechExceptions", null, null, new MExecutionRequest());
        Object[] resultValues = collector.values().toArray();
        boolean finalResult = true;
        for (Object result : resultValues) {
          ArrayList list = (ArrayList) result;
          for (Object res : list) {
            finalResult &= (boolean) res;
          }
        }
        assertTrue(finalResult);
      }

      {
        // execute "dobatchScan" method of endpoint co-processor.
        Map<Integer, List<Object>> collector = table.coprocessorService(SCAN_COPROCESSOR_CLASS,
            "dobatchScan", null, null, new MExecutionRequest());
        Object[] resultValues = collector.values().toArray();
        boolean finalResult = true;
        for (Object result : resultValues) {
          ArrayList list = (ArrayList) result;
          for (Object res : list) {
            finalResult &= (boolean) res;
          }
        }
        assertTrue(finalResult);
      }

      {
        // execute "doDefaultScan" method of endpoint co-processor.
        Map<Integer, List<Object>> collector = table.coprocessorService(SCAN_COPROCESSOR_CLASS,
            "doDefaultScan", null, null, new MExecutionRequest());
        Object[] resultValues = collector.values().toArray();
        boolean finalResult = true;
        for (Object result : resultValues) {
          ArrayList list = (ArrayList) result;
          for (Object res : list) {
            finalResult &= (boolean) res;
          }
        }
        assertTrue(finalResult);
      }

      {
        // execute "doScanWithStartStopKeysTest" method of endpoint co-processor.
        Map<Integer, List<Object>> collector = table.coprocessorService(SCAN_COPROCESSOR_CLASS,
            "doScanWithStartStopKeysTest", null, null, new MExecutionRequest());
        Object[] resultValues = collector.values().toArray();
        boolean finalResult = true;
        for (Object result : resultValues) {
          ArrayList list = (ArrayList) result;
          for (Object res : list) {
            finalResult &= (boolean) res;
          }
        }
        assertTrue(finalResult);
      }
    }
  }

  // Scan related test-cases
  private void verifyScan(MTable table, String[] keys, Long timestamp) {
    // GET - For Debugging or cross verification
    // MResult result = table.get(new MGet(Bytes.toBytes(keys[0])));

    // Test Scan Exceptions
    doScanExceptionsTest(table, keys, timestamp);

    doBatchScanTest(table, keys, timestamp);

    doDefaultScanTest(table, keys, timestamp);

    doScanWithStartStopKeysTest(table, keys, timestamp);

    doScanTestInCoprocessor(table, keys, timestamp);
  }

  private void verifyGet(MTable table, String[] keys, Long timestamp) {
    // Simple Get
    Row result = table.get(new Get(Bytes.toBytes(keys[0])));
    // System.out.println("MTableClientScanUsingConfigFwkDUnit.verifyGet " + result.size());
    int noOfRecordsInGet = 0;
    for (int rowIndex = 0; rowIndex < keys.length; rowIndex++) {
      Row getResult = table.get(new Get(keys[rowIndex]));
      assertTrue("Result is empty", !result.isEmpty());
      {
        byte[] rowKey = getResult.getRowId();

        // Validate Keys
        String rowKeyExpected = keys[rowIndex];
        // System.out.println("XXX TEST.Scan :: " + "-------------------------SCAN
        // ROW-------------------------");
        // System.out.println("XXX TEST.Scan :: " + "Row expected: " + rowKeyExpected);
        // System.out.println("XXX TEST.Scan :: " + "Row received: " + Arrays.toString(rowKey));
        assertEquals(rowKeyExpected, Bytes.toString(rowKey));

        // Test- Assert if number of cols are equal
        List<Cell> cells = getResult.getCells();
        System.out.println("XXX TEST.scan Total-Columns = " + cells.size());
        assertEquals(NUM_COLUMNS, cells.size());

        for (int columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex++) {
          Cell cell = cells.get(columnIndex);
          byte[] colNameRec = cell.getColumnName();
          byte[] colNameExp = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);

          // Verify ColumnNames for the Row
          // System.out.println("XXX TEST.Scan :: " + "ColumnName expected: " +
          // Arrays.toString(colNameExp));
          // System.out.println("XXX TEST.Scan :: " + "ColumnName received: " +
          // Arrays.toString(colNameRec));
          assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));

          //// Verify Column-Values for the Row
          byte[] colValRec = (byte[]) cell.getColumnValue();
          byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + rowIndex + columnIndex);
          // System.out.println("XXX TEST.Scan :: " + "ColumnValue expected: " +
          // Arrays.toString(colValExp));
          // System.out.println("XXX TEST.Scan :: " + "ColumnValue received: " +
          // Arrays.toString(colValRec));
          assertEquals(0, Bytes.compareTo(colValExp, colValRec));

        }
      }
      noOfRecordsInGet++;
    }
    assertEquals(keys.length, noOfRecordsInGet);

  }
}
