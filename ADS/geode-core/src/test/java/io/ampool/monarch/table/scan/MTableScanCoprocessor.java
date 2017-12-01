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

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.coprocessor.MCoprocessorContext;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import org.junit.Assert;

import java.util.*;

public class MTableScanCoprocessor extends MCoprocessor {

  String TABLENAME = "testTable";
  String[] keys = {"005", "006", "007", "008", "009"};
  Long timestamp = 123456798l;
  int NUM_COLUMNS = 5;
  String COLUMNNAME_PREFIX = "COL", VALUE_PREFIX = "VAL";

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

  public boolean chechExceptions(MCoprocessorContext context) {
    System.out.println("MTableDeleteCoprocessor.chechExceptions");
    MExecutionRequest request = context.getRequest();
    MTable table = context.getTable();

    return true;

  }

  public boolean dobatchScan(MCoprocessorContext context) {
    MTable table = context.getTable();
    Scan scan = new Scan();
    scan.enableBatchMode();
    scan.setBatchSize(1000);
    scan.setClientQueueSize(10);

    verifyScanResult(table, keys, timestamp, scan);
    return true;
  }

  public boolean doDefaultScan(MCoprocessorContext context) {
    MTable table = context.getTable();
    Scan scan = new Scan();
    verifyScanResult(table, keys, timestamp, scan);
    return true;
  }

  public boolean doScanWithStartStopKeysTest(MCoprocessorContext context) {
    MTable table = context.getTable();
    try {
      Scan scan = new Scan();
      scan.setStartRow(Bytes.toBytes("000"));
      scan.setStopRow(Bytes.toBytes("111"));

      verifyScanResult(table, keys, timestamp, scan);
    } catch (IllegalArgumentException iae) {
      Assert.assertEquals(table.getTableDescriptor().getTableType(), MTableType.UNORDERED);
    }

    return true;
  }

  private void verifyScanResult(MTable table, String[] keys, Long timestamp, Scan scan) {
    Scanner scanner = table.getScanner(scan);

    Assert.assertNotNull(scanner);

    Row row = scanner.next();

    Assert.assertNotNull(row);

    int noOfRecordsInScan = 0;
    byte[] pastRowKey = new byte[0];
    int rowIndex = 0;

    Assert.assertTrue("Result is empty", !row.isEmpty());

    while (row != null) {
      // System.out.println("Test.verifyScan-2");
      // check columns and rowkey value is right
      byte[] rowKey = row.getRowId();
      // System.out.println("NNNN verifyScan :: " + "Row REcvd: " + Arrays.toString(rowKey));
      // check if this row is greter than pervious
      // ignore for first row key
      Assert.assertTrue(rowKey.length > 0);

      // TEST:: ORDERED_TABLE, New row key should always be greater than the lastRowKey
      if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED
          && rowIndex > 0) {
        Assert.assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
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
        Assert.assertTrue(isKeyFound);

      } else {
        // ORDERED_TABLE case
        rowKeyExpected = keys[rowIndex];
        // System.out.println("XXX TEST.Scan :: " + "-------------------------SCAN
        // ROW-------------------------");
        // System.out.println("XXX TEST.Scan :: " + "Row expected: " + rowKeyExpected);
        // System.out.println("XXX TEST.Scan :: " + "Row received: " + Arrays.toString(rowKey));
        Assert.assertEquals(rowKeyExpected, Bytes.toString(rowKey));
      }

      // Test- Assert if number of cols are equal
      List<Cell> cells = row.getCells();
      // System.out.println("XXX TEST.scan Total-Columns = " + cells.size());
      Assert.assertEquals(NUM_COLUMNS + 1, cells.size());

      for (int columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex++) {
        Cell cell = cells.get(columnIndex);
        byte[] colNameRec = cell.getColumnName();
        byte[] colNameExp = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);

        // Verify ColumnNames for the Row
        // System.out.println("XXX TEST.Scan :: " + "ColumnName expected: " +
        // Arrays.toString(colNameExp));
        // System.out.println("XXX TEST.Scan :: " + "ColumnName received: " +
        // Arrays.toString(colNameRec));
        Assert.assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));

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
        Assert.assertEquals(0, Bytes.compareTo(colValExp, colValRec));

      }
      pastRowKey = rowKey;
      noOfRecordsInScan++;
      rowIndex++;
      row = scanner.next();
      if (keys.length != noOfRecordsInScan) {
        Assert.assertTrue("Result is empty", !row.isEmpty());
      }
    }

    Assert.assertEquals(keys.length, noOfRecordsInScan);

    scanner.close();
  }
}
