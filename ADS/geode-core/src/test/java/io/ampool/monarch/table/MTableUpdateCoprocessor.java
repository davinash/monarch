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
package io.ampool.monarch.table;

import static junit.framework.TestCase.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.coprocessor.MCoprocessorContext;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MTableUpdateCoprocessor extends MCoprocessor {
  public static String[] keys =
      {"000", "001", "010", "020", "030", "040", "050", "060", "070", "080", "090", "099", "100"};

  public MTableUpdateCoprocessor() {}

  public boolean updateRowsUsingPutOnMTableRegion(MCoprocessorContext context) {
    MTableRegion table = context.getMTableRegion();
    MExecutionRequest request = context.getRequest();
    String updateStr = (String) request.getArguments();
    byte[] startKey = table.getStartKey();
    byte[] stopKey = table.getEndKey();

    for (int keyIndex = 0; keyIndex < keys.length; keyIndex++) {
      Exception e = null;
      Put myput1 = new Put(Bytes.toBytes(keys[keyIndex]));
      for (int i = 0; i < MTableDUnitConfigFramework.NUM_COLUMNS; i++) {
        myput1.addColumn(MTableDUnitConfigFramework.COLUMNNAME_PREFIX + i,
            Bytes.toBytes(MTableDUnitConfigFramework.VALUE_PREFIX + updateStr + i));
      }
      try {
        System.out.println("Putting key: " + keys[keyIndex]);
        table.put(myput1);
        System.out
            .println("Succesfully put key: " + Arrays.toString(Bytes.toBytes(keys[keyIndex])));
      } catch (Exception e1) {
        e = e1;
        System.err.println("Start key: " + Arrays.toString(startKey));
        System.err.println("End key: " + Arrays.toString(stopKey));
        System.err.println("Incoming key: " + Arrays.toString(Bytes.toBytes(keys[keyIndex])));
        // E.printStackTrace();
      }

      if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
        if (Bytes.compareTo(Bytes.toBytes(keys[keyIndex]), startKey) >= 0
            && Bytes.compareTo(Bytes.toBytes(keys[keyIndex]), stopKey) <= 0) {
          assertNull(e);
        } else {
          assertNotNull(e);
        }
      }
    }
    return true;
  }

  public boolean updateRowsUsingPut(MCoprocessorContext context) {
    MTable table = context.getTable();
    MExecutionRequest request = context.getRequest();
    String updateStr = (String) request.getArguments();

    for (int keyIndex = 0; keyIndex < keys.length; keyIndex++) {
      Put myput1 = new Put(Bytes.toBytes(keys[keyIndex]));
      for (int i = 0; i < MTableDUnitConfigFramework.NUM_COLUMNS; i++) {
        myput1.addColumn(MTableDUnitConfigFramework.COLUMNNAME_PREFIX + i,
            Bytes.toBytes(MTableDUnitConfigFramework.VALUE_PREFIX + updateStr + i));
      }
      table.put(myput1);
    }
    return true;
  }

  public boolean updateRowsUsingPutAll(MCoprocessorContext context) {
    MTable table = context.getTable();
    MExecutionRequest request = context.getRequest();
    String updateStr = (String) request.getArguments();

    List<Put> puts = new ArrayList<>();
    for (int keyIndex = 0; keyIndex < keys.length; keyIndex++) {
      Put myput1 = new Put(Bytes.toBytes(keys[keyIndex]));
      for (int i = 0; i < MTableDUnitConfigFramework.NUM_COLUMNS; i++) {
        myput1.addColumn(MTableDUnitConfigFramework.COLUMNNAME_PREFIX + i,
            Bytes.toBytes(MTableDUnitConfigFramework.VALUE_PREFIX + updateStr + i));
      }
      puts.add(myput1);
    }
    table.put(puts);
    return true;
  }

  public static void verifyGetsCommon(MTable table, String updatedStr, String ctx) {
    for (int keyIndex = 0; keyIndex < keys.length; keyIndex++) {
      Get myget1 = new Get(Bytes.toBytes(keys[keyIndex]));
      Row result = table.get(myget1);
      assertNotNull(result);
      assertFalse(result.isEmpty());
      assertTrue(Bytes.toString(result.getRowId()).compareTo(keys[keyIndex]) == 0);
      List<Cell> cells = result.getCells();
      assertTrue(cells.size() == MTableDUnitConfigFramework.NUM_COLUMNS + 1 /* GEN- 1696 */);
      for (int i = 0; i < MTableDUnitConfigFramework.NUM_COLUMNS - 1 /* GEN- 1696 */; i++) {
        Cell cell = cells.get(i);
        System.out.println("====" + ctx + " GET==== The column value for: (" + keys[keyIndex]
            + "):col :" + Bytes.toString(cell.getColumnName()) + "is: "
            + Bytes.toString((byte[]) cell.getColumnValue()));
        assertTrue(Bytes.toString((byte[]) cell.getColumnValue())
            .compareTo(MTableDUnitConfigFramework.VALUE_PREFIX + updatedStr + i) == 0);
      }
    }

    // Verify using GetAll
    List<Get> gets = new ArrayList<>();
    for (int keyIndex = 0; keyIndex < keys.length; keyIndex++) {
      Get myget1 = new Get(Bytes.toBytes(keys[keyIndex]));
      gets.add(myget1);
    }
    Row[] results = table.get(gets);
    assertNotNull(results);
    assertEquals(keys.length, results.length);
    for (int i = 0; i < results.length; i++) {
      Row result = results[i];
      assertNotNull(result);
      assertFalse(result.isEmpty());
      assertTrue(keys[i].compareTo(Bytes.toString(result.getRowId())) == 0);
      List<Cell> cells = result.getCells();
      assertEquals(MTableDUnitConfigFramework.NUM_COLUMNS + 1 /* GEN-1696 */, cells.size());
      for (int j = 0; j < MTableDUnitConfigFramework.NUM_COLUMNS - 1 /* GEN-1696 */; j++) {
        Cell cell = cells.get(j);
        System.out.println("====" + ctx + " GETALL==== The column value for: (" + keys[i]
            + ") col :" + Bytes.toString(cell.getColumnName()) + "is: "
            + Bytes.toString((byte[]) cell.getColumnValue()));
        assertTrue(Bytes.toString((byte[]) cell.getColumnValue())
            .compareTo(MTableDUnitConfigFramework.VALUE_PREFIX + updatedStr + j) == 0);
      }
    }
  }

  public boolean verifyGets(MCoprocessorContext context) {
    MTable table = context.getTable();
    MExecutionRequest request = context.getRequest();
    String updateStr = (String) request.getArguments();

    verifyGetsCommon(table, updateStr, "SERVER");
    return true;
  }
}
