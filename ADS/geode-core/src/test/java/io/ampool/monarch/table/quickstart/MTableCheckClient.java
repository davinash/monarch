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
package io.ampool.monarch.table.quickstart;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Sample MTable Quickstart example showing use of checkAndPut and checkAndDelete.
 *
 * @since 0.5.0.0
 */
public class MTableCheckClient {

  public static void printRow(MTable table, int rownum, boolean shouldExist, String msg) {
    System.out.println("\nColumns for row " + rownum + " " + msg);
    Get get = new Get(Bytes.toBytes("rowKey" + rownum));
    Row result = table.get(get);
    if (shouldExist) {
      List<Cell> row = result.getCells();
      for (Cell cell : row) {
        System.out.println("ColumnName   => " + Bytes.toString(cell.getColumnName())
            + " AND ColumnValue => " + Bytes.toString(((byte[]) cell.getColumnValue())));
      }
    } else {
      if (result == null) {
        System.out.println("row for key rowKey" + rownum + " does not exist");
      } else {
        if (result.isEmpty()) {
          System.out.println("row for key rowKey" + rownum + " does not exist (result was empty)");
        } else {
          System.out.println("row for key rowKey" + rownum + " exists");
        }
      }
    }
    System.out.println("\n");
  }

  public static void main(String args[]) {
    System.out.println("MTable Quickstart example for checkAndPut and checkAndDelete");

    MClientCache clientCache = new MClientCacheFactory().set("log-file", "/tmp/MTableClient.log")
        .addPoolLocator("127.0.0.1", 10334).create();
    System.out.println("Connection to monarch distributed system is successfully done!");

    List<String> columnNames = Arrays.asList("NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ");
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(Bytes.toBytes(columnNames.get(0)))
        .addColumn(Bytes.toBytes(columnNames.get(1))).addColumn(Bytes.toBytes(columnNames.get(2)))
        .addColumn(Bytes.toBytes(columnNames.get(3))).addColumn(Bytes.toBytes(columnNames.get(4)))
        .addColumn(Bytes.toBytes(columnNames.get(5))).setRedundantCopies(1).
        // setTableType(MTableType.UNORDERED).
        setTotalNumOfSplits(4).setMaxVersions(1);

    Admin admin = clientCache.getAdmin();
    String tableName = "CheckTable";
    try {
      admin.deleteTable(tableName);
    } catch (Exception exc) {
    }
    try {
      Thread.sleep(2);
    } catch (Exception exc) {
    }
    MTable table = admin.createTable(tableName, tableDescriptor);
    System.out.println("Table " + tableName + " created successfully!");

    int NUM_OF_COLUMNS = 6;
    Put record = new Put(Bytes.toBytes("rowKey"));
    for (int i = 0; i < 10000; i++) {
      record.setRowKey(Bytes.toBytes("rowKey" + i));
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        record.addColumn(Bytes.toBytes(columnNames.get(colIndex)),
            Bytes.toBytes("val" + i + "" + colIndex));
      }
      table.put(record);
      record.clear();
    }

    printRow(table, 5, true, "after initial row put");

    // modify the row with a basic put

    Put put = new Put(Bytes.toBytes("rowKey" + 5));
    put.addColumn(Bytes.toBytes(columnNames.get(4)), Bytes.toBytes("testput"));
    try {
      table.put(put);
    } catch (Exception exc) {
      exc.printStackTrace();
    }

    printRow(table, 5, true, "after put to row 5 col 4");

    put = new Put(Bytes.toBytes("rowKey" + 5));
    put.addColumn(Bytes.toBytes(columnNames.get(4)), Bytes.toBytes("testput2"));
    try {
      table.put(put);
    } catch (Exception exc) {
      exc.printStackTrace();
    }

    printRow(table, 5, true, "after put again to row 5 col 4");

    printRow(table, 3, true, "before row 3 check operations");

    // put a change that should fail (check will fail)

    // do checkAndPut
    Put cput = new Put(Bytes.toBytes("rowKey" + 3));
    cput.addColumn(Bytes.toBytes(columnNames.get(4)), Bytes.toBytes("testcheckandput"));
    boolean result = false;
    try {
      result = table.checkAndPut(Bytes.toBytes("rowKey" + 3), Bytes.toBytes(columnNames.get(5)),
          Bytes.toBytes("foobar"), cput);
    } catch (Exception ioe) {
      ioe.printStackTrace();
    }
    if (result)
      System.out.println("checkAndPut succeeded (--> SHOULD FAIL)");
    else
      System.out.println("checkAndPut failed (should fail)");

    printRow(table, 3, true, "after failed check and put");

    // put a change that should succeed (check will pass)

    cput = new Put(Bytes.toBytes("rowKey" + 3));
    cput.addColumn(Bytes.toBytes(columnNames.get(4)), Bytes.toBytes("testcheckandput"));

    try {
      result = table.checkAndPut(Bytes.toBytes("rowKey" + 3), Bytes.toBytes(columnNames.get(5)),
          Bytes.toBytes("val35"), cput);
    } catch (Exception ioe) {
      ioe.printStackTrace();
    }
    if (result)
      System.out.println("checkAndPut succeeded (should succeed)");
    else
      System.out.println("checkAndPut failed (--> SHOULD SUCCEED)");

    printRow(table, 3, true, "after succeeded check and put");

    // put a change where check row is same as change row

    cput = new Put(Bytes.toBytes("rowKey" + 3));
    cput.addColumn(Bytes.toBytes(columnNames.get(3)), Bytes.toBytes("testcheckandputsame"));

    try {
      result = table.checkAndPut(Bytes.toBytes("rowKey" + 3), Bytes.toBytes(columnNames.get(3)),
          Bytes.toBytes("val33"), cput);
    } catch (Exception ioe) {
      ioe.printStackTrace();
    }
    if (result)
      System.out.println("checkAndPut succeeded (should succeed)");
    else
      System.out.println("checkAndPut failed (--> SHOULD SUCCEED)");

    printRow(table, 3, true, "after succeeded check and put to same col");

    // delete a row with a check that will fail (row not changed)

    Delete del = new Delete(Bytes.toBytes("rowKey" + 3));
    try {
      result = table.checkAndDelete(Bytes.toBytes("rowKey" + 3), Bytes.toBytes(columnNames.get(3)),
          Bytes.toBytes("foobar"), del);
    } catch (Exception exc) {
      exc.printStackTrace();
    }

    if (result)
      System.out.println("checkAndDelete succeeded (--> SHOULD FAIL)");
    else
      System.out.println("checkAndDelete failed (should fail)");

    printRow(table, 3, true, "after failed check and delete");

    // delete a column (should succeed)

    del = new Delete(Bytes.toBytes("rowKey" + 3));
    del.addColumn(Bytes.toBytes(columnNames.get(4)));
    try {
      result = table.checkAndDelete(Bytes.toBytes("rowKey" + 3), Bytes.toBytes(columnNames.get(1)),
          Bytes.toBytes("val31"), del);
    } catch (Exception exc) {
      exc.printStackTrace();
    }

    if (result)
      System.out.println("checkAndDelete succeeded (should succeed)");
    else
      System.out.println("checkAndDelete failed (--> SHOULD SUCCEED)");

    printRow(table, 3, true, "after succeeded check and delete col 4");

    // delete a row with a check that will pass (row deleted)

    del = new Delete(Bytes.toBytes("rowKey" + 3));
    try {
      result = table.checkAndDelete(Bytes.toBytes("rowKey" + 3), Bytes.toBytes(columnNames.get(2)),
          Bytes.toBytes("val32"), del);
    } catch (Exception exc) {
      exc.printStackTrace();
    }

    if (result)
      System.out.println("checkAndDelete succeeded (should succeed)");
    else
      System.out.println("checkAndDelete failed (--> SHOULD SUCCEED)");

    printRow(table, 3, false, "after succeeded check and delete entire row");

    admin.deleteTable(tableName);
    System.out.println("Table is deleted successfully!");

    clientCache.close();
    System.out.println("Connection to monarch DS closed successfully!");
  }
}
