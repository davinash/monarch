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
import java.util.Iterator;
import java.util.List;

/**
 * Sample MTable Quickstart example. It performs following table operations. create a connection
 * with monarch distributed system (DS). Create Table named "EmployeeTable" with columns ["NAME",
 * "ID", "AGE", "SALARY", "DEPT", "DOJ"] Insert row with different versions, update selected columns
 * value. Retrieve row value given a specified timestamp, no timestamp (latest version), given
 * selected columns. Delete row at specified timestamp, Delete a values of given columns from a row,
 * delete entire row.
 *
 * @since 0.2.0.0
 */
public class MTableClient {

  public static void main(String args[]) {
    System.out.println("MTable Quickstart example!");

    // Step:1 create a connection with monarch distributed system (DS).
    // create a configuration and connect to monarch locator.
    MClientCache clientCache = new MClientCacheFactory().set("log-file", "/tmp/MTableClient.log")
        .addPoolLocator("127.0.0.1", 10334).create();
    System.out.println("Connection to monarch distributed system is successfully done!");

    // Step:2 create a Table.
    List<String> columnNames = Arrays.asList("NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ");
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(Bytes.toBytes(columnNames.get(0)))
        .addColumn(Bytes.toBytes(columnNames.get(1))).addColumn(Bytes.toBytes(columnNames.get(2)))
        .addColumn(Bytes.toBytes(columnNames.get(3))).addColumn(Bytes.toBytes(columnNames.get(4)))
        .addColumn(Bytes.toBytes(columnNames.get(5))).setMaxVersions(5);

    Admin admin = clientCache.getAdmin();
    String tableName = "EmployeeTable";
    MTable table = admin.createTable(tableName, tableDescriptor);
    System.out.println("Table [EmployeeTable] is created successfully!");

    // Step-3: Insert rows into table.
    /*
     * For a key "rowKey", following values will be populated at different timestamp [100, 200, 300,
     * 400, 500]
     * 
     * timestamp column-1 column-2 column-3 column-4 column-5 column-6
     * ------------------------------------------------------------------------------- 100 val00
     * val01 val02 val03 val04 val05 200 val10 val11 val12 val13 val14 val15 300 val20 val21 val22
     * val23 val24 val25 400 val30 val31 val32 val33 val34 val35 500 val40 val41 val42 val43 val44
     * val45
     */
    int TABLE_MAX_VERSIONS = 5;
    int NUM_OF_COLUMNS = 6;
    Put record = new Put(Bytes.toBytes("rowKey"));
    for (int versionIndex = 0; versionIndex < TABLE_MAX_VERSIONS; versionIndex++) {
      long timestamp = (versionIndex + 1) * 100;
      record.setTimeStamp(timestamp);
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        record.addColumn(Bytes.toBytes(columnNames.get(colIndex)),
            Bytes.toBytes("val" + versionIndex + "" + colIndex));
      }
      table.put(record);
      System.out.println("Row inserted at timestamp - " + timestamp);
    }


    // Step:3 - Retrieve row value.
    // Retrieve a row value at specified timestamp.
    Get get = new Get(Bytes.toBytes("rowKey"));
    get.setTimeStamp(400L);
    Row result = table.get(get);
    int columnIndex = 0;
    List<Cell> row = result.getCells();
    for (Cell cell : row) {
      System.out.println("ColumnName   => " + Bytes.toString(cell.getColumnName())
          + " AND ColumnValue  => " + Bytes.toString((byte[]) cell.getColumnValue()));
      columnIndex++;
    }
    System.out.println("Retrieved a row entry at specified timestamp successfully!");

    // Retrieve a selected columns of a row
    Get getRecord = new Get(Bytes.toBytes("rowKey"));
    getRecord.addColumn(Bytes.toBytes("NAME"));
    getRecord.addColumn(Bytes.toBytes("ID"));
    getRecord.addColumn(Bytes.toBytes("SALARY"));

    result = table.get(getRecord);
    List<Cell> selectedRow = result.getCells();
    columnIndex = 0;
    for (Cell cell : selectedRow) {
      System.out.println("ColumnName   => " + Bytes.toString(cell.getColumnName())
          + " AND ColumnValue  => " + Bytes.toString((byte[]) cell.getColumnValue()));
      columnIndex++;
    }
    System.out.println("Retrieved selected columns for a given row successfully!");

    // Run scan on table
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      System.out.println("Key " + Bytes.toString(res.getRowId()));
      selectedRow = res.getCells();
      columnIndex = 0;
      for (Cell cell : selectedRow) {
        System.out.println("ColumnName   => " + Bytes.toString(cell.getColumnName())
            + " AND ColumnValue  => " + Bytes.toString((byte[]) cell.getColumnValue()));
        columnIndex++;
      }
    }

    // Step-4 - Delete row
    // Delete row at specified timestamp
    Delete delete = new Delete(Bytes.toBytes("rowKey"));
    delete.setTimestamp(400L);
    table.delete(delete);
    System.out.println("Deleted a row entry at specified timestamp successfully!");

    // Delete a values of given columns from a row
    Delete colDelete = new Delete(Bytes.toBytes("rowKey"));
    colDelete.addColumn(Bytes.toBytes("AGE"));
    colDelete.addColumn(Bytes.toBytes("DEPT"));
    table.delete(delete);
    System.out.println("deleted values for selected columns of a given row successfully!");

    // Delete a row. All versions of a row will be deleted.
    delete = new Delete(Bytes.toBytes("rowKey"));
    table.delete(delete);
    System.out.println("deleted all versions for a given key successfully!");

    // Step-5: Delete a table
    admin.deleteTable(tableName);
    System.out.println("Table is deleted successfully!");

    // Step-6: close the Monarch connection
    clientCache.close();
    System.out.println("Connection to monarch DS closed successfully!");
  }
}
