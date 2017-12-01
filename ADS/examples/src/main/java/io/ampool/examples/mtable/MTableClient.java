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

package io.ampool.examples.mtable;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Schema;

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
  private static final String[] COLUMN_NAMES =
      new String[] {"NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ"};

  public static void main(String args[]) {
    System.out.println("MTable Quickstart example!");

    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }

    // Step:1 create client that connects to the Ampool cluster via locator.
    final Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, "/tmp/MTableClient.log");
    final AmpoolClient aClient = new AmpoolClient(locator_host, locator_port, props);
    System.out.println("AmpoolClient connected to the cluster successfully!");

    // Step:2 create the schema.
    final Schema schema = new Schema(COLUMN_NAMES);
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setSchema(schema);

    Admin admin = aClient.getAdmin();
    String tableName = "EmployeeTable";

    MTable table = admin.createMTable(tableName, tableDescriptor);
    System.out.println("Table [EmployeeTable] is created successfully!");

    // Step-3: Insert rows into table.
    int NUM_OF_ENTRIES = 5;
    int NUM_OF_COLUMNS = 6;

    for (int rowIndex = 1; rowIndex <= NUM_OF_ENTRIES; rowIndex++) {
      Put record = new Put(Bytes.toBytes("rowKey" + rowIndex));
      long timestamp = (rowIndex) * 100;
      record.setTimeStamp(timestamp);
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAMES[colIndex]),
            Bytes.toBytes("val" + rowIndex + "" + colIndex));
      }
      table.put(record);
      System.out.println("Row inserted with timestamp - " + timestamp);
      System.out.println();
    }

    // Step:3 - Retrieve row value.
    // Retrieve a row value at specified timestamp.
    Get get = new Get(Bytes.toBytes("rowKey1"));
    get.setTimeStamp(100L);
    Row result = table.get(get);
    int columnIndex = 0;
    List<Cell> row = result.getCells();
    for (int i = 0; i < row.size() - 1; i++) {
      System.out.println("ColumnName   => " + Bytes.toString(row.get(i).getColumnName())
          + " AND ColumnValue  => " + Bytes.toString((byte[]) row.get(i).getColumnValue()));
      columnIndex++;
    }
    System.out.println("Retrieved a row entry at specified timestamp successfully!");
    System.out.println();

    // Retrieve a selected columns of a row
    Get getRecord = new Get(Bytes.toBytes("rowKey2"));
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
    System.out.println();

    // Run scan on table
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      System.out.println("Key: " + Bytes.toString(res.getRowId()));
    }
    System.out.println();

    // Step-4 - Delete row
    // Delete row at specified timestamp
    Delete delete = new Delete(Bytes.toBytes("rowKey3"));
    delete.setTimestamp(300L);
    table.delete(delete);
    System.out.println("Deleted a row entry at specified timestamp successfully!");
    System.out.println();

    // Delete a values of given columns from a row
    Delete colDelete = new Delete(Bytes.toBytes("rowKey4"));
    colDelete.addColumn(Bytes.toBytes("AGE"));
    colDelete.addColumn(Bytes.toBytes("DEPT"));
    table.delete(delete);
    System.out.println("deleted values for selected columns of a given row successfully!");
    System.out.println();

    // Delete a row. All versions of a row will be deleted.
    delete = new Delete(Bytes.toBytes("rowKey5"));
    table.delete(delete);
    System.out.println("deleted all versions for a given key successfully!");
    System.out.println();

    // Step-5: Delete a table
    admin.deleteMTable(tableName);
    System.out.println("Table is deleted successfully!");
    System.out.println();

    // Step-6: close the client connection
    aClient.close();
    System.out.println("Connection to monarch DS closed successfully!");
  }
}
