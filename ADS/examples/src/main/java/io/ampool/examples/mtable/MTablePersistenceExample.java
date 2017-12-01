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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;


/**
 * Example for using local disk persistence for the mtables both ordered and unordered All the
 * persistent disk files are created in the server dir MTablePersistenceExample false
 *
 */
public class MTablePersistenceExample {
  private static final String MTABLE_ORDERED = "EMPLOYEE_ORDERED";
  private static final String MTABLE_UNORDERED = "EMPLOYEE_UNORDERED";
  private static final String MTABLE_ORDERED_PERSISTENT = "EMPLOYEE_ORDERED_PERSISTENT";
  private static final String MTABLE_UNORDERED_PERSISTENT = "EMPLOYEE_UNORDERED_PERSISTENT";

  private static List<String> columnNames =
      Arrays.asList("NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ");

  public static void main(String[] args) {
    System.out.println("========Running MTable persistence example ===================");
    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }

    connect(locator_host, locator_port);

    // ordered table
    createMTable(MTABLE_ORDERED, true, false);
    createMTable(MTABLE_UNORDERED, false, false);
    createMTable(MTABLE_ORDERED_PERSISTENT, true, true);
    createMTable(MTABLE_UNORDERED_PERSISTENT, false, true);

    insertValues(MTABLE_ORDERED);
    insertValues(MTABLE_UNORDERED);
    insertValues(MTABLE_ORDERED_PERSISTENT);
    insertValues(MTABLE_UNORDERED_PERSISTENT);

    retrieveValues(MTABLE_ORDERED);
    retrieveValues(MTABLE_UNORDERED);
    retrieveValues(MTABLE_ORDERED_PERSISTENT);
    retrieveValues(MTABLE_UNORDERED_PERSISTENT);

    runScan(MTABLE_ORDERED);
    runScan(MTABLE_UNORDERED);
    runScan(MTABLE_ORDERED_PERSISTENT);
    runScan(MTABLE_UNORDERED_PERSISTENT);

    deletetable(MTABLE_ORDERED);
    deletetable(MTABLE_UNORDERED);
    deletetable(MTABLE_ORDERED_PERSISTENT);
    deletetable(MTABLE_UNORDERED_PERSISTENT);

    disconnect();
    System.out.println("========Running MTable persistence example finished =============");

  }

  private static AmpoolClient client;

  public static void connect(final String locator, final int locatorPort) {
    // connect to ampool locator and create mcache
    Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, "/tmp/MTableClient.log");
    client = new AmpoolClient(locator, locatorPort);
    System.out.println("Connection to monarch distributed system is successfully done!");
  }

  /**
   * create mcache and associated tables
   */
  public static void createMTable(String tableName, boolean ordered, boolean persistent) {

    MTableDescriptor tableDescriptor = null;
    if (ordered) {
      tableDescriptor = new MTableDescriptor();
    } else {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    }
    Schema schema = new Schema.Builder().column(columnNames.get(0)).column(columnNames.get(1))
        .column(columnNames.get(2)).column(columnNames.get(3)).column(columnNames.get(4))
        .column(columnNames.get(5)).build();
    tableDescriptor.setSchema(schema);


    if (persistent) {
      tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    }

    client.getAdmin().createMTable(tableName, tableDescriptor);
    System.out.println("Table [" + tableName + "] is created successfully!");
  }


  /**
   * For a key "rowKey", following values will be populated at different timestamp [100, 200, 300,
   * 400, 500]
   * <p>
   * timestamp column-1 column-2 column-3 column-4 column-5 column-6
   * ------------------------------------------------------------------------------- 100 val00 val01
   * val02 val03 val04 val05 200 val10 val11 val12 val13 val14 val15 300 val20 val21 val22 val23
   * val24 val25 400 val30 val31 val32 val33 val34 val35 500 val40 val41 val42 val43 val44 val45
   *
   * @param tableName name of the mtable
   */
  public static void insertValues(String tableName) {
    System.out.println("Inserting values in table " + tableName);

    int TABLE_MAX_VERSIONS = 5;
    int NUM_OF_COLUMNS = 6;
    MTable mTable = MCacheFactory.getAnyInstance().getMTable(tableName);
    Put record = new Put(Bytes.toBytes("rowKey"));
    for (int versionIndex = 0; versionIndex < TABLE_MAX_VERSIONS; versionIndex++) {
      long timestamp = (versionIndex + 1) * 100;
      if (mTable.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED)
        record.setTimeStamp(timestamp);
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        record.addColumn(Bytes.toBytes(columnNames.get(colIndex)),
            Bytes.toBytes("val" + versionIndex + "" + colIndex));
      }
      mTable.put(record);
      System.out.println("Row inserted at timestamp - " + timestamp);
    }
  }


  private static void retrieveValues(String tableName) {
    System.out.println("Retrieving values from " + tableName);
    MTable mTable = client.getMTable(tableName);
    Get get = new Get(Bytes.toBytes("rowKey"));
    if (mTable.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED)
      get.setTimeStamp(400L);
    Row result = mTable.get(get);
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

    result = mTable.get(getRecord);
    List<Cell> selectedRow = result.getCells();
    columnIndex = 0;
    for (Cell cell : selectedRow) {
      System.out.println("ColumnName   => " + Bytes.toString(cell.getColumnName())
          + " AND ColumnValue  => " + Bytes.toString((byte[]) cell.getColumnValue()));
      columnIndex++;
    }
    System.out.println("Retrieved selected columns for a given row successfully!");
  }

  private static void runScan(String tableName) {
    System.out.println("Running scan on table " + tableName);
    MTable mTable = client.getMTable(tableName);
    Scanner scanner = mTable.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      System.out.println("Key" + Bytes.toString(res.getRowId()));
    }
  }

  private static void deletetable(String tableName) {
    client.getAdmin().deleteMTable(tableName);
    System.out.println("Table " + tableName + " is deleted successfully!");


  }

  private static void disconnect() {
    // Step-6: close the Monarch connection
    client.close();
    System.out.println("Connection to monarch DS closed successfully!");
  }
}
