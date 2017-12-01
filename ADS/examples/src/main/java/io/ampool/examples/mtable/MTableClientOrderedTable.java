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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.google.common.base.Strings;
import io.ampool.client.AmpoolClient;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.exceptions.MCacheDeleteFailedException;
import io.ampool.monarch.table.exceptions.RowKeyDoesNotExistException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterBase;

/**
 * Sample Quickstart example for MTableType ORDERED_VERSIONED. ORDERED_VERSIONED type table allows
 * range/scan queries over any arbitrary key range. Also a new version is created for each row when
 * any of the columns get updated in a row Mtable can be configured to keep last N versions of the
 * rows.
 *
 * Following code performs simple table operations. Create a connection with monarch distributed
 * system (DS). Create Table named "EmployeeTable" with columns ["NAME", "ID", "AGE", "SALARY",
 * "DEPT", "DOJ"] Columns are created with type byte array. See MTableClientWithColumnTypes on how
 * to create Mtable using specific column types. Insert row with different versions, update selected
 * columns value. Retrieve row value *** w/ specified version (aka timestamp), *** w/ no timestamp
 * (latest version), *** w/ retrieve only specified selected columns. Scan table with specifying
 * some key range. Apply filter on the rowkey to filter some records from the scan Delete row at
 * specified timestamp, Delete a values of given columns from a row, delete entire row.
 *
 * @since 0.2.0.0
 */
public class MTableClientOrderedTable {

  static byte[] RowIDKeySpaceStart = "000".getBytes();
  static byte[] RowIDKeySpaceEnd = "100".getBytes();
  static byte[] scanStartKey = "020".getBytes();
  static byte[] scanStopKey = "080".getBytes();
  static byte[] scanFilterStartKey = "040".getBytes();
  static byte[] scanFilterStopKey = "050".getBytes();
  AmpoolClient aClient = null;
  public Admin admin = null;
  String tableName = "EmployeeTable";
  public MTable table = null;
  public int TABLE_MAX_VERSIONS = 5;
  public int NUM_OF_COLUMNS = 6;
  public List<String> columnNames = Arrays.asList("NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ");


  public MTableClientOrderedTable(final String locatorHost, final int locatorPort) {
    // Step:1 create a connection with monarch distributed system (DS).
    this.aClient = new AmpoolClient(locatorHost, locatorPort);
    System.out.println("Connection to monarch distributed system is successfully done!\n");

    // Step:2 create a Table.
    // Type: ORDERED_VERSIONED i.e. rows are ordered by rowkey column and table also supports
    // versioning of the row when any of columns are updated with new values
    // Replicas: Two redundant copies of each partition/split in memory
    // Splits: Also referred as partitions or buckets.
    // Range partitioning supported w/
    // default 8 Byte key space is uniformly split into 113 default number of splits/buckets.
    // In example below, key range 0 to 100 split into 10 buckets
    // StartRangeKey is Start key (0) of first bucket
    // StopRangekey is End key (100) for the last bucket
    // Persistence: Along with memory, Data is also persisted to local disk for recovery
    // Versions: Max versions for each row stored.
    int NUM_REPLICAS = 2;
    int NUM_SPLITS = 10;
    MTableDescriptor tableDescriptor = new MTableDescriptor();

    this.admin = this.aClient.getAdmin();
    if (this.admin.existsMTable(this.tableName)) {
      this.admin.deleteMTable(this.tableName);
    }
    /* build the schema from available columns */
    Schema schema = new Schema.Builder().column(columnNames.get(0)).column(columnNames.get(1))
        .column(columnNames.get(2)).column(columnNames.get(3)).column(columnNames.get(4))
        .column(columnNames.get(5)).build();

    tableDescriptor.setMaxVersions(TABLE_MAX_VERSIONS).setRedundantCopies(NUM_REPLICAS)
        .setTotalNumOfSplits(NUM_SPLITS).setStartStopRangeKey(RowIDKeySpaceStart, RowIDKeySpaceEnd)
        .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS).setSchema(schema);

    this.table = this.admin.createMTable(this.tableName, tableDescriptor);
    System.out.println("Table [EmployeeTable] is created successfully!\n");
  }


  public static void main(String args[]) {
    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }

    System.out.println("Ordered_Versioned Type MTable - Quickstart example!\n");
    MTableClientOrderedTable orderedTable =
        new MTableClientOrderedTable(locator_host, locator_port);

    /*
     * Step-3: Insert single row into table with multiple versions.
     *
     * For a key "78", following values will be populated at different timestamp [100, 200, 300,
     * 400, 500]
     *
     * timestamp column-1 column-2 column-3 column-4 column-5 column-6
     * ------------------------------------------------------------------------------- 100 val00
     * val01 val02 val03 val04 val05 200 val10 val11 val12 val13 val14 val15 300 val20 val21 val22
     * val23 val24 val25 400 val30 val31 val32 val33 val34 val35 500 val40 val41 val42 val43 val44
     * val45
     */

    byte[] row78 = "078".getBytes();
    String row78_s = Arrays.toString(row78);
    System.out
        .println("STEP:3 Inserting Row ID [078][" + row78_s + "] w/ version timestamps 100 - 600]");
    Put record = new Put(row78);
    for (int versionIndex = 0; versionIndex < orderedTable.TABLE_MAX_VERSIONS + 1; versionIndex++) {
      long timestamp = (versionIndex + 1) * 100;
      record.setTimeStamp(timestamp);
      for (int colIndex = 0; colIndex < orderedTable.NUM_OF_COLUMNS; colIndex++) {
        record.addColumn(Bytes.toBytes(orderedTable.columnNames.get(colIndex)),
            Bytes.toBytes("val" + versionIndex + "" + colIndex));
      }
      orderedTable.table.put(record);
    }

    // Display table state
    orderedTable.readTable(orderedTable.table, null, null);

    /*
     * Step-4: Insert or Update a row (row78) atomically, if a latest version of it's specific
     * column cell (row78/column-1) value matches the specified value (val40)
     */
    System.out.println(
        "STEP:4 Updating Row [ID:078][byte:" + row78_s + "][version:600] w/ UpdateVal600{0-5}");
    try {
      Put newRecord = new Put(row78);
      newRecord.setTimeStamp(600L);
      for (int colIndex = 0; colIndex < orderedTable.NUM_OF_COLUMNS; colIndex++) {
        newRecord.addColumn(Bytes.toBytes(orderedTable.columnNames.get(colIndex)),
            Bytes.toBytes("UpdateVal" + "600" + colIndex));
      }
      orderedTable.table.checkAndPut(row78, Bytes.toBytes(orderedTable.columnNames.get(0)),
          Bytes.toBytes("val50"), newRecord);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Display table state
    orderedTable.readTable(orderedTable.table, null, null);

    /*
     * Step-5: Insert batch of rows using singe table put. Faster insert than individual row puts
     */
    System.out.println("STEP:5 Inserting Batch of Rows at timestamp [100] - ");
    long version = 100;
    int batchSize = 10;
    Random r = new Random();
    List<String> listOfRowKeysAdded = new ArrayList<>();
    List<Put> rows = new ArrayList<Put>(2 * batchSize);
    for (int i = 0; i < 2 * batchSize; i++) {
      String rowID = null;
      do {
        int j = r.nextInt(100);
        rowID = Strings.padStart(String.valueOf(j), 3, '0');
      } while (listOfRowKeysAdded.contains(rowID));
      if (rowID.equals("078"))
        continue;
      listOfRowKeysAdded.add(rowID);
      Put row = new Put(rowID.getBytes());
      row.setTimeStamp(version);
      for (int colIndex = 0; colIndex < orderedTable.NUM_OF_COLUMNS; colIndex++) {
        row.addColumn(Bytes.toBytes(orderedTable.columnNames.get(colIndex)),
            Bytes.toBytes("val-" + rowID + "-" + version + "-" + colIndex));
      }
      rows.add(row);
    }
    orderedTable.table.put(rows);

    // Display table state
    orderedTable.readTable(orderedTable.table, null, null);

    // Step-6: - Retrieve all columns of a specified version of a row (row78)
    System.out.println("STEP:6 Retrieving all columns of  Row [078] at version timestamp [400]");
    orderedTable.readTable(orderedTable.table, "078", 400L);

    // Step-7: - Retrieve a selected columns of a row. If no version/timestamp specified latest
    // version will
    // will be retrieved.
    System.out.println("STEP:7 Retrieving selected columns [NAME, ID, SALARY] of Row [078]");
    Get getRecord = new Get(row78);
    getRecord.setTimeStamp(200L);
    getRecord.addColumn(Bytes.toBytes("NAME"));
    getRecord.addColumn(Bytes.toBytes("ID"));
    getRecord.addColumn(Bytes.toBytes("SALARY"));
    Row result = orderedTable.table.get(getRecord);
    List<Cell> selectedRow = result.getCells();
    System.out.println("Row [078], Version [200], Cells [" + selectedRow.toString() + "]");
    for (Cell cell : selectedRow) {
      System.out
          .println("Row [078], Version [200], ColumnName [" + Bytes.toString(cell.getColumnName())
              + "], ColumnValue [" + Bytes.toString((byte[]) cell.getColumnValue()) + "]");
    }

    // Step-8: -- Scan the table from key range 20 to 80.
    // -- Include the row w/ start key if present
    // -- Filter the rows out w/ rowID between 40-50.
    System.out.println(
        "STEP-8: Scanning rows w/ row key ranges [20-80], plus filtering rows between [40-50]");
    Scan scanner = new Scan();
    scanner.setBatchSize(10);
    scanner.setStartRow(scanStartKey);
    scanner.setStopRow(scanStopKey);
    scanner.setIncludeStartRow(true);
    Filter filter = new FilterBase() {
      @Override
      public boolean filterRowKey(Row result) {
        if (Bytes.compareTo(result.getRowId(), scanFilterStopKey) <= 0
            || Bytes.compareTo(result.getRowId(), scanFilterStartKey) > 0) {
          return false;
        }
        return true;
      }

      @Override
      public boolean hasFilterRow() {
        return true;
      }

      @Override
      public boolean hasFilterCell() {
        return false;
      }
    };
    scanner.setFilter(filter);
    Scanner results = orderedTable.table.getScanner(scanner);
    Iterator itr = results.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      System.out.println(
          "Row ID [" + Bytes.toString(res.getRowId()) + "], Values [" + res.getCells() + "]");
    }

    // Step-9 - Delete row
    // Delete row at specified version/timestamp
    System.out.println("STEP:9 Deleting a row [078] at version timestamp [300]");
    Delete delete = new Delete(row78);
    delete.setTimestamp(300L);
    orderedTable.table.delete(delete);

    // Display table state
    orderedTable.readTable(orderedTable.table, null, null);


    // Step-10 - Reset values of given columns to null
    // Note: This will create new version of the row with AGE/DEPT values as null.
    System.out.println("STEP:10 Updating values of columns [AGE, DEPT] of row [078] to NULL");
    Delete colDelete = new Delete(row78);
    colDelete.setTimestamp(600);
    colDelete.addColumn(Bytes.toBytes("AGE"));
    colDelete.addColumn(Bytes.toBytes("DEPT"));
    orderedTable.table.delete(colDelete);

    // Display table state
    orderedTable.readTable(orderedTable.table, "078", null);

    // Step-11: Delete a row. All versions of a row will be deleted.
    System.out.println("STEP:11 Deleting all versions of row [078]");
    delete = new Delete(row78);
    orderedTable.table.delete(delete);

    // Display table state
    orderedTable.readTable(orderedTable.table, null, null);


    // Step-12: Delete a row if specific column matches the specified value
    System.out.println("STEP:12 Deleting row [023] if column [0] matches valXXX");
    byte[] row23 = "023".getBytes();
    Delete del23 = new Delete(row23);
    try {
      orderedTable.table.checkAndDelete(row23, Bytes.toBytes(orderedTable.columnNames.get(0)),
          Bytes.toBytes("valXXX"), del23);
    } catch (MCacheDeleteFailedException | RowKeyDoesNotExistException e) {
      System.out.println("No matching row to delete for a specified column ["
          + orderedTable.columnNames.get(0) + "] w/ Value [valXXX]");
    } catch (Exception ioe) {
      ioe.printStackTrace();
    }

    // Display table state
    orderedTable.readTable(orderedTable.table, null, null);

    // Step-13: Delete a table
    orderedTable.admin.deleteMTable(orderedTable.tableName);
    System.out.println("Table is deleted successfully!");

    // Step-14: close the Monarch connection
    orderedTable.aClient.close();
    System.out.println("Connection to monarch DS closed successfully!");
  }

  /*
   * If rowID is null then scan latest versions of all the rows (ignore version specified) If
   * version is null, get the row w/ rowID w/ latest version timestamp if rowID and version both are
   * not null, get the specific row w/ rowID/version timestamp
   */
  public void readTable(MTable table, String rowID, Long version) {
    /*
     * If rowID == null Scan the entire table w/ latest version, ignore the version timestamp
     */
    System.out.println("*******************************************************************");
    if (rowID == null) {
      System.out.println("Latest version of all rows in MTable [" + table.getName() + "]");
      System.out.println("*******************************************************************");
      Scan scanner = new Scan();
      scanner.setMaxVersions();
      scanner.setBatchSize(10);
      Scanner results = table.getScanner(scanner);
      Iterator itr = results.iterator();
      while (itr.hasNext()) {
        displayRow((Row) itr.next());
      }
    } else {

      Get row = new Get(rowID.getBytes());
      if (version != null) {
        row.setTimeStamp(version);
        System.out.println(
            "Version [" + version + "] of row [" + rowID + "] in MTable [" + table.getName() + "]");
      } else {
        System.out
            .println("Latest version of row [" + rowID + "] in MTable [" + table.getName() + "]");
      }
      System.out.println("*******************************************************************");
      displayRow(table.get(row));
    }
    System.out.println("*******************************************************************\n");
  }

  public void displayRow(Row res) {
    System.out.print("RowKey [" + Bytes.toString(res.getRowId()) + "], Version ["
        + res.getRowTimeStamp() + "], Values {");
    for (Cell cell : res.getCells()) {
      System.out.print("[" + Bytes.toString(cell.getColumnName()) + ":"
          + Bytes.toString((byte[]) cell.getColumnValue()) + "]");
    }
    System.out.println("}");
  }
}
