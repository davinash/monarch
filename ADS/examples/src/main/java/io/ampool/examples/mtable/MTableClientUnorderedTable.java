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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Strings;
import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
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
 * Sample MTable Quickstart example. It performs following table operations. create a connection
 * with monarch distributed system (DS). Create Table named "EmployeeTable" with columns ["NAME",
 * "ID", "AGE", "SALARY", "DEPT", "DOJ"] Insert row with different versions, update selected columns
 * value. Retrieve row value given a specified timestamp, no timestamp (latest version), given
 * selected columns. Delete row at specified timestamp, Delete a values of given columns from a row,
 * delete entire row.
 *
 * @since 0.2.0.0
 */
public class MTableClientUnorderedTable {

  public Admin admin = null;
  public String tableName = "UEmployeeTable";
  public MTable table = null;
  private AmpoolClient ampoolClient = null;
  public int NUM_OF_COLUMNS = 6;
  public List<String> columnNames = Arrays.asList("NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ");


  public MTableClientUnorderedTable(String locatorHost, int locatorPort) {
    // Step:1 create a connection with monarch distributed system (DS).
    Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG,
        "/tmp/MTableClientUnorderedTable.log");
    ampoolClient = new AmpoolClient(locatorHost, locatorPort, props);
    System.out.println("Connection to monarch distributed system is successfully done!");

    // Step:2 create a Table.
    // Type: ORDERED_VERSIONED i.e. rows are ordered by rowkey column and table also supports
    // versioning of the row when any of columns are updated with new values
    // Replicas: Two redundant copies of each partition/split in memory
    // Splits: Also referred as partitions or buckets.
    // Hash partitioning is supported based on the RowKey and number of splits
    // (No support for Range Partitioning yet as of 0.3.3.x)
    // Persistence: Along with memory, Data is also persisted to local disk for recovery
    // Versions: (Not support for Versioning yet as of 0.3.3.x)
    int NUM_REPLICAS = 2;
    int NUM_SPLITS = 10;
    MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    tableDescriptor.setRedundantCopies(NUM_REPLICAS).setTotalNumOfSplits(NUM_SPLITS)
        .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS)
        .setSchema(new Schema(columnNames.toArray(new String[0])));

    admin = ampoolClient.getAdmin();
    if (admin.existsMTable(tableName)) {
      admin.deleteMTable(tableName);
    }
    table = admin.createMTable(tableName, tableDescriptor);
    System.out.println("Table [" + tableName + "] is created successfully!\n");
  }

  public static void main(String args[]) {
    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }

    System.out.println("Unordered Type MTable Quickstart example!\n");

    // Step-1, Step-2: Connect to Ampool and create table
    MTableClientUnorderedTable unorderedTable =
        new MTableClientUnorderedTable(locator_host, locator_port);

    // Step-3: Insert single row into table with multiple versions.
    /*
     * For a key "rowKey", following values will be populated at different timestamp 100
     * 
     * column-1 column-2 column-3 column-4 column-5 column-6
     * ------------------------------------------------------------------------------- val0 val1
     * val2 val3 val4 val5
     * 
     * NOTE: setTimestamp i.e versioning is NOT supported for unordered table
     */
    System.out.println("STEP:3 Inserting Row ID [row001]");
    Put record = new Put(Bytes.toBytes("row001"));
    for (int colIndex = 0; colIndex < unorderedTable.NUM_OF_COLUMNS; colIndex++) {
      record.addColumn(Bytes.toBytes(unorderedTable.columnNames.get(colIndex)),
          Bytes.toBytes("val" + colIndex));
    }
    unorderedTable.table.put(record);
    readTable(unorderedTable.table, null, null);

    // Step-4: Insert batch of rows using singe table put
    // NOTE: Versioning is NOT supported for Unordered Table
    System.out.println("STEP:4 Inserting batch or rows");
    int batchSize = 10;
    List<Put> rows = new ArrayList<Put>(2 * batchSize);
    for (int i = 0; i < 2 * batchSize; i++) {
      String iStr = Strings.padStart(String.valueOf(i + 2), 3, '0');
      String rowID = "row" + iStr;
      Put row = new Put(rowID.getBytes());
      for (int colIndex = 0; colIndex < unorderedTable.NUM_OF_COLUMNS; colIndex++) {
        row.addColumn(Bytes.toBytes(unorderedTable.columnNames.get(colIndex)),
            Bytes.toBytes("val-" + i + 2 + "-" + colIndex));
      }
      rows.add(row);
    }
    unorderedTable.table.put(rows);
    readTable(unorderedTable.table, null, null);

    // Step-5: - Retrieve row value.
    System.out.println("STEP:5 Retrieving row [row010]");
    Get get = new Get(Bytes.toBytes("row010"));
    Row result = unorderedTable.table.get(get);
    displayRow(result);

    // Step-6: Retrieve a selected columns of a row
    // NOTE: Versioning is NOT supported for Unordered Table
    System.out.println("STEP:6 Retrieve selectec [columns NAME, ID, SALARY] of row [row011]");
    Get getRecord = new Get(Bytes.toBytes("row011"));
    getRecord.addColumn(Bytes.toBytes("NAME"));
    getRecord.addColumn(Bytes.toBytes("ID"));
    getRecord.addColumn(Bytes.toBytes("SALARY"));
    result = unorderedTable.table.get(getRecord);
    displayRow(result);

    /*
     * Step-7: Scan the table. If bucket ids are not specified entire table will be scanned else
     * only specified buckets are scanned. Note: Unordered table only support scan at table or
     * bucket level and NOT for arbitrary key range
     */
    System.out.println("STEP:7 Scan rows from selected list of buckets [0th, 3rd, 5th]");
    Scan scanner = new Scan();
    scanner.setBatchSize(100);
    Set<Integer> buckets = new HashSet<Integer>(Arrays.asList(0, 3, 5));
    scanner.setBucketIds(buckets);
    Scanner scanner1 = unorderedTable.table.getScanner(scanner);
    Iterator itr = scanner1.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      displayRow(res);
    }

    // Step-8 - Delete row
    // Delete row w/ specified rowkey
    System.out.println("Deleting a row (row004)");
    Delete delete = new Delete(Bytes.toBytes("row004"));
    unorderedTable.table.delete(delete);
    readTable(unorderedTable.table, null, null);


    // Step-9: Reset values of given columns from a row to null
    System.out.println("STEP:9 Updating values of columns [AGE, DEPT] of row [row005] to NULL");
    Delete colDelete = new Delete(Bytes.toBytes("rowKey"));
    colDelete.addColumn(Bytes.toBytes("AGE"));
    colDelete.addColumn(Bytes.toBytes("DEPT"));
    unorderedTable.table.delete(delete);
    readTable(unorderedTable.table, null, null);

    // Step-5: Delete a table
    unorderedTable.admin.deleteMTable(unorderedTable.tableName);
    System.out.println("Table is deleted successfully!");

    // Step-6: close the Monarch connection
    unorderedTable.ampoolClient.close();
    System.out.println("Connection to monarch DS closed successfully!");
  }

  /*
   * If rowID is null then scan latest versions of all the rows (ignore version specified) If
   * version is null, get the row w/ rowID w/ latest version timestamp if rowID and version both are
   * not null, get the specific row w/ rowID/version timestamp
   */
  public static void readTable(MTable table, String rowID, Long version) {
    /*
     * If rowID == null Scan the entire table w/ latest version, ignore the version timestamp
     */
    System.out.println("*******************************************************************");
    if (rowID == null) {
      System.out.println("Latest version of all rows in MTable [" + table.getName() + "]");
      System.out.println("*******************************************************************");
      Scan scanner = new Scan();
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

  public static void displayRow(Row res) {
    System.out.print("RowKey [" + Bytes.toString(res.getRowId()) + "], Version ["
        + res.getRowTimeStamp() + "], Values {");
    for (Cell cell : res.getCells()) {
      System.out.print("[" + Bytes.toString(cell.getColumnName()) + ":"
          + Bytes.toString((byte[]) cell.getColumnValue()) + "]");
    }
    System.out.println("}");
  }
}
