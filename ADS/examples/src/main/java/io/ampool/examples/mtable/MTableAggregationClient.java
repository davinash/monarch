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

import java.util.Properties;

import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.client.coprocessor.AggregationClient;

/**
 * Sample MTable Quickstart example. It performs following table operations. create a connection
 * with monarch distributed system (DS). Create Table named "EmployeeTable" with columns ["NAME",
 * "ID", "AGE", "SALARY", "DEPT", "DOJ"] Insert rows with different versions. Run aggragtion client
 * to get row count. Delete entire row. Delete table.
 *
 * @since 0.2.0.0
 */
public class MTableAggregationClient {

  public static void main(String args[]) {
    System.out.println("MTable Quickstart example!");

    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }

    // Step:1 create a connection with monarch distributed system (DS).
    Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, "/tmp/MTableClient.log");
    AmpoolClient ampoolClient = new AmpoolClient(locator_host, locator_port, props);
    System.out.println("Connection to monarch distributed system is successfully done!");

    // Step:2 create a Table.
    String[] columnNames = {"NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ"};
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setMaxVersions(5);
    tableDescriptor.setSchema(new Schema(columnNames));

    Admin admin = ampoolClient.getAdmin();
    String tableName = "EmployeeTable";

    MTable table = admin.createMTable(tableName, tableDescriptor);
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
    int NUM_OF_ROWS = 5;
    int TABLE_MAX_VERSIONS = 5;
    int NUM_OF_COLUMNS = 6;
    for (int rowIndex = 1; rowIndex <= NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes("rowKey" + rowIndex));
      for (int versionIndex = 0; versionIndex < TABLE_MAX_VERSIONS; versionIndex++) {
        long timestamp = (versionIndex + 1) * 100;
        record.setTimeStamp(timestamp);
        for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
          record.addColumn(Bytes.toBytes(columnNames[colIndex]),
              Bytes.toBytes("val" + versionIndex + "" + colIndex));
        }
        table.put(record);
      }
      System.out.println("Row with rowkey \"" + ("rowKey" + rowIndex) + "\" inserted");
    }


    // Step 4: Get row count
    System.out.println("\nRunning aggration client");
    AggregationClient aggregator = new AggregationClient();
    long rowCount = aggregator.rowCount(table, new Scan());

    System.out.println("Total No of rows in table " + tableName + " are " + rowCount);

    System.out.println("\nRunning aggration client with specified range");
    aggregator = new AggregationClient();
    rowCount = aggregator.rowCount(table,
        new Scan(Bytes.toBytes("rowKey" + 3), Bytes.toBytes("rowKey" + 5)));
    System.out.println("No of rows in table " + tableName
        + " between rowKey range \"rowKey3\" and \"rowKey5\" are " + rowCount);

    // Step-5: Delete a table
    admin.deleteMTable(tableName);
    System.out.println("\nTable is deleted successfully!");

    // Step-6: close the Monarch connection
    ampoolClient.close();
    System.out.println("Connection to monarch DS closed successfully!");
  }
}
