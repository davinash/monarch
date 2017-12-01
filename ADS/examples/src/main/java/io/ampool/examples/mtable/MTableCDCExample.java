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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.CDCConfig;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.types.BasicTypes;

/**
 * MTableCDCExample This example shows MTable CDC listener. It performs following table operations.
 * 1. Create a table EmployeeTableCDC with following schema -------------------- | NAME | STRING | |
 * ID | INT | | AGE | INT | | SALARY | DOUBLE | | DEPT | STRING | --------------------- 2. Table is
 * attached with CDC Listener using API addCDCStream 3. Perform 50 INSERT Operations 4. Perform 20
 * UPDATE Operations 5. Perform 15 DELETE Operations
 *
 * CDC Listener will create a file in /tmp directory ( /tmp/EmployeeData.csv ) This file is CSV file
 * with following schema
 *
 * -------------------------------------------------------------- | EVENTID | NAME | ID | AGE |
 * SALARY | DEPT | OPERATION_TYPE | --------------------------------------------------------------
 *
 */

public class MTableCDCExample {
  public static void main(String args[]) throws InterruptedException {
    System.out.println("MTableCDCExample Quickstart example!");

    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }

    // Step:1 create a connection with monarch distributed system (DS).
    // create a configuration and connect to monarch locator.
    Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, "/tmp/MTableClient.log");
    AmpoolClient aClient = new AmpoolClient(locator_host, locator_port, props);
    System.out.println("Connection to monarch distributed system is successfully done!");

    // Step:2 create a Table.
    List<String> columnNames = Arrays.asList("NAME", "ID", "AGE", "SALARY", "DEPT");
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setMaxVersions(1);
    Schema schema = new Schema.Builder().column(columnNames.get(0), BasicTypes.STRING)
        .column(columnNames.get(1), BasicTypes.INT).column(columnNames.get(2), BasicTypes.INT)
        .column(columnNames.get(3), BasicTypes.DOUBLE).column(columnNames.get(4), BasicTypes.STRING)
        .build();
    tableDescriptor.setSchema(schema);

    CDCConfig config = tableDescriptor.createCDCConfig();
    config.setPersistent(true);

    tableDescriptor.addCDCStream("EmployeeTableCDC", "io.ampool.examples.mtable.MTableCDCListener",
        config);

    Admin admin = aClient.getAdmin();
    String tableName = "EmployeeTableCDC";

    MTable table = admin.createMTable(tableName, tableDescriptor);
    System.out.println("Table [EmployeeTableCDC] is created successfully!");

    for (int i = 0; i < 50; i++) {
      Put employeeRecord = new Put(Bytes.toBytes("Key-" + i));
      employeeRecord.addColumn(columnNames.get(0), "Name-" + i);
      employeeRecord.addColumn(columnNames.get(1), i);
      employeeRecord.addColumn(columnNames.get(2), (i + 1) * 10);
      employeeRecord.addColumn(columnNames.get(3), (i + 2) * 100.0);
      employeeRecord.addColumn(columnNames.get(4), "Dept-" + (i / 10));

      table.put(employeeRecord);
    }

    for (int i = 10; i < 30; i++) {
      Put employeeRecord = new Put(Bytes.toBytes("Key-" + i));
      employeeRecord.addColumn(columnNames.get(0), "Name-" + i);
      employeeRecord.addColumn(columnNames.get(1), i);
      employeeRecord.addColumn(columnNames.get(2), (i + 1) * 10);
      employeeRecord.addColumn(columnNames.get(3), (i + 2) * 100.0);
      employeeRecord.addColumn(columnNames.get(4), "Dept-" + (i / 10));

      table.put(employeeRecord);
    }

    for (int i = 15; i < 40; i++) {
      Delete employeeRecord = new Delete(Bytes.toBytes("Key-" + i));
      table.delete(employeeRecord);
    }

    TimeUnit.SECONDS.sleep(20);

    // Step-5: Delete a table
    admin.deleteMTable(tableName);
    System.out.println("Table is deleted successfully!");

    // Step-6: close the Monarch connection
    aClient.close();
    System.out.println("Connection to monarch DS closed successfully!");
  }
}
