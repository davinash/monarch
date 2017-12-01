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

import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.filter.internal.TimestampFilter;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Sample MTable quick-start example. This example demonstrate the use of APIs to scan
 * multi-versioned table. This example also showcases how to use TimestampFilter to filter based on
 * timestamp (version identifier) in ORDERED_VERSIONED tables
 * 
 * @since 1.4.2
 */
public class MTableMultiVersionTableExample {

  public static void main(String args[]) {
    System.out.println("MTable MultiVersion table example!");

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
    final int TABLE_MAX_VERSIONS = 5;

    Schema schema =
        new Schema.Builder().column("ID", BasicTypes.INT).column("SALARY", BasicTypes.INT).build();

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setSchema(schema);
    tableDescriptor.setMaxVersions(TABLE_MAX_VERSIONS);

    Admin admin = aClient.getAdmin();
    String tableName = "EmployeeTable";

    MTable table = admin.createMTable(tableName, tableDescriptor);
    System.out.println("Table [" + tableName + "] is created successfully!");

    // Step-3: Insert rows into table.
    final int NUM_RECS = 10;
    final String ROW_KEY_PREFIX = "Key_";
    int SALARY_INCREMENTER = 100000;
    int baseSalary = 5000000;

    for (int i = 1; i <= NUM_RECS; i++) {
      Put record = new Put(Bytes.toBytes(ROW_KEY_PREFIX + i));
      for (int j = 1; j <= TABLE_MAX_VERSIONS; j++) {
        record.setTimeStamp(j);
        record.addColumn("ID", i * 1000);
        record.addColumn("SALARY", (baseSalary + SALARY_INCREMENTER * j));
        table.put(record);
      }
    }

    // Run scan on table
    System.out
        .println("--------------------------SIMPLE SCAN---------------------------------------");
    Scan scan = new Scan();
    Scanner scanner = table.getScanner(scan);
    Iterator itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      rowFormatter(res.getRowId(), res.getRowTimeStamp(), res.getCells());
    }
    System.out
        .println("----------------------------------------------------------------------------");

    System.out
        .println("---------------ALL VERSIONS SCAN [ RECENT -> OLDER ]------------------------");
    scan = new Scan();
    scan.setMaxVersions(TABLE_MAX_VERSIONS, false);
    scanner = table.getScanner(scan);
    itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        rowFormatter(res.getRowId(), k, v.getCells());
      });
      System.out.println();
    }
    System.out
        .println("----------------------------------------------------------------------------");

    System.out
        .println("----------------ALL VERSIONS SCAN [ OLDER -> RECENT ]-----------------------");
    scan = new Scan();
    scan.setMaxVersions(TABLE_MAX_VERSIONS, true);
    scanner = table.getScanner(scan);
    itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        rowFormatter(res.getRowId(), k, v.getCells());
      });
      System.out.println();
    }
    System.out
        .println("----------------------------------------------------------------------------");


    System.out.println("----------------TIMESTAMP FILTER [ TS > 3 ]-----------------------");
    scan = new Scan();
    scan.setMaxVersions(TABLE_MAX_VERSIONS, false);
    // apply above filter on each version of row and get qualified versions only
    scan.setFilterOnLatestVersionOnly(false);
    scan.setFilter(new TimestampFilter(CompareOp.GREATER, 3l));
    scanner = table.getScanner(scan);
    itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        rowFormatter(res.getRowId(), k, v.getCells());
      });
      System.out.println();
    }
    System.out
        .println("----------------------------------------------------------------------------");


    System.out.println(
        "----------------TIMESTAMP FILTER [ TS > 3 ] with All versions-----------------------");
    scan = new Scan();
    scan.setMaxVersions(TABLE_MAX_VERSIONS, false);
    // apply above filter on top version of row only and if qualified get complete row
    scan.setFilterOnLatestVersionOnly(true);
    scan.setFilter(new TimestampFilter(CompareOp.GREATER, 3l));
    scanner = table.getScanner(scan);
    itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        rowFormatter(res.getRowId(), k, v.getCells());
      });
      System.out.println();
    }
    System.out
        .println("----------------------------------------------------------------------------");


    System.out
        .println("------------SingleColumnValue FILTER [ From all versions only ]-------------");
    scan = new Scan();
    scan.setMaxVersions(TABLE_MAX_VERSIONS, false);
    scan.setFilter(new SingleColumnValueFilter("SALARY", CompareOp.GREATER, 5300000));
    scanner = table.getScanner(scan);
    itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        rowFormatter(res.getRowId(), k, v.getCells());
      });
      System.out.println();
    }
    System.out
        .println("----------------------------------------------------------------------------");

    System.out
        .println("------------SingleColumnValue FILTER [ On latest version only ]-------------");
    scan = new Scan();
    scan.setFilter(new SingleColumnValueFilter("SALARY", CompareOp.GREATER, 5300000));
    scan.setFilterOnLatestVersionOnly(true);
    scanner = table.getScanner(scan);
    itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        rowFormatter(res.getRowId(), k, v.getCells());
      });
      System.out.println();
    }
    System.out
        .println("----------------------------------------------------------------------------");

    // delete the table
    admin.deleteMTable(tableName);

  }

  private static void rowFormatter(byte[] key, Long timestamp, List<Cell> cells) {
    System.out.println(
        "KEY: " + Bytes.toString(key) + " \t " + " VERSION_ID: " + timestamp + " \t " + " ID: "
            + cells.get(0).getColumnValue() + " \t " + " SALARY: " + cells.get(1).getColumnValue());
  }
}
