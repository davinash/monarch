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
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * This example demonstrates the delete with filter functionality for the mtables. API
 * deleteWithFilters for mtable is used for deleting the records with matching filter.
 */
public class DeleteWithFiltersExample {
  private static final String[] COLUMN_NAMES =
      new String[] {"NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ", "DOE"};
  private static final int TABLE_MAX_VERSIONS = 5;
  private static final int NUM_OF_ROWS = 1000;
  private static final String tableName = "EmployeeTable";

  public static void main(String args[]) {
    System.out.println("Running DeleteWithFiltersExample !");
    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }
    final AmpoolClient ampoolClient = connect(locator_host, locator_port);
    createTable(ampoolClient);
    ingestRecords(ampoolClient);
    System.out.print("All records : ");
    scanRecords(ampoolClient, null);
    System.out.println();

    // create filters with ID < 50 and DOE is null
    Filter idColumnFilter = new SingleColumnValueFilter("ID", CompareOp.LESS, 50);
    Filter doeColumnFilter = new SingleColumnValueFilter("DOE", CompareOp.EQUAL, null);
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
        .addFilter(idColumnFilter).addFilter(doeColumnFilter);

    System.out.print("Number of Records matching filter: ");
    List<byte[]> keys = scanRecords(ampoolClient, filterList);
    System.out.println();

    System.out.println("Deleting the records with matched filter.");
    deleteWithFilters(ampoolClient, keys, filterList);

    System.out.print("Number of Records after delete: ");
    scanRecords(ampoolClient, null);
    System.out.println();

    deleteTable(ampoolClient);
    closeClient(ampoolClient);
  }

  private static AmpoolClient connect(String locator_host, int locator_port) {
    final Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG,
        "/tmp/DeleteWithFiltersExample.log");
    final AmpoolClient aClient = new AmpoolClient(locator_host, locator_port, props);
    System.out.println("AmpoolClient connected to the cluster successfully!");
    return aClient;
  }

  private static void createTable(AmpoolClient ampoolClient) {
    /* build the schema from available columns */
    Schema schema = new Schema.Builder().column(COLUMN_NAMES[0], BasicTypes.STRING)
        .column(COLUMN_NAMES[1], BasicTypes.INT).column(COLUMN_NAMES[2], BasicTypes.INT)
        .column(COLUMN_NAMES[3], BasicTypes.LONG).column(COLUMN_NAMES[4], BasicTypes.STRING)
        .column(COLUMN_NAMES[5], BasicTypes.STRING).column(COLUMN_NAMES[6], BasicTypes.STRING)
        .build();
    MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    tableDescriptor.setSchema(schema);
    tableDescriptor.setMaxVersions(TABLE_MAX_VERSIONS);
    ampoolClient.getAdmin().createMTable(tableName, tableDescriptor);
    System.out.println("Table [EmployeeTable] is created successfully!");
  }

  private static void ingestRecords(final AmpoolClient ampoolClient) {
    MTable table = ampoolClient.getMTable(tableName);
    for (int i = 0; i < NUM_OF_ROWS; i++) {
      for (int j = 1; j <= TABLE_MAX_VERSIONS; j++) {
        Put record = new Put(Bytes.toBytes("rowKey" + i));
        record.addColumn(COLUMN_NAMES[0], "ABC" + i);
        record.addColumn(COLUMN_NAMES[1], i);
        record.addColumn(COLUMN_NAMES[2], 35);
        record.addColumn(COLUMN_NAMES[3], (long) (10000 + j));
        record.addColumn(COLUMN_NAMES[4], "DEPT");
        record.addColumn(COLUMN_NAMES[5], "01/01/2017");
        if (j == TABLE_MAX_VERSIONS) {
          record.addColumn(COLUMN_NAMES[6], "31/12/2017");
        } else {
          record.addColumn(COLUMN_NAMES[6], null);
        }
        table.put(record);
      }
    }
  }

  private static List<byte[]> scanRecords(final AmpoolClient ampoolClient,
      final FilterList filterList) {
    // Run scan on table which retrieves all the rows
    List<byte[]> scannedKeys = new ArrayList<>();
    Scan scan = new Scan();
    scan.setMaxVersions();
    scan.setFilterOnLatestVersionOnly(false);
    if (filterList != null) {
      scan.setFilter(filterList);
    }
    Scanner scanner = ampoolClient.getMTable(tableName).getScanner(scan);
    Iterator itr = scanner.iterator();
    int numRecords = 0;
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      scannedKeys.add(res.getRowId());
      Iterator<Map.Entry<Long, SingleVersionRow>> iterator =
          res.getAllVersions().entrySet().iterator();
      while (iterator.hasNext()) {
        iterator.next();
        numRecords++;
      }
    }
    scanner.close();
    System.out.print(numRecords);
    return scannedKeys;
  }

  private static void deleteWithFilters(final AmpoolClient ampoolClient, final List<byte[]> keys,
      final FilterList filterList) {
    MTable mTable = ampoolClient.getMTable(tableName);
    Map<Delete, Filter> deleteFilterMap = new HashMap<>();
    Iterator<byte[]> keysIterator = keys.iterator();

    while (keysIterator.hasNext()) {
      deleteFilterMap.put(new Delete(keysIterator.next()), filterList);
    }
    mTable.delete(deleteFilterMap);
  }


  private static void deleteTable(final AmpoolClient ampoolClient) {
    ampoolClient.getAdmin().deleteMTable(tableName);
    System.out.println("Table is deleted successfully!");
  }

  private static void closeClient(final AmpoolClient ampoolClient) {
    ampoolClient.close();
    System.out.println("Connection to monarch DS closed successfully!");
  }
}
