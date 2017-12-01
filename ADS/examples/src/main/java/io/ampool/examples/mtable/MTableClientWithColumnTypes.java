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

import java.sql.Date;
import java.util.List;
import java.util.Properties;

import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.interfaces.DataType;

/**
 * The sample example for using column types with MTable.
 * <p>
 * It has two examples: Example-1: It creates a table with following columns and respective types:
 * NAME, ID, AGE, SALARY, DEPT, DOJ STRING, LONG, INT, DOUBLE, STRING, DATE
 * <p>
 * The example demonstrates: insert sample data into MTable and then retrieve it.
 * <p>
 * Example-2: It creates a table with single column having complex type STRUCT and contains the
 * above mentioned columns as part of it in same order.
 * struct<NAME:STRING,ID:LONG,AGE:INT,SALARY:DOUBLE,DEPT:STRING,DOJ:DATE>
 * <p>
 * The data is put into table and then retrieved (get) as a single structure which is an array of
 * objects. An array of objects of different types (like STRING/LONG/INT etc.) are put and retrieved
 * as a single column.
 * <p>
 * <p>
 * Since version: 0.3.2.0
 */
public class MTableClientWithColumnTypes {

  private static final String[] columnNames =
      new String[] {"NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ"};

  private static final DataType[] columnTypes = new DataType[] {BasicTypes.STRING, BasicTypes.LONG,
      BasicTypes.INT, BasicTypes.DOUBLE, BasicTypes.STRING, BasicTypes.DATE};

  /**
   * sample data
   **/
  private static final Object[][] sampleData =
      new Object[][] {{"Name_1", 1L, 11, 11.111, "ABC", Date.valueOf("2016-01-01")},
          {"Name_2", 2L, 22, 22.222, "ABC", Date.valueOf("2016-02-02")},
          {"Name_3", 3L, 33, 33.333, "PQR", Date.valueOf("2016-03-03")},
          {"Name_4", 4L, 44, 44.444, "PQR", Date.valueOf("2016-04-04")},
          {"Name_5", 5L, 55, 55.555, "XYZ", Date.valueOf("2016-05-05")},};

  public static void main(String[] args) {
    final AmpoolClient client = getClient(args);

    /** simple example **/
    exampleUsingColumnTypes(client);

    /** example for STRUCT **/
    exampleUsingStruct(client);

  }

  /**
   * A simple example using multiple columns with types (basic).
   *
   * @param client the client cache
   */
  private static void exampleUsingColumnTypes(final AmpoolClient client) {
    final String tableName = "EmployeeTable";
    MTableDescriptor td = new MTableDescriptor();
    td.setSchema(new Schema(columnNames, columnTypes));
    final MTable mTable = client.getAdmin().createMTable(tableName, td);

    /** putting the data into table **/
    Put put;
    for (int i = 0; i < sampleData.length; i++) {
      put = new Put("rowKey_" + i);
      for (int j = 0; j < sampleData[i].length; j++) {
        put.addColumn(columnNames[j], sampleData[i][j]);
      }
      mTable.put(put);
    }

    /** retrieving all columns **/
    /**
     * Sample output: Retrieving all columns: | Name_1 | 1 | 11 | 11.111 | ABC | 2016-01-01 | |
     * Name_2 | 2 | 22 | 22.222 | ABC | 2016-02-02 | | Name_3 | 3 | 33 | 33.333 | PQR | 2016-03-03 |
     * | Name_4 | 4 | 44 | 44.444 | PQR | 2016-04-04 | | Name_5 | 5 | 55 | 55.555 | XYZ | 2016-05-05
     * |
     */
    Get get;
    Row row;
    List<Cell> cells;
    System.out.println("Retrieving all columns:");
    for (int i = 0; i < sampleData.length; i++) {
      get = new Get("rowKey_" + i);
      row = mTable.get(get);
      cells = row.getCells();
      for (int j = 0; j < cells.size(); j++) {
        System.out.printf("| %6s  ", cells.get(j).getColumnValue());
      }
      System.out.println("|");
    }
    System.out.println();

    /** retrieving the selected columns **/
    /**
     * Sample output: Retrieving selected columns: | Name_1 | 11.111 | | Name_2 | 22.222 | | Name_3
     * | 33.333 | | Name_4 | 44.444 | | Name_5 | 55.555 |
     */
    System.out.println("Retrieving selected columns:");
    for (int i = 0; i < sampleData.length; i++) {
      get = new Get("rowKey_" + i);
      /** additionally you can provided the selected columns, if you want **/
      get.addColumn(columnNames[3].getBytes());
      get.addColumn(columnNames[0].getBytes());
      row = mTable.get(get);
      cells = row.getCells();
      for (int j = 0; j < cells.size(); j++) {
        System.out.printf("| %6s  ", cells.get(j).getColumnValue());
      }
      System.out.println("|");
      System.out.println();
    }
    client.getAdmin().deleteMTable(tableName);
  }

  /**
   * An example demonstrating usage of complex type struct.
   *
   * @param client the client cache
   */
  private static void exampleUsingStruct(final AmpoolClient client) {
    final String table = "struct_table";
    final String column = "struct_column";
    final String structTypeStr =
        "struct<NAME:STRING,ID:LONG,AGE:INT,SALARY:DOUBLE,DEPT:STRING,DOJ:DATE>";

    /** create descriptor and table **/
    MTableDescriptor td = new MTableDescriptor();
    // td.addColumn(column, stringType); // deprecated
    td.setSchema(new Schema.Builder().column(column, structTypeStr).build());
    final MTable sTable = client.getAdmin().createMTable(table, td);

    /** put the data into table **/
    Put put;
    for (int i = 0; i < sampleData.length; i++) {
      put = new Put("rowKey_" + i);
      put.addColumn(column, sampleData[i]);
      sTable.put(put);
    }

    /** get the data from table **/
    /**
     * Sample output: Retrieving struct_column: | Name_1 | 1 | 11 | 11.111 | ABC | 2016-01-01 | |
     * Name_2 | 2 | 22 | 22.222 | ABC | 2016-02-02 | | Name_3 | 3 | 33 | 33.333 | PQR | 2016-03-03 |
     * | Name_4 | 4 | 44 | 44.444 | PQR | 2016-04-04 | | Name_5 | 5 | 55 | 55.555 | XYZ | 2016-05-05
     * |
     */
    System.out.println("Retrieving struct_column:");
    Object[] values;
    Get get;
    for (int i = 0; i < sampleData.length; i++) {
      get = new Get("rowKey_" + i);
      values = (Object[]) sTable.get(get).getCells().get(0).getColumnValue();
      for (int j = 0; j < values.length; j++) {
        System.out.printf("| %6s  ", values[j]);
      }
      System.out.println("|");
      System.out.println();
    }
    client.getAdmin().deleteMTable(table);
  }

  /**
   * Create and return the client cache.
   *
   * @return the client cache
   *
   */
  private static AmpoolClient getClient(String[] args) {
    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }

    Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG,
        "/tmp/MTableClientWithColumnTypes.log");
    return new AmpoolClient(locator_host, locator_port, props);
  }
}
