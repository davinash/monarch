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

package io.ampool.monarch.kafka.connect;

import io.ampool.monarch.common.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.MConfiguration;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableColumnType;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MCacheInternalErrorException;
import io.ampool.monarch.table.exceptions.MTableNotExistsException;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.types.BasicTypes;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MonarchUtils {
  public static final String REGION = "monarch.region.name";
  public static final String LOCATOR_HOST = "monarch.locator.host";
  public static final String LOCATOR_HOST_DEFAULT = "localhost";
  public static final String LOCATOR_PORT = "monarch.locator.port";
  public static final int LOCATOR_PORT_DEFAULT = 10334;
  public static final String SPLIT_SIZE_KEY = "monarch.split.size";
  public static final String MONARCH_BATCH_SIZE = "monarch.batch.size";
  public static final String BLOCK_SIZE = "monarch.block.size";
  public static final int MONARCH_BATCH_SIZE_DEFAULT = 10000;

  public static final String MONARCH_TABLE_TYPE = "monarch.table.type";
  public static final String DEFAULT_TABLE_TYPE = "immutable";

  public static final String MONARCH_PARTITIONING_COLUMN = "monarch.partitioning.column";

  public static final String REDUNDANCY = "monarch.redundancy";
  public static final int REDUNDANCY_DEFAULT = 0;

  public static final String BUCKETS = "monarch.buckets";
  public static final int BUCKETS_DEFAULT = 113;

  // Persistence properties for SOUTH
  public static final String PERSIST = "monarch.enable.persistence";

  public static final String KEY_BLOCKS_SFX = "blocks";
  /** to convert nano-seconds to milli-seconds **/
  public static final int NS_TO_SEC = 1000000000;

  public static final String META_TABLE_SFX = "_meta";
  public static final List<String> META_TABLE_COL_LIST = Collections.singletonList("Data");
  public static final String META_TABLE_COL = new String("Data");

  private static MClientCache connection = null;
  private static final Logger logger = LoggerFactory.getLogger(MonarchUtils.class);

  public static MClientCache getConnection(String tableName, String locatorPortAsString) {

    final Map<String, String> parameters = new HashMap<>();
    parameters.put(MonarchUtils.LOCATOR_PORT, locatorPortAsString);
    parameters.put(MonarchUtils.REGION, tableName);

    String locatorHost = parameters.get(MonarchUtils.LOCATOR_HOST);
    if (locatorHost == null) {
      locatorHost = MonarchUtils.LOCATOR_HOST_DEFAULT;
    }

    int locatorPort;
    String port = parameters.get(MonarchUtils.LOCATOR_PORT);
    if (port == null) {
      locatorPort  = MonarchUtils.LOCATOR_PORT_DEFAULT;
    }
    else {
      locatorPort = Integer.parseInt(port);
    }

    MConfiguration mConf = MConfiguration.create();
    mConf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, locatorHost);
    mConf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, locatorPort);
    connection = MClientCacheFactory.getOrCreate(mConf);
    return connection;
  }

  public static void destroyTable(String tableName,
      String locatorPortAsString) throws Exception {

    final Map<String, String> parameters = new HashMap<>();
    parameters.put(MonarchUtils.LOCATOR_PORT, locatorPortAsString);
    parameters.put(MonarchUtils.REGION, tableName);

    connection = getConnection(tableName, locatorPortAsString);
    Admin admin = connection.getAdmin();
    try {
      if(admin.mTableExists(tableName)) {
        admin.deleteMTable(tableName);
      }
    } catch (MTableNotExistsException ex) {
      logger.error("Monarch table not present on server. ");
    } catch (MCacheInternalErrorException ex) {
      logger.error("Failed to delete table.", ex);
      ex.printStackTrace();
      throw new Exception(ex.getMessage());
    }
  }

  public static void createConnectionAndMTable(String tableName, String locatorPort) throws Exception {
    final Map<String, String> parameters = new HashMap<>();
    parameters.put(MonarchUtils.LOCATOR_PORT, locatorPort);
    parameters.put(MonarchUtils.REGION, tableName);

    connection = getConnection(tableName, locatorPort);

    List<String> columnNames = Arrays.asList("url", "id", "zipcode", "status");
    MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    tableDescriptor.addColumn(columnNames.get(0), new MTableColumnType("STRING")).
        addColumn(columnNames.get(1), new MTableColumnType("INT")).
        addColumn(columnNames.get(2), new MTableColumnType("INT")).
        addColumn(columnNames.get(3), new MTableColumnType("INT"));
    Admin admin = connection.getAdmin();

    MTable table = admin.createMTable(tableName, tableDescriptor);
//    System.out.println("Table [EmployeeTable] is created successfully!");
  }

  //
  public static void createConnectionAndMTable_LogicalTypes(String tableName, String locatorPort) throws Exception {

    final Map<String, String> parameters = new HashMap<>();
    parameters.put(MonarchUtils.LOCATOR_PORT, locatorPort);
    parameters.put(MonarchUtils.REGION, tableName);

    connection = getConnection(tableName, locatorPort);

    List<String> columnNames = Arrays.asList("the_int", "the_decimal", "the_date", "the_time", "the_timestamp");
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(columnNames.get(0), BasicTypes.INT)
        .addColumn(columnNames.get(1), BasicTypes.BIG_DECIMAL)
        .addColumn(columnNames.get(2), BasicTypes.DATE)
        .addColumn(columnNames.get(3), BasicTypes.TIMESTAMP)
        .addColumn(columnNames.get(4), BasicTypes.TIMESTAMP);

    Admin admin = connection.getAdmin();
    MTable table = admin.createMTable(tableName, tableDescriptor);
//    System.out.println("Table [ " + table.getName() + " ] is created successfully!");
  }

  public static void createConnectionAndMTable_2(String tableName, String locatorPort) throws Exception {

    final Map<String, String> parameters = new HashMap<>();
    parameters.put(MonarchUtils.LOCATOR_PORT, locatorPort);
    parameters.put(MonarchUtils.REGION, tableName);

    connection = getConnection(tableName, locatorPort);

    List<String> columnNames = Arrays.asList("the_byte", "the_short", "the_int", "the_long", "the_float", "the_double", "the_bool", "the_string", "the_bytes"/*, "the_decimal", "the_date"*/);
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(columnNames.get(0), new MTableColumnType("BYTE"))
        .addColumn(columnNames.get(1), new MTableColumnType("SHORT"))
        .addColumn(columnNames.get(2), new MTableColumnType("INT"))
        .addColumn(columnNames.get(3), new MTableColumnType("LONG"))
        .addColumn(columnNames.get(4), new MTableColumnType("FLOAT"))
        .addColumn(columnNames.get(5), new MTableColumnType("DOUBLE"))
        .addColumn(columnNames.get(6), new MTableColumnType("BOOLEAN"))
        .addColumn(columnNames.get(7), new MTableColumnType("STRING"))
        .addColumn(columnNames.get(8), new MTableColumnType("BINARY")
            //.addColumn(columnNames.get(9), new MTableColumnType("BIG_DECIMAL"));
            //.addColumn(columnNames.get(9), new MTableColumnType("DATE")
        );

    Admin admin = connection.getAdmin();
    MTable table = admin.createMTable(tableName, tableDescriptor);
//    System.out.println("Table [ " + table.getName() + " ] is created successfully!");
  }

  public static void createConnectionAndFTable_2(String tableName, String locatorPort) throws Exception {

    final Map<String, String> parameters = new HashMap<>();
    parameters.put(MonarchUtils.LOCATOR_PORT, locatorPort);
    parameters.put(MonarchUtils.REGION, tableName);

    connection = getConnection(tableName, locatorPort);

    List<String> columnNames = Arrays.asList("the_byte", "the_short", "the_int", "the_long", "the_float", "the_double", "the_bool", "the_string", "the_bytes"/*, "the_decimal", "the_date"*/);
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    tableDescriptor.addColumn(columnNames.get(0), new MTableColumnType("BYTE"))
      .addColumn(columnNames.get(1), new MTableColumnType("SHORT"))
      .addColumn(columnNames.get(2), new MTableColumnType("INT"))
      .addColumn(columnNames.get(3), new MTableColumnType("LONG"))
      .addColumn(columnNames.get(4), new MTableColumnType("FLOAT"))
        .addColumn(columnNames.get(5), new MTableColumnType("DOUBLE"))
        .addColumn(columnNames.get(6), new MTableColumnType("BOOLEAN"))
        .addColumn(columnNames.get(7), new MTableColumnType("STRING"))
        .addColumn(columnNames.get(8), new MTableColumnType("BINARY"));
      //.addColumn(columnNames.get(9), new MTableColumnType("BIG_DECIMAL"));
      //.addColumn(columnNames.get(9), new MTableColumnType("DATE"));

    //

    Admin admin = connection.getAdmin();

    FTable table = admin.createFTable(tableName, tableDescriptor);
//    System.out.println("Table " + tableName + " is created successfully!");
  }

  public static void createMTable(String tableName, MClientCache clientCache, MTableType tableType) throws Exception {
    List<String> columnNames = Arrays.asList("url", "id", "zipcode", "status");
    MTableDescriptor tableDescriptor = new MTableDescriptor(tableType);
    tableDescriptor.addColumn(columnNames.get(0), new MTableColumnType("STRING")).
      addColumn(columnNames.get(1), new MTableColumnType("INT")).
      addColumn(columnNames.get(2), new MTableColumnType("INT")).
      addColumn(columnNames.get(3), new MTableColumnType("INT"));

    Admin admin = clientCache.getAdmin();

    MTable table = admin.createMTable(tableName, tableDescriptor);
    Assert.assertNotNull(table);
//    System.out.println("Table " + tableName + " is created successfully!");
  }

  public static void createFTable(String tableName, MClientCache clientCache) throws Exception {
    List<String> columnNames = Arrays.asList("url", "id", "zipcode", "status");
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    tableDescriptor.addColumn(columnNames.get(0), new MTableColumnType("STRING")).
      addColumn(columnNames.get(1), new MTableColumnType("INT")).
      addColumn(columnNames.get(2), new MTableColumnType("INT")).
      addColumn(columnNames.get(3), new MTableColumnType("INT"));

    Admin admin = clientCache.getAdmin();

    FTable table = admin.createFTable(tableName, tableDescriptor);
    Assert.assertNotNull(table);
//    System.out.println("Table " + tableName + " is created successfully!");
  }

  public static void createFTableWithLogicalTypes(String tableName, MClientCache clientCache) throws Exception {
    List<String> columnNames = Arrays.asList("the_int", "the_decimal", "the_date", "the_time", "the_timestamp");
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    tableDescriptor.addColumn(columnNames.get(0), BasicTypes.INT)
        .addColumn(columnNames.get(1), BasicTypes.BIG_DECIMAL)
        .addColumn(columnNames.get(2), BasicTypes.DATE)
        .addColumn(columnNames.get(3), BasicTypes.TIMESTAMP)
        .addColumn(columnNames.get(4), BasicTypes.TIMESTAMP);

    Admin admin = clientCache.getAdmin();

    FTable table = admin.createFTable(tableName, tableDescriptor);
    Assert.assertNotNull(table);
//    System.out.println("FTable " + tableName + " is created successfully!");
  }

}