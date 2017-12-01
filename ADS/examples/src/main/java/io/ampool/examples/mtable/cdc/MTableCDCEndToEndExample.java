/*
 * ========================================================================= * Copyright (c) 2015
 * Ampool, Inc. All Rights Reserved. * This product is protected by U.S. and international copyright
 * * and intellectual property laws.
 * *=========================================================================
 *
 */

package io.ampool.examples.mtable.cdc;

import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.CDCConfig;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.MEventOperation;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.TypeHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * This example shows MTable CDC listener listening the events and writing them into FTable.
 * Processing FTable events and again writing to different MTable. At last both original and newly
 * created MTable should match {@link MTableCDCEndToEndListener} is used to listen and write change
 * events.
 */
public class MTableCDCEndToEndExample {

  public static Map<String, BasicTypes> columns = new LinkedHashMap<>();
  public static String ORIGINAL_TABLE_PREFIX = "original_";
  public static String RECREATED_TABLE_PREFIX = "recreated_";
  public static String EVENTS_TABLE_PREFIX = "events_";
  public static int RECORD_COUNT = 1000;

  static {
    columns.put("BIN_COL", BasicTypes.BINARY);
    columns.put("SHORT_COL", BasicTypes.SHORT);
    columns.put("VARCHAR_COL", BasicTypes.VARCHAR);
    columns.put("DOUBLE_COL", BasicTypes.DOUBLE);
    columns.put("DATE_COL", BasicTypes.DATE);
    columns.put("BIGDEC_COL", BasicTypes.BIG_DECIMAL);
    columns.put("BOOL_COL", BasicTypes.BOOLEAN);
    columns.put("BYTE_COL", BasicTypes.BYTE);
    columns.put("CHAR_COL", BasicTypes.CHAR);
    columns.put("CHARS_COL", BasicTypes.CHARS);
    columns.put("FLOAT_COL", BasicTypes.FLOAT);
    columns.put("INT_COL", BasicTypes.INT);
    columns.put("LONG_COL", BasicTypes.LONG);
    columns.put("STRING_COL", BasicTypes.STRING);
    columns.put("TS_COL", BasicTypes.TIMESTAMP);
  }


  public static void main(String args[]) throws InterruptedException {
    System.out.println("MTable CDC listener Example!");

    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }
    String tableName = "MTableCDCEndToEndExample";
    String originalTableName = ORIGINAL_TABLE_PREFIX + tableName;
    String recreatedTableName = RECREATED_TABLE_PREFIX + tableName;
    String eventsTableName = EVENTS_TABLE_PREFIX + tableName;

    // Step:1 create a connection with monarch distributed system (DS).
    // create a configuration and connect to monarch locator.
    Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, "/tmp/MTableClient.log");
    AmpoolClient aClient = new AmpoolClient(locator_host, locator_port, props);
    System.out.println("Connection to monarch distributed system is successfully done!");

    // Step:2 create a Table.
    createTables(aClient, tableName);

    // put N record
    MTable originalTable = aClient.getMTable(originalTableName);
    List<String> keysInserted = RandomDataGenerator.putRecords(originalTable, RECORD_COUNT, null);
    System.out.println("No of keys inserted: " + keysInserted.size());

    // update
    List<String> keysUpdated = RandomDataGenerator.putRecords(originalTable, RECORD_COUNT,
        keysInserted.subList(0, RandomDataGenerator.getRandomInt(keysInserted.size())));
    System.out.println("No of keys updated: " + keysUpdated.size());

    // do partial delete
    List<String> keysPartiallyDeleted = RandomDataGenerator.deleteRecords(originalTable,
        keysInserted.subList(0, RandomDataGenerator.getRandomInt(keysInserted.size())), true);
    System.out.println("No of keys partially deleted: " + keysPartiallyDeleted.size());

    // delete
    List<String> keysDeleted = RandomDataGenerator.deleteRecords(originalTable,
        keysInserted.subList(0, RandomDataGenerator.getRandomInt(keysInserted.size())), false);
    System.out.println("No of keys deleted: " + keysDeleted.size());

    // wait till events are in sync
    System.out.println("Waiting for events to get recorded...");
    TimeUnit.SECONDS.sleep(60);

    // verify number of events
    FTable eventsTable = aClient.getFTable(eventsTableName);
    long totalCount = MTableUtils.getTotalCount(eventsTable, Collections.emptySet(), null, false);
    System.out.println(
        "No Of events recorded: " + totalCount + " No Of events expected: " + (keysInserted.size()
            + keysUpdated.size() + keysDeleted.size() + keysPartiallyDeleted.size()));

    MTable recreatedTable = aClient.getMTable(recreatedTableName);
    List<String> metaColumns = Arrays.asList("EVENTID", "OPERATION_TYPE", "RowKey", "VersionID");
    // recreate MTable
    Iterator<Row> scanner = eventsTable.getScanner(new Scan()).iterator();
    while (scanner.hasNext()) {
      Row next = scanner.next();
      List<Cell> cells = next.getCells();
      // get operation type
      Cell operationType = cells.get(1);
      if (MEventOperation.valueOf((String) operationType.getColumnValue()) == MEventOperation.CREATE
          || MEventOperation
              .valueOf((String) operationType.getColumnValue()) == MEventOperation.UPDATE) {
        Put put = new Put((byte[]) cells.get(2).getColumnValue());
        put.setTimeStamp((long) cells.get(3).getColumnValue());
        for (int i = 4; i < cells.size(); i++) {
          Cell cell = cells.get(i);
          // Ignore insertion TS
          if (!Bytes.toString(cell.getColumnName()).equals("__INSERTION_TIMESTAMP__")) {
            put.addColumn(Bytes.toString(cell.getColumnName()), cell.getColumnValue());
          }
        }
        recreatedTable.put(put);
      } else if (MEventOperation
          .valueOf((String) operationType.getColumnValue()) == MEventOperation.DELETE) {
        Delete delete = new Delete((byte[]) cells.get(2).getColumnValue());
        recreatedTable.delete(delete);
      }
    }

    long totalCount1 =
        MTableUtils.getTotalCount(recreatedTable, Collections.emptySet(), null, false);
    long totalCount2 =
        MTableUtils.getTotalCount(originalTable, Collections.emptySet(), null, false);
    System.out.println("Count from recreated table " + totalCount1);
    System.out.println("Count from original table  " + totalCount2);

    boolean isError = validateRecords(originalTable, recreatedTable);

    // Step-5: Delete a table
    Admin admin = aClient.getAdmin();
    admin.deleteMTable(originalTableName);
    admin.deleteMTable(recreatedTableName);
    admin.deleteFTable(eventsTableName);
    System.out.println("Table is deleted successfully!");

    // Step-6: close the Monarch connection
    aClient.close();
    System.out.println("Connection to monarch DS closed successfully!");
  }

  private static boolean validateRecords(MTable originalTable, MTable recreatedTable) {
    Iterator<Row> originalTableScanner = originalTable.getScanner(new Scan()).iterator();
    Iterator<Row> recreatedTableScanner = recreatedTable.getScanner(new Scan()).iterator();

    List<byte[]> nonMatchingKeys = new ArrayList<>();
    while (originalTableScanner.hasNext() && recreatedTableScanner.hasNext()) {
      Row originalRow = originalTableScanner.next();
      Row recreatedRow = recreatedTableScanner.next();
      if (Bytes.compareTo(originalRow.getRowId(), recreatedRow.getRowId()) != 0) {
        System.out.println("Error : Original and Recreated Table rows are not matching");
      }
      List<Cell> originalRowCells = originalRow.getCells();
      List<Cell> recreatedRowCells = recreatedRow.getCells();
      if (originalRowCells.size() != recreatedRowCells.size()) {
        System.out.println("Error : Original and Recreated Table cells are not matching");
      }
      for (int i = 0; i < originalRowCells.size() && i < recreatedRowCells.size(); i++) {
        if (originalRowCells.get(i).getColumnValue() != null
            && recreatedRowCells.get(i).getColumnValue() != null) {
          if (originalRowCells.get(i).getColumnType() != BasicTypes.BINARY) {
            if (!originalRowCells.get(i).getColumnValue()
                .equals(recreatedRowCells.get(i).getColumnValue())) {
              System.out.println(
                  "Original Cell Name: " + Bytes.toString(originalRowCells.get(i).getColumnName())
                      + " Recreated Cell Name: "
                      + Bytes.toString(recreatedRowCells.get(i).getColumnName()));
              System.out.println("Original value:["
                  + TypeHelper.deepToString(originalRowCells.get(i).getColumnValue()) + "]");
              System.out.println("Recreated value:["
                  + TypeHelper.deepToString(recreatedRowCells.get(i).getColumnValue()) + "]");
              nonMatchingKeys.add(originalRow.getRowId());
              System.out.println("---------------------------------------------------");
              System.out.println(
                  "Error : Original and Recreated Table cell value is not matching for Key "
                      + Arrays.toString(originalRow.getRowId()));
            }
          } else {
            if (Bytes.compareTo((byte[]) originalRowCells.get(i).getColumnValue(),
                ((byte[]) recreatedRowCells.get(i).getColumnValue())) != 0) {
              System.out.println(
                  "Original Cell Name: " + Bytes.toString(originalRowCells.get(i).getColumnName())
                      + " Recreated Cell Name: "
                      + Bytes.toString(recreatedRowCells.get(i).getColumnName()));
              System.out.println("Original value "
                  + TypeHelper.deepToString(originalRowCells.get(i).getColumnValue()));
              System.out.println("Recreated value "
                  + TypeHelper.deepToString(recreatedRowCells.get(i).getColumnValue()));
              nonMatchingKeys.add(originalRow.getRowId());
              System.out.println("---------------------------------------------------");
              System.out.println("Error : Original and Recreated Table cell value is not matching");
            }
          }
        }
      }
    }

    boolean isError = false;
    if (nonMatchingKeys.size() > 0) {
      isError = true;
      System.out.println("Non matching keys " + nonMatchingKeys.size());
    }
    return isError;
  }

  private static void createTables(AmpoolClient aClient, String tableName) {
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    MTableDescriptor mTableDescriptor1 = new MTableDescriptor();

    Schema.Builder mSchemaBuilder = new Schema.Builder();
    Schema.Builder fSchemaBuilder = new Schema.Builder();

    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    Arrays.asList("EVENTID", "OPERATION_TYPE", "RowKey", "VersionID").forEach(name -> {
      if (name.equalsIgnoreCase("RowKey")) {
        fSchemaBuilder.column(name, BasicTypes.BINARY);
        fTableDescriptor.setPartitioningColumn("RowKey");
      } else if (name.equalsIgnoreCase("VersionID")) {
        fSchemaBuilder.column(name, BasicTypes.LONG);
      } else {
        fSchemaBuilder.column(name, BasicTypes.STRING);
      }
    });

    columns.forEach((name, type) -> {
      mSchemaBuilder.column(name, type);
      fSchemaBuilder.column(name, type);
    });
    Schema mtableSchema = mSchemaBuilder.build();
    mTableDescriptor.setSchema(mtableSchema);
    mTableDescriptor1.setSchema(mtableSchema);
    CDCConfig config = mTableDescriptor.createCDCConfig();
    config.setPersistent(true);

    mTableDescriptor.addCDCStream("MTableCDCEndToEndListener",
        "io.ampool.examples.mtable.cdc.MTableCDCEndToEndListener", config);

    // corrosponding FTable will have following additional columns
    Schema ftableSchema = fSchemaBuilder.build();

    fTableDescriptor.setSchema(ftableSchema);

    Admin admin = aClient.getAdmin();
    admin.createMTable(ORIGINAL_TABLE_PREFIX + tableName, mTableDescriptor);
    admin.createMTable(RECREATED_TABLE_PREFIX + tableName, mTableDescriptor1);
    admin.createFTable(EVENTS_TABLE_PREFIX + tableName, fTableDescriptor);

    System.out
        .println("Tables ["
            + String.join(",", ORIGINAL_TABLE_PREFIX + tableName,
                RECREATED_TABLE_PREFIX + tableName, EVENTS_TABLE_PREFIX + tableName)
            + "] are created successfully!");

  }
}
