package io.ampool.examples.ftable;

import java.sql.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.types.BasicTypes;

/**
 * This example is to demonstrate configuration required to attach tier stores to the table.
 * <p>
 * It requires tier stores already created with valid configurations. Refer:
 * http://docs.ampool-inc.com/adocs/w_MASH/commands-ref.html#createTierStore
 * <p>
 * This example uses "DefaultLocalORCStore" and "HDFSBasedORCStore" tier stores to form hierarchy.
 * "DefaultLocalORCStore" is created at the start of the server.
 * <p>
 * "HDFSBasedORCStore" can be created by following
 * http://docs.ampool-inc.com/adocs/w_MASH/commands-ref.html#initializingDefaultStores
 */
public class FTableWithTieredStoresExample {

  private static final String tableName = "FTableExampleWithTierStores";
  private static final String[] columnNames = {"NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ"};
  private static AmpoolClient client;
  private static final int numofBatches = 1;
  private static final int batchSize = 1000;

  public static TierStoreConfiguration DEFAULT_CONFIGURATION = new TierStoreConfiguration();

  // connect to the ampool cluster using ampool locator host and port
  private static void connect(final String locator_host, final int locator_port) {
    Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, "/tmp/FTableExample.log");
    client = new AmpoolClient(locator_host, locator_port, props);
    System.out.println("Connection to monarch distributed system is successfully done!");
  }

  // create ftable
  private static void createTable() {
    final Admin admin = client.getAdmin();
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    final Schema schema = new Schema.Builder().column(columnNames[0], BasicTypes.CHARS)
        .column(columnNames[1], BasicTypes.CHARS).column(columnNames[2], BasicTypes.INT)
        .column(columnNames[3], BasicTypes.INT).column(columnNames[4], BasicTypes.CHARS)
        .column(columnNames[5], BasicTypes.DATE).build();
    tableDescriptor.setSchema(schema);

    // set the partitioning column
    tableDescriptor.setPartitioningColumn(columnNames[1]);

    // add tier stores
    // The order in which stores are added is followed to create hierarchy
    LinkedHashMap<String, TierStoreConfiguration> storeHierarchy = new LinkedHashMap<>();
    TierStoreConfiguration localStoreConcfiguration = new TierStoreConfiguration();

    Properties configProps = new Properties();
    // set expiration to 1 hr, so records inserted before 1 hour will be expired to
    // HDFSBasedORCStore
    configProps.setProperty("time-to-expire", "0.16");
    // Partitions are created are of 0.16 hour or 10 min by a ORC writer as defaults, so move the
    // same partitions as it is.
    configProps.setProperty("partition-interval", "0.16");
    localStoreConcfiguration.setTierProperties(configProps);

    storeHierarchy.put("DefaultLocalORCStore", localStoreConcfiguration);

    // use default properties
    // time-to-expire = LONG.MAX_VALUE
    // partition-interval 10 min or 0.16 hour
    storeHierarchy.put("HDFSBasedORCStore", DEFAULT_CONFIGURATION);

    tableDescriptor.addTierStores(storeHierarchy);

    // delete the table if exists
    if (admin.existsFTable(tableName)) {
      admin.deleteFTable(tableName);
    }

    // create table
    admin.createFTable(tableName, tableDescriptor);
  }

  private static void appendRecords() {
    final FTable fTable = client.getFTable(tableName);

    // ingest records using batch append
    for (int batchIndex = 0; batchIndex < numofBatches; batchIndex++) {
      Record[] records = new Record[batchSize];
      for (int recordIndex = 0; recordIndex < batchSize; recordIndex++) {
        Record record = new Record();
        record.add(columnNames[0], "NAME" + recordIndex);
        record.add(columnNames[1], "ID" + recordIndex);
        record.add(columnNames[2], 10 + recordIndex);
        record.add(columnNames[3], 10000 * (recordIndex));
        record.add(columnNames[4], "DEPT");
        record.add(columnNames[5], new Date(System.currentTimeMillis()));
        records[recordIndex] = record;
      }

      fTable.append(records);
      System.out.println("Batch " + batchIndex + " completed");
    }

  }

  private static void scanRecords() {
    final FTable fTable = client.getFTable(tableName);
    final Scanner scanner = fTable.getScanner(new Scan());
    final Iterator<Row> iterator = scanner.iterator();

    int recordCount = 0;
    while (iterator.hasNext()) {
      recordCount++;
      final Row result = iterator.next();
      System.out.println("============= Record " + recordCount + " =============");
      // read the columns
      final List<Cell> cells = result.getCells();
      // NAME
      System.out.println(
          Bytes.toString(cells.get(0).getColumnName()) + " : " + cells.get(0).getColumnValue());

      // ID
      System.out.println(
          Bytes.toString(cells.get(1).getColumnName()) + " : " + cells.get(1).getColumnValue());

      // AGE
      System.out.println(
          Bytes.toString(cells.get(2).getColumnName()) + " : " + cells.get(2).getColumnValue());

      // SALARY
      System.out.println(
          Bytes.toString(cells.get(3).getColumnName()) + " : " + cells.get(3).getColumnValue());

      // DEPT
      System.out.println(
          Bytes.toString(cells.get(4).getColumnName()) + " : " + cells.get(4).getColumnValue());

      // DOJ
      System.out.println(
          Bytes.toString(cells.get(5).getColumnName()) + " : " + cells.get(5).getColumnValue());
      System.out.println();
    }
    System.out.println("Successfully scanned " + recordCount + "records.");
  }

  // create ftable
  private static void deleteTable() {
    final Admin admin = client.getAdmin();
    if (admin.existsFTable(tableName)) {
      admin.deleteFTable(tableName);
    }
  }

  // disconnect the ampool locator
  private static void disconnect() {
    client.close();
  }

  public static void main(String[] args) {
    System.out.println("Running FTable example!");

    // get the locator host and port.
    // default is localhost and 10334
    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }

    // connect to the ampool cluster
    connect(locator_host, locator_port);

    // create table
    createTable();

    // ingest records
    // Based on memory/heap setting records will be archived to hierarchy
    appendRecords();

    // scan the ingested records
    scanRecords();

    // delete table
    deleteTable();

    // disconnect from ampool cluster
    disconnect();
  }


}
