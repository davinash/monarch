package io.ampool.examples;


import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.ArchiveConfiguration;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.filter.internal.TimestampFilter;
import io.ampool.monarch.types.CompareOp;

import java.util.Iterator;
import java.util.Properties;

/**
 * This example demonstrates how ampool table data can be archived to an external store like HDFS.
 * Table data is archived to an external store based on the specified filter. Before running this
 * example change the ArchiveConfiguration properties.
 */
public class ArchiveTableToExternalStoreExample {
  private static final String[] COLUMN_NAMES =
      new String[] {"NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ"};

  public static void main(String args[]) {
    System.out.println("Running ArchiveTableToExternalStoreExample example!");
    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }

    // Step:1 create client that connects to the Ampool cluster via locator.
    final Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG,
        "/tmp/ArchiveTableToExternalStoreExample.log");

    final AmpoolClient aClient = new AmpoolClient(locator_host, locator_port, props);
    System.out.println("AmpoolClient connected to the cluster successfully!");

    // Step:2 create the schema.
    final Schema schema = new Schema(COLUMN_NAMES);
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setSchema(schema);

    Admin admin = aClient.getAdmin();
    String tableName = "EmployeeTable";

    MTable table = admin.createMTable(tableName, tableDescriptor);
    System.out.println("Table [EmployeeTable] is created successfully!");

    int NUM_OF_COLUMNS = 6;
    int NUM_OF_ROWS = 10000;
    for (int i = 0; i < NUM_OF_ROWS; i++) {
      Put record = new Put(Bytes.toBytes("rowKey" + i));
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAMES[colIndex]), Bytes.toBytes("val" + colIndex));
      }
      table.put(record);
    }

    // Run scan on table
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    int numRecords = 0;
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      numRecords++;
    }
    System.out.println("Number of Records before archive : " + numRecords);

    // create the archive configuration and archive table to external store
    ArchiveConfiguration archiveConfiguration = new ArchiveConfiguration();
    Properties properties = new Properties();
    properties.setProperty("hadoop.site.xml.path", "");
    properties.setProperty(ArchiveConfiguration.BASE_DIR_PATH, "extstore");
    archiveConfiguration.setFsProperties(properties);
    admin.archiveTable(tableName, new TimestampFilter(CompareOp.GREATER, 0l), archiveConfiguration);

    // Run scan on table and check the records count.
    scanner = table.getScanner(new Scan());
    itr = scanner.iterator();
    numRecords = 0;
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      numRecords++;
    }
    System.out.println("Number of Records after archive : " + numRecords);

    // Step-5: Delete a table
    admin.deleteMTable(tableName);
    System.out.println("Table is deleted successfully!");

    // Step-6: close the client connection
    aClient.close();
    System.out.println("Connection to monarch DS closed successfully!");
  }
}
