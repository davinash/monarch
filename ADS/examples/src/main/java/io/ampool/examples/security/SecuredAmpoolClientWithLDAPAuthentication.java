package io.ampool.examples.security;

import static org.apache.geode.management.internal.security.ResourceConstants.PASSWORD;
import static org.apache.geode.management.internal.security.ResourceConstants.USER_NAME;

import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;
import org.apache.geode.distributed.internal.DistributionConfig;

import java.util.Iterator;
import java.util.Properties;

/**
 * This example demonstrates how ampool client can connect to the secured Ampool cluster. This
 * example uses the LDAP authentication which is supported by the Ampool cluster. Before running
 * this example please refer to instructions on setting up secured ampool cluster with LDAP
 * authentication. Also specify the LDAP properties in the example below.
 */
public class SecuredAmpoolClientWithLDAPAuthentication {
  private static final String[] COLUMN_NAMES =
      new String[] {"NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ"};

  public static void main(String args[]) {
    System.out.println("Running SecuredAmpoolClientWithLDAPAuthentication example!");

    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }

    // Step:1 create client that connects to the Ampool cluster via locator.
    final Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG,
        "/tmp/SecuredAmpoolClientWithLDAPAuthentication.log");
    props.setProperty(DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME,
        "io.ampool.security.AmpoolAuthInitClient.create");
    props.setProperty(USER_NAME, "guest");
    props.setProperty(PASSWORD, "guest");

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
    Put record = new Put(Bytes.toBytes("rowKey"));
    for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
      record.addColumn(Bytes.toBytes(COLUMN_NAMES[colIndex]), Bytes.toBytes("val" + colIndex));
    }
    table.put(record);

    // Run scan on table
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      System.out.println("Key" + Bytes.toString(res.getRowId()));
    }

    // Step-5: Delete a table
    admin.deleteMTable(tableName);
    System.out.println("Table is deleted successfully!");

    // Step-6: close the client connection
    aClient.close();
    System.out.println("Connection to monarch DS closed successfully!");
  }
}
