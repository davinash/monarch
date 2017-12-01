package io.ampool.examples.mtable;

import java.util.Properties;

import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

/**
 * Sample Quickstart example for Observer Coprocessor execution on MTable. Added a observer
 * coprocessor "io.ampool.examples.mtable.SampleTableObserver" to tableDescriptor during table
 * creation.
 *
 * Observer co-processors are just like database triggers, i.e. they execute user provided custom
 * code after/before the occurrence of certain events (for example after/before a get or put). The
 * code is automatically executed before/after the corresponding operations are performed.
 *
 * Note: start locator and server with example jar (ampool-examples-x.x.x.jar) in classpath for
 * running this example.
 */
public class MTableObserverCoprocessor {

  private static final String TABLE_NAME = "EmployeeTable";
  private static final String COL1 = "NAME";
  private static final String COL2 = "ID";
  private static final String COL3 = "AGE";
  private static final String COL4 = "SALARY";

  private static String OBSERVER_COPROCESSOR_CLASS =
      "io.ampool.examples.mtable.SampleTableObserver";

  private static AmpoolClient client;

  /**
   * Steps followed during MTableObserverCoprocessor example. 1. createMTable - postOpen() Called
   * after the table is created. 2. mtable put - prePut() Called before client PUT operation is
   * performed. 3. mtable put - postPut() Called after client PUT operation is performed. 4. mtable
   * get - preGet() Called before client GET operation is performed. 5. mtable get - postGet()
   * Called after client GET operation is performed. 6. mtable delete - preDelete() Called before
   * client DELETE operation is performed. 7. mtable checkAndPut - preCheckAndPut() Called before
   * PUT but after check. 8. mtable checkAndPut - postCheckAndPut() Called after PUT of CheckAndPut
   * operation. 9. mtable checkAndDelete - preCheckAndDelete() Called before DELETE but after check.
   * 10. mtable checkAndDelete - postCheckAndDelete() Called before DELETE but after check. 11.
   * deleteTable - preClose() Called before a table is deleted. 12. deleteTable - postClose() Called
   * after a table is deleted.
   */

  public static void main(String args[]) {

    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }

    // Step:1 create a connection with monarch distributed system (DS).
    Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG,
        "/tmp/MTableObserverCoprocessorExample.log");
    client = new AmpoolClient(locator_host, locator_port, props);
    System.out.println("Connection to monarch distributed system is successfully done!");

    try {
      MTable mtable = createTable(TABLE_NAME);
      System.out.println("Table " + "[" + TABLE_NAME + "]" + "is created successfully!");

      String row1 = "row1";
      Put put = new Put(Bytes.toBytes(row1));
      put.addColumn(Bytes.toBytes(COL1), Bytes.toBytes("JOHN"));
      put.addColumn(Bytes.toBytes(COL2), Bytes.toBytes(1));
      put.addColumn(Bytes.toBytes(COL3), Bytes.toBytes(30));
      put.addColumn(Bytes.toBytes(COL4), Bytes.toBytes(10000));

      mtable.put(put);
      System.out.println("Row with rowkey " + row1 + " inserted");

      Get get = new Get(Bytes.toBytes(row1));
      mtable.get(get);
      System.out.println("Row with rowkey " + row1 + " retrieved");

      Delete delete = new Delete(Bytes.toBytes(row1));
      mtable.delete(delete);
      System.out.println("Row with rowkey " + row1 + " deleted");

      String row2 = "row2";
      put = new Put(Bytes.toBytes(row2));
      put.addColumn(Bytes.toBytes(COL1), Bytes.toBytes("JASON"));
      put.addColumn(Bytes.toBytes(COL2), Bytes.toBytes(2));
      put.addColumn(Bytes.toBytes(COL3), Bytes.toBytes(36));
      put.addColumn(Bytes.toBytes(COL4), Bytes.toBytes(18000));

      mtable.put(put);
      System.out.println("Row with rowkey " + row2 + " inserted");

      Put checkAndPut = new Put(Bytes.toBytes(row2));
      checkAndPut.addColumn(Bytes.toBytes(COL1), Bytes.toBytes("JASON"));
      checkAndPut.addColumn(Bytes.toBytes(COL2), Bytes.toBytes(2));
      checkAndPut.addColumn(Bytes.toBytes(COL3), Bytes.toBytes(37));
      checkAndPut.addColumn(Bytes.toBytes(COL4), Bytes.toBytes(19000));

      mtable.checkAndPut(Bytes.toBytes(row2), Bytes.toBytes(COL3), Bytes.toBytes(36), checkAndPut);
      System.out.println("Row with rowkey " + row2 + " updated with checks");

      Delete checkAndDelete = new Delete(Bytes.toBytes(row2));

      mtable.checkAndDelete(Bytes.toBytes(row2), Bytes.toBytes(COL3), Bytes.toBytes(37),
          checkAndDelete);
      System.out.println("Row with rowkey " + row2 + " deleted with checks");

    } catch (MCoprocessorException cce) {

    } finally {
      deleteTable(TABLE_NAME);
    }
  }

  private static MTable createTable(String tableName) {
    MTableDescriptor tableDescriptor = new MTableDescriptor();

    Schema schema =
        new Schema.Builder().column(COL1).column(COL2).column(COL3).column(COL4).build();
    tableDescriptor.setSchema(schema);

    // Add a observer coprocessor
    tableDescriptor.addCoprocessor(OBSERVER_COPROCESSOR_CLASS);

    return client.getAdmin().createMTable(tableName, tableDescriptor);
  }

  private static void deleteTable(String tableName) {
    Admin admin = client.getAdmin();
    if (admin.existsMTable(tableName)) {
      admin.deleteMTable(tableName);
    }
  }

}

