package io.ampool.examples.mtable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

/**
 * Sample Quickstart example for Coprocessor execution on MTable. This example shows how coprocessor
 * is used to count total number of rows in a table.
 */
public class MTableCoprocessorExample {

  private static final String TABLE_NAME = "EmployeeTable";
  private static final String COL1 = "NAME";
  private static final String COL2 = "ID";
  private static final String COL3 = "AGE";
  private static final String COL4 = "SALARY";

  final static int numBuckets = 113;
  final static int numOfEntries = 1000;

  private static String ROW_COUNT_COPROCESSOR_CLASS =
      "io.ampool.examples.mtable.SampleRowCountCoprocessor";

  private static AmpoolClient client;

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
        "/tmp/MTableCoprocessorExample.log");
    client = new AmpoolClient(locator_host, locator_port, props);
    System.out.println("Connection to monarch distributed system is successfully done!");

    try {
      MTable mtable = createTable(TABLE_NAME);
      insertRows(mtable);

      System.out.println("Running aggregation client to get row count for table " + TABLE_NAME);

      // execute coprocessor to get row count
      Scan scan = new Scan();
      String startKey = "rowkey-0";
      String stopKey = "rowkey-" + (numOfEntries);
      scan.setStartRow(Bytes.toBytes(startKey));
      // scan.setStopRow(Bytes.toBytes(stopKey));
      MExecutionRequest request = new MExecutionRequest();
      request.setScanner(scan);

      // This is how you pass arguments to the coprocessor execution
      /*
       * ArrayList<Integer> list = new ArrayList<Integer>(); list.add(i1); list.add(i2);
       * list.add(i3); request.setArguments(list);
       */

      Map<Integer, List<Object>> collector = mtable.coprocessorService(ROW_COUNT_COPROCESSOR_CLASS,
          "rowCount", scan.getStartRow(), scan.getStopRow(), request);

      final Iterator<Entry<Integer, List<Object>>> entryItr = collector.entrySet().iterator();
      long rowCount = 0L;
      while (entryItr.hasNext()) {
        final Entry<Integer, List<Object>> entry = entryItr.next();
        int bucketId = entry.getKey();
        List<Object> resultList = entry.getValue();
        final Iterator<Object> resultItr = resultList.iterator();
        while (resultItr.hasNext()) {
          Object result = resultItr.next();
          if (result instanceof MCoprocessorException || result instanceof Exception) {
            // This is how to check exception from result object.
            // And handle as needed
          } else if (result instanceof Long) {
            rowCount += (Long) result;
          }
        }
      }

      System.out.println("Row count: " + rowCount);



    } catch (MCoprocessorException cce) {
      // Fail the example
      System.exit(1);
    } finally {
      deleteTable(TABLE_NAME);
    }
  }

  private static MTable createTable(String tableName) {
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    Schema schema =
        new Schema.Builder().column(COL1).column(COL2).column(COL3).column(COL4).build();
    tableDescriptor.setSchema(schema);
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setTotalNumOfSplits(numBuckets);

    Admin admin = client.getAdmin();
    MTable mtable = admin.createMTable(tableName, tableDescriptor);
    // assertEquals(mtable.getName(), tableName);
    return mtable;
  }

  private static void deleteTable(String tableName) {
    client.getAdmin().deleteMTable(tableName);
  }

  private static void insertRows(MTable mtable) {
    for (int keyIndex = 0; keyIndex < numOfEntries; keyIndex++) {
      Put myput1 = new Put(Bytes.toBytes("rowkey-" + padWithZero(keyIndex, 3)));
      myput1.addColumn(Bytes.toBytes(COL1), Bytes.toBytes("col" + keyIndex));
      myput1.addColumn(Bytes.toBytes(COL2), Bytes.toBytes(keyIndex + 10));
      myput1.addColumn(Bytes.toBytes(COL3), Bytes.toBytes(keyIndex + 10));
      myput1.addColumn(Bytes.toBytes(COL4), Bytes.toBytes(keyIndex + 10));
      mtable.put(myput1);
    }
  }

  private static String padWithZero(final int value, final int maxSize) {
    String valueString = String.valueOf(value);
    for (int index = valueString.length(); index <= maxSize; index++) {
      valueString = "0" + valueString;
    }
    return valueString;
  }

}
