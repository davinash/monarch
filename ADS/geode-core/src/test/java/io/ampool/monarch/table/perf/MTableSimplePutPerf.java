package io.ampool.monarch.table.perf;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class MTableSimplePutPerf {
  /**
   * Helper method which generate the data and keep in a file
   *
   * @param numOfRecords
   * @param numOfColumns
   * @param lenOfColumn
   * @param lenOfRowKey
   */
  private void generateInputData(final int numOfRecords, final int numOfColumns,
      final int lenOfColumn, final int lenOfRowKey) throws IOException {
    MTablePerfUtils.generateDataIfRequired(numOfRecords, numOfColumns, lenOfColumn, lenOfRowKey,
        this.getClass().getCanonicalName() + ".simple.put", false);
  }

  private void putSomeRecords(final int numOfColumns) throws IOException {
    MClientCache clientCache = new MClientCacheFactory().addPoolLocator("127.0.0.1", 10334)
        .set("log-file", "/tmp/MTableClient.log").create();

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int i = 0; i < numOfColumns; i++) {
      tableDescriptor.addColumn(Bytes.toBytes("COLUMN" + i));
    }
    tableDescriptor.setRedundantCopies(1);

    Admin admin = clientCache.getAdmin();
    String tableName = "Performance";
    MTable table = admin.createTable(tableName, tableDescriptor);

    BufferedReader in = new BufferedReader(
        new FileReader("/tmp/" + this.getClass().getCanonicalName() + ".simple.put"));
    String str;
    str = in.readLine();


    long totalTime = 0;
    int size = 0;
    while ((str = in.readLine()) != null) {
      String[] ar = str.split(",");
      byte[] rowKey = Bytes.toBytes(ar[0]);
      Put putRecord = new Put(rowKey);
      for (int i = 1; i <= numOfColumns; i++) {
        putRecord.addColumn(Bytes.toBytes("COLUMN" + (i - 1)), Bytes.toBytes(ar[i]));
      }

      long start = System.currentTimeMillis();
      table.put(putRecord);
      long end = System.currentTimeMillis();
      totalTime += (end - start);

      size += putRecord.getSerializedSize();
    }
    System.out.println("Put finished in " + totalTime + " MilliSeconds for Size = "
        + (size / 1024) / 1024 + " Mega Bytes ");
    in.close();
    clientCache.getAdmin().deleteTable(tableName);
    clientCache.close();

  }


  public static void main(String args[]) throws IOException {
    int generateNewData = 0;
    int numberOfRecords = 0;
    int numberOfColumns = 0;
    int lenthOfColumn = 0;
    int lenOfRowKey = 0;
    try {
      generateNewData = Integer.parseInt(args[0]);
      numberOfRecords = Integer.parseInt(args[1]);
      numberOfColumns = Integer.parseInt(args[2]);
      lenthOfColumn = Integer.parseInt(args[3]);
      lenOfRowKey = Integer.parseInt(args[4]);

    } catch (NumberFormatException e) {
      System.err.println("Argument  must be an integer.");
      System.exit(1);
    }


    MTableSimplePutPerf mspp = new MTableSimplePutPerf();
    int numOfColumns = 10;
    mspp.generateInputData(100000, numOfColumns, 300, 20);
    System.out.println("Data Generation is complete ...");
    mspp.putSomeRecords(numOfColumns);
    mspp.putSomeRecords(numOfColumns);
  }


}
