package io.ampool.monarch.table.perf;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.MTableImpl;
import io.ampool.monarch.types.BasicTypes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MTablePerfEsgyn {
  private static final String DATA_INPUT_FILE = "/data/work/Scan/upload.dat";

  public static void main(String[] args) throws IOException {
    MClientCache clientCache = new MClientCacheFactory().set("log-file", "/tmp/MTableClient.log")
        .addPoolLocator("127.0.0.1", 10334).create();


    MTableDescriptor tableDescriptor = new MTableDescriptor();

    String[] columnNames = {"c10K", "c1K", "c100", "c10", "c1"};

    for (int i = 0; i < columnNames.length; i++) {
      tableDescriptor.addColumn(columnNames[i], new MTableColumnType(BasicTypes.INT));
    }

    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setMaxVersions(1);

    Admin admin = clientCache.getAdmin();
    String tableName = "t106a_m";
    MTable table = admin.createTable(tableName, tableDescriptor);

    BufferedReader in = new BufferedReader(new FileReader(DATA_INPUT_FILE));

    String str;
    List<Put> listOfPuts = new ArrayList<>();
    int totalCount = 0;
    long totalElapsedTime = 0L;

    while ((str = in.readLine()) != null) {
      String[] fields = str.split("\\|");
      byte[] rowKey = Bytes.toBytes(fields[0]);
      Put singlePut = new Put(rowKey);
      for (int i = 1; i < fields.length; i++) {
        singlePut.addColumn(columnNames[i - 1], Integer.valueOf(fields[i]));
      }
      listOfPuts.add(singlePut);
      totalCount++;

      if (listOfPuts.size() == 1000) {
        long time = System.nanoTime();
        table.put(listOfPuts);
        totalElapsedTime += (System.nanoTime() - time);
        listOfPuts.clear();
      }
    }
    if (listOfPuts.size() != 0) {
      long time = System.nanoTime();
      table.put(listOfPuts);
      totalElapsedTime += (System.nanoTime() - time);
      ((MTableImpl) table).resetByteArraySizeBatchPut();
    }

    System.out.println("-------------------    doBatchPuts MTABLE   --------------------- ");
    System.out.println("TotalCount         = " + totalCount);
    System.out.println("Time Taken (ms)    = " + (totalElapsedTime / 1_000_000));
    in.close();


    Scan scan = new Scan();
    scan.setClientQueueSize(100_000);
    Scanner scanner = table.getScanner(scan);
    long time = System.nanoTime();
    Row currentRow = scanner.next();
    long count = 0;
    while (currentRow != null) {
      // List<MCell> cells = currentRow.getCells();
      // for (MCell cell : cells) {
      // System.out.println("COLUMN NAME => " + new String(cell.getColumnName()));
      // System.out.println("COLUMN VALUE => " + cell.getColumnValue());
      // }
      count++;
      currentRow = scanner.next();
    }
    long endtime = System.nanoTime();
    System.out
        .println("Time Taken (Seconds)    = " + (((double) (endtime - time)) / 1_000_000_000));
    scanner.close();
    admin.deleteTable(tableName);
    assert (count == totalCount);
  }
}
