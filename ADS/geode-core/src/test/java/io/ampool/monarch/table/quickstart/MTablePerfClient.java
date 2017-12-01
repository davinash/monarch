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

package io.ampool.monarch.table.quickstart;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * Sample MTable Quickstart example. It performs following table operations. create a connection
 * with monarch distributed system (DS). Create Table named "EmployeeTable" with columns ["NAME",
 * "ID", "AGE", "SALARY", "DEPT", "DOJ"] Insert row with different versions, update selected columns
 * value. Retrieve row value given a specified timestamp, no timestamp (latest version), given
 * selected columns. Delete row at specified timestamp, Delete a values of given columns from a row,
 * delete entire row.
 *
 * @since 0.2.0.0
 */
public class MTablePerfClient {

  public static void usage(String issue) {
    System.out.println("");
    if (issue != null) {
      System.out.println("Error in parsing arguments near " + issue);
      System.out.println("");
    }

    System.out.println("\nUsage: MTablePerfClient [options]\n\n" + "valid options:\n"
        + "--records <number of records>\t\tThe number of records to put into the table for scan\n"
        + "--size <record size>\t\t\tThe size of the column data; table has fixed 6 columns so row size is size*6\n"
        + "--put_oper <put or putall>\t\tThe typeof operation to use for putting data (put, putall or noput)\n"
        + "--putall_batch <put list size>\t\tThe number of records to use in a batch for putall\n"
        + "--scan_batch <scan batch size>\t\tThe queue size for streaming or batch size if batch mode\n"
        + "--unordered\t\t\t\tIndicates if table is unordered (default is ordered)\n"
        + "--scankeys [true | false]\t\tIndicates whether or not to return keys in scan (default is true)\n"
        + "--splits <num splits>\t\t\tSets the number of splits (buckets) to use (default is 4)\n"
        + "--batching\t\t\t\tIndicates do a batch mode (interactive) scan, then exit when done\n"
        + "--parallel\t\t\t\tIndicates do a parallel scan (scan buckets in parallel)\n"
        + "--threads number\t\t\tSets the number of scan threads (default is one scan)\n"
        + "--checkscan\t\t\t\tUsed for ordered tables (default) only to check order on returned results\n"
        + "--chunk size\t\t\t\tSets the chunk size for returned data in rows (else uses server default)\n"
        + "--nocleanup\t\t\t\tIndicates the test table should not be re-created on start (if it exists) "
        + "and deleted on exit (by default it will be re-created/deleted)\n"
        + "--help\t\t\t\t\tPrint this help message\n");
    System.out.println("--checkscan requires that scankeys be true (default)");
    System.out.println(
        "\"--put_oper noput\" is for tables left in memory that you do not want to reload");
    System.out.println("");
    System.exit(1);
  }

  public static void putAllData(MTable table, List<String> columnNames, int numRecords,
      int dataSize, int batchSize, boolean unordered) {
    int NUM_OF_COLUMNS = 6;

    System.out.println("start put as list");
    long starttime = System.nanoTime();
    long totalPutTime = 0;
    long putStartTime = 0;

    byte[] data = new byte[dataSize];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    int count = 0;
    int numRecordsPut = numRecords;
    List<Put> putList = new ArrayList<Put>(batchSize);

    while (numRecordsPut > 0) {
      putList.clear();

      for (int i = 0; i < batchSize; i++) {
        Put record = new Put(Bytes.toBytes("rowKey" + numRecordsPut));
        if (unordered == false) {
          long timestamp = (i + 1) * 100;
          record.setTimeStamp(timestamp);
        }
        for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
          // make it a bit different each column
          data[colIndex] = (byte) colIndex;
          record.addColumn(Bytes.toBytes(columnNames.get(colIndex)), data);
        }
        putList.add(record);
        numRecordsPut -= 1;
        if (numRecordsPut == 0) {
          break;
        }
      }
      putStartTime = System.nanoTime();
      count += putList.size();
      table.put(putList);
      // System.out.print(".");
      totalPutTime += (System.nanoTime() - putStartTime);
    }
    // System.out.print("\n");

    long totalTime = System.nanoTime() - starttime;
    System.out.println("put took " + totalTime + " nanoseconds or "
        + ((double) totalTime / 1_000_000_000) + " secs for " + count + " rows");
    System.out.println("actual put operations took " + totalPutTime + " nanoseconds");
  }

  public static void putData(MTable table, List<String> columnNames, int numRecords, int dataSize,
      boolean unordered) {
    int NUM_OF_COLUMNS = 6;
    // can re-use the MPut
    Put record = new Put(Bytes.toBytes("rowKey"));

    byte[] data = new byte[dataSize];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    System.out.println("start put");
    long starttime = System.nanoTime();
    long totalPutTime = 0;
    long putStartTime = 0;

    for (int i = 0; i < numRecords; i++) {
      record.setRowKey(Bytes.toBytes("rowKey" + i));
      if (unordered == false) {
        long timestamp = (i + 1) * 100;
        record.setTimeStamp(timestamp);
      }

      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        // make it a bit different each column
        data[colIndex] = (byte) colIndex;
        record.addColumn(Bytes.toBytes(columnNames.get(colIndex)), data);
      }
      putStartTime = System.nanoTime();
      table.put(record);
      record.clear();
      totalPutTime += (System.nanoTime() - putStartTime);
    }

    long totalTime = System.nanoTime() - starttime;
    System.out.println("put took " + totalTime + " nanoseconds for " + numRecords + " rows");
    System.out.println("actual put operations took " + totalPutTime + " nanoseconds");
  }

  class Scannit implements Runnable {
    MTable table;
    int scanBatch;
    boolean batching;
    boolean unordered;
    boolean scan_keys;
    boolean set_scan_keys;
    int chunkSize;
    byte[] badKey;
    HashSet<String> checkSet;
    boolean parallel;
    int numRecords;
    int count;

    public Scannit(MTable table, int scanBatch, boolean batching, boolean unordered,
        boolean scan_keys, boolean set_scan_keys, int chunkSize, byte[] badKey,
        HashSet<String> checkSet, boolean parallel) {
      this.table = table;
      this.scanBatch = scanBatch;
      this.batching = batching;
      this.unordered = unordered;
      this.scan_keys = scan_keys;
      this.set_scan_keys = set_scan_keys;
      this.chunkSize = chunkSize;
      this.badKey = badKey;
      this.checkSet = checkSet;
      this.parallel = parallel;
      this.count = 0;
    }

    public void run() {
      // Run scan on table
      Scan scan = new Scan();
      scan.setClientQueueSize(scanBatch);
      if (batching) {
        scan.enableBatchMode();
        scan.setBatchSize(scanBatch);
      }
      if (set_scan_keys) {
        scan.setReturnKeysFlag(scan_keys);
      }
      if (chunkSize > 0) {
        scan.setMessageChunkSize(chunkSize);
      }
      System.out.println("scan client queue size set to " + scan.getClientQueueSize());
      Scanner scanner;
      if (parallel) {
        scanner = table.getParallelScanner(scan);
      } else {
        scanner = table.getScanner(scan);
      }
      Iterator itr = scanner.iterator();
      System.out.println("start scan...");
      long starttime = System.nanoTime();
      byte[] lastKey = null;
      while (itr.hasNext()) {
        count += 1;
        Row res = (Row) itr.next();
        // selectedRow = res.getCells();
        byte[] key = res.getRowId();
        if (!unordered && !parallel) {
          if (lastKey != null) {
            if (Bytes.compareTo(key, lastKey) < 0) {
              throw new IllegalStateException(
                  "key " + Bytes.toString(key) + " is not >= key " + Bytes.toString(lastKey));
            }
          }
        }
        if (scan_keys == true) {
          if (key == null) {
            throw new IllegalStateException("key for row " + count + " is null");
          }
          if (badKey != null) {
            if (Bytes.compareTo(key, badKey) == 0) {
              throw new IllegalStateException(
                  "key " + Bytes.toString(key) + "has been deleted but is returned");
            }
          }
          if (checkSet != null) {
            if (checkSet.add(new String(key)) == false) {
              throw new IllegalStateException("key is duplicated in scan " + new String(key));
            }
          }
          lastKey = key;
        } else {
          if (key != null) {
            throw new IllegalStateException(
                "key for row " + count + " is not null, but return keys is false");
          }
        }
      }
      long totalTime = System.nanoTime() - starttime;
      if (lastKey != null) {
        System.out.println("Last key was \"" + new String(lastKey) + "\"");
      }
      System.out.println("Took " + totalTime + " nanoseconds or "
          + (double) totalTime / 1_000_000_000 + " secs to scan " + count + " rows");
      scanner.close();
    }

    public int getCount() {
      return this.count;
    }
  }

  public Scannit getscanner(MTable table, int scanBatch, boolean batching, boolean unordered,
      boolean scan_keys, boolean set_scan_keys, int chunkSize, byte[] badKey,
      HashSet<String> checkSet, boolean parallel) {
    return new Scannit(table, scanBatch, batching, unordered, scan_keys, set_scan_keys, chunkSize,
        badKey, checkSet, parallel);
  }

  public static int doScan(MTable table, int scanBatch, boolean batching, boolean unordered,
      boolean scan_keys, boolean set_scan_keys, int chunkSize, byte[] badKey,
      HashSet<String> checkSet, boolean parallel, int numScanThreads) {
    long startTime = System.nanoTime();
    MTablePerfClient perf = new MTablePerfClient();
    if (numScanThreads < 1) {
      numScanThreads = 1;
    }
    Thread[] threads = new Thread[numScanThreads];

    Scannit scanner = null;

    for (int i = 0; i < numScanThreads; i++) {
      scanner = perf.getscanner(table, scanBatch, batching, unordered, scan_keys, set_scan_keys,
          chunkSize, badKey, checkSet, parallel);
      threads[i] = new Thread(scanner);
      threads[i].start();
      System.out.println("started scanner " + i);
    }
    for (int i = 0; i < numScanThreads; i++) {
      try {
        threads[i].join();
        System.out.println("scanner " + i + " completed");
      } catch (Exception exc) {
      }
    }
    long totalTime = (System.nanoTime() - startTime);
    System.out.println("Took " + totalTime + " nanoseconds or " + (double) totalTime / 1_000_000_000
        + " secs for all scanners to complete");
    return scanner.getCount();
  }

  public static int doScan(MTable table, int scanBatch, boolean batching, boolean unordered,
      boolean scan_keys, boolean set_scan_keys, int chunkSize, byte[] badKey,
      HashSet<String> checkSet, boolean parallel) {
    // Run scan on table
    Scan scan = new Scan();
    scan.setClientQueueSize(scanBatch);
    if (batching) {
      scan.enableBatchMode();
      scan.setBatchSize(scanBatch);
    }
    if (set_scan_keys) {
      scan.setReturnKeysFlag(scan_keys);
    }
    if (chunkSize > 0) {
      scan.setMessageChunkSize(chunkSize);
    }
    System.out.println("scan client queue size set to " + scan.getClientQueueSize());
    Scanner scanner;
    if (parallel) {
      scanner = table.getParallelScanner(scan);
    } else {
      scanner = table.getScanner(scan);
    }
    Iterator itr = scanner.iterator();
    int count = 0;
    System.out.println("start scan...");
    long starttime = System.nanoTime();
    byte[] lastKey = null;
    while (itr.hasNext()) {
      count += 1;
      Row res = (Row) itr.next();
      // selectedRow = res.getCells();
      byte[] key = res.getRowId();
      if (!unordered && !parallel) {
        if (lastKey != null) {
          if (Bytes.compareTo(key, lastKey) < 0) {
            throw new IllegalStateException(
                "key " + Bytes.toString(key) + " is not >= key " + Bytes.toString(lastKey));
          }
        }
      }
      if (scan_keys == true) {
        if (key == null) {
          throw new IllegalStateException("key for row " + count + " is null");
        }
        if (badKey != null) {
          if (Bytes.compareTo(key, badKey) == 0) {
            throw new IllegalStateException(
                "key " + Bytes.toString(key) + "has been deleted but is returned");
          }
        }
        if (checkSet != null) {
          if (checkSet.add(new String(key)) == false) {
            throw new IllegalStateException("key is duplicated in scan " + new String(key));
          }
        }
        lastKey = key;
      } else {
        if (key != null) {
          throw new IllegalStateException(
              "key for row " + count + " is not null, but return keys is false");
        }
      }
    }
    long totalTime = System.nanoTime() - starttime;
    if (lastKey != null) {
      System.out.println("Last key was \"" + new String(lastKey) + "\"");
    }
    System.out.println("Took " + totalTime + " nanoseconds or " + (double) totalTime / 1_000_000_000
        + " secs to scan " + count + " rows");
    scanner.close();
    return count;
  }

  // This is an example of how to do an interactive paged scan using
  // Batch mode and nextBatch().
  public static void doBatchScan(MTable table, int scanBatch) {
    // Run scan on table
    Scan scan = new Scan();
    scan.setBatchSize(scanBatch);
    scan.enableBatchMode();
    scan.setReturnKeysFlag(true);
    System.out.println("scan batch size set to " + scan.getBatchSize());
    Scanner scanner = table.getScanner(scan);
    Row[] results = scanner.nextBatch();
    int count = 0, pageCount = 0;
    System.out.println("Batching through results");
    while (results.length > 0) {
      pageCount++;
      for (int i = 0; i < results.length; i++) {
        count++;
        byte[] key = results[i].getRowId();
        System.out.println("" + count + ") " + " rowID: " + Bytes.toString(key));
      }
      System.out.print("End page " + pageCount
          + ", Hit enter or type [c | C] to continue, type [e | E] to end: ");
      String input = System.console().readLine();
      if (input.startsWith("e") || input.startsWith("E")) {
        System.out.println("Ending scan.");
        break;
      }
      results = scanner.nextBatch();
    }
    System.out.println("total page count was " + pageCount);
    System.out.println("total row count was " + count);
    scanner.close(); // need to close, especially if we ended without reading all rows
  }

  public static void cleanUp(MClientCache clientCache, Admin admin, String tableName,
      boolean deleteTable, RuntimeException endException) {
    if (deleteTable) {
      admin.deleteTable(tableName);
      System.out.println("Table " + tableName + " deleted successfully");
    } else {
      System.out.println("Table " + tableName + " left in memory (not deleted)");
    }
    clientCache.close();
    System.out.println("Connection to monarch DS closed successfully");
    if (endException != null) {
      throw endException;
    }
  }

  public static void main(String args[]) {
    System.out.println("MTable Quickstart Scan Performance Examples");

    int numRecords = 100;
    int putAllBatch = 10;
    int scanBatch = 2000;
    boolean batching = false;
    int dataSize = 130;
    String putOper = "put";
    boolean partition = true;
    boolean scan_keys = true;
    boolean set_scan_keys = false;
    boolean checkscan = false;
    int numSplits = 4;
    int chunkSize = 0;
    boolean unordered = false;
    boolean parallel = false;
    int numScanThreads = 1;
    boolean cleanup = true;

    if (args.length > 0) {
      int index = 0;
      try {
        while (index < args.length) {
          if (args[index].equals("--records")) {
            numRecords = new Integer(args[index + 1]).intValue();
            index += 1;
          } else if (args[index].equals("--putall_batch")) {
            putAllBatch = new Integer(args[index + 1]).intValue();
            index += 1;
          } else if (args[index].equals("--scan_batch")) {
            scanBatch = new Integer(args[index + 1]).intValue();
            index += 1;
          } else if (args[index].equals("--batching")) {
            batching = true;
          } else if (args[index].equals("--parallel")) {
            parallel = true;
          } else if (args[index].equals("--size")) {
            dataSize = new Integer(args[index + 1]).intValue();
            index += 1;
          } else if (args[index].equals("--splits")) {
            numSplits = new Integer(args[index + 1]).intValue();
            index += 1;
          } else if (args[index].equals("--chunk")) {
            chunkSize = new Integer(args[index + 1]).intValue();
            index += 1;
          } else if (args[index].equals("--scankeys")) {
            if (args[index + 1].trim().equalsIgnoreCase("true"))
              scan_keys = true;
            else if (args[index + 1].trim().equalsIgnoreCase("false"))
              scan_keys = false;
            set_scan_keys = true;
            index += 1;
          } else if (args[index].equals("--unordered")) {
            unordered = true;
            partition = false;
          } else if (args[index].equals("--checkscan")) {
            checkscan = true;
          } else if (args[index].equals("--nocleanup")) {
            cleanup = false;
          } else if (args[index].equals("--threads")) {
            numScanThreads = new Integer(args[index + 1]).intValue();
            index += 1;
          } else if (args[index].equals("--put_oper")) {
            putOper = args[index + 1].toLowerCase();
            if (!putOper.equals("put") && !putOper.equals("putall") && !putOper.equals("noput")) {
              usage(args[index]);
            }
            index += 1;
          } else if (args[index].equals("--usage") || args[index].equals("--help")
              || args[index].equals("-help")) {
            usage(null);
          } else {
            usage(args[index]);
          }
          index += 1;
        }
      } catch (Exception exc) {
        System.out.println("Exc " + exc.toString());
        usage(args[index]);
      }
    }

    HashSet<String> checkSet = null;

    if (checkscan) {
      checkSet = new HashSet<>(numRecords);
    }

    if (partition && unordered) {
      System.out.println("\nError: cannot specify both \"--partitioned\" and \"--unordered\"");
      usage(null);
    }
    if (checkscan && !scan_keys) {
      System.out.println("\nError: cannot specify both \"--checkscan\" and \"--scankeys false\"");
      usage(null);
    }

    System.out.println("\nrunning test with: --records " + numRecords + " --size " + dataSize
        + " --put_oper " + putOper + " --putall_batch " + putAllBatch + " --scan_batch " + scanBatch
        + " --partition " + partition + " --unordered " + unordered + " --splits " + numSplits
        + " --chunk " + chunkSize + " --scankeys " + scan_keys + " --checkscan " + checkscan
        + " --parallel " + parallel + " --threads " + numScanThreads + "\n");

    // Create a connection with Ampool distributed system (DS).
    // Create a configuration and connect to Ampool locator.
    MClientCache clientCache = new MClientCacheFactory().set("log-file", "/tmp/MTableClient.log")
        .addPoolLocator("127.0.0.1", 10334).create();
    System.out.println("Connection to Ampool distributed system is successfully done!");

    List<String> columnNames = Arrays.asList("NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ");
    MTableDescriptor tableDescriptor = null;

    if (unordered) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }

    /*
     * tableDescriptor.addColumn(Bytes.toBytes(columnNames.get(0))).
     * addColumn(Bytes.toBytes(columnNames.get(1))). addColumn(Bytes.toBytes(columnNames.get(2))).
     * addColumn(Bytes.toBytes(columnNames.get(3))). addColumn(Bytes.toBytes(columnNames.get(4))).
     * addColumn(Bytes.toBytes(columnNames.get(5))). setRedundantCopies(1). setTotalNumOfSplits(4);
     * 
     * if ( ! unordered ) { tableDescriptor. setMaxVersions(1); }
     */

    tableDescriptor.addColumn(Bytes.toBytes(columnNames.get(0)))
        .addColumn(columnNames.get(1), new MTableColumnType(BasicTypes.BINARY))
        .addColumn(columnNames.get(2), new MTableColumnType(BasicTypes.BINARY))
        .addColumn(columnNames.get(3), new MTableColumnType(BasicTypes.BINARY))
        .addColumn(columnNames.get(4), new MTableColumnType(BasicTypes.BINARY))
        .addColumn(columnNames.get(5), new MTableColumnType(BasicTypes.BINARY))
        .setRedundantCopies(1).setTotalNumOfSplits(numSplits);

    if (!unordered) {
      tableDescriptor.setMaxVersions(1);
    }

    if (partition) {
      int maxLen = Integer.toString(numRecords - 1).length();
      String maxKeyNum = "";
      for (int i = 0; i < maxLen - 1; i++) {
        maxKeyNum += "9";
      }
      tableDescriptor.setStartStopRangeKey("rowKey0".getBytes(), ("rowKey" + maxKeyNum).getBytes());
    }

    MTable table = null;
    Admin admin = clientCache.getAdmin();
    String tableName = "PerfTable";
    if (cleanup) {
      try {
        admin.deleteTable(tableName);
      } catch (Exception exc) {
      }
      table = admin.createTable(tableName, tableDescriptor);
      System.out.println("Table [" + tableName + "] created successfully");
      if (putOper.equals("noput")) {
        System.out.println("WARNING: \"noput\" defined and table created as new, will be empty.");
      }
    } else {
      table = clientCache.getTable(tableName);
      if (table != null) {
        System.out.println("Table [" + tableName + "] found in cache");
      } else {
        System.out.println("Table [" + tableName + "] not found in cache, creating");
        table = admin.createTable(tableName, tableDescriptor);
        System.out.println("Table [" + tableName + "] created successfully");
        if (putOper.equals("noput")) {
          System.out.println("WARNING: \"noput\" defined and table created as new, will be empty.");
        }
      }
    }

    // put the data using the specified operation.
    if (putOper.equals("put")) {
      putData(table, columnNames, numRecords, dataSize, unordered);
    } else if (putOper.equals("putall")) {
      putAllData(table, columnNames, numRecords, dataSize, putAllBatch, unordered);
    }

    if (batching) {
      // just do a batch scan and exit
      doBatchScan(table, scanBatch);
      cleanUp(clientCache, admin, tableName, cleanup, null);
      System.exit(0);
    }

    // Retrieve a single row value.
    // Retrieve a row value at specified timestamp.
    Get get = new Get(Bytes.toBytes("rowKey" + 1));
    Row result = table.get(get);
    List<Cell> row = result.getCells();
    for (Cell cell : row) {
      System.out.println("ColumnName   => " + Bytes.toString(cell.getColumnName())
          + " AND ColumnValue length  => " + ((byte[]) cell.getColumnValue()).length);
    }
    System.out.println("Retrieved a row entry at specified timestamp successfully");

    // Retrieve a selected columns of a row
    Get getRecord = new Get(Bytes.toBytes("rowKey" + 2));
    getRecord.addColumn(Bytes.toBytes("NAME"));
    getRecord.addColumn(Bytes.toBytes("ID"));
    getRecord.addColumn(Bytes.toBytes("SALARY"));

    result = table.get(getRecord);
    List<Cell> selectedRow = result.getCells();
    for (Cell cell : selectedRow) {
      System.out.println("ColumnName   => " + Bytes.toString(cell.getColumnName())
          + " AND ColumnValue length  => " + ((byte[]) cell.getColumnValue()).length);
    }
    System.out.println("Retrieved selected columns for a given row successfully");

    System.out.println("Waiting 3 seconds for gc...");
    try {
      System.gc();
      Thread.sleep(3000);
    } catch (InterruptedException iexc) {
    }

    int count = 0;

    // Run a forward scan on the table
    if (numScanThreads <= 1) {
      count = doScan(table, scanBatch, batching, unordered, scan_keys, set_scan_keys, chunkSize,
          null, checkSet, parallel);
    } else {
      count = doScan(table, scanBatch, batching, unordered, scan_keys, set_scan_keys, chunkSize,
          null, checkSet, parallel, numScanThreads);
    }

    Delete delete;

    // Delete a row. All versions of a row will be deleted.
    int rowToDelete = (numRecords / 2);
    System.out.println("Deleting row with key rowKey" + rowToDelete);
    delete = new Delete(Bytes.toBytes("rowKey" + rowToDelete));
    table.delete(delete);
    System.out.println("Deleted all versions for a given key successfully");

    if (checkSet != null)
      checkSet.clear();

    System.out.println("Waiting 3 seconds for gc...");
    try {
      System.gc();
      Thread.sleep(3000);
    } catch (InterruptedException iexc) {
    }

    int newCount = 0;

    // Run another scan on the modified table
    if (numScanThreads <= 1) {
      newCount = doScan(table, scanBatch, batching, unordered, scan_keys, set_scan_keys, chunkSize,
          Bytes.toBytes("rowKey" + rowToDelete), checkSet, parallel);
    } else {
      newCount = doScan(table, scanBatch, batching, unordered, scan_keys, set_scan_keys, chunkSize,
          Bytes.toBytes("rowKey" + rowToDelete), checkSet, parallel, numScanThreads);
    }

    RuntimeException endException = null;

    if (newCount != count - 1) {
      System.out.println("Scan after delete test FAILED, got " + newCount + " rows, expected "
          + (count - 1) + " rows");
      endException = new RuntimeException("Scan after delete test FAILED, got " + newCount
          + " rows, expected " + (count - 1) + " rows");
    }

    cleanUp(clientCache, admin, tableName, cleanup, endException);
  }
}
