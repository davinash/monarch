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
package io.ampool.monarch.table.perf;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MTablePerfSML {
  public static List<byte[]> allKeys = null;

  private static void generateData(PerfDataSize dataSize, long numOfRows) throws IOException {

    File f = new File(dataSize.getFileName());
    FileOutputStream fos = new FileOutputStream(f);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

    StringBuilder sb = new StringBuilder();
    int eachColumnSize = dataSize.getBytesPerRow() / dataSize.getNumOfColumns() + 1;
    for (int i = 0; i < numOfRows; i++) {
      for (int col = 0; col < dataSize.getNumOfColumns(); col++) {
        sb.append(MTablePerf.getRandomString(eachColumnSize)).append('|');
      }
      bw.write(sb.toString());
      bw.newLine();
      sb.setLength(0);
    }
    bw.close();
  }

  private static void doPuts(PerfDataSize dataSize, MTable table) throws IOException {

    FileInputStream inputStream = null;
    java.util.Scanner sc = null;
    List<Put> listOfPuts = new ArrayList<>();
    int totalCount = 0;
    long totalElapsedTime = 0L;

    try {
      inputStream = new FileInputStream(dataSize.getFileName());
      sc = new java.util.Scanner(inputStream, "UTF-8");
      while (sc.hasNextLine()) {
        String line = sc.nextLine();
        String[] ar = line.split("\\|");
        byte[] rowKey = allKeys.get(totalCount);
        Put singlePut = new Put(rowKey);
        for (int i = 0; i < dataSize.getNumOfColumns(); i++) {
          singlePut.addColumn(Bytes.toBytes("COLUMN" + i), Bytes.toBytes(ar[i]));
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
      // note that Scanner suppresses exceptions
      if (sc.ioException() != null) {
        throw sc.ioException();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
      if (sc != null) {
        sc.close();
      }
    }
    if (listOfPuts.size() != 0) {
      long time = System.nanoTime();
      table.put(listOfPuts);
      totalElapsedTime += (System.nanoTime() - time);
    }

    System.out.println("------------------- MTABLE  BATCH PUT  [" + dataSize.getName()
        + "] --------------------- ");
    System.out.println("TotalCount         = " + totalCount);
    System.out.println("Time Taken (ms)    = " + (totalElapsedTime / 1_000_000));
  }

  private static void doGets(PerfDataSize dataSize, MTable table) {
    List<Get> listOfGets = new ArrayList<>();
    long totalCount = 0;
    long totalElapsedTime = 0L;

    for (byte[] rowKey : allKeys) {
      Get singleGet = new Get(rowKey);
      for (int i = 0; i < dataSize.getNumOfColumns(); i++) {
        singleGet.addColumn(Bytes.toBytes("COLUMN" + i));
      }
      listOfGets.add(singleGet);
      totalCount++;

      if (listOfGets.size() == 1000) {
        long time = System.nanoTime();
        Row[] results = table.get(listOfGets);
        totalElapsedTime += (System.nanoTime() - time);
        listOfGets.clear();
      }
    }
    if (listOfGets.size() != 0) {
      long time = System.nanoTime();
      Row[] results = table.get(listOfGets);
      listOfGets.clear();
      totalElapsedTime += (System.nanoTime() - time);
    }

    System.out.println("------------------- MTABLE  BATCH GET  [" + dataSize.getName()
        + "] --------------------- ");
    System.out.println("TotalCount         = " + totalCount);
    System.out.println("Time Taken (ms)    = " + (totalElapsedTime / 1_000_000));
  }

  private static void doScan(PerfDataSize dataSize, MTable table) throws IOException {
    long time = System.nanoTime();
    Scan scan = new Scan();
    scan.setClientQueueSize(100000);
    Scanner scanner = table.getScanner(scan);
    Row row = scanner.next();
    long totalCount = 0;
    while (row != null) {
      totalCount++;
      row = scanner.next();
    }
    scanner.close();
    long totalElapsedTime = (System.nanoTime() - time);

    System.out.println(
        "------------------- MTABLE  SCAN  [" + dataSize.getName() + "] --------------------- ");
    System.out.println("TotalCount         = " + totalCount);
    System.out.println("Time Taken (ms)    = " + (totalElapsedTime / 1_000_000));
  }

  private static void runAllOps(PerfDataSize dataSize, MTable table) throws IOException {
    doPuts(dataSize, table);
    doGets(dataSize, table);
    doScan(dataSize, table);
  }


  private static void runPerf(PerfDataSize dataSize, long numOfRows, int numOfPartitions,
      MClientCache clientCache, boolean generateNewData) throws IOException {
    if (generateNewData) {
      System.out.println("Generating new Data ....");
      generateData(dataSize, numOfRows);
    }

    if (allKeys == null || allKeys.isEmpty() || allKeys.size() != numOfRows) {
      System.out.println("Generating Keys with equal distribution ....");
      Map<Integer, List<byte[]>> keysForAllBuckets =
          MTablePerf.getKeysForAllBuckets(numOfPartitions, (int) numOfRows / numOfPartitions);

      allKeys = new ArrayList<byte[]>(keysForAllBuckets.size());
      keysForAllBuckets.forEach((BID, KEY_LIST) -> {
        KEY_LIST.forEach((KEY) -> {
          allKeys.add(KEY);
        });
      });
    }

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int i = 0; i < dataSize.getNumOfColumns(); i++) {
      tableDescriptor.addColumn(Bytes.toBytes("COLUMN" + i));
    }
    tableDescriptor.setRedundantCopies(2);
    tableDescriptor.setMaxVersions(1);

    Admin admin = clientCache.getAdmin();
    String tableName = "Performance";
    MTable table = admin.createTable(tableName, tableDescriptor);

    runAllOps(dataSize, table);

    clientCache.getAdmin().deleteTable(tableName);

  }

  public static void main(String[] args) throws IOException {

    MClientCache clientCache = new MClientCacheFactory().addPoolLocator("127.0.0.1", 10334)
        .set("log-file", "/tmp/MTableClient.log").create();

    boolean generateData = Integer.valueOf(args[1]) == 1 ? true : false;
    System.out.println("Generate Data   = " + generateData);


    runPerf(PerfDataSize.SMALL, 1000000, 100, clientCache, generateData);
    runPerf(PerfDataSize.MEDIUM, 1000000, 100, clientCache, generateData);
    runPerf(PerfDataSize.LARGE, 1000000, 100, clientCache, generateData);

    clientCache.close();
  }
}
