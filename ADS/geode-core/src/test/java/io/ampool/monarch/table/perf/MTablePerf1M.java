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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MTablePerf1M {
  private static final int NUM_OF_COLUMNS = 10;
  private static final String TABLE_NAME = "Table_1M";
  private static long NUM_OF_ROWS = 0;

  private MTable createTable(MClientCache clientCache) {
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int i = 0; i < NUM_OF_COLUMNS; i++) {
      tableDescriptor.addColumn(Bytes.toBytes("COLUMN" + i));
    }
    tableDescriptor.setRedundantCopies(2);
    tableDescriptor.setMaxVersions(1);
    tableDescriptor.setTotalNumOfSplits(4);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);

    Admin admin = clientCache.getAdmin();
    return admin.createTable(TABLE_NAME, tableDescriptor);
  }

  private void deleteTable(MClientCache clientCache) {
    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  private void doRun(String locatorHost, int locatorPort, long startRowIndex, long endRowIndex,
      boolean createTable) {
    MClientCache clientCache = new MClientCacheFactory().addPoolLocator(locatorHost, locatorPort)
        .set("log-file", "/tmp/MTableClient.log").create();
    System.out.println("Got the connection with Locator [" + locatorHost + ":" + locatorPort + "]");

    MTable table = null;

    if (createTable) {
      System.out.println("Creating a new Table");
      table = createTable(clientCache);
    } else {
      table = clientCache.getTable(TABLE_NAME);
    }
    System.out.println("Populating Data from Index " + startRowIndex + " to " + endRowIndex);
    populateData(table, startRowIndex, endRowIndex);

    // deleteTable(clientCache);
    clientCache.close();

  }

  private void populateData(MTable table, long startRowIndex, long endRowIndex) {
    List<Put> putList = new ArrayList<>();
    long totalElapsedTime = 0L;
    long rowCount = 0;
    long batchCount = 0;
    for (long row = startRowIndex; row < (endRowIndex - startRowIndex); row++) {
      if (putList.size() == 1000) {
        batchCount++;
        if (batchCount % 1000 == 0) {
          System.out.println("Processed Batch # " + batchCount);
        }
        long time = System.nanoTime();
        table.put(putList);
        totalElapsedTime += (System.nanoTime() - time);
        rowCount += putList.size();
        putList.clear();
      }
      Put singlePut = new Put(Bytes.toBytes(row));
      for (int i = 0; i < NUM_OF_COLUMNS; i++) {
        singlePut.addColumn(Bytes.toBytes("COLUMN" + i), Bytes.toBytes("FIXED_VALUE"));
      }
      putList.add(singlePut);
    }

    System.out.println("Processed Batch # " + batchCount);
    long time = System.nanoTime();
    table.put(putList);
    totalElapsedTime += (System.nanoTime() - time);
    rowCount += putList.size();
    putList.clear();

    System.out
        .println(" Put Took " + TimeUnit.SECONDS.convert(totalElapsedTime, TimeUnit.NANOSECONDS)
            + " Seconds " + " for " + rowCount + " Rows ");

  }

  public static void main(String[] args) {
    if (args.length < 5) {
      System.err.println(
          "Arguments are <LocatorHost> <LocatorPort> <StartRowIndex> <EndRowIndex> [1 ( to Create Table ) | 0]");
      System.exit(1);
    }
    String locatorHost = args[0];
    int locatorPort = Integer.parseInt(args[1]);
    long startRowIndex = Long.parseLong(args[2]);
    long endRowIndex = Long.parseLong(args[3]);
    boolean createTable = Integer.parseInt(args[4]) == 1 ? true : false;

    new MTablePerf1M().doRun(locatorHost, locatorPort, startRowIndex, endRowIndex, createTable);
  }

}
