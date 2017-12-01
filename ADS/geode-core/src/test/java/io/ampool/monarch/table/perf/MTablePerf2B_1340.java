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
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MTablePerf2B_1340 {

  private static int NUM_OF_COLUMNS = 2;
  private static String TABLE_NAME = "2B_ROWS_TABLE";
  private static long NUM_OF_ROWS = 500L;
  private static int PUT_BATCH_SIZE = 100;
  private static Random rand = new Random();
  private static char[] chars =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

  private void createTable(MClientCache clientCache, int index) {
    MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    for (int i = 0; i < NUM_OF_COLUMNS; i++) {
      tableDescriptor.addColumn(Bytes.toBytes("COLUMN" + i));
    }
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS);

    Admin admin = clientCache.getAdmin();
    admin.createTable(TABLE_NAME + "_" + index, tableDescriptor);

  }

  private void deleteTable(MClientCache clientCache) {
    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  public static String getRandomString(final int len) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      char c = chars[rand.nextInt(chars.length)];
      sb.append(c);
    }
    return sb.toString();
  }


  private void populateData(MClientCache clientCache, int index) {
    MTable table = clientCache.getTable(TABLE_NAME + "_" + index);
    List<Put> putList = new ArrayList<>();
    long totalElapsedTime = 0L;
    long batchCount = 0;
    for (long row = 0; row < NUM_OF_ROWS; row++) {

      if (putList.size() == 1000) {
        batchCount++;
        if ((batchCount % 1000) == 0) {
          System.out.println("Batch Put #" + batchCount);
        }

        long time = System.nanoTime();
        table.put(putList);
        totalElapsedTime += (System.nanoTime() - time);
        putList.clear();
      }
      Put singlePut = new Put(Bytes.toBytes(row));
      for (int i = 0; i < NUM_OF_COLUMNS; i++) {
        singlePut.addColumn(Bytes.toBytes("COLUMN" + i), Bytes.toBytes(getRandomString(2)));
      }
      putList.add(singlePut);
    }

    System.out.println("Batch Put #" + batchCount++);
    long time = System.nanoTime();
    table.put(putList);
    totalElapsedTime += (System.nanoTime() - time);
    putList.clear();

    System.out.println(" Put Took "
        + TimeUnit.SECONDS.convert(totalElapsedTime, TimeUnit.NANOSECONDS) + " Seconds ");
  }


  private void runPerf() {
    MClientCache clientCache = new MClientCacheFactory().set("log-file", "/tmp/MTableClient.log")
        .addPoolLocator("127.0.0.1", 10334).create();

    int numOfRegions = Integer.getInteger("num.regions", 100);

    long elapsedTime = 0;
    for (int i = 0; i < numOfRegions; i++) {
      System.out.println("Table -> " + i);
      long x = System.nanoTime();
      createTable(clientCache, i);
      elapsedTime += (System.nanoTime() - x);
      // populateData(clientCache, i );
    }
    System.out.println("Time Taken => " + elapsedTime + " Seconds");
    System.out.println("Time Taken => " + (double) elapsedTime / 1000000000.0 + " Seconds");

    // deleteTable(clientCache);

    clientCache.close();

  }


  public static void main(String[] args) {
    new MTablePerf2B_1340().runPerf();
  }

}
