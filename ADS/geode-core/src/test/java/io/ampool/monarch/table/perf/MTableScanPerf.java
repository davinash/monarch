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

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCacheFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class MTableScanPerf {

  private static int NUM_OF_COLUMNS = 8;
  private static String TABLE_NAME = "2B_ROWS_TABLE";
  private static long NUM_OF_ROWS = 10_000_000L;
  private static int PUT_BATCH_SIZE = 1000;
  private static int DATA_SIZE = 10;


  private void createTable(MClientCache clientCache) {
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int i = 0; i < NUM_OF_COLUMNS; i++) {
      tableDescriptor.addColumn(Bytes.toBytes("COLUMN" + i));
    }
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setMaxVersions(1);

    Admin admin = clientCache.getAdmin();
    admin.createTable(TABLE_NAME, tableDescriptor);

  }

  private void deleteTable(MClientCache clientCache) {
    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  private Random rand = new Random();

  private long nextLong(final long lower, final long upper) throws IllegalArgumentException {
    if (lower >= upper) {
      throw new IllegalArgumentException("Wrong Input");
    }
    final long max = (upper - lower) + 1;
    if (max <= 0) {
      while (true) {
        final long r = rand.nextLong();
        if (r >= lower && r <= upper) {
          return r;
        }
      }
    } else if (max < Integer.MAX_VALUE) {
      return lower + rand.nextInt((int) max);
    } else {
      return lower + nextLong(max);
    }
  }

  private long nextLong(final long n) throws IllegalArgumentException {
    if (n > 0) {
      final byte[] byteArray = new byte[8];
      long bits;
      long val;
      do {
        rand.nextBytes(byteArray);
        bits = 0;
        for (final byte b : byteArray) {
          bits = (bits << 8) | (((long) b) & 0xffL);
        }
        bits &= 0x7fffffffffffffffL;
        val = bits % n;
      } while (bits - val + (n - 1) < 0);
      return val;
    }
    throw new IllegalArgumentException("something wrong ");
  }


  private List<byte[]> getKeysInRange(byte[] start, byte[] stop, long numOfKeys) {
    byte[] aPadded;
    byte[] bPadded;
    Set<byte[]> result = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    if (start.length < stop.length) {
      aPadded = Bytes.padTail(start, stop.length - start.length);
      bPadded = stop;
    } else if (stop.length < start.length) {
      aPadded = start;
      bPadded = Bytes.padTail(stop, start.length - stop.length);
    } else {
      aPadded = start;
      bPadded = stop;
    }
    if (Bytes.compareTo(aPadded, bPadded) >= 0) {
      throw new IllegalArgumentException("b <= a");
    }
    if (numOfKeys <= 0) {
      throw new IllegalArgumentException("numOfKeys cannot be <= 0");
    }

    byte[] prependHeader = {1, 0};
    final BigInteger startBI = new BigInteger(Bytes.add(prependHeader, aPadded));
    final BigInteger stopBI = new BigInteger(Bytes.add(prependHeader, bPadded));

    BigInteger diffBI = stopBI.subtract(startBI);
    long difference = diffBI.longValue();
    if (diffBI.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0) {
      difference = Long.MAX_VALUE;
    }
    byte[] padded = null;
    for (int i = 0; i < numOfKeys; i++) {
      do {
        BigInteger keyBI = startBI.add(BigInteger.valueOf(nextLong(0, difference)));
        padded = keyBI.toByteArray();
        if (padded[1] == 0)
          padded = Bytes.tail(padded, padded.length - 2);
        else
          padded = Bytes.tail(padded, padded.length - 1);
      } while (!result.add(padded));
    }
    return new ArrayList<>(result);
  }


  private Map<Integer, List<byte[]>> getKeysForAllBuckets(int numOfPartitions,
      long numOfKeysEachPartition) {
    Map<Integer, Pair<byte[], byte[]>> splitsMap =
        MTableUtils.getUniformKeySpaceSplit(numOfPartitions);
    // assertEquals(numOfPartitions, splitsMap.size());
    Map<Integer, List<byte[]>> bucket2KeysMap = new HashMap<>();

    splitsMap.forEach((K, V) -> {
      List<byte[]> keysInRange =
          getKeysInRange(V.getFirst(), V.getSecond(), numOfKeysEachPartition);
      // keysInRange.forEach((B) -> allKeys.add(B));
      bucket2KeysMap.put(K, keysInRange);
    });

    return bucket2KeysMap;
  }

  private List<byte[]> populateData(MClientCache clientCache) {
    MTable table = clientCache.getTable(TABLE_NAME);
    List<Put> putList = new ArrayList<>();
    long totalElapsedTime = 0L;
    long batchCount = 0;

    byte[] data = new byte[DATA_SIZE];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(),
            1 /* NUM_OF_ROWS / table.getTableDescriptor().getTotalNumOfSplits() */);

    List<byte[]> allKeys = new ArrayList<byte[]>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> {
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    for (byte[] key : allKeys) {
      if (putList.size() == PUT_BATCH_SIZE) {
        batchCount++;
        if ((batchCount % PUT_BATCH_SIZE) == 0) {
          System.out.println("Batch Put #" + batchCount);
        }

        long time = System.nanoTime();
        table.put(putList);
        totalElapsedTime += (System.nanoTime() - time);
        putList.clear();
      }

      Put singlePut = new Put(key);
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        // make it a bit different each column
        data[colIndex] = (byte) colIndex;
        singlePut.addColumn(Bytes.toBytes("COLUMN" + colIndex), data);
      }
      putList.add(singlePut);
    }

    System.out.println("Batch Put #" + batchCount++);
    long time = System.nanoTime();
    table.put(putList);
    totalElapsedTime += (System.nanoTime() - time);
    putList.clear();

    System.out
        .println("PUT Took " + TimeUnit.NANOSECONDS.convert(totalElapsedTime, TimeUnit.NANOSECONDS)
            + " Nano Seconds for " + allKeys.size() + " Rows");

    System.out
        .println("PUT Took " + TimeUnit.SECONDS.convert(totalElapsedTime, TimeUnit.NANOSECONDS)
            + " Seconds for " + allKeys.size() + " Rows");

    return allKeys;
  }


  private void doGetAll(List<byte[]> allKeys) {
    Region geodeCacheRegion = ClientCacheFactory.getAnyInstance().getRegion(TABLE_NAME);
    Set<MTableKey> getAllKeys = new LinkedHashSet<>(5000);
    long totalElapsedTime = 0L;
    long counter = 0;
    for (byte[] key : allKeys) {
      if (getAllKeys.size() == 5000) {
        long starttime = System.nanoTime();
        Map result = geodeCacheRegion.getAll(getAllKeys);
        totalElapsedTime += System.nanoTime() - starttime;
        counter += result.size();
        getAllKeys.clear();
      }
      getAllKeys.add(new MTableKey(key));
    }
    if (getAllKeys.size() != 0) {
      long starttime = System.nanoTime();
      Map result = geodeCacheRegion.getAll(getAllKeys);
      totalElapsedTime += System.nanoTime() - starttime;
      counter += result.size();
      getAllKeys.clear();
    }

    System.out.println(
        "GetAll Took " + TimeUnit.NANOSECONDS.convert(totalElapsedTime, TimeUnit.NANOSECONDS)
            + " Nano Seconds for  " + counter + " Records");

    System.out
        .println("GetAll Took " + TimeUnit.SECONDS.convert(totalElapsedTime, TimeUnit.NANOSECONDS)
            + " Seconds for  " + counter + " Records");
  }

  private void doScan(MClientCache clientCache) {
    MTable table = clientCache.getTable(TABLE_NAME);
    Scan scan = new Scan();
    scan.enableBatchMode();
    Scanner scanner = table.getScanner(scan);
    Iterator itr = scanner.iterator();
    System.out.println("start scan...");
    int count = 0;
    long starttime = System.nanoTime();
    while (itr.hasNext()) {
      count += 1;
      Row res = (Row) itr.next();
      byte[] key = res.getRowId();
    }
    long totalElapsedTime = System.nanoTime() - starttime;
    System.out
        .println("SCAN Took " + TimeUnit.NANOSECONDS.convert(totalElapsedTime, TimeUnit.NANOSECONDS)
            + " Nano Seconds for  " + count + " Records");

    System.out
        .println("SCAN Took " + TimeUnit.SECONDS.convert(totalElapsedTime, TimeUnit.NANOSECONDS)
            + " Seconds for  " + count + " Records");
  }

  private void runPerf() {
    MClientCache clientCache = new MClientCacheFactory().set("log-file", "/tmp/MTableClient.log")
        .addPoolLocator("127.0.0.1", 10334).create();

    createTable(clientCache);
    List<byte[]> allKeys = populateData(clientCache);
    for (int i = 0; i < 10; i++) {
      doScan(clientCache);
    }

    for (int i = 0; i < 10; i++) {
      doGetAll(allKeys);
    }
    deleteTable(clientCache);
    clientCache.close();
  }

  public static void main(String[] args) {
    new MTableScanPerf().runPerf();
  }

}
