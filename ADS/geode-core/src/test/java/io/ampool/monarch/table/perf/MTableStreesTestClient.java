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
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class MTableStreesTestClient {
  private static final String TABLE_NAME = "MTableStreesTestClient";
  private static final int NUM_OF_COLUMNS = 10;
  private static final String COLUMN_NAME_PREFIX = "Column-";
  private static final String VALUE_PREFIX = "Value-";
  private static int NUM_OF_ROWS = 0;
  private List<byte[]> allKeys = null;
  private static final String BLACK_BOARD_REGION_1 = "BLACK_BOARD_REGION_1";
  private static final String BLACK_BOARD_REGION_2 = "BLACK_BOARD_REGION_2";

  private boolean crud_mode = false;
  private boolean scan_mode = false;

  private Random rand = new Random();

  private Region<Integer, ByteArrayKey> blackBoardRegion = null;
  private Region<ByteArrayKey, Integer> blackBoardRegion2 = null;

  public static void usage(String issue) {
    System.out.println("");
    if (issue != null) {
      System.out.println("Error in parsing arguments near " + issue);
      System.out.println("");
    }
  }


  public long nextLong(final long lower, final long upper) throws IllegalArgumentException {
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

  public List<byte[]> getKeysInRange(byte[] start, byte[] stop, int numOfKeys) {
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

  public Map<Integer, List<byte[]>> getKeysForAllBuckets(int numOfPartitions,
      int numOfKeysEachPartition) {
    Map<Integer, Pair<byte[], byte[]>> splitsMap =
        MTableUtils.getUniformKeySpaceSplit(numOfPartitions);
    Map<Integer, List<byte[]>> bucket2KeysMap = new HashMap<>();

    splitsMap.forEach((K, V) -> {
      List<byte[]> keysInRange =
          getKeysInRange(V.getFirst(), V.getSecond(), numOfKeysEachPartition);
      bucket2KeysMap.put(K, keysInRange);
    });
    return bucket2KeysMap;
  }

  private MTable createTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex),
          BasicTypes.STRING);
    }
    Admin admin = clientCache.getAdmin();
    return admin.createTable(TABLE_NAME, tableDescriptor);
  }

  private void performRandomScanOperations() throws InterruptedException {
    System.out.println("Client Doing Random SCAN Operations");
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);

    long index = 0;
    while (true) {

      Scan scan = new Scan();
      Scanner scanner = table.getScanner(scan);

      Iterator itr = scanner.iterator();
      System.out.println("start scan... ");

      while (itr.hasNext()) {
        Row result = (Row) itr.next();
        byte[] key = result.getRowId();
        System.out.println("Scan Return Row with Key -> " + Arrays.toString(key));

        if (NUM_OF_COLUMNS != result.size()) {
          System.out.println("NUM_OF_COLUMNS Mismatch");
          System.out.println("Expected NUM_OF_COLUMNS = " + NUM_OF_COLUMNS);
          System.out.println("Actual   NUM_OF_COLUMNS = " + result.size());
          System.exit(1);
        }
        if (result.isEmpty()) {
          System.out.println("Result Set is Empty");
          System.exit(1);
        }

        int columnIndex = 0;
        List<Cell> row = result.getCells();
        for (Cell cell : row) {
          byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
          String exptectedValue = null;
          String columnValue = (String) cell.getColumnValue();

          // System.out.println("Column Value -> " + columnValue);
          if (columnValue.contains("1")) {
            exptectedValue = VALUE_PREFIX + 1;
          } else if (columnValue.contains("2")) {
            exptectedValue = VALUE_PREFIX + 2;
          } else if (columnValue.contains("3")) {
            exptectedValue = VALUE_PREFIX + 3;
          } else if (columnValue.contains("4")) {
            exptectedValue = VALUE_PREFIX + 4;
          } else if (columnValue.contains("5")) {
            exptectedValue = VALUE_PREFIX + 5;
          } else if (columnValue.contains("0")) {
            exptectedValue = VALUE_PREFIX + 0;
          }


          // System.out.println("Expected Value -> " + exptectedValue);
          if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
            System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
            System.out.println("actualColumnName   => " + Arrays.toString(cell.getColumnName()));
            System.out.println("Invalid Values for Column Name");
            System.exit(1);
          }
          if (exptectedValue.equals(cell.getColumnValue()) == false) {
            System.out.println("exptectedValue => " + exptectedValue);
            System.out.println("actualValue    => " + cell.getColumnValue());
            System.out.println("Invalid Values for Column Value");
            System.exit(1);
          }
          columnIndex++;
        }
      }
      scanner.close();
      System.out.println("MTableStreesTestClient.performRandomScanOperations. Sleeping ....");
      TimeUnit.MILLISECONDS.sleep(500);
    }
  }

  private void generateOrLoadKeysInMemory(int numOfSplits) throws IOException {
    Map<Integer, List<byte[]>> keysForAllBuckets = getKeysForAllBuckets(numOfSplits, NUM_OF_ROWS);
    this.allKeys = new ArrayList<byte[]>(numOfSplits * NUM_OF_ROWS);
    keysForAllBuckets.forEach((BID, KEY_LIST) -> {
      KEY_LIST.forEach((KEY) -> {
        this.allKeys.add(KEY);
      });
    });
  }

  private int getNextRandomInRange(int min, int max) {
    return this.rand.nextInt(max - min + 1) + min;
  }

  private void performPUTOperation(final MTable table) throws InterruptedException {
    int valueSuffix = getNextRandomInRange(0, 4);
    byte[] key = allKeys.get(getNextRandomInRange(0, allKeys.size() - 1));
    System.out.println("PUT OPERATION Key ->    " + Arrays.toString(key));
    ByteArrayKey keyForBlackBoardRegion = new ByteArrayKey(key);

    Put record = new Put(key);
    for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
      Object v = new String(VALUE_PREFIX + valueSuffix);
      record.addColumn(COLUMN_NAME_PREFIX + columnIndex, v);
    }
    table.put(record);
    this.blackBoardRegion.put(valueSuffix, keyForBlackBoardRegion);
    this.blackBoardRegion2.put(keyForBlackBoardRegion, valueSuffix);
  }

  private void performGETOperation(final MTable table) {
    int nextKeyIndex = getNextRandomInRange(0, allKeys.size() - 1);
    byte[] key = allKeys.get(nextKeyIndex);
    ByteArrayKey tableKeyFromBlackBoard = this.blackBoardRegion.get(nextKeyIndex);
    if (tableKeyFromBlackBoard != null) {
      System.out.println("GET OPERATION Key ->    " + Arrays.toString(key));
      Get get = new Get(key);
      Row result = table.get(get);
      if (NUM_OF_COLUMNS != result.size()) {
        System.out.println("NUM_OF_COLUMNS Mismatch");
        System.out.println("Expected NUM_OF_COLUMNS = " + NUM_OF_COLUMNS);
        System.out.println("Actual   NUM_OF_COLUMNS = " + result.size());
        System.exit(1);
      }

      if (result.isEmpty()) {
        System.out.println("Result Set is Empty");
        System.exit(1);
      }

      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (Cell cell : row) {

        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + nextKeyIndex);

        if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out.println("actualColumnName   => " + Arrays.toString(cell.getColumnName()));
          System.out.println("Invalid Values for Column Name");
          System.exit(1);
        }
        if (!Bytes.equals(exptectedValue, (byte[]) cell.getColumnValue())) {
          System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
          System.out
              .println("actualValue    => " + Arrays.toString((byte[]) cell.getColumnValue()));
          System.out.println("Invalid Values for Column Value");
          System.exit(1);
        }
        columnIndex++;
      }
    }
  }

  private void performDELETEOperation(MTable table) {
    int nextKeyIndex = getNextRandomInRange(0, allKeys.size() - 1);
    byte[] key = allKeys.get(nextKeyIndex);
    System.out.println("DELETE OPERATION Key -> " + Arrays.toString(key));
    ByteArrayKey tableKeyFromBlackBoard = this.blackBoardRegion.get(nextKeyIndex);
    if (tableKeyFromBlackBoard != null) {
      Delete deleteRecord = new Delete(key);
      table.delete(deleteRecord);
      this.blackBoardRegion.destroy(nextKeyIndex);
      this.blackBoardRegion2.destroy(tableKeyFromBlackBoard);
    }
  }

  private void performRandomCrudOperations() throws IOException, InterruptedException {
    System.out.println("Client Doing Random CRUD Operations");


    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    if (table == null) {
      table = createTable();
      System.out.println("Table is Created");
    }
    System.out.println("Generating Random keys for equal Distribution");
    generateOrLoadKeysInMemory(table.getTableDescriptor().getTotalNumOfSplits());

    int putCounter = 0;
    while (true) {
      int OP_CODE = getNextRandomInRange(1, 3);
      switch (OP_CODE) {
        case 1: // PUT
          performPUTOperation(table);
          putCounter++;
          break;
        case 2: // GET
          // performGETOperation(table);
          break;
        case 3: // DELETE
          // if (putCounter % 10 == 0) {
          // performDELETEOperation(table);
          // putCounter = 0;
          // }
          break;
        default:
          throw new RuntimeException("Invalid Op code Generated");
      }
      TimeUnit.MILLISECONDS.sleep(500);
    }
  }

  private void runTest(String[] args) throws IOException, InterruptedException {
    if (args.length > 0) {
      int index = 0;
      try {
        while (index < args.length) {
          if (args[index].equals("--crud-mode")) {
            this.crud_mode = true;
          } else if (args[index].equals("--scan-mode")) {
            this.scan_mode = true;
          } else if (args[index].equals("--num-of-rows")) {
            this.NUM_OF_ROWS = new Integer(args[index + 1]).intValue();
            index += 1;
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
    // this.scan_mode = true;
    if (this.crud_mode == false && this.scan_mode == false) {
      System.out
          .println("\nError: Need to spcify one of the option \"--crud-mode\" or \"--scan-mode\"");
      usage(null);
    }
    if (this.crud_mode == true && this.scan_mode == true) {
      System.out.println("\nError: Cannot have both the option \"--crud-mode\" or \"--scan-mode\"");
      usage(null);
    }


    if (this.crud_mode == true) {
      performRandomCrudOperations();
    } else if (this.scan_mode == true) {
      performRandomScanOperations();
    }
  }

  public MTableStreesTestClient() {

    MClientCache clientCache = new MClientCacheFactory().set("log-file", "/tmp/MTableClient.log")
        .addPoolLocator("127.0.0.1", 10334).create();

    ClientRegionFactory<Integer, ByteArrayKey> crf =
        ((MonarchCacheImpl) MClientCacheFactory.getAnyInstance())
            .createClientRegionFactory(ClientRegionShortcut.PROXY);
    this.blackBoardRegion = crf.create(BLACK_BOARD_REGION_1);

    ClientRegionFactory<ByteArrayKey, Integer> crf1 =
        ((MonarchCacheImpl) MClientCacheFactory.getAnyInstance())
            .createClientRegionFactory(ClientRegionShortcut.PROXY);
    this.blackBoardRegion2 = crf1.create(BLACK_BOARD_REGION_2);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    new MTableStreesTestClient().runTest(args);
  }
}
