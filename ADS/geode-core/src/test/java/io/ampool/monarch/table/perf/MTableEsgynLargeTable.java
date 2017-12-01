package io.ampool.monarch.table.perf;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.MConfiguration;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.types.BasicTypes;

/**
 * Test case trying to reproduce the Esgyn Issue See GEN-778
 */


/*
 * SCHEMA OF THE TABLE CREATE TABLE TRAFODION.JAVABENCH.ED_TABLE_01 ( PRIM_KEY INT NO DEFAULT NOT
 * NULL NOT DROPPABLE SERIALIZED , PRIM_VALUE VARCHAR(22) CHARACTER SET ISO88591 COLLATEDEFAULT NO
 * DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_01 INT NO DEFAULT NOT NULL NOT DROPPABLE
 * SERIALIZED, FOREIGN_KEY_02 INT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_03 INT
 * NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_04 INT NO DEFAULT NOT NULL NOT
 * DROPPABLE SERIALIZED, FOREIGN_KEY_05 INT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED,
 * FOREIGN_KEY_06 INT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_07 INT NO DEFAULT
 * NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_08 INT NO DEFAULT NOT NULL NOT DROPPABLE
 * SERIALIZED, FOREIGN_KEY_09 INT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_11 INT
 * NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_12 INT NO DEFAULT NOT NULL NOT
 * DROPPABLE SERIALIZED, FOREIGN_KEY_13 INT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED,
 * FOREIGN_KEY_14 INT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_15 INT NO DEFAULT
 * NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_16 INT NO DEFAULT NOT NULL NOT DROPPABLE
 * SERIALIZED, FOREIGN_KEY_17 INT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_18 INT
 * NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_19 INT NO DEFAULT NOT NULL NOT
 * DROPPABLE SERIALIZED, FOREIGN_KEY_20 INT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED,
 * FOREIGN_KEY_21 INT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_22 INT NO DEFAULT
 * NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_23 INT NO DEFAULT NOT NULL NOT DROPPABLE
 * SERIALIZED, FOREIGN_KEY_24 INT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_25 INT
 * NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_26 INT NO DEFAULT NOT NULL NOT
 * DROPPABLE SERIALIZED, FOREIGN_KEY_27 INT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED,
 * FOREIGN_KEY_28 INT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_29 INT NO DEFAULT
 * NOT NULL NOT DROPPABLE SERIALIZED, FOREIGN_KEY_30 INT NO DEFAULT NOT NULL NOT DROPPABLE
 * SERIALIZED, COLUMN_01 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT
 * DROPPABLE SERIALIZED, COLUMN_02 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT
 * NULL NOT DROPPABLE SERIALIZED, COLUMN_03 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO
 * DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_04 VARCHAR(22) CHARACTER SET ISO88591 COLLATE
 * DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_05 VARCHAR(22) CHARACTER SET
 * ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_06 VARCHAR(22)
 * CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_07
 * VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED,
 * COLUMN_08 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE
 * SERIALIZED, COLUMN_09 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT
 * DROPPABLE SERIALIZED, COLUMN_11 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT
 * NULL NOT DROPPABLE SERIALIZED, COLUMN_12 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO
 * DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_13 VARCHAR(22) CHARACTER SET ISO88591 COLLATE
 * DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_14 VARCHAR(22) CHARACTER SET
 * ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_15 VARCHAR(22)
 * CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_16
 * VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED,
 * COLUMN_17 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE
 * SERIALIZED, COLUMN_18 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT
 * DROPPABLE SERIALIZED, COLUMN_19 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT
 * NULL NOT DROPPABLE SERIALIZED, COLUMN_20 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO
 * DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_21 VARCHAR(22) CHARACTER SET ISO88591 COLLATE
 * DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_22 VARCHAR(22) CHARACTER SET
 * ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_23 VARCHAR(22)
 * CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_24
 * VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED,
 * COLUMN_25 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE
 * SERIALIZED, COLUMN_26 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT
 * DROPPABLE SERIALIZED, COLUMN_27 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO DEFAULT NOT
 * NULL NOT DROPPABLE SERIALIZED, COLUMN_28 VARCHAR(22) CHARACTER SET ISO88591 COLLATE DEFAULT NO
 * DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_29 VARCHAR(22) CHARACTER SET ISO88591 COLLATE
 * DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, COLUMN_30 VARCHAR(22) CHARACTER SET
 * ISO88591 COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE SERIALIZED, INTEGER_FACT INT NO
 * DEFAULT NOT NULL NOT DROPPABLE PRIMARY KEY (PRIM_KEY ASC) ) SALT USING 4 PARTITIONS
 */

public class MTableEsgynLargeTable {

  private static final int NUM_OF_COLUMNS = 64;
  private static final String TABLE_NAME = "Esgyn_Large_Table";

  private static final Map<String, BasicTypes> columnNameToTypeMap = new HashMap<>();

  static {
    columnNameToTypeMap.put("PRIM_KEY", BasicTypes.INT);
    columnNameToTypeMap.put("PRIM_VALUE", BasicTypes.CHARS);
    for (int i = 1; i <= 30; i++) {
      columnNameToTypeMap.put("FOREIGN_KEY_" + i, BasicTypes.INT);
    }
    for (int i = 1; i <= 30; i++) {
      columnNameToTypeMap.put("COLUMN_" + i, BasicTypes.CHARS);
    }
    columnNameToTypeMap.put("INTEGER_FACT", BasicTypes.INT);
  }


  private MTable createTable(final MClientCache clientCache, long tableNumber) {
    MTableDescriptor tableDescriptor = new MTableDescriptor();

    columnNameToTypeMap.forEach((COLUMN_NAME, COLUMN_TYPE) -> {
      tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME), COLUMN_TYPE);
    });
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setMaxVersions(1);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    tableDescriptor.setTotalNumOfSplits(4);

    Admin admin = clientCache.getAdmin();
    return admin.createTable(TABLE_NAME + "_" + tableNumber, tableDescriptor);

  }

  private Random rand = new Random();

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
        if (padded[1] == 0) {
          padded = Bytes.tail(padded, padded.length - 2);
        } else {
          padded = Bytes.tail(padded, padded.length - 1);
        }
      } while (!result.add(padded));
    }
    return new ArrayList<>(result);
  }

  private Map<Integer, List<byte[]>> getKeysForAllBuckets(long numOfPartitions,
      long numOfKeysEachPartition) {
    Map<Integer, Pair<byte[], byte[]>> splitsMap =
        MTableUtils.getUniformKeySpaceSplit((int) numOfPartitions);
    Map<Integer, List<byte[]>> bucket2KeysMap = new HashMap<>();

    splitsMap.forEach((K, V) -> {
      List<byte[]> keysInRange =
          getKeysInRange(V.getFirst(), V.getSecond(), numOfKeysEachPartition);
      bucket2KeysMap.put(K, keysInRange);
    });

    return bucket2KeysMap;
  }

  final String columnValue = "abcdefghijklmnopqrstuv";

  private void populateTable(final MTable table, final long maxNumberOfRows) {

    final Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(4, maxNumberOfRows / 4);
    List<byte[]> allKeys = new ArrayList<>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, K) -> K.forEach((KEY) -> allKeys.add(KEY)));

    long rowIndex = 0;
    List<Put> listOfPuts = new ArrayList<>();
    for (final byte[] singleKey : allKeys) {
      Put putRecord = new Put(singleKey);
      for (final Entry<String, BasicTypes> column : columnNameToTypeMap.entrySet()) {
        if (column.getValue() == BasicTypes.INT) {
          putRecord.addColumn(column.getKey(), 5);
        } else if (column.getValue() == BasicTypes.CHARS) {
          putRecord.addColumn(column.getKey(), columnValue);
        } else {
          throw new RuntimeException("Invalid Column Type found for this test");
        }
      }
      listOfPuts.add(putRecord);
      if (listOfPuts.size() == 100) {
        table.put(listOfPuts);
        listOfPuts.clear();
      }
      rowIndex++;
    }

    if (!listOfPuts.isEmpty()) {
      table.put(listOfPuts);
    }

  }

  private void run() {
    MClientCache clientCache = new MClientCacheFactory().set("log-file", "/tmp/MTableClient.log")
        .addPoolLocator("127.0.0.1", 10334).create();

    for (int i = 10; i < 24; i++) {
      long maxNumberOfRows = (long) Math.pow(2, i);
      System.out.println(" Populating MTable with " + maxNumberOfRows + " Rows ");
      MTable table = createTable(clientCache, maxNumberOfRows);
      populateTable(table, maxNumberOfRows);

      Scanner scanner = table.getScanner(new Scan());
      Row row = scanner.next();

      long rowScanned = 0;
      while (row != null) {
        // System.out.println("KEY -> " + Arrays.toString(row.getRowId()));
        rowScanned++;
        row = scanner.next();
      }
      System.out.println("Number of rows scanned -> " + rowScanned);
      if (rowScanned != maxNumberOfRows) {
        System.out.println("Scan Did not return expected number of rows");
        System.exit(1);
      }
      scanner.close();
      clientCache.getAdmin().deleteTable(TABLE_NAME + "_" + maxNumberOfRows);
    }
    clientCache.close();
  }


  public static void main(String[] args) {
    new MTableEsgynLargeTable().run();

  }


}
