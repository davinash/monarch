package io.ampool.monarch.table.perf;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.MTableImpl;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientRegionShortcut;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class MTablePerf {

  private static final String DATA_INPUT_FILE = "/tools/perf/scan.data";

  private static Random rand = new Random();
  private static char[] chars =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();


  private static long nextLong(final long lower, final long upper) throws IllegalArgumentException {
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


  private static long nextLong(final long n) throws IllegalArgumentException {
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

  private static List<byte[]> getKeysInRange(byte[] start, byte[] stop, int numOfKeys) {
    byte[] aPadded;
    byte[] bPadded;
    List<byte[]> result = new ArrayList<>();
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

    for (int i = 0; i < numOfKeys; i++) {
      BigInteger keyBI = startBI.add(BigInteger.valueOf(nextLong(0, difference)));
      byte[] padded = keyBI.toByteArray();
      if (padded[1] == 0)
        padded = Bytes.tail(padded, padded.length - 2);
      else
        padded = Bytes.tail(padded, padded.length - 1);

      result.add(padded);
    }
    return result;
  }

  public static Map<Integer, List<byte[]>> getKeysForAllBuckets(int numOfPartitions,
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

  public static String getRandomString(final int len) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      char c = chars[rand.nextInt(chars.length)];
      sb.append(c);
    }
    return sb.toString();
  }

  private static void generateData() throws IOException {
    int numOfRecords = 113 * 10000;
    int numOfColumns = 10;


    File f = new File(DATA_INPUT_FILE);

    FileOutputStream fos = new FileOutputStream(f);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

    StringBuilder sb = new StringBuilder();
    for (int r = 0; r < numOfRecords - 1; r++) {
      for (int i = 0; i < numOfColumns; i++) {
        sb.append(getRandomString(100)).append((','));
      }
      bw.write(sb.toString());
      bw.newLine();
      sb.setLength(0);
    }
    bw.close();

  }

  private static List<byte[]> getAllKeys() {
    Map<Integer, List<byte[]>> keysForAllBuckets = getKeysForAllBuckets(113, 10000);
    List<byte[]> allKeys = new ArrayList<byte[]>(keysForAllBuckets.size());

    keysForAllBuckets.forEach((BID, KEY_LIST) -> {
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });
    return allKeys;
  }



  private static void doScan() throws IOException {

    MTable table = MClientCacheFactory.getAnyInstance().getTable("Performance");
    long scanStartTime = System.nanoTime();
    Scan scan = new Scan();
    // scan.setCaching(1000);
    scan.setClientQueueSize(10000);
    Scanner scanner = table.getScanner(scan);
    Row result = scanner.next();
    int totalCount = 0;
    while (result != null) {
      totalCount++;
      result = scanner.next();
    }
    long totalScanTime = (System.nanoTime() - scanStartTime);
    System.out.println(
        "TotalCount= " + totalCount + "; TimeInMScan (ms)\t= " + (totalScanTime / 1_000_000));
    scanner.close();
  }

  private static void doBatchGets(MTable table, List<byte[]> allKeys, int rowBatchPuts)
      throws IOException {
    BufferedReader in = new BufferedReader(new FileReader(DATA_INPUT_FILE));
    long batchGetStartTime = System.nanoTime();
    List<Get> listOfGets = new ArrayList<>();
    int totalCount = 0;

    while (in.readLine() != null) {
      byte[] rowKey = allKeys.get(totalCount);
      Get singleGet = new Get(rowKey);
      for (int i = 0; i < 10; i++) {
        singleGet.addColumn(Bytes.toBytes("COLUMN" + i));
      }
      listOfGets.add(singleGet);
      totalCount++;
      if (listOfGets.size() == 1000) {
        Row[] result = table.get(listOfGets);
        listOfGets.clear();
      }
    }

    if (listOfGets.size() != 0) {
      Row[] result = table.get(listOfGets);
    }
    long totalBatchGetTime = (System.nanoTime() - batchGetStartTime);
    System.out.println("TotalCount= " + totalCount + "; TimeInMGet_BatchMGet (ms)\t= "
        + (totalBatchGetTime / 1_000_000));
    in.close();
  }



  private static void doGets(MTable table, List<byte[]> allKeys, int rows) {
    long readTime = System.nanoTime();
    int readCount = 0;
    for (readCount = 0; readCount < rows; readCount++) {
      Get getRecord = new Get(allKeys.get(readCount));
      Row row = table.get(getRecord);
    }
    long totalReadTime = (System.nanoTime() - readTime);
    System.out.println(
        "TotalCount= " + readCount + "; TimeInGet_MGet (ms)\t= " + (totalReadTime / 1_000_000));
  }

  private static int doPuts(MTable table, List<byte[]> allKeys) throws IOException {
    BufferedReader in = new BufferedReader(new FileReader(DATA_INPUT_FILE));
    String str;
    long time = System.nanoTime();
    int totalCount = 0;

    while ((str = in.readLine()) != null) {
      String[] ar = str.split(",");
      byte[] rowKey = allKeys.get(totalCount);
      Put singlePut = new Put(rowKey);
      for (int i = 0; i < 10; i++) {
        singlePut.addColumn(Bytes.toBytes("COLUMN" + i), Bytes.toBytes(ar[i]));
      }
      table.put(singlePut);
      totalCount++;
    }
    long totalTime = (System.nanoTime() - time);
    System.out.println(
        "TotalCount= " + totalCount + "; TimeInPut_MPut (ms)\t= " + (totalTime / 1_000_000));
    return totalCount;
  }

  private static void doBatchRegionPuts(List<byte[]> allKeys) throws IOException {
    MClientCache clientCache1 = MClientCacheFactory.getAnyInstance();
    Region<Object, Object> plane_geode_region = clientCache1
        .createClientRegionFactory(ClientRegionShortcut.PROXY).create("PLANE_GEODE_REGION");


    BufferedReader in = new BufferedReader(new FileReader(DATA_INPUT_FILE));
    String str;


    Map<ByteArrayKey, byte[]> multiplePuts = new HashMap<>();
    int totalCount = 0;
    long totalBytes = 0L;
    long totalElapsedTime = 0L;
    while ((str = in.readLine()) != null) {
      ByteArrayKey rowKey = new ByteArrayKey(allKeys.get(totalCount));
      byte[] v = Bytes.toBytes(str);

      multiplePuts.put(rowKey, v);
      totalCount++;
      totalBytes += v.length + rowKey.getByteArray().length;

      if (multiplePuts.size() == 1000) {
        long time = System.nanoTime();
        plane_geode_region.putAll(multiplePuts);
        totalElapsedTime += (System.nanoTime() - time);
        multiplePuts.clear();
      }
    }

    if (multiplePuts.size() != 0) {
      long time = System.nanoTime();
      plane_geode_region.putAll(multiplePuts);
      totalElapsedTime += (System.nanoTime() - time);
      multiplePuts.clear();
    }

    long totalBactchPutTimeInSeconds = (totalElapsedTime / 1_000_000_000);
    long bytesPerSeconds = (totalBytes / totalBactchPutTimeInSeconds);

    System.out.println("-------------------    doBatchRegionPuts   --------------------- ");
    System.out.println("TotalCount         = " + totalCount);
    System.out.println("Time Taken (ms)    = " + (totalElapsedTime / 1_000_000));
    System.out.println("TotalBytes         = " + totalBytes);
    System.out.println("Bytes Per Seconds  = " + (bytesPerSeconds / (1024 * 1024)));

    in.close();
  }


  private static int doBatchPutsOrdered(MClientCache clientCache, List<byte[]> allKeys)
      throws IOException {
    BufferedReader in = new BufferedReader(new FileReader(DATA_INPUT_FILE));
    String str;


    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int i = 0; i < 10; i++) {
      tableDescriptor.addColumn(Bytes.toBytes("COLUMN" + i));
    }
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setMaxVersions(1);

    Admin admin = clientCache.getAdmin();
    String tableName = "Performance";
    MTable table = admin.createTable(tableName, tableDescriptor);

    List<Put> listOfPuts = new ArrayList<>();
    int totalCount = 0;
    long totalBytes = 0L;
    long totalElapsedTime = 0L;
    while ((str = in.readLine()) != null) {
      String[] ar = str.split(",");
      byte[] rowKey = allKeys.get(totalCount);
      Put singlePut = new Put(rowKey);
      for (int i = 0; i < 10; i++) {
        singlePut.addColumn(Bytes.toBytes("COLUMN" + i), Bytes.toBytes(ar[i]));
      }
      listOfPuts.add(singlePut);
      totalCount++;

      if (listOfPuts.size() == 1000) {
        long time = System.nanoTime();
        table.put(listOfPuts);
        totalElapsedTime += (System.nanoTime() - time);
        totalBytes += ((MTableImpl) table).getByteArraySizeBatchPut();
        ((MTableImpl) table).resetByteArraySizeBatchPut();
        listOfPuts.clear();
      }
    }

    if (listOfPuts.size() != 0) {
      long time = System.nanoTime();
      table.put(listOfPuts);
      totalElapsedTime += (System.nanoTime() - time);
      totalBytes += ((MTableImpl) table).getByteArraySizeBatchPut();
      ((MTableImpl) table).resetByteArraySizeBatchPut();
    }
    long totalBactchPutTimeInSeconds = (totalElapsedTime / 1_000_000_000);
    long bytesPerSeconds = (totalBytes / totalBactchPutTimeInSeconds);

    System.out.println("-------------------    doBatchPuts MTABLE   --------------------- ");
    System.out.println("TotalCount         = " + totalCount);
    System.out.println("Time Taken (ms)    = " + (totalElapsedTime / 1_000_000));
    System.out.println("TotalBytes         = " + totalBytes);
    System.out.println("Bytes Per Seconds  = " + (bytesPerSeconds / (1024 * 1024)));

    in.close();

    // clientCache.getAdmin().deleteTable(tableName);

    return totalCount;

  }


  private static int doBatchPutsUnOrdered(MClientCache clientCache, List<byte[]> allKeys)
      throws IOException {
    BufferedReader in = new BufferedReader(new FileReader(DATA_INPUT_FILE));
    String str;


    MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    for (int i = 0; i < 10; i++) {
      tableDescriptor.addColumn(Bytes.toBytes("COLUMN" + i));
    }
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setMaxVersions(1);

    Admin admin = clientCache.getAdmin();
    String tableName = "PerformanceUnordered";
    MTable table = admin.createTable(tableName, tableDescriptor);

    List<Put> listOfPuts = new ArrayList<>();
    int totalCount = 0;
    long totalBytes = 0L;
    long totalElapsedTime = 0L;
    while ((str = in.readLine()) != null) {
      String[] ar = str.split(",");
      byte[] rowKey = allKeys.get(totalCount);
      Put singlePut = new Put(rowKey);
      for (int i = 0; i < 10; i++) {
        singlePut.addColumn(Bytes.toBytes("COLUMN" + i), Bytes.toBytes(ar[i]));
      }
      listOfPuts.add(singlePut);
      totalCount++;

      if (listOfPuts.size() == 1000) {
        long time = System.nanoTime();
        table.put(listOfPuts);
        totalElapsedTime += (System.nanoTime() - time);
        totalBytes += ((MTableImpl) table).getByteArraySizeBatchPut();
        ((MTableImpl) table).resetByteArraySizeBatchPut();
        listOfPuts.clear();
      }
    }

    if (listOfPuts.size() != 0) {
      long time = System.nanoTime();
      table.put(listOfPuts);
      totalElapsedTime += (System.nanoTime() - time);
      totalBytes += ((MTableImpl) table).getByteArraySizeBatchPut();
      ((MTableImpl) table).resetByteArraySizeBatchPut();
    }
    long totalBactchPutTimeInSeconds = (totalElapsedTime / 1_000_000_000);
    long bytesPerSeconds = (totalBytes / totalBactchPutTimeInSeconds);

    System.out
        .println("-------------------    doBatchPutsUnOrdered MTABLE   --------------------- ");
    System.out.println("TotalCount         = " + totalCount);
    System.out.println("Time Taken (ms)    = " + (totalElapsedTime / 1_000_000));
    System.out.println("TotalBytes         = " + totalBytes);
    System.out.println("Bytes Per Seconds  = " + (bytesPerSeconds / (1024 * 1024)));

    in.close();

    clientCache.getAdmin().deleteTable(tableName);

    return totalCount;

  }


  private static void doBatchGetsOrdered(MClientCache clientCache, List<byte[]> allKeys)
      throws IOException {
    BufferedReader in = new BufferedReader(new FileReader(DATA_INPUT_FILE));
    List<Get> listOfGets = new ArrayList<>();
    int totalCount = 0;
    long totalTime = 0;
    long totalBytes = 0;

    // create Table,
    /*
     * MTableDescriptor tableDescriptor = new MTableDescriptor(); for (int i = 0; i < 10; i++) {
     * tableDescriptor.addColumn(Bytes.toBytes("COLUMN" + i)); }
     * tableDescriptor.setRedudantCopies(1); tableDescriptor.setMaxVersions(1);
     *
     * MAdmin admin = clientCache.getAdmin(); String tableName = "Performance"; MTable table =
     * admin.createTable(tableName, tableDescriptor);
     */
    String tableName = "Performance";
    MTable table = clientCache.getTable(tableName);

    while (in.readLine() != null) {
      byte[] rowKey = allKeys.get(totalCount);
      Get singleGet = new Get(rowKey);
      for (int i = 0; i < 10; i++) {
        singleGet.addColumn(Bytes.toBytes("COLUMN" + i));
      }
      listOfGets.add(singleGet);
      totalCount++;
      if (listOfGets.size() == 1000) {
        // Start time
        long time = System.nanoTime();
        Row[] result = table.get(listOfGets);
        totalTime += (System.nanoTime() - time);
        // EndTIme

        // total bytes
        for (Row row : result) {
          totalBytes += row.getCells().stream().mapToInt(e -> e.getValueArray().length).sum();
        }
        listOfGets.clear();
      }
    }

    if (listOfGets.size() != 0) {
      long time = System.nanoTime();
      Row[] result = table.get(listOfGets);
      totalTime += (System.nanoTime() - time);

      // total bytes
      for (Row row : result) {
        totalBytes += row.getCells().stream().mapToInt(e -> e.getValueArray().length).sum();
      }
    }

    // long totalBatchGetTime = (System.nanoTime() - batchGetStartTime);
    // System.out.println("TotalCount= " + totalCount + "; TimeInMGet_BatchMGet (ms)\t= " +
    // (totalBatchGetTime / 1_000_000));

    long totalBactchGetTimeInSeconds = (totalTime / 1_000_000_000);
    long bytesPerSeconds = (totalBytes / totalBactchGetTimeInSeconds);

    System.out.println(
        "-------------------  MTable (Ordered_SkipList) :: Batch_Gets(GetAll)   --------------------- ");
    System.out.println("TotalCount         = " + totalCount);
    System.out.println("Time Taken (ms)    = " + (totalTime / 1_000_000));
    System.out.println("TotalBytes         = " + totalBytes);
    System.out.println("Bytes Per Seconds(MB)  = " + (bytesPerSeconds / (1024 * 1024)));


    in.close();
    // clientCache.getAdmin().deleteTable(tableName);
  }

  public static void main(String[] args) throws IOException {
    List<byte[]> allKeys = getAllKeys();

    // generateData();

    MClientCache clientCache = new MClientCacheFactory().set("log-file", "/tmp/MTableClient.log")
        .addPoolLocator("127.0.0.1", 10334).create();

    // doBatchRegionPuts(allKeys);
    doBatchPutsOrdered(clientCache, allKeys);
    doScan();
    // doBatchPutsUnOrdered(clientCache, allKeys);

    doBatchGetsOrdered(clientCache, allKeys);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable("Performance");
    // doBatchPutsUnOrdered(clientCache, allKeys);

    // doBatchGets(table, allKeys, rowBatchPuts);

    /*
     * int rows = doPuts(table, allKeys); doGets(table, allKeys, rows);
     */


    /*
     * int rowBatchPuts = doBatchPuts(table, allKeys);
     *
     * //doScan(table);
     *
     */

    clientCache.close();
  }

}
