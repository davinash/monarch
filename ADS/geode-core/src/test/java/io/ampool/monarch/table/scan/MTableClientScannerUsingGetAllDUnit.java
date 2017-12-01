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
package io.ampool.monarch.table.scan;

import static io.ampool.monarch.types.BasicTypes.BINARY;
import static io.ampool.monarch.types.BasicTypes.DOUBLE;
import static io.ampool.monarch.types.BasicTypes.INT;
import static io.ampool.monarch.types.BasicTypes.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.ampool.client.AmpoolClient;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.MClientScannerUsingGetAll;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.interfaces.DataType;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.VersionedThinLRURegionEntryHeapObjectKey;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

@Category(MonarchTest.class)
public class MTableClientScannerUsingGetAllDUnit extends MTableDUnitHelper {
  public MTableClientScannerUsingGetAllDUnit() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();
  Host host = Host.getHost(0);
  private VM vm0 = host.getVM(0);
  private VM vm1 = host.getVM(1);
  private VM vm2 = host.getVM(2);
  private VM client1 = host.getVM(3);

  private final int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "MTableClientScannerUsingGetAllDUnit";
  private final String VALUE_PREFIX = "VALUE";
  private final String COLUMN_NAME_PREFIX = "COLUMN";

  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);

    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);

    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            MCacheFactory.getAnyInstance().close();
            return null;
          }
        }));

    super.tearDown2();
  }

  private List<byte[]> doPuts(int numRecordsPerBucket) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), numRecordsPerBucket);

    List<byte[]> allKeys = new ArrayList<>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> KEY_LIST.forEach((KEY) -> {
      allKeys.add(KEY);
    }));

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * numRecordsPerBucket),
        allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    return allKeys;
  }

  private void createTable() {
    // MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    // MTableDescriptor tableDescriptor = new MTableDescriptor();
    // for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
    // tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    // }
    // tableDescriptor.setRedundantCopies(1);
    // MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    // assertEquals(table.getName(), TABLE_NAME);
    // assertNotNull(table);
    createAndGetTable(TABLE_NAME, NUM_OF_COLUMNS, COLUMN_NAME_PREFIX, null);
  }

  private MTable createAndGetTable(final String tableName, final int numOfColumns,
      final String columnPrefix, final Map<Integer, Pair<byte[], byte[]>> keySpace) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    final Schema.Builder sb = new Schema.Builder();
    IntStream.range(0, numOfColumns).forEach(i -> sb.column(columnPrefix + i));

    MTableDescriptor td = new MTableDescriptor();
    td.setRedundantCopies(1);
    td.setSchema(sb.build());
    if (keySpace != null) {
      td.setKeySpace(keySpace);
    }

    MTable table = clientCache.getAdmin().createMTable(tableName, td);
    assertNotNull(table);
    assertEquals(table.getName(), tableName);

    return table;
  }

  private void doScan(int batchSize, List<byte[]> listOfKeys, int numRecordsPerBucket)
      throws IOException {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);
    Scan scan = new Scan();
    if (batchSize > 0) {
      scan.setClientQueueSize(batchSize);
    }
    Scanner scanner = table.getScanner(scan);
    Row currentRow = scanner.next();

    int rowIndex = 0;

    Collections.sort(listOfKeys, Bytes.BYTES_COMPARATOR);

    while (currentRow != null) {
      List<Cell> cells = currentRow.getCells();
      // System.out.println("--------------- " + Arrays.toString(currentRow.getRowId()) + "
      // -------------------");
      if (Bytes.compareTo(listOfKeys.get(rowIndex), currentRow.getRowId()) != 0) {
        Assert.fail("Order Mismatch from the Scanner");
      }
      int columnCount = 0;
      for (Cell cell : cells) {
        columnCount++;
        // System.out.println("COLUMN NAMe -> " + Arrays.toString(cell.getColumnName()));
        // System.out.println("COLUMN VALUE -> " + Arrays.toString((byte[])cell.getColumnValue()));
      }
      assertEquals(NUM_OF_COLUMNS + 1, columnCount);
      rowIndex++;
      currentRow = scanner.next();
    }
    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * numRecordsPerBucket),
        rowIndex);
    scanner.close();
  }

  private void doTestFaultsScan(int batchSize, List<byte[]> listOfKeys, boolean nextFault,
      boolean nextInteralFault, boolean dontClose, int numberToScan) throws IOException {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);
    Scan scan = new Scan();
    if (batchSize > 0) {
      scan.setClientQueueSize(batchSize);
    }
    Scanner scanner = table.getScanner(scan);
    boolean gotFault = false;

    if (nextFault) {
      ((MClientScannerUsingGetAll) scanner).setInjectNextFault(true);
    }
    if (nextInteralFault) {
      ((MClientScannerUsingGetAll) scanner).setInjectNextInternalFault(true);
    }

    int rowIndex = 0;

    Collections.sort(listOfKeys, Bytes.BYTES_COMPARATOR);

    try {
      Row currentRow = scanner.next();

      while (currentRow != null) {
        if (Bytes.compareTo(listOfKeys.get(rowIndex), currentRow.getRowId()) != 0) {
          Assert.fail("Order Mismatch from the Scanner");
        }
        rowIndex++;
        if (numberToScan > 0 && rowIndex >= numberToScan) {
          break;
        }
        currentRow = scanner.next();
      }
    } catch (Exception e) {
      if (nextFault || nextInteralFault) {
        gotFault = true;
        assertTrue((e.getCause() instanceof IllegalStateException));
      } else {
        throw e;
      }
    }

    if (nextFault || nextInteralFault) {
      assertTrue(gotFault);
    }

    if (!dontClose) {
      scanner.close();
    }
  }

  @Test
  public void testClientScannerUsingGetAllwrtBatchSize() throws IOException {
    createTable();

    int NUM_OF_ROWS_PER_BUCKET = 300;
    List<byte[]> listOfKeys = doPuts(NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size less than number of records per bucket
    int batchSize = 5;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    batchSize = 25;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size half the number of records per bucket
    batchSize = 40;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size equal to number of records per bucket
    batchSize = 50;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size greater than number of records per bucket
    batchSize = 100;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size multiple of number of records per bucket
    batchSize = -1;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testClientScannerUsingGetAllwrtNumRecords() throws IOException {
    createTable();

    int NUM_OF_ROWS_PER_BUCKET = 20;
    List<byte[]> listOfKeys = doPuts(NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size less than number of records per bucket
    int batchSize = -1;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    batchSize = NUM_OF_ROWS_PER_BUCKET - 5;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size half the number of records per bucket
    batchSize = NUM_OF_ROWS_PER_BUCKET / 2;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size equal to number of records per bucket
    batchSize = NUM_OF_ROWS_PER_BUCKET;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size greater than number of records per bucket
    batchSize = NUM_OF_ROWS_PER_BUCKET + 5;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size multiple of number of records per bucket
    batchSize = NUM_OF_ROWS_PER_BUCKET * 2;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testClientScannerUsingGetAllwrtChunkSize() throws IOException {
    int chunkSize = BaseCommand.maximumChunkSize;
    createTable();

    int NUM_OF_ROWS_PER_BUCKET = chunkSize * 3;
    List<byte[]> listOfKeys = doPuts(NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size less than chunk size
    int batchSize = -1;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    batchSize = chunkSize - 5;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size half the chunk size
    batchSize = chunkSize / 2;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size equal to chunk size
    batchSize = chunkSize;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size greater than chunk size
    batchSize = chunkSize + 5;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size multiple of chunk size
    batchSize = chunkSize * 2;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    // Setting batch size multiple of chunk size
    batchSize = NUM_OF_ROWS_PER_BUCKET;
    doScan(batchSize, listOfKeys, NUM_OF_ROWS_PER_BUCKET);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test(timeout = 10000)
  public void testClientScannerUsingGetAllFaults() throws IOException {
    createTable();

    int NUM_OF_ROWS_PER_BUCKET = 100;
    List<byte[]> listOfKeys = doPuts(NUM_OF_ROWS_PER_BUCKET);
    // test exception in internal scan
    doTestFaultsScan(50, listOfKeys, false, true, false, 0);
    // test exception in external scan (next())
    doTestFaultsScan(50, listOfKeys, true, false, false, 0);
    // test client close before completed
    doTestFaultsScan(50, listOfKeys, false, false, false, 10);
    // test client completes but fails to close
    doTestFaultsScan(50, listOfKeys, false, false, true, 0);
  }

  private void doPagedScan(int batchSize, int expectedNumRows, int pageLimit) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    Scan scan = new Scan();
    scan.setBatchSize(batchSize);
    scan.enableBatchMode();
    scan.setReturnKeysFlag(true);
    Scanner scanner = table.getScanner(scan);
    Row[] results = scanner.nextBatch();
    int count = 0, pageCount = 0;
    while (results.length > 0 && pageCount <= pageLimit) {
      pageCount++;
      count += results.length;
      results = scanner.nextBatch();
    }
    assertTrue(pageCount <= pageLimit); // test for incorrect number of pages
    assertEquals(count, expectedNumRows);
    scanner.close();
  }

  @Test
  public void testClientPagedScanUsingGetAll() throws IOException {
    createTable();

    int NUM_OF_ROWS_PER_BUCKET = 203;
    List<byte[]> listOfKeys = doPuts(NUM_OF_ROWS_PER_BUCKET);
    int numRows = listOfKeys.size();

    // test basic paged scan
    doPagedScan(100, numRows, (numRows / 100) + 1);

    // check operation if requested page > number rows
    doPagedScan(numRows + 1, numRows, 2);
  }

  private final String[] COL_NAME = {"Name", "ID", "Age", "Salary", "City"};
  protected final String KEY_PREFIX = "KEY";

  private MTable createTable(final String name, int maxEntries, boolean ordered) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    final Schema schema = new Schema.Builder().column(COL_NAME[0]).column(COL_NAME[1])
        .column(COL_NAME[2]).column(COL_NAME[3]).column(COL_NAME[4]).build();
    MTableDescriptor tableDescriptor =
        ordered ? new MTableDescriptor() : new MTableDescriptor(MTableType.UNORDERED);
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setSchema(schema);

    return clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
  }

  public void __DISABLED__testMTableEntriesEvictionScanTest() throws IOException {
    int totalEntries = 100;
    int maxEntries = 5;
    MTable table = createTable(TABLE_NAME, maxEntries, true);
    for (int i = 0; i < totalEntries; i++) {
      Put put = new Put(KEY_PREFIX + i);
      for (int j = 0; j < COL_NAME.length; j++) {
        put.addColumn(COL_NAME[j], Bytes.toBytes(VALUE_PREFIX + "-" + COL_NAME[j]));
      }
      table.put(put);
    }

    int entriesEvicted = 0;
    if (totalEntries > maxEntries) {
      entriesEvicted = totalEntries - maxEntries;
    }
    // verifyKeysOnServers(entriesEvicted);

    Scanner scanner = table.getScanner(new Scan());
    int count = 0;
    Row next = null;
    do {
      next = scanner.next();
      if (next != null) {
        count++;
      }
    } while (next != null);
    assertEquals(maxEntries, count);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  public void verifyKeysOnServers(int evictedEntries) {

    new ArrayList<>(Arrays.asList(vm0)).forEach((VM) -> VM.invoke(new SerializableCallable() {
      int evictedEntriesCount = 0;

      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        PartitionedRegion pr = (PartitionedRegion) cache.getRegion(TABLE_NAME);
        Set<BucketRegion> allLocalPrimaryBucketRegions =
            pr.getDataStore().getAllLocalPrimaryBucketRegions();
        for (BucketRegion bucket : allLocalPrimaryBucketRegions) {
          RowTupleConcurrentSkipListMap internalMap =
              (RowTupleConcurrentSkipListMap) bucket.getRegionMap().getInternalMap();
          Map realMap = internalMap.getInternalMap();
          realMap.forEach((KEY, VALUE) -> {
            MTableKey key = (MTableKey) KEY;
            VersionedThinLRURegionEntryHeapObjectKey obj =
                (VersionedThinLRURegionEntryHeapObjectKey) VALUE;
            if (Token.NOT_A_TOKEN != obj.getValueAsToken()) {
              evictedEntriesCount++;
            }
          });
        }
        assertEquals(evictedEntries, evictedEntriesCount);
        return null;
      }
    }));
  }

  @Test
  public void testScanAfterDelete() throws IOException {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    // MTableDescriptor tableDescriptor = new MTableDescriptor();
    // for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
    // tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    // }
    // tableDescriptor.setRedundantCopies(1);
    // MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    // assertEquals(table.getName(), TABLE_NAME);
    // assertNotNull(table);
    //

    createTable();
    MTable table = clientCache.getMTable(TABLE_NAME);
    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), 20);

    List<byte[]> allKeys = new ArrayList<>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> KEY_LIST.forEach((KEY) -> {
      allKeys.add(KEY);
    }));

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * 20), allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    Scanner scanner = table.getScanner(new Scan());
    assertNotNull(scanner);
    Row row = scanner.next();
    while (row != null) {
      row = scanner.next();
    }
    scanner.close();

    for (int i = 0; i < 10; i++) {
      Delete delete = new Delete(allKeys.get(i));
      table.delete(delete);
    }

    Scanner scannerd = table.getScanner(new Scan());
    assertNotNull(scannerd);
    Row rowd = scannerd.next();
    while (rowd != null) {
      rowd = scannerd.next();
    }
    scannerd.close();

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testClientRefAcrossBatch() {
    createTable();

    int NUM_OF_ROWS_PER_BUCKET = 50;
    List<byte[]> listOfKeys = doPuts(NUM_OF_ROWS_PER_BUCKET);

    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    int batchSize = 10;
    Scan scan = new Scan();
    scan.setClientQueueSize(batchSize);
    Scanner scanner = table.getScanner(scan);
    assertNotNull(scanner);
    Row firstRow = scanner.next();
    byte[] firstRowId = firstRow.getRowId();
    Row row = firstRow;
    int count = 0;
    while (row != null && count++ < batchSize) {
      row = scanner.next();
    }
    assertEquals(new ByteArrayKey(firstRowId), new ByteArrayKey(firstRow.getRowId()));
    scanner.close();

  }

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpace() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("000"), Bytes.toBytes("004")));
    map.put(1, new Pair<>(Bytes.toBytes("005"), Bytes.toBytes("006")));
    map.put(2, new Pair<>(Bytes.toBytes("007"), Bytes.toBytes("008")));
    map.put(3, new Pair<>(Bytes.toBytes("009"), Bytes.toBytes("010")));
    map.put(4, new Pair<>(Bytes.toBytes("011"), Bytes.toBytes("019")));
    return map;
  }

  @Test
  public void testScanner1() throws InterruptedException {
    for (int iter = 0; iter < 3; iter++) {
      System.out.println("MTableAllOpsDUnitTest.testScanner iteration = " + iter);

      String COLUMN_PREFIX = "COL", VALUE_PREFIX = "VAL";
      int NUM_COLUMNS = 5;

      MClientCache clientCache = MClientCacheFactory.getAnyInstance();
      MTable table = createAndGetTable(TABLE_NAME, NUM_COLUMNS, COLUMN_PREFIX, getKeySpace());

      String[] keys = {"002", "004", "006", "008", "009"};

      for (int rowInd = 0; rowInd < keys.length; rowInd++) {
        Put put = new Put(Bytes.toBytes(keys[rowInd]));

        for (int colInd = 0; colInd < NUM_COLUMNS; colInd++) {
          put.addColumn(Bytes.toBytes(COLUMN_PREFIX + colInd),
              Bytes.toBytes(VALUE_PREFIX + rowInd + colInd));
        }
        table.put(put);
      }
      //
      Thread.sleep(1000);
      Scan scan = new Scan();
      scan.setStartRow(Bytes.toBytes("000"));
      scan.setStopRow(Bytes.toBytes("019"));
      Scanner scanner = table.getScanner(scan);
      Row row = scanner.next();

      assertNotNull(row);
      int noOfRecordsInScan = 0;
      byte[] pastRowKey = new byte[0];
      int rowIndex = 0;

      assertTrue("Result is empty", !row.isEmpty());
      while (row != null) {
        // check columns and rowkey value is right
        byte[] rowKey = row.getRowId();
        System.out.println("NNNN verifyScan :: " + "Row REcvd: " + Arrays.toString(rowKey));
        // check if this row is greter than pervious
        // ignore for first row key
        assertTrue(rowKey.length > 0);

        // check row key value
        String rowKeyExpected = keys[rowIndex];
        System.out.println(
            "XXX TEST.Scan :: " + "-------------------------SCAN ROW-------------------------");
        System.out.println("XXX TEST.Scan :: " + "Row expected: " + rowKeyExpected);
        System.out.println("XXX TEST.Scan :: " + "Row received: " + Arrays.toString(rowKey));
        assertEquals(0, Bytes.compareTo(Bytes.toBytes(keys[rowIndex]), rowKey));

        // assertEquals(0, rowKeyExpected, Arrays.toString(rowKey)));
        // assertEquals(new String(VALUE_PREFIX + 1), Bytes.toString((byte[])
        // mCell.getColumnValue()));
        // Bytes.compareTo(rowKeyExpected

        // Test- Assert if number of cols are equal
        List<Cell> cells = row.getCells();
        System.out.println("XXX TEST.scan Total-Columns = " + cells.size());
        // temp hack
        assertEquals(NUM_COLUMNS + 1, cells.size());

        for (int columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex++) {
          Cell cell = cells.get(columnIndex);
          byte[] colNameRec = cell.getColumnName();
          byte[] colNameExp = Bytes.toBytes(COLUMN_PREFIX + columnIndex);

          // Verify ColumnNames for the Row
          System.out
              .println("XXX TEST.Scan :: " + "ColumnName expected: " + Arrays.toString(colNameExp));
          System.out
              .println("XXX TEST.Scan :: " + "ColumnName received: " + Arrays.toString(colNameRec));
          assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));

          //// Verify Column-Values for the Row
          byte[] colValRec = (byte[]) cell.getColumnValue();
          byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + rowIndex + columnIndex);
          System.out
              .println("XXX TEST.Scan :: " + "ColumnValue expected: " + Arrays.toString(colValExp));
          System.out
              .println("XXX TEST.Scan :: " + "ColumnValue received: " + Arrays.toString(colValRec));
          assertEquals(0, Bytes.compareTo(colValExp, colValRec));

        }
        pastRowKey = rowKey;
        noOfRecordsInScan++;
        rowIndex++;
        row = scanner.next();
        if (keys.length != noOfRecordsInScan) {
          assertTrue("Result is empty", !row.isEmpty());
        }

      }
      scanner.close();

      clientCache.getAdmin().deleteTable(TABLE_NAME);
    }
  }


  /**
   * Tests multiversion scan. Scan and fetch more than 1 version
   */
  @Test
  public void testScannerMultiversionFetch() throws InterruptedException {
    String tableName = getTestMethodName();
    String COLUMN_PREFIX = "COL", VALUE_PREFIX = "VAL";
    int NUM_COLUMNS = 5;
    int NUM_VERSIONS = 5;

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    final Schema.Builder sb = new Schema.Builder();
    IntStream.range(0, NUM_COLUMNS).forEach(i -> sb.column(COLUMN_PREFIX + i));

    MTableDescriptor td = new MTableDescriptor();
    td.setRedundantCopies(1);
    td.setSchema(sb.build());
    td.setMaxVersions(NUM_VERSIONS);

    MTable table = clientCache.getAdmin().createMTable(tableName, td);
    assertNotNull(table);
    assertEquals(table.getName(), tableName);

    String[] keys = {"002", "004", "006", "008", "009"};

    for (int rowInd = 0; rowInd < keys.length; rowInd++) {
      for (int i = 0; i < NUM_VERSIONS; i++) {
        Put put = new Put(Bytes.toBytes(keys[rowInd]));

        for (int colInd = 0; colInd < NUM_COLUMNS; colInd++) {
          put.addColumn(Bytes.toBytes(COLUMN_PREFIX + colInd),
              Bytes.toBytes(VALUE_PREFIX + rowInd + colInd));
        }
        table.put(put);
      }
    }
    //
    Thread.sleep(1000);

    Scan scan = new Scan();
    scan.setMaxVersions(NUM_VERSIONS, false);
    Scanner scanner = table.getScanner(scan);
    Iterator<Row> iterator = scanner.iterator();
    int noOfRecordsInScan = 0;

    while (iterator.hasNext()) {
      Row next = iterator.next();
      assertEquals(NUM_COLUMNS + 1, next.getCells().size());
      Map<Long, SingleVersionRow> allVersions = next.getAllVersions();
      assertEquals(NUM_VERSIONS, allVersions.size());
      // order of the TS
      Long lastTS = Long.MAX_VALUE;
      Iterator<Long> iterator1 = allVersions.keySet().iterator();
      while (iterator1.hasNext()) {
        Long currTS = iterator1.next();
        assertTrue(currTS < lastTS);
        lastTS = currTS;
      }
      noOfRecordsInScan++;
    }

    assertEquals(keys.length, noOfRecordsInScan);

    System.out.println("-------------------------------------------------------------------------");

    scan = new Scan();
    scan.setMaxVersions(NUM_VERSIONS, true);
    scanner = table.getScanner(scan);
    iterator = scanner.iterator();
    noOfRecordsInScan = 0;

    while (iterator.hasNext()) {
      Row next = iterator.next();
      Map<Long, SingleVersionRow> allVersions = next.getAllVersions();
      assertEquals(NUM_VERSIONS, allVersions.size());
      // order of the TS
      Long lastTS = Long.MIN_VALUE;
      Iterator<Long> iterator1 = allVersions.keySet().iterator();
      while (iterator1.hasNext()) {
        Long currTS = iterator1.next();
        assertTrue(currTS > lastTS);
        lastTS = currTS;
      }
      noOfRecordsInScan++;
    }

    assertEquals(keys.length, noOfRecordsInScan);

    scanner.close();

    clientCache.getAdmin().deleteTable(tableName);
  }

  /**
   * Tests multiversion scan. Scan and fetch more than 1 version
   */
  @Test
  public void testScannerMultiversionFiltersWithNullValue() throws InterruptedException {
    String tableName = getTestMethodName();
    String COLUMN_PREFIX = "COL", VALUE_PREFIX = "VAL";
    int NUM_COLUMNS = 5;
    int NUM_VERSIONS = 3;
    int numBuckets = 3;
    int excessVersions = 5;
    int numOfEntries = 15;
    int maxSz = 3;
    int startEntry = 2;
    int stopEntry = 46;

    final String COL1 = "NAME";
    final String COL2 = "ID";
    final String COL3 = "AGE";
    final String COL4 = "SALARY";

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    Function<String, MTable> createTable = new Function<String, MTable>() {

      @Override
      public MTable apply(String tableName) {
        // MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
        MTableDescriptor tableDescriptor = new MTableDescriptor();
        Schema schema = new Schema.Builder().column(COL1, BasicTypes.STRING.toString())
            .column(COL2, BasicTypes.INT.toString()).column(COL3, BasicTypes.INT.toString())
            .column(COL4, BasicTypes.INT.toString()).build();

        tableDescriptor.setSchema(schema);
        tableDescriptor.setMaxVersions(NUM_VERSIONS);
        tableDescriptor.setTotalNumOfSplits(numBuckets);

        Admin admin = getmClientCache().getAdmin();
        // Check & Create MTable
        if (admin.existsMTable(tableName)) {
          admin.deleteMTable(tableName); // Delete Existing Table.
          System.out.println("DELETED Existing MTable : " + tableName);
        }
        MTable mtable = admin.createMTable(tableName, tableDescriptor);
        System.out.println("Table [" + tableName + "] is created successfully!");
        return mtable;
      }
    };

    Function<MTable, Boolean> insertRows = new Function<MTable, Boolean>() {
      @Override
      public Boolean apply(MTable mtable) {
        Random random = new Random(0L);

        int maxInputVersions = NUM_VERSIONS + excessVersions;
        for (int ver = 1; ver <= maxInputVersions; ver++) {
          for (int keyIndex = 0; keyIndex < numOfEntries; keyIndex++) {
            Put myput1 = new Put(Bytes.toBytes("row-" + padWithZero(keyIndex, maxSz)));
            // System.out.println("Current VERSION = " + ver);
            myput1.setTimeStamp(ver);
            // myput1.setTimeStamp(maxInputVersions - ver);
            // myput1.setTimeStamp(0);
            int m = keyIndex % maxInputVersions;
            if ((m == 0) && (ver == 1)) {
              mtable.put(myput1);
            } else if (ver <= m) {
              String strName = "Name" + padWithZero(keyIndex, maxSz);

              int randomSal = (int) (random.nextInt(50000 - 10000) + 10000);
              myput1.addColumn(COL1, strName); // Name
              myput1.addColumn(COL2, keyIndex); // ID
              if (ver % 2 == 0) {
                myput1.addColumn(COL3, keyIndex + 1); // Age
              } else {
                myput1.addColumn(COL4, randomSal); // Salary
              }
              mtable.put(myput1);
            }
          }
        }
        return true;
      }
    };

    MTable mtable = createTable.apply(tableName);
    //
    Put tempPut = new Put(Bytes.toBytes("row-0010"));
    tempPut.setTimeStamp(1);
    tempPut.addColumn(COL1, "Name0010");
    tempPut.addColumn(COL2, 10);
    tempPut.addColumn(COL4, 30000);
    mtable.put(tempPut);

    tempPut = new Put(Bytes.toBytes("row-0010"));
    tempPut.setTimeStamp(2);
    tempPut.addColumn(COL1, "Name0010");
    tempPut.addColumn(COL2, 10);
    tempPut.addColumn(COL3, 11);
    mtable.put(tempPut);

    // insertRows.apply(mtable);

    Scan scan = new Scan();
    Scanner scanner = null;
    Iterator<Row> itr = null;

    System.out.println("\n---------------ALL VERSIONS SCAN [ RECENT -> OLDER ] -----------\n");
    scan = new Scan();
    // scan.setReversed(true);
    // scan.setMaxVersions(tabMaxVersions, false);
    scan.setMaxVersions();
    scanner = mtable.getScanner(scan);
    itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        rowFormatter(res.getRowId(), k, v.getCells());
      });
      System.out.println();
    }
    System.out.println("----------------------------------------------------\n");

    System.out
        .println("\n------- MULTIPLE COLUMN FILTER [ FilterOnLatestVersionOnly = FALSE ] -----\n");
    scan = new Scan();
    // scan.setReversed(true);
    scan.setMaxVersions(NUM_VERSIONS, false);
    scan.setFilterOnLatestVersionOnly(false);
    if (mtable.getTableDescriptor().getTableType() != MTableType.UNORDERED) {
      scan.setStartRow(Bytes.toBytes("row-" + padWithZero(startEntry, maxSz)));
      scan.setStopRow(Bytes.toBytes("row-" + padWithZero(stopEntry, maxSz)));
    }

    // scan.setFilter(new SingleColumnValueFilter("AGE", CompareOp.GREATER, 5));

    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("AGE", CompareOp.GREATER_OR_EQUAL, 6);
    SingleColumnValueFilter filter2 =
        new SingleColumnValueFilter("ID", CompareOp.LESS_OR_EQUAL, 12);
    FilterList filtList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    filtList.addFilter(filter1).addFilter(filter2);

    scan.setFilter(filtList);
    // scan.setFilter(filter2);
    scanner = mtable.getScanner(scan);
    itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      System.out.println(
          "MTableClientScannerUsingGetAllDUnit.testScannerMultiversionFiltersWithNullValue :: 946 row class "
              + res.getClass());
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        assertEquals(2l, (long) k);
        List<Cell> cells = v.getCells();
        cells.forEach(cell -> {
          String cellName = Bytes.toString(cell.getColumnName());
          Object cellValue = cell.getColumnValue();
          if (cellName.equals(COL1)) {
            assertEquals("Name0010", (String) cellValue);
          } else if (cellName.equals(COL2)) {
            assertEquals(10, (int) cellValue);
          } else if (cellName.equals(COL3)) {
            assertEquals(11, (int) cellValue);
          } else if (cellName.equals(COL4)) {
            assertEquals(30000, (int) cellValue);
          }
        });
        rowFormatter(res.getRowId(), k, cells);
      });
      System.out.println();
    }
    System.out.println("----------------------------------------------------\n");


  }

  private static void rowFormatter(byte[] key, Long timestamp, List<Cell> cells) {
    System.out.println("KEY: " + Bytes.toString(key) + " \t " + " VERSION_ID: " + timestamp + " \t "
        + "NAME: " + cells.get(0).getColumnValue() + "\t" + "ID: " + cells.get(1).getColumnValue()
        + "\t" + "AGE: " + cells.get(2).getColumnValue() + "\t" + "SALARY: "
        + cells.get(3).getColumnValue());
  }

  private static String padWithZero(final int value, final int maxSize) {
    String valueString = String.valueOf(value);
    for (int index = valueString.length(); index <= maxSize; index++) {
      valueString = "0" + valueString;
    }
    return valueString;

  }


  /**
   * Tests multiversion scan. Scan and fetch more than 1 version
   */
  @Test
  public void testScannerMultiversionFetchUnordered() throws InterruptedException {
    String tableName = getTestMethodName();
    String COLUMN_PREFIX = "COL", VALUE_PREFIX = "VAL";
    int NUM_COLUMNS = 5;
    int NUM_VERSIONS = 5;

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    final Schema.Builder sb = new Schema.Builder();
    IntStream.range(0, NUM_COLUMNS).forEach(i -> sb.column(COLUMN_PREFIX + i));

    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    td.setRedundantCopies(1);
    td.setSchema(sb.build());
    td.setMaxVersions(NUM_VERSIONS);

    MTable table = clientCache.getAdmin().createMTable(tableName, td);
    assertNotNull(table);
    assertEquals(table.getName(), tableName);

    String[] keys = {"002", "004", "006", "008", "009"};

    for (int rowInd = 0; rowInd < keys.length; rowInd++) {
      for (int i = 0; i < NUM_VERSIONS; i++) {
        Put put = new Put(Bytes.toBytes(keys[rowInd]));

        for (int colInd = 0; colInd < NUM_COLUMNS; colInd++) {
          put.addColumn(Bytes.toBytes(COLUMN_PREFIX + colInd),
              Bytes.toBytes(VALUE_PREFIX + rowInd + colInd));
        }
        table.put(put);
      }
    }
    //
    Thread.sleep(1000);

    Scan scan = new Scan();
    scan.setMaxVersions(NUM_VERSIONS, false);
    Scanner scanner = table.getScanner(scan);
    Iterator<Row> iterator = scanner.iterator();
    int noOfRecordsInScan = 0;

    while (iterator.hasNext()) {
      Row next = iterator.next();
      assertEquals(NUM_COLUMNS + 1, next.getCells().size());
      Map<Long, SingleVersionRow> allVersions = next.getAllVersions();
      assertEquals(NUM_VERSIONS, allVersions.size());
      // order of the TS
      Long lastTS = Long.MAX_VALUE;
      Iterator<Long> iterator1 = allVersions.keySet().iterator();
      while (iterator1.hasNext()) {
        Long currTS = iterator1.next();
        assertTrue(currTS < lastTS);
        lastTS = currTS;
      }
      noOfRecordsInScan++;
    }

    assertEquals(keys.length, noOfRecordsInScan);

    System.out.println("-------------------------------------------------------------------------");

    scan = new Scan();
    scan.setMaxVersions(NUM_VERSIONS, true);
    scanner = table.getScanner(scan);
    iterator = scanner.iterator();
    noOfRecordsInScan = 0;

    while (iterator.hasNext()) {
      Row next = iterator.next();
      Map<Long, SingleVersionRow> allVersions = next.getAllVersions();
      assertEquals(NUM_VERSIONS, allVersions.size());
      // order of the TS
      Long lastTS = Long.MIN_VALUE;
      Iterator<Long> iterator1 = allVersions.keySet().iterator();
      while (iterator1.hasNext()) {
        Long currTS = iterator1.next();
        assertTrue(currTS > lastTS);
        lastTS = currTS;
      }
      noOfRecordsInScan++;
    }

    assertEquals(keys.length, noOfRecordsInScan);

    scanner.close();

    clientCache.getAdmin().deleteTable(tableName);
  }

  @Test
  public void testSimpleMultiVersion() {
    final String tableName = "test_simple_multi_version";
    final String[] COLUMNS = {"c_1", "c_2", "c_3", "c_4", MTableUtils.KEY_COLUMN_NAME};
    final DataType[] TYPES = {INT, STRING, DOUBLE, STRING, BINARY};
    final Object[] VALUES = new Object[] {111, "abc_123", 333.333, "efg", null};
    MTableDescriptor td = new MTableDescriptor();
    td.setSchema(new Schema(COLUMNS, TYPES));
    td.setMaxVersions(10);

    AmpoolClient ac = new AmpoolClient();
    ac.getAdmin().createMTable(tableName, td);

    MTable table = ac.getMTable(tableName);
    Put put = new Put("key_1");
    put.addColumn(COLUMNS[0], VALUES[0]);
    put.addColumn(COLUMNS[1], VALUES[2]);
    put.addColumn(COLUMNS[2], VALUES[2]);
    table.put(put);

    put.addColumn(COLUMNS[0], VALUES[0]);
    put.addColumn(COLUMNS[1], VALUES[2]);
    put.addColumn(COLUMNS[2], VALUES[2]);
    table.put(put);

    put.addColumn(COLUMNS[0], VALUES[0]);
    put.addColumn(COLUMNS[1], VALUES[2]);
    put.addColumn(COLUMNS[2], VALUES[2]);
    table.put(put);

    assertEquals("Incorrect number of results.", 1, getScanCount(null, table));
    assertEquals("Incorrect number of results.", 1, getScanCount(1, table));
    assertEquals("Incorrect number of results.", 3, getScanCount(3, table));
    assertEquals("Incorrect number of results.", 3, getScanCount(100, table));
  }

  private int getScanCount(final Integer scanMaxVersions, final MTable table) {
    int count = 0;
    Scan scan = new Scan();
    if (scanMaxVersions != null) {
      scan.setMaxVersions(scanMaxVersions, false);
    }
    for (Row row : table.getScanner(scan)) {
      count += row.getAllVersions().size();
    }
    return count;
  }

  /**
   * Return the total number of result-sender threads from all servers.
   * 
   * @return the number of scan-result-sender threads
   */
  private int getSenderThreadCount() {
    int totalSenderThreadCount = 0;
    for (VM vm : new VM[] {vm0, vm1, vm2}) {
      totalSenderThreadCount += (long) vm.invoke(new SerializableCallable<Object>() {
        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        @Override
        public Object call() throws Exception {
          return Thread.getAllStackTraces().keySet().stream()
              .filter(e -> e.getName().startsWith("ScanResultSender")).count();
        }
      });
    }
    return totalSenderThreadCount;
  }

  /**
   * Execute scan and return the total result count.
   * 
   * @param table the MTable
   * @param startKey the start-key to be used for range scan
   * @param endKey the end-key to be used for range scan
   * @return total number of results in the specified range
   */
  private int getScanCount(final MTable table, final String startKey, final String endKey) {
    final Scan scan = new Scan();
    if (startKey != null) {
      scan.setStartRow(Bytes.toBytes(startKey));
    }
    if (endKey != null) {
      scan.setStopRow(Bytes.toBytes(endKey));
    }
    Scanner scanner = table.getScanner(scan);

    int count = 0;
    for (final Row ignored : scanner) {
      count++;
    }
    scanner.close();

    return count;
  }

  @Test
  public void testNoSenderThreadsAfterScan() throws InterruptedException {
    String COLUMN_PREFIX = "COL", VALUE_PREFIX = "VAL";
    int NUM_COLUMNS = 5;

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable table = createAndGetTable(TABLE_NAME, NUM_COLUMNS, COLUMN_PREFIX, getKeySpace());

    String[] keys = {"002", "004", "006", "008", "009"};

    for (int rowInd = 0; rowInd < keys.length; rowInd++) {
      Put put = new Put(Bytes.toBytes(keys[rowInd]));

      for (int colInd = 0; colInd < NUM_COLUMNS; colInd++) {
        put.addColumn(Bytes.toBytes(COLUMN_PREFIX + colInd),
            Bytes.toBytes(VALUE_PREFIX + rowInd + colInd));
      }
      table.put(put);
    }
    Thread.sleep(1_000);

    /* scan with valid range */
    assertEquals("Incorrect count from scan.", 2, getScanCount(table, "000", "005"));
    assertEquals("ScanResultSender thread count should be 0.", 0, getSenderThreadCount());

    /* scan with valid bucket but no data in the specified range */
    assertEquals("Incorrect count from scan.", 0, getScanCount(table, "000", "001"));
    assertEquals("SenderThread count should be 0.", 0, getSenderThreadCount());

    /* full-table scan */
    assertEquals("Incorrect count from scan.", 5, getScanCount(table, null, null));
    assertEquals("ScanResultSender thread count should be 0.", 0, getSenderThreadCount());

    /* scan with range outside key-space */
    assertEquals("Incorrect count from scan.", 0, getScanCount(table, "222", "333"));
    assertEquals("SenderThread count should be 0.", 0, getSenderThreadCount());

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }
}
