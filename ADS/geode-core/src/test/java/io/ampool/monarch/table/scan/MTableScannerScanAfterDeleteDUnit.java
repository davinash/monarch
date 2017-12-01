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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;

import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.VersionedThinLRURegionEntryHeapObjectKey;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class MTableScannerScanAfterDeleteDUnit extends MTableDUnitHelper {
  public MTableScannerScanAfterDeleteDUnit() {
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
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
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

  private final String[] COL_NAME = {"Name", "ID", "Age", "Salary", "City"};
  protected final String KEY_PREFIX = "KEY";

  private MTable createTable(final String name, int maxEntries, boolean ordered) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.addColumn(Bytes.toBytes(COL_NAME[0]));
    tableDescriptor.addColumn(Bytes.toBytes(COL_NAME[1]));
    tableDescriptor.addColumn(Bytes.toBytes(COL_NAME[2]));
    tableDescriptor.addColumn(Bytes.toBytes(COL_NAME[3]));
    tableDescriptor.addColumn(Bytes.toBytes(COL_NAME[4]));

    return clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
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
  public void testScanAfterRandomDelete() throws IOException {
    int numberOfKeysPerBucket = 20;
    int splits = 10;
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setTotalNumOfSplits(splits);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);

    Map<Integer, List<byte[]>> keysForAllBuckets = getKeysForAllBuckets(
        table.getTableDescriptor().getTotalNumOfSplits(), numberOfKeysPerBucket);
    // this is for test to precheck which key is coming
    TreeMap<Integer, List<byte[]>> sortedKeyMapByBucketId = new TreeMap<>();
    sortedKeyMapByBucketId.putAll(keysForAllBuckets);

    // sorting first by bucket id and then by keys as they will be placed into the mtable
    List<byte[]> allKeys = new ArrayList<>(sortedKeyMapByBucketId.size());
    sortedKeyMapByBucketId.forEach((BID, KEY_LIST) -> {
      Collections.sort(KEY_LIST, Bytes.BYTES_COMPARATOR);
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    int totalRecordsIns =
        (table.getTableDescriptor().getTotalNumOfSplits() * numberOfKeysPerBucket);
    assertEquals(totalRecordsIns, allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    // GET part use if necessary

    // allKeys.forEach((K) -> {
    // MGet record = new MGet(K);
    // MResult getRes = table.get(record);
    // List<MCell> getCells = getRes.getCells();
    // assertEquals(NUM_OF_COLUMNS,getCells.size());
    //
    // for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
    // byte[] expectValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
    // byte[] getValue = (byte[])getCells.get(columnIndex).getColumnValue();
    // byte[] getValue2 = getCells.get(columnIndex).getValueArray();
    // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // Expected: "+Arrays.toString(expectValue));
    // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // RECValueObj: "+Arrays.toString(getValue));
    // //System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // RECValueArr: "+Arrays.toString(getValue2));
    // assertEquals(0,Bytes.compareTo(expectValue,getValue));
    // //assertEquals(0,Bytes.compareTo(expectValue,getValue2));
    // }
    // });

    // scan and check values
    byte[] pastRowKey = new byte[0];
    int rowKeyIndexScanner = 0;
    Scanner scanner = table.getScanner(new Scan());
    assertNotNull(scanner);
    Row row = scanner.next();
    int noOfRecordsInScan = 0;
    while (row != null) {
      // check columns and rowkey value is right
      byte[] rowKey = row.getRowId();
      // check if this row is greter than pervious
      // ignore for first row key
      assertTrue(rowKey.length > 0);
      if (noOfRecordsInScan > 0) {
        // new row key should be greater
        assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
      }

      // // check row key value
      byte[] rowKeyExpected = allKeys.get(rowKeyIndexScanner++);
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "-------------------------NEW ROW-------------------------");
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row expected: " + Arrays.toString(rowKeyExpected));
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row received: " + Arrays.toString(rowKey));
      assertEquals(0, Bytes.compareTo(rowKeyExpected, rowKey));

      List<Cell> cells = row.getCells();
      // assert if number of cols are equal
      assertEquals(NUM_OF_COLUMNS + 1, cells.size());

      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        Cell cell = cells.get(columnIndex);
        byte[] colNameRec = cell.getColumnName();
        byte[] colNameExp = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        // check for name
        assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColName: "+Bytes.toString(colNameRec));
        byte[] colValRec = (byte[]) cell.getColumnValue();
        byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + columnIndex);
        // check for value
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColValueRec: "+Bytes.toString(colValRec));
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColValueExp: "+Bytes.toString(colValExp));

        // special get done in case values are not matching. Currently commenting out for debug
        // purpose only
        // if(Bytes.compareTo(colValExp,colValRec)!=0){
        // // uunexpectedly they are not same now get
        // MGet mget = new MGet(rowKeyExpected);
        // mget.addColumn(colNameRec);
        // MResult getRes = table.get(mget);
        // assertFalse(getRes.isEmpty());
        // List<MCell> getCells = getRes.getCells();
        // assertEquals(1,getCells.size());
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "Scanned records: "+noOfRecordsInScan);
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colName: "+Bytes.toString(getCells.get(0).getColumnName()));
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colValue: "+Bytes.toString(getCells.get(0).getValueArray()));
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colValue: "+getCells.get(0).getColumnValue());
        //
        // }
        assertEquals(0, Bytes.compareTo(colValExp, colValRec));
      }
      pastRowKey = rowKey;
      noOfRecordsInScan++;
      row = scanner.next();
    }
    assertEquals(totalRecordsIns, noOfRecordsInScan);
    scanner.close();

    int recordToDel = 10;
    List<byte[]> keysDeleted = new ArrayList<>(recordToDel);
    List<Integer> keysIndexDeleted = new ArrayList<>(recordToDel);
    for (int i = 0; i < recordToDel; i++) {
      // testing by random del
      boolean gotUniqueRecordIndex = false;
      byte[] key = null;
      do {
        int recordToBeDelIndex = new Random().nextInt(allKeys.size());
        if (!keysIndexDeleted.contains(recordToBeDelIndex)) {
          keysIndexDeleted.add(recordToBeDelIndex);
          key = allKeys.get(recordToBeDelIndex);
          gotUniqueRecordIndex = true;
        }
      } while (!gotUniqueRecordIndex);

      Delete delete = new Delete(key);
      table.delete(delete);
      keysDeleted.add(key);
    }

    Scanner scannerd = table.getScanner(new Scan());
    assertNotNull(scannerd);

    Row rowd = scannerd.next();
    noOfRecordsInScan = 0;
    rowKeyIndexScanner = 0;

    while (rowd != null) {

      while (keysIndexDeleted.contains(rowKeyIndexScanner)) {
        rowKeyIndexScanner++;
      }

      // check columns and rowkey value is right
      byte[] rowKey = rowd.getRowId();
      // check if this row is greter than pervious
      // ignore for first row key
      assertTrue(rowKey.length > 0);
      if (noOfRecordsInScan > 0) {
        // new row key should be greater
        assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
      }

      // check if rowKey is not one which is deleted earlier
      keysDeleted.forEach(deletedKey -> {
        assertTrue(Bytes.compareTo(rowKey, deletedKey) != 0);
      });

      // // check row key value
      byte[] rowKeyExpected = allKeys.get(rowKeyIndexScanner++);
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "-------------------------NEW ROW-------------------------");
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row expected: " + Arrays.toString(rowKeyExpected));
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row received: " + Arrays.toString(rowKey));
      assertEquals(0, Bytes.compareTo(rowKeyExpected, rowKey));

      List<Cell> cells = rowd.getCells();
      // assert if number of cols are equal
      assertEquals(NUM_OF_COLUMNS + 1, cells.size());

      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        Cell cell = cells.get(columnIndex);
        byte[] colNameRec = cell.getColumnName();
        byte[] colNameExp = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        // check for name
        assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "ColName: "
            + Bytes.toString(colNameRec));
        byte[] colValRec = (byte[]) cell.getColumnValue();
        byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + columnIndex);
        // check for value
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
            + "ColValueRec: " + Bytes.toString(colValRec));
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
            + "ColValueExp: " + Bytes.toString(colValExp));
        assertEquals(0, Bytes.compareTo(colValExp, colValRec));
      }
      pastRowKey = rowKey;
      noOfRecordsInScan++;
      rowd = scannerd.next();
    }
    assertEquals(totalRecordsIns - recordToDel, noOfRecordsInScan);
    scannerd.close();

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testScanAfterDelete() throws IOException {
    int numberOfKeysPerBucket = 20;
    int splits = 10;
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setTotalNumOfSplits(splits);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);

    Map<Integer, List<byte[]>> keysForAllBuckets = getKeysForAllBuckets(
        table.getTableDescriptor().getTotalNumOfSplits(), numberOfKeysPerBucket);
    // this is for test to precheck which key is coming
    TreeMap<Integer, List<byte[]>> sortedKeyMapByBucketId = new TreeMap<>();
    sortedKeyMapByBucketId.putAll(keysForAllBuckets);

    // sorting first by bucket id and then by keys as they will be placed into the mtable
    List<byte[]> allKeys = new ArrayList<>(sortedKeyMapByBucketId.size());
    sortedKeyMapByBucketId.forEach((BID, KEY_LIST) -> {
      Collections.sort(KEY_LIST, Bytes.BYTES_COMPARATOR);
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    int totalRecordsIns =
        (table.getTableDescriptor().getTotalNumOfSplits() * numberOfKeysPerBucket);
    assertEquals(totalRecordsIns, allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    // GET part use if necessary

    // allKeys.forEach((K) -> {
    // MGet record = new MGet(K);
    // MResult getRes = table.get(record);
    // List<MCell> getCells = getRes.getCells();
    // assertEquals(NUM_OF_COLUMNS,getCells.size());
    //
    // for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
    // byte[] expectValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
    // byte[] getValue = (byte[])getCells.get(columnIndex).getColumnValue();
    // byte[] getValue2 = getCells.get(columnIndex).getValueArray();
    // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // Expected: "+Arrays.toString(expectValue));
    // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // RECValueObj: "+Arrays.toString(getValue));
    // //System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // RECValueArr: "+Arrays.toString(getValue2));
    // assertEquals(0,Bytes.compareTo(expectValue,getValue));
    // //assertEquals(0,Bytes.compareTo(expectValue,getValue2));
    // }
    // });

    // scan and check values
    byte[] pastRowKey = new byte[0];
    int rowKeyIndexScanner = 0;
    Scanner scanner = table.getScanner(new Scan());
    assertNotNull(scanner);
    Row row = scanner.next();
    int noOfRecordsInScan = 0;
    while (row != null) {
      // check columns and rowkey value is right
      byte[] rowKey = row.getRowId();
      // check if this row is greter than pervious
      // ignore for first row key
      assertTrue(rowKey.length > 0);
      if (noOfRecordsInScan > 0) {
        // new row key should be greater
        assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
      }

      // // check row key value
      byte[] rowKeyExpected = allKeys.get(rowKeyIndexScanner++);
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "-------------------------NEW ROW-------------------------");
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row expected: " + Arrays.toString(rowKeyExpected));
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row received: " + Arrays.toString(rowKey));
      assertEquals(0, Bytes.compareTo(rowKeyExpected, rowKey));

      List<Cell> cells = row.getCells();
      // assert if number of cols are equal
      assertEquals(NUM_OF_COLUMNS + 1, cells.size());

      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        Cell cell = cells.get(columnIndex);
        byte[] colNameRec = cell.getColumnName();
        byte[] colNameExp = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        // check for name
        assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColName: "+Bytes.toString(colNameRec));
        byte[] colValRec = (byte[]) cell.getColumnValue();
        byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + columnIndex);
        // check for value
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColValueRec: "+Bytes.toString(colValRec));
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColValueExp: "+Bytes.toString(colValExp));

        // special get done in case values are not matching. Currently commenting out for debug
        // purpose only
        // if(Bytes.compareTo(colValExp,colValRec)!=0){
        // // uunexpectedly they are not same now get
        // MGet mget = new MGet(rowKeyExpected);
        // mget.addColumn(colNameRec);
        // MResult getRes = table.get(mget);
        // assertFalse(getRes.isEmpty());
        // List<MCell> getCells = getRes.getCells();
        // assertEquals(1,getCells.size());
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "Scanned records: "+noOfRecordsInScan);
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colName: "+Bytes.toString(getCells.get(0).getColumnName()));
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colValue: "+Bytes.toString(getCells.get(0).getValueArray()));
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colValue: "+getCells.get(0).getColumnValue());
        //
        // }
        assertEquals(0, Bytes.compareTo(colValExp, colValRec));
      }
      pastRowKey = rowKey;
      noOfRecordsInScan++;
      row = scanner.next();
    }
    assertEquals(totalRecordsIns, noOfRecordsInScan);
    scanner.close();

    int recordToDel = 10;
    List<byte[]> keysDeleted = new ArrayList<>(recordToDel);
    List<Integer> keysIndexDeleted = new ArrayList<>(recordToDel);
    for (int i = 0; i < recordToDel; i++) {
      // testing by random del
      int recordToBeDelIndex = i;
      byte[] key = allKeys.get(recordToBeDelIndex);
      Delete delete = new Delete(key);
      table.delete(delete);
      keysDeleted.add(key);
      keysIndexDeleted.add(recordToBeDelIndex);
    }

    Scanner scannerd = table.getScanner(new Scan());
    assertNotNull(scannerd);

    Row rowd = scannerd.next();
    noOfRecordsInScan = 0;
    rowKeyIndexScanner = 0;

    while (rowd != null) {

      while (keysIndexDeleted.contains(rowKeyIndexScanner)) {
        rowKeyIndexScanner++;
        System.out.println(
            "MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "Skipping rowKeyIndexer");
      }

      // check columns and rowkey value is right
      byte[] rowKey = rowd.getRowId();
      // check if this row is greter than pervious
      // ignore for first row key
      assertTrue(rowKey.length > 0);
      if (noOfRecordsInScan > 0) {
        // new row key should be greater
        assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
      }

      // check if rowKey is not one which is deleted earlier
      keysDeleted.forEach(deletedKey -> {
        assertTrue(Bytes.compareTo(rowKey, deletedKey) != 0);
      });

      // // check row key value
      byte[] rowKeyExpected = allKeys.get(rowKeyIndexScanner++);
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "-------------------------NEW ROW-------------------------");
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row expected: " + Arrays.toString(rowKeyExpected));
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row received: " + Arrays.toString(rowKey));
      assertEquals(0, Bytes.compareTo(rowKeyExpected, rowKey));

      List<Cell> cells = rowd.getCells();
      // assert if number of cols are equal
      assertEquals(NUM_OF_COLUMNS + 1, cells.size());

      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        Cell cell = cells.get(columnIndex);
        byte[] colNameRec = cell.getColumnName();
        byte[] colNameExp = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        // check for name
        assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "ColName: "
            + Bytes.toString(colNameRec));
        byte[] colValRec = (byte[]) cell.getColumnValue();
        byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + columnIndex);
        // check for value
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
            + "ColValueRec: " + Bytes.toString(colValRec));
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
            + "ColValueExp: " + Bytes.toString(colValExp));
        assertEquals(0, Bytes.compareTo(colValExp, colValRec));
      }
      pastRowKey = rowKey;
      noOfRecordsInScan++;
      rowd = scannerd.next();
    }
    assertEquals(totalRecordsIns - recordToDel, noOfRecordsInScan);
    scannerd.close();

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  public static int doUnorderedScan(MTable table, byte[] badKey) {
    // Run scan on table
    Scan scan = new Scan();
    scan.setReturnKeysFlag(true); // return keys so we can validate row removed
    Scanner scanner = table.getScanner(scan);
    Iterator itr = scanner.iterator();
    int count = 0;
    while (itr.hasNext()) {
      count += 1;
      Row res = (Row) itr.next();
      byte[] key = res.getRowId();
      if (key == null) {
        Assert.fail("null key when return keys set to true");
      }
      if (badKey != null) {
        if (Bytes.compareTo(key, badKey) == 0) {
          Assert.fail("deleted key still present in table");
        }
      }
    }
    return count;
  }

  @Test
  public void testScanAfterDeleteUnordered() {
    int numRecords = 2000, numColumns = 5;
    String tableName = "scanAfterDelUnord";
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(Bytes.toBytes("col" + i));
    }
    tableDescriptor.setRedundantCopies(1).setTotalNumOfSplits(4);

    MTable table = clientCache.getAdmin().createTable(tableName, tableDescriptor);
    Put record = new Put(Bytes.toBytes("rowKey"));

    byte[] data = new byte[32];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    for (int i = 0; i < numRecords; i++) {
      record.setRowKey(Bytes.toBytes("rowKey" + i));

      for (int colIndex = 0; colIndex < numColumns; colIndex++) {
        record.addColumn(Bytes.toBytes("col" + colIndex), data);
      }
      table.put(record);
      record.clear();
    }

    int beforeCount = doUnorderedScan(table, null);

    int rowToDelete = (numRecords / 2);
    byte[] rowToDeleteKey = Bytes.toBytes("rowKey" + rowToDelete);
    Delete delete = new Delete(rowToDeleteKey);
    table.delete(delete);

    int afterCount = doUnorderedScan(table, rowToDeleteKey);

    assertEquals(beforeCount - 1, afterCount);

    clientCache.getAdmin().deleteTable(tableName);
  }

  @Test
  public void testScanAfterDeleteCellOffsetCalc() throws IOException {
    int numberOfKeysPerBucket = 20;
    int splits = 10;
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setTotalNumOfSplits(splits);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);

    Map<Integer, List<byte[]>> keysForAllBuckets = getKeysForAllBuckets(
        table.getTableDescriptor().getTotalNumOfSplits(), numberOfKeysPerBucket);
    // this is for test to precheck which key is coming
    TreeMap<Integer, List<byte[]>> sortedKeyMapByBucketId = new TreeMap<>();
    sortedKeyMapByBucketId.putAll(keysForAllBuckets);

    // sorting first by bucket id and then by keys as they will be placed into the mtable
    List<byte[]> allKeys = new ArrayList<>(sortedKeyMapByBucketId.size());
    sortedKeyMapByBucketId.forEach((BID, KEY_LIST) -> {
      Collections.sort(KEY_LIST, Bytes.BYTES_COMPARATOR);
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    int totalRecordsIns =
        (table.getTableDescriptor().getTotalNumOfSplits() * numberOfKeysPerBucket);
    assertEquals(totalRecordsIns, allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    // GET part use if necessary

    // allKeys.forEach((K) -> {
    // MGet record = new MGet(K);
    // MResult getRes = table.get(record);
    // List<MCell> getCells = getRes.getCells();
    // assertEquals(NUM_OF_COLUMNS,getCells.size());
    //
    // for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
    // byte[] expectValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
    // byte[] getValue = (byte[])getCells.get(columnIndex).getColumnValue();
    // byte[] getValue2 = getCells.get(columnIndex).getValueArray();
    // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // Expected: "+Arrays.toString(expectValue));
    // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // RECValueObj: "+Arrays.toString(getValue));
    // //System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // RECValueArr: "+Arrays.toString(getValue2));
    // assertEquals(0,Bytes.compareTo(expectValue,getValue));
    // //assertEquals(0,Bytes.compareTo(expectValue,getValue2));
    // }
    // });

    // scan and check values
    byte[] pastRowKey = new byte[0];
    int rowKeyIndexScanner = 0;
    Scanner scanner = table.getScanner(new Scan());
    assertNotNull(scanner);
    Row row = scanner.next();
    int noOfRecordsInScan = 0;
    while (row != null) {
      // check columns and rowkey value is right
      byte[] rowKey = row.getRowId();
      // check if this row is greter than pervious
      // ignore for first row key
      assertTrue(rowKey.length > 0);
      if (noOfRecordsInScan > 0) {
        // new row key should be greater
        assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
      }

      // // check row key value
      byte[] rowKeyExpected = allKeys.get(rowKeyIndexScanner++);
      // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
      // "-------------------------NEW ROW-------------------------");
      // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "Row
      // expected: "+Arrays.toString(rowKeyExpected));
      // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "Row
      // received: "+Arrays.toString(rowKey));
      assertEquals(0, Bytes.compareTo(rowKeyExpected, rowKey));

      List<Cell> cells = row.getCells();
      // assert if number of cols are equal
      assertEquals(NUM_OF_COLUMNS + 1, cells.size());

      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        Cell cell = cells.get(columnIndex);
        byte[] colNameRec = cell.getColumnName();
        byte[] colNameExp = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        // check for name
        assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColName: "+Bytes.toString(colNameRec));

        byte[] valArray = cell.getValueArray();
        int cellLength = cell.getValueLength();
        int cellOffset = cell.getValueOffset();

        // calc cell value from offset and length
        byte[] colValExp = new byte[cellLength];
        System.arraycopy(valArray, cellOffset, colValExp, 0, cellLength);

        byte[] colValRec = (byte[]) cell.getColumnValue();
        // byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + columnIndex);
        // check for value
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColValueRec: "+Bytes.toString(colValRec));
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColValueExp: "+Bytes.toString(colValExp));

        // special get done in case values are not matching. Currently commenting out for debug
        // purpose only
        // if(Bytes.compareTo(colValExp,colValRec)!=0){
        // // uunexpectedly they are not same now get
        // MGet mget = new MGet(rowKeyExpected);
        // mget.addColumn(colNameRec);
        // MResult getRes = table.get(mget);
        // assertFalse(getRes.isEmpty());
        // List<MCell> getCells = getRes.getCells();
        // assertEquals(1,getCells.size());
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "Scanned records: "+noOfRecordsInScan);
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colName: "+Bytes.toString(getCells.get(0).getColumnName()));
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colValue: "+Bytes.toString(getCells.get(0).getValueArray()));
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colValue: "+getCells.get(0).getColumnValue());
        //
        // }
        assertEquals(0, Bytes.compareTo(colValExp, colValRec));
      }
      pastRowKey = rowKey;
      noOfRecordsInScan++;
      row = scanner.next();
    }
    assertEquals(totalRecordsIns, noOfRecordsInScan);
    scanner.close();

    int recordToDel = 10;
    List<byte[]> keysDeleted = new ArrayList<>(recordToDel);
    List<Integer> keysIndexDeleted = new ArrayList<>(recordToDel);
    for (int i = 0; i < recordToDel; i++) {
      // testing by random del
      int recordToBeDelIndex = i;
      byte[] key = allKeys.get(recordToBeDelIndex);
      Delete delete = new Delete(key);
      table.delete(delete);
      keysDeleted.add(key);
      keysIndexDeleted.add(recordToBeDelIndex);
    }

    Scanner scannerd = table.getScanner(new Scan());
    assertNotNull(scannerd);

    Row rowd = scannerd.next();
    noOfRecordsInScan = 0;
    rowKeyIndexScanner = recordToDel;

    while (rowd != null) {

      // check columns and rowkey value is right
      byte[] rowKey = rowd.getRowId();
      // check if this row is greter than pervious
      // ignore for first row key
      assertTrue(rowKey.length > 0);
      if (noOfRecordsInScan > 0) {
        // new row key should be greater
        assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
      }

      // check if rowKey is not one which is deleted earlier
      keysDeleted.forEach(deletedKey -> {
        assertTrue(Bytes.compareTo(rowKey, deletedKey) != 0);
      });

      // // check row key value
      byte[] rowKeyExpected = allKeys.get(rowKeyIndexScanner++);
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "-------------------------NEW ROW-------------------------");
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row expected: " + Arrays.toString(rowKeyExpected));
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row received: " + Arrays.toString(rowKey));
      assertEquals(0, Bytes.compareTo(rowKeyExpected, rowKey));

      List<Cell> cells = rowd.getCells();
      // assert if number of cols are equal
      assertEquals(NUM_OF_COLUMNS + 1, cells.size());

      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        Cell cell = cells.get(columnIndex);
        byte[] colNameRec = cell.getColumnName();
        byte[] colNameExp = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        // check for name
        assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "ColName: "
            + Bytes.toString(colNameRec));
        byte[] colValRec = (byte[]) cell.getColumnValue();
        // byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        byte[] valArray = cell.getValueArray();
        int cellLength = cell.getValueLength();
        int cellOffset = cell.getValueOffset();

        // calc cell value from offset and length
        byte[] colValExp = new byte[cellLength];
        System.arraycopy(valArray, cellOffset, colValExp, 0, cellLength);

        // check for value
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
            + "ColValueRec: " + Bytes.toString(colValRec));
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
            + "ColValueExp: " + Bytes.toString(colValExp));
        assertEquals(0, Bytes.compareTo(colValExp, colValRec));
      }
      pastRowKey = rowKey;
      noOfRecordsInScan++;
      rowd = scannerd.next();
    }
    assertEquals(totalRecordsIns - recordToDel, noOfRecordsInScan);
    scannerd.close();

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testScanNRowFetch() throws IOException {
    int numberOfKeysPerBucket = 10;
    int splits = 10;
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setTotalNumOfSplits(splits);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);

    Map<Integer, List<byte[]>> keysForAllBuckets = getKeysForAllBuckets(
        table.getTableDescriptor().getTotalNumOfSplits(), numberOfKeysPerBucket);
    // this is for test to precheck which key is coming
    TreeMap<Integer, List<byte[]>> sortedKeyMapByBucketId = new TreeMap<>();
    sortedKeyMapByBucketId.putAll(keysForAllBuckets);

    // sorting first by bucket id and then by keys as they will be placed into the mtable
    List<byte[]> allKeys = new ArrayList<>(sortedKeyMapByBucketId.size());
    sortedKeyMapByBucketId.forEach((BID, KEY_LIST) -> {
      Collections.sort(KEY_LIST, Bytes.BYTES_COMPARATOR);
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    int totalRecordsIns =
        (table.getTableDescriptor().getTotalNumOfSplits() * numberOfKeysPerBucket);
    assertEquals(totalRecordsIns, allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    // GET part use if necessary

    // allKeys.forEach((K) -> {
    // MGet record = new MGet(K);
    // MResult getRes = table.get(record);
    // List<MCell> getCells = getRes.getCells();
    // assertEquals(NUM_OF_COLUMNS,getCells.size());
    //
    // for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
    // byte[] expectValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
    // byte[] getValue = (byte[])getCells.get(columnIndex).getColumnValue();
    // byte[] getValue2 = getCells.get(columnIndex).getValueArray();
    // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // Expected: "+Arrays.toString(expectValue));
    // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // RECValueObj: "+Arrays.toString(getValue));
    // //System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // RECValueArr: "+Arrays.toString(getValue2));
    // assertEquals(0,Bytes.compareTo(expectValue,getValue));
    // //assertEquals(0,Bytes.compareTo(expectValue,getValue2));
    // }
    // });

    // scan and check values
    byte[] pastRowKey = new byte[0];
    int rowKeyIndexScanner = 0;
    Scanner scanner = table.getScanner(new Scan());
    assertNotNull(scanner);
    Row row = scanner.next();
    int noOfRecordsInScan = 0;
    while (row != null) {
      // check columns and rowkey value is right
      byte[] rowKey = row.getRowId();
      // check if this row is greter than pervious
      // ignore for first row key
      assertTrue(rowKey.length > 0);
      if (noOfRecordsInScan > 0) {
        // new row key should be greater
        assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
      }

      // // check row key value
      byte[] rowKeyExpected = allKeys.get(rowKeyIndexScanner++);
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "-------------------------NEW ROW-------------------------");
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row expected: " + Arrays.toString(rowKeyExpected));
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row received: " + Arrays.toString(rowKey));
      assertEquals(0, Bytes.compareTo(rowKeyExpected, rowKey));

      List<Cell> cells = row.getCells();
      // assert if number of cols are equal
      assertEquals(NUM_OF_COLUMNS + 1, cells.size());

      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        Cell cell = cells.get(columnIndex);
        byte[] colNameRec = cell.getColumnName();
        byte[] colNameExp = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        // check for name
        assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColName: "+Bytes.toString(colNameRec));

        byte[] valArray = cell.getValueArray();
        int cellLength = cell.getValueLength();
        int cellOffset = cell.getValueOffset();

        // calc cell value from offset and length
        byte[] colValExp = new byte[cellLength];
        System.arraycopy(valArray, cellOffset, colValExp, 0, cellLength);

        byte[] colValRec = (byte[]) cell.getColumnValue();
        // byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + columnIndex);
        // check for value
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColValueRec: "+Bytes.toString(colValRec));
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColValueExp: "+Bytes.toString(colValExp));

        // special get done in case values are not matching. Currently commenting out for debug
        // purpose only
        // if(Bytes.compareTo(colValExp,colValRec)!=0){
        // // uunexpectedly they are not same now get
        // MGet mget = new MGet(rowKeyExpected);
        // mget.addColumn(colNameRec);
        // MResult getRes = table.get(mget);
        // assertFalse(getRes.isEmpty());
        // List<MCell> getCells = getRes.getCells();
        // assertEquals(1,getCells.size());
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "Scanned records: "+noOfRecordsInScan);
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colName: "+Bytes.toString(getCells.get(0).getColumnName()));
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colValue: "+Bytes.toString(getCells.get(0).getValueArray()));
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colValue: "+getCells.get(0).getColumnValue());
        //
        // }
        assertEquals(0, Bytes.compareTo(colValExp, colValRec));
      }
      pastRowKey = rowKey;
      noOfRecordsInScan++;
      row = scanner.next();
    }
    assertEquals(totalRecordsIns, noOfRecordsInScan);
    scanner.close();

    int recordToDel = 1;
    List<byte[]> keysDeleted = new ArrayList<>(recordToDel);
    List<Integer> keysIndexDeleted = new ArrayList<>(recordToDel);
    for (int i = 0; i < recordToDel; i++) {
      // testing by random del
      int recordToBeDelIndex = i;
      byte[] key = allKeys.get(recordToBeDelIndex);
      Delete delete = new Delete(key);
      table.delete(delete);
      keysDeleted.add(key);
      keysIndexDeleted.add(recordToBeDelIndex);
    }
    assertTrue(keysDeleted.size() == recordToDel);
    assertTrue(keysIndexDeleted.size() == recordToDel);

    Scanner scannerd = table.getScanner(new Scan());
    assertNotNull(scannerd);


    // test by fetching n records
    int fetchSize = 5;

    Row[] rown = scannerd.next(fetchSize);
    int howManyTimesNextNIsCalled = 1;

    System.out.println(
        "MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: " + "Size: " + rown.length);
    assertTrue(rown.length <= fetchSize);
    noOfRecordsInScan = 0;
    rowKeyIndexScanner = recordToDel;

    int i = 0;
    while (i < rown.length) {
      Row rowd = rown[i];
      assertNotNull(rowd);
      // check columns and rowkey value is right
      byte[] rowKey = rowd.getRowId();
      // check if this row is greter than pervious
      // ignore for first row key
      assertTrue(rowKey.length > 0);
      if (noOfRecordsInScan > 0) {
        // new row key should be greater
        assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
      }

      // check if rowKey is not one which is deleted earlier
      keysDeleted.forEach(deletedKey -> {
        assertTrue(Bytes.compareTo(rowKey, deletedKey) != 0);
      });

      // // check row key value
      byte[] rowKeyExpected = allKeys.get(rowKeyIndexScanner++);
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "-------------------------NEW ROW-------------------------");
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row expected: " + Arrays.toString(rowKeyExpected));
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row received: " + Arrays.toString(rowKey));
      assertEquals(0, Bytes.compareTo(rowKeyExpected, rowKey));

      List<Cell> cells = rowd.getCells();
      // assert if number of cols are equal
      assertEquals(NUM_OF_COLUMNS + 1, cells.size());

      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        Cell cell = cells.get(columnIndex);
        byte[] colNameRec = cell.getColumnName();
        byte[] colNameExp = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        // check for name
        assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "ColName: "
            + Bytes.toString(colNameRec));
        byte[] colValRec = (byte[]) cell.getColumnValue();
        // byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        byte[] valArray = cell.getValueArray();
        int cellLength = cell.getValueLength();
        int cellOffset = cell.getValueOffset();

        // calc cell value from offset and length
        byte[] colValExp = new byte[cellLength];
        System.arraycopy(valArray, cellOffset, colValExp, 0, cellLength);

        // check for value
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
            + "ColValueRec: " + Bytes.toString(colValRec));
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
            + "ColValueExp: " + Bytes.toString(colValExp));
        assertEquals(0, Bytes.compareTo(colValExp, colValRec));
      }
      pastRowKey = rowKey;
      noOfRecordsInScan++;
      i++;
      if (rown.length == i) {
        rown = scannerd.next(fetchSize);
        howManyTimesNextNIsCalled++;
        i = 0;
      }
    }
    assertEquals(totalRecordsIns - recordToDel, noOfRecordsInScan);
    System.out.println("MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: " + "Iterations :"
        + howManyTimesNextNIsCalled);
    // +1 as it needs 1 extra iteration
    // assertEquals(((totalRecordsIns - recordToDel)/fetchSize)+1,howManyTimesNextNIsCalled);
    scannerd.close();

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testScanNRowFetchPartialDel() throws IOException {
    int numberOfKeysPerBucket = 10;
    int splits = 10;
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setTotalNumOfSplits(splits);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);

    Map<Integer, List<byte[]>> keysForAllBuckets = getKeysForAllBuckets(
        table.getTableDescriptor().getTotalNumOfSplits(), numberOfKeysPerBucket);
    // this is for test to precheck which key is coming
    TreeMap<Integer, List<byte[]>> sortedKeyMapByBucketId = new TreeMap<>();
    sortedKeyMapByBucketId.putAll(keysForAllBuckets);

    // sorting first by bucket id and then by keys as they will be placed into the mtable
    List<byte[]> allKeys = new ArrayList<>(sortedKeyMapByBucketId.size());
    sortedKeyMapByBucketId.forEach((BID, KEY_LIST) -> {
      Collections.sort(KEY_LIST, Bytes.BYTES_COMPARATOR);
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    int totalRecordsIns =
        (table.getTableDescriptor().getTotalNumOfSplits() * numberOfKeysPerBucket);
    assertEquals(totalRecordsIns, allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    // GET part use if necessary

    // allKeys.forEach((K) -> {
    // MGet record = new MGet(K);
    // MResult getRes = table.get(record);
    // List<MCell> getCells = getRes.getCells();
    // assertEquals(NUM_OF_COLUMNS,getCells.size());
    //
    // for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
    // byte[] expectValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
    // byte[] getValue = (byte[])getCells.get(columnIndex).getColumnValue();
    // byte[] getValue2 = getCells.get(columnIndex).getValueArray();
    // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // Expected: "+Arrays.toString(expectValue));
    // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // RECValueObj: "+Arrays.toString(getValue));
    // //System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // RECValueArr: "+Arrays.toString(getValue2));
    // assertEquals(0,Bytes.compareTo(expectValue,getValue));
    // //assertEquals(0,Bytes.compareTo(expectValue,getValue2));
    // }
    // });

    // scan and check values
    byte[] pastRowKey = new byte[0];
    int rowKeyIndexScanner = 0;
    Scanner scanner = table.getScanner(new Scan());
    assertNotNull(scanner);
    Row row = scanner.next();
    int noOfRecordsInScan = 0;
    while (row != null) {
      // check columns and rowkey value is right
      byte[] rowKey = row.getRowId();
      // check if this row is greter than pervious
      // ignore for first row key
      assertTrue(rowKey.length > 0);
      if (noOfRecordsInScan > 0) {
        // new row key should be greater
        assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
      }

      // // check row key value
      byte[] rowKeyExpected = allKeys.get(rowKeyIndexScanner++);
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "-------------------------NEW ROW-------------------------");
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row expected: " + Arrays.toString(rowKeyExpected));
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row received: " + Arrays.toString(rowKey));
      assertEquals(0, Bytes.compareTo(rowKeyExpected, rowKey));

      List<Cell> cells = row.getCells();
      // assert if number of cols are equal
      assertEquals(NUM_OF_COLUMNS + 1, cells.size());

      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        Cell cell = cells.get(columnIndex);
        byte[] colNameRec = cell.getColumnName();
        byte[] colNameExp = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        // check for name
        assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColName: "+Bytes.toString(colNameRec));

        byte[] valArray = cell.getValueArray();
        int cellLength = cell.getValueLength();
        int cellOffset = cell.getValueOffset();

        // calc cell value from offset and length
        byte[] colValExp = new byte[cellLength];
        System.arraycopy(valArray, cellOffset, colValExp, 0, cellLength);

        byte[] colValRec = (byte[]) cell.getColumnValue();
        // byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + columnIndex);
        // check for value
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColValueRec: "+Bytes.toString(colValRec));
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColValueExp: "+Bytes.toString(colValExp));

        // special get done in case values are not matching. Currently commenting out for debug
        // purpose only
        // if(Bytes.compareTo(colValExp,colValRec)!=0){
        // // uunexpectedly they are not same now get
        // MGet mget = new MGet(rowKeyExpected);
        // mget.addColumn(colNameRec);
        // MResult getRes = table.get(mget);
        // assertFalse(getRes.isEmpty());
        // List<MCell> getCells = getRes.getCells();
        // assertEquals(1,getCells.size());
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "Scanned records: "+noOfRecordsInScan);
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colName: "+Bytes.toString(getCells.get(0).getColumnName()));
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colValue: "+Bytes.toString(getCells.get(0).getValueArray()));
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colValue: "+getCells.get(0).getColumnValue());
        //
        // }
        assertEquals(0, Bytes.compareTo(colValExp, colValRec));
      }
      pastRowKey = rowKey;
      noOfRecordsInScan++;
      row = scanner.next();
    }
    assertEquals(totalRecordsIns, noOfRecordsInScan);
    scanner.close();

    int recordToDel = 1;
    List<byte[]> keysDeleted = new ArrayList<>(recordToDel);
    List<Integer> keysIndexDeleted = new ArrayList<>(recordToDel);
    for (int i = 0; i < recordToDel; i++) {
      // testing by random del
      int recordToBeDelIndex = i;
      byte[] key = allKeys.get(recordToBeDelIndex);
      Delete delete = new Delete(key);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        delete.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex));
      }
      table.delete(delete);
      keysDeleted.add(key);
      keysIndexDeleted.add(recordToBeDelIndex);
    }
    assertTrue(keysDeleted.size() == recordToDel);
    assertTrue(keysIndexDeleted.size() == recordToDel);

    Scanner scannerd = table.getScanner(new Scan());
    assertNotNull(scannerd);


    // test by fetching n records
    int fetchSize = 5;

    Row[] rown = scannerd.next(fetchSize);
    int howManyTimesNextNIsCalled = 1;

    System.out.println(
        "MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: " + "Size: " + rown.length);
    assertTrue(rown.length <= fetchSize);
    noOfRecordsInScan = 0;
    rowKeyIndexScanner = recordToDel;

    int i = 0;
    while (i < rown.length) {
      Row rowd = rown[i];
      assertNotNull(rowd);
      // check columns and rowkey value is right
      byte[] rowKey = rowd.getRowId();
      // check if this row is greter than pervious
      // ignore for first row key
      assertTrue(rowKey.length > 0);
      if (noOfRecordsInScan > 0) {
        // new row key should be greater
        assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
      }

      // check if rowKey is not one which is deleted earlier
      keysDeleted.forEach(deletedKey -> {
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: " + "key: "
            + Bytes.toString(rowKey));
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: "
            + "deleted key: " + Bytes.toString(deletedKey));
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: "
            + "Performing get after delete...");
        Get get = new Get(deletedKey);
        Row res = table.get(get);
        byte[] rowKeyGet = res.getRowId();
        List<Cell> cells = res.getCells();
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: "
            + "Cells from get: " + cells.size());
        cells.forEach(cell -> {
          System.out.println("MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: "
              + "cell name: " + Bytes.toString(cell.getColumnName()) + " cell value: "
              + Arrays.toString((byte[]) cell.getColumnValue()));

        });

        assertTrue(res.isEmpty());
        assertTrue(Bytes.compareTo(rowKey, deletedKey) != 0);
      });

      // // check row key value
      byte[] rowKeyExpected = allKeys.get(rowKeyIndexScanner++);
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "-------------------------NEW ROW-------------------------");
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row expected: " + Arrays.toString(rowKeyExpected));
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row received: " + Arrays.toString(rowKey));
      assertEquals(0, Bytes.compareTo(rowKeyExpected, rowKey));

      List<Cell> cells = rowd.getCells();
      // assert if number of cols are equal
      assertEquals(NUM_OF_COLUMNS + 1, cells.size());

      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        Cell cell = cells.get(columnIndex);
        byte[] colNameRec = cell.getColumnName();
        byte[] colNameExp = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        // check for name
        assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "ColName: "
            + Bytes.toString(colNameRec));
        byte[] colValRec = (byte[]) cell.getColumnValue();
        // byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        byte[] valArray = cell.getValueArray();
        int cellLength = cell.getValueLength();
        int cellOffset = cell.getValueOffset();

        // calc cell value from offset and length
        byte[] colValExp = new byte[cellLength];
        System.arraycopy(valArray, cellOffset, colValExp, 0, cellLength);

        // check for value
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
            + "ColValueRec: " + Bytes.toString(colValRec));
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
            + "ColValueExp: " + Bytes.toString(colValExp));
        assertEquals(0, Bytes.compareTo(colValExp, colValRec));
      }
      pastRowKey = rowKey;
      noOfRecordsInScan++;
      i++;
      if (rown.length == i) {
        rown = scannerd.next(fetchSize);
        howManyTimesNextNIsCalled++;
        i = 0;
      }
    }
    assertEquals(totalRecordsIns - recordToDel, noOfRecordsInScan);
    System.out.println("MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: " + "Iterations :"
        + howManyTimesNextNIsCalled);
    // +1 as it needs 1 extra iteration
    // assertEquals(((totalRecordsIns - recordToDel)/fetchSize)+1,howManyTimesNextNIsCalled);
    scannerd.close();

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testMultiThreadScanNRowFetchPartialDel() throws IOException {
    int numberOfKeysPerBucket = 10;
    int splits = 10;
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setTotalNumOfSplits(splits);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);

    Map<Integer, List<byte[]>> keysForAllBuckets = getKeysForAllBuckets(
        table.getTableDescriptor().getTotalNumOfSplits(), numberOfKeysPerBucket);
    // this is for test to precheck which key is coming
    TreeMap<Integer, List<byte[]>> sortedKeyMapByBucketId = new TreeMap<>();
    sortedKeyMapByBucketId.putAll(keysForAllBuckets);

    // sorting first by bucket id and then by keys as they will be placed into the mtable
    List<byte[]> allKeys = new ArrayList<>(sortedKeyMapByBucketId.size());
    sortedKeyMapByBucketId.forEach((BID, KEY_LIST) -> {
      Collections.sort(KEY_LIST, Bytes.BYTES_COMPARATOR);
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    int totalRecordsIns =
        (table.getTableDescriptor().getTotalNumOfSplits() * numberOfKeysPerBucket);
    assertEquals(totalRecordsIns, allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    // GET part use if necessary

    // allKeys.forEach((K) -> {
    // MGet record = new MGet(K);
    // MResult getRes = table.get(record);
    // List<MCell> getCells = getRes.getCells();
    // assertEquals(NUM_OF_COLUMNS,getCells.size());
    //
    // for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
    // byte[] expectValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
    // byte[] getValue = (byte[])getCells.get(columnIndex).getColumnValue();
    // byte[] getValue2 = getCells.get(columnIndex).getValueArray();
    // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // Expected: "+Arrays.toString(expectValue));
    // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // RECValueObj: "+Arrays.toString(getValue));
    // //System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "PREGET
    // RECValueArr: "+Arrays.toString(getValue2));
    // assertEquals(0,Bytes.compareTo(expectValue,getValue));
    // //assertEquals(0,Bytes.compareTo(expectValue,getValue2));
    // }
    // });

    // scan and check values
    byte[] pastRowKey = new byte[0];
    int rowKeyIndexScanner = 0;
    Scanner scanner = table.getScanner(new Scan());
    assertNotNull(scanner);
    Row row = scanner.next();
    int noOfRecordsInScan = 0;
    while (row != null) {
      // check columns and rowkey value is right
      byte[] rowKey = row.getRowId();
      // check if this row is greter than pervious
      // ignore for first row key
      assertTrue(rowKey.length > 0);
      if (noOfRecordsInScan > 0) {
        // new row key should be greater
        assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
      }

      // // check row key value
      byte[] rowKeyExpected = allKeys.get(rowKeyIndexScanner++);
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "-------------------------NEW ROW-------------------------");
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row expected: " + Arrays.toString(rowKeyExpected));
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
          + "Row received: " + Arrays.toString(rowKey));
      assertEquals(0, Bytes.compareTo(rowKeyExpected, rowKey));

      List<Cell> cells = row.getCells();
      // assert if number of cols are equal
      assertEquals(NUM_OF_COLUMNS + 1, cells.size());

      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        Cell cell = cells.get(columnIndex);
        byte[] colNameRec = cell.getColumnName();
        byte[] colNameExp = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        // check for name
        assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColName: "+Bytes.toString(colNameRec));

        byte[] valArray = cell.getValueArray();
        int cellLength = cell.getValueLength();
        int cellOffset = cell.getValueOffset();

        // calc cell value from offset and length
        byte[] colValExp = new byte[cellLength];
        System.arraycopy(valArray, cellOffset, colValExp, 0, cellLength);

        byte[] colValRec = (byte[]) cell.getColumnValue();
        // byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + columnIndex);
        // check for value
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColValueRec: "+Bytes.toString(colValRec));
        // System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "ColValueExp: "+Bytes.toString(colValExp));

        // special get done in case values are not matching. Currently commenting out for debug
        // purpose only
        // if(Bytes.compareTo(colValExp,colValRec)!=0){
        // // uunexpectedly they are not same now get
        // MGet mget = new MGet(rowKeyExpected);
        // mget.addColumn(colNameRec);
        // MResult getRes = table.get(mget);
        // assertFalse(getRes.isEmpty());
        // List<MCell> getCells = getRes.getCells();
        // assertEquals(1,getCells.size());
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " +
        // "Scanned records: "+noOfRecordsInScan);
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colName: "+Bytes.toString(getCells.get(0).getColumnName()));
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colValue: "+Bytes.toString(getCells.get(0).getValueArray()));
        //// System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: " + "GET
        // colValue: "+getCells.get(0).getColumnValue());
        //
        // }
        assertEquals(0, Bytes.compareTo(colValExp, colValRec));
      }
      pastRowKey = rowKey;
      noOfRecordsInScan++;
      row = scanner.next();
    }
    assertEquals(totalRecordsIns, noOfRecordsInScan);
    scanner.close();

    int recordToDel = 1;
    List<byte[]> keysDeleted = new ArrayList<>(recordToDel);
    List<Integer> keysIndexDeleted = new ArrayList<>(recordToDel);
    for (int i = 0; i < recordToDel; i++) {
      // testing by random del
      int recordToBeDelIndex = i;
      byte[] key = allKeys.get(recordToBeDelIndex);
      Delete delete = new Delete(key);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        delete.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex));
      }
      table.delete(delete);
      keysDeleted.add(key);
      keysIndexDeleted.add(recordToBeDelIndex);
    }
    assertTrue(keysDeleted.size() == recordToDel);
    assertTrue(keysIndexDeleted.size() == recordToDel);

    Scanner scannerd = table.getScanner(new Scan());
    assertNotNull(scannerd);

    spawnNThreadsForScan(5, table, recordToDel, keysDeleted, allKeys, totalRecordsIns);

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  private void spawnNThreadsForScan(int numberOfThreads, MTable table, int recordsDeleted,
      List<byte[]> keysDeleted, List<byte[]> allKeys, int totalRecordsIns) {
    ExecutorService pool = Executors.newFixedThreadPool(numberOfThreads);

    List threds = new ArrayList();
    for (int i = 0; i < numberOfThreads; i++) {
      threds.add(new ScannerThread(table, recordsDeleted, keysDeleted, allKeys, totalRecordsIns));
    }
    try {
      pool.invokeAll(threds);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  class ScannerThread implements Callable {

    private final MTable table;
    private final int recordsDeleted;
    private final List<byte[]> keysDeleted;
    private final List<byte[]> allKeys;
    private final int totalRecordsIns;

    public ScannerThread(MTable table, int recordsDeleted, List<byte[]> keysDeleted,
        List<byte[]> allKeys, int totalRecordsIns) {
      this.table = table;
      this.recordsDeleted = recordsDeleted;
      this.keysDeleted = keysDeleted;
      this.allKeys = allKeys;
      this.totalRecordsIns = totalRecordsIns;
    }

    public void run() {
      // test by fetching n records
      int fetchSize = 5;

      Scanner scannerd = table.getScanner(new Scan());
      Row[] rown = scannerd.next(fetchSize);
      int howManyTimesNextNIsCalled = 1;

      System.out.println(
          "MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: " + "Size: " + rown.length);
      assertTrue(rown.length <= fetchSize);
      int noOfRecordsInScan = 0;
      int rowKeyIndexScanner = recordsDeleted;
      byte[] pastRowKey = new byte[0];
      int i = 0;
      while (i < rown.length) {
        Row rowd = rown[i];
        assertNotNull(rowd);
        // check columns and rowkey value is right
        byte[] rowKey = rowd.getRowId();
        // check if this row is greter than pervious
        assertTrue(rowKey.length > 0);
        if (noOfRecordsInScan > 0) {
          // ignore for first row key
          // new row key should be greater
          assertTrue(Bytes.compareTo(rowKey, pastRowKey) > 0);
        }

        // check if rowKey is not one which is deleted earlier
        keysDeleted.forEach(deletedKey -> {
          System.out.println("MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: " + "key: "
              + Bytes.toString(rowKey));
          System.out.println("MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: "
              + "deleted key: " + Bytes.toString(deletedKey));
          System.out.println("MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: "
              + "Performing get after delete...");
          Get get = new Get(deletedKey);
          Row res = table.get(get);
          byte[] rowKeyGet = res.getRowId();
          List<Cell> cells = res.getCells();
          System.out.println("MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: "
              + "Cells from get: " + cells.size());
          cells.forEach(cell -> {
            System.out.println("MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: "
                + "cell name: " + Bytes.toString(cell.getColumnName()) + " cell value: "
                + Arrays.toString((byte[]) cell.getColumnValue()));

          });

          assertTrue(res.isEmpty());
          assertTrue(Bytes.compareTo(rowKey, deletedKey) != 0);
        });

        // // check row key value
        byte[] rowKeyExpected = allKeys.get(rowKeyIndexScanner++);
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
            + "-------------------------NEW ROW-------------------------");
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
            + "Row expected: " + Arrays.toString(rowKeyExpected));
        System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
            + "Row received: " + Arrays.toString(rowKey));
        assertEquals(0, Bytes.compareTo(rowKeyExpected, rowKey));

        List<Cell> cells = rowd.getCells();
        // assert if number of cols are equal
        assertEquals(NUM_OF_COLUMNS + 1, cells.size());

        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          Cell cell = cells.get(columnIndex);
          byte[] colNameRec = cell.getColumnName();
          byte[] colNameExp = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
          // check for name
          assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));
          System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
              + "ColName: " + Bytes.toString(colNameRec));
          byte[] colValRec = (byte[]) cell.getColumnValue();
          // byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + columnIndex);

          byte[] valArray = cell.getValueArray();
          int cellLength = cell.getValueLength();
          int cellOffset = cell.getValueOffset();

          // calc cell value from offset and length
          byte[] colValExp = new byte[cellLength];
          System.arraycopy(valArray, cellOffset, colValExp, 0, cellLength);

          // check for value
          System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
              + "ColValueRec: " + Bytes.toString(colValRec));
          System.out.println("MTableScannerScanAfterDeleteDUnit.testScanAfterDelete :: "
              + "ColValueExp: " + Bytes.toString(colValExp));
          assertEquals(0, Bytes.compareTo(colValExp, colValRec));
        }
        pastRowKey = rowKey;
        noOfRecordsInScan++;
        i++;
        if (rown.length == i) {
          rown = scannerd.next(fetchSize);
          howManyTimesNextNIsCalled++;
          i = 0;
        }
      }
      assertEquals(totalRecordsIns - recordsDeleted, noOfRecordsInScan);
      System.out.println("MTableScannerScanAfterDeleteDUnit.testScanNRowFetch :: " + "Iterations :"
          + howManyTimesNextNIsCalled + "Thread ID:- " + Thread.currentThread().getId()
          + " Timestamp " + new Date(System.currentTimeMillis()));
      scannerd.close();
    }

    @Override
    public Object call() throws Exception {
      run();
      return null;
    }
  }
}
