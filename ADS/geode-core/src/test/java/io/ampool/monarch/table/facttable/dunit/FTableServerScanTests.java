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

package io.ampool.monarch.table.facttable.dunit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MException;
import io.ampool.monarch.table.facttable.FTableTestHelper;
import io.ampool.monarch.table.facttable.FTableTestHelper.EvictionTrigger;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.KeyOnlyFilter;
import io.ampool.monarch.table.filter.RowFilter;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.ftable.internal.FTableImpl;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.CompareOp;
import io.ampool.utils.TimestampUtil;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(FTableTest.class)
public class FTableServerScanTests extends MTableDUnitHelper {

  public static String INSERTION_TIMESTAMP = "INSERTION_TIMESTAMP";

  private static final int NUM_OF_COLUMNS = 5;
  private static final int NUM_OF_SPLITS = 113;
  private static final int NUM_ROWS = 200;
  private static final String COLUMN_NAME_PREFIX = "COL";

  private static final int redundancy = 2;

  private int actualRows;

  private static boolean isOverflowEnabled = false;

  private String unsupportedFilterMessage =
          "Unsupported filters : " + "[class io.ampool.monarch.table.filter.RowFilter, "
                  + "class io.ampool.monarch.table.filter.KeyOnlyFilter]";

  private Host host = null;
  private VM vm0 = null;
  private VM vm1 = null;
  private VM vm2 = null;
  private VM vm3 = null;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());
    createClientCache(vm3);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache(vm3);
    closeMClientCache();
    super.tearDown2();
  }


  @Test
  public void testFTableScanBasic() {
    testFTableScanBasicInternal(0);
  }

  @Test
  public void testFTableScanBasicR() {
    testFTableScanBasicInternal(1); // with redundancy
  }

  /**
   * Verify if FTable.scan api is working
   */
  private void testFTableScanBasicInternal(int index) {
    String tableName = getTestMethodName() + index;
    final FTable table = createFTable(tableName, getFTableDescriptor(NUM_OF_SPLITS, index));
    assertNotNull(table);
    vm0.invoke(() -> {
      verifyTableOnServer(tableName, getFTableDescriptor(NUM_OF_SPLITS, index));
    });
    vm1.invoke(() -> {
      verifyTableOnServer(tableName, getFTableDescriptor(NUM_OF_SPLITS, index));
    });
    vm2.invoke(() -> {
      verifyTableOnServer(tableName, getFTableDescriptor(NUM_OF_SPLITS, index));
    });

    Record record = new Record();

    for (int i = 0; i < NUM_ROWS; i++) {
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        System.out.println("FTableScanDUnitTest.testFTableScanBasic :: index: " + colIndex
                + " Putting byte[] " + Arrays.toString(Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex)));
        record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
      }
      table.append(record);
    }

    if (isOverflowEnabled) {
      final EvictionTrigger trigger = FTableTestHelper.createEvictionTrigger(tableName);
      vm0.invoke(() -> {
        trigger.call();
      });
      vm1.invoke(() -> {
        trigger.call();
      });
      vm2.invoke(() -> {
        trigger.call();
      });
    }

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS, index);


    // scan test
    final Scan scan = new Scan();
    final List<Integer> recordCounts = new ArrayList<>();
    recordCounts.add(vm0.invoke("t1", new SerializableCallable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return runScanAtServer(tableName, scan, null, null);
      }
    }));

    recordCounts.add(vm1.invoke("t1", new SerializableCallable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return runScanAtServer(tableName, scan, null, null);
      }
    }));

    recordCounts.add(vm2.invoke("t1", new SerializableCallable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return runScanAtServer(tableName, scan, null, null);
      }
    }));

    final Integer finalCount = recordCounts.stream().mapToInt(i -> i).sum();

    assertEquals(NUM_ROWS, finalCount.intValue());

    deleteFTable(tableName);
  }

  /**
   * Verify if FTable.scan with selected columns
   */
  @Test
  public void testFTableScanSelectedColumns() {
    testFTableScanSelectedColumnsInternal(0);
  }

  @Test
  public void testFTableScanSelectedColumnsR() {
    testFTableScanSelectedColumnsInternal(1); // redundancy
  }

  private void testFTableScanSelectedColumnsInternal(int index) {
    String tableName = getTestMethodName() + index;
    FTableDescriptor ftd = getFTableDescriptor(NUM_OF_SPLITS, index);
    final FTable table = createFTable(tableName, ftd);
    assertNotNull(table);
    vm0.invoke(() -> {
      verifyTableOnServer(tableName, ftd);
    });
    vm1.invoke(() -> {
      verifyTableOnServer(tableName, ftd);
    });
    vm2.invoke(() -> {
      verifyTableOnServer(tableName, ftd);
    });

    Record record = new Record();
    for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
      System.out.println("FTableScanDUnitTest.testFTableScanBasic :: index: " + colIndex
              + " Putting byte[] " + Arrays.toString(Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex)));
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }

    if (isOverflowEnabled) {
      final EvictionTrigger trigger = FTableTestHelper.createEvictionTrigger(tableName);
      vm0.invoke(() -> {
        trigger.call();
      });
      vm1.invoke(() -> {
        trigger.call();
      });
      vm2.invoke(() -> {
        trigger.call();
      });
    }

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS, index);

    // scan test
    final Scan scan = new Scan();
    scan.setColumns(Arrays.asList(2));

    ArrayList<byte[]> selCols = new ArrayList<byte[]>();
    // add above added positions to selCols
    selCols.add(Bytes.toBytes(COLUMN_NAME_PREFIX + 2));
    try {
      runAtServerAndVerifyCount(tableName, scan, NUM_ROWS, selCols, ftd);
    } catch (Exception e) {
      fail("Not expected any exception");
    }

    System.out.println("FTableServerScanTests.testFTableScanSelectedColumns :: "
            + "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
    // selected cols by addColumn method
    Scan scan1 = new Scan();
    scan1 = scan1.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 1));
    selCols = new ArrayList<byte[]>();
    // add above added positions to selCols
    // note zeroth col is INSERTION_TIMESTAMP
    selCols.add(Bytes.toBytes(COLUMN_NAME_PREFIX + 1));
    try {
      runAtServerAndVerifyCount(tableName, scan1, NUM_ROWS, selCols, ftd);
    } catch (Exception e) {
      fail("Not expected any exception");
    }

    deleteFTable(tableName);
    // TODO Handle error condition when both addCol and setCol methods are used
  }

  /**
   * Till now batch mode is not enabled for server side scan.
   */
  // @Test
  public void testFTableScanBatchModeEnabled() {
    String tableName = getTestMethodName();
    final FTable table = createFTable(tableName, getFTableDescriptor(NUM_OF_SPLITS, 0));
    assertNotNull(table);
    vm0.invoke(() -> {
      verifyTableOnServer(tableName, getFTableDescriptor(NUM_OF_SPLITS, 0));
    });
    vm1.invoke(() -> {
      verifyTableOnServer(tableName, getFTableDescriptor(NUM_OF_SPLITS, 0));
    });
    vm2.invoke(() -> {
      verifyTableOnServer(tableName, getFTableDescriptor(NUM_OF_SPLITS, 0));
    });

    Record record = new Record();
    for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }

    if (isOverflowEnabled) {
      final EvictionTrigger trigger = FTableTestHelper.createEvictionTrigger(tableName);
      vm0.invoke(() -> {
        trigger.call();
      });
      vm1.invoke(() -> {
        trigger.call();
      });
      vm2.invoke(() -> {
        trigger.call();
      });
    }

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS, 0);

    // scan test
    Scan scan = new Scan();
    scan.enableBatchMode();
    Scanner scanner = null;
    try {
      scanner = table.getScanner(scan);
    } catch (MException ex) {
      assertTrue(
              ex.getMessage().equalsIgnoreCase("Batch mode is not supported for immutable tables"));
      return;
    }
    fail("Expected exception but not received");
  }

  /**
   * Verify scan. With row key filter Using as row key is currently timestamp is key
   *
   */
  @Test
  public void testFTableScanRowKeyFilter() {
    testFTableScanRowKeyFilterInternal(0);
  }

  @Test
  public void testFTableScanRowKeyFilterR() {
    testFTableScanRowKeyFilterInternal(1);
  }

  private void testFTableScanRowKeyFilterInternal(int index) {
    String tableName = getTestMethodName() + index;
    FTableDescriptor fTableDescriptor = getFTableDescriptor(NUM_OF_SPLITS, index);
    final FTable table = createFTable(tableName, fTableDescriptor);
    assertNotNull(table);
    vm0.invoke(() -> {
      verifyTableOnServer(tableName, fTableDescriptor);
    });
    vm1.invoke(() -> {
      verifyTableOnServer(tableName, fTableDescriptor);
    });
    vm2.invoke(() -> {
      verifyTableOnServer(tableName, fTableDescriptor);
    });

    Record record = new Record();
    for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
      System.out.println("FTableScanDUnitTest.testFTableScanBasic :: index: " + colIndex
              + " Putting byte[] " + Arrays.toString(Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex)));
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
    }
    long insertionStartTime = TimestampUtil.getCurrentTime();
    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }

    // get all keys from server
    final Set<Object> keys = ((FTableImpl) table).getTableRegion().keySetOnServer();

    final Iterator<Object> iterator = keys.iterator();
    Set<Object> sortedKeySet = new TreeSet();
    while (iterator.hasNext()) {
      final BlockValue blockValue =
              (BlockValue) ((FTableImpl) table).getTableRegion().get(iterator.next());
      // sortedKeySet.addAll(blockValue.);
    }
    final Iterator<Object> itr = sortedKeySet.iterator();

    if (isOverflowEnabled) {
      final EvictionTrigger trigger = FTableTestHelper.createEvictionTrigger(tableName);
      vm0.invoke(() -> {
        trigger.call();
      });
      vm1.invoke(() -> {
        trigger.call();
      });
      vm2.invoke(() -> {
        trigger.call();
      });
    }

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS, index);

    // scan test
    Scan scan = new Scan();


    Filter filter =
            new RowFilter(CompareOp.LESS_OR_EQUAL, Bytes.toBytes(TimestampUtil.getCurrentTime()));
    scan.setFilter(filter);

    try {
      runAtServerAndVerifyCount(tableName, scan, 0, null, fTableDescriptor);
      fail("Expected an exception");
    } catch (Exception ex) {
      System.out.println(
              "FTableServerScanTests.testFTableScanRowKeyFilter :: " + "Checking for exception");
      assertEquals(unsupportedFilterMessage, ex.getCause().getMessage());
    }

    deleteFTable(tableName);
  }

  /**
   * Verify scan. With row key filter Using as row key is currently timestamp is key
   */
  @Test
  public void testFTableScanFilterList() throws InterruptedException {
    testFTableScanFilterListInternal(0);
  }

  @Test
  public void testFTableScanFilterListR() throws InterruptedException {
    testFTableScanFilterListInternal(1);
  }

  private void testFTableScanFilterListInternal(int index) throws InterruptedException {
    String tableName = getTestMethodName() + index;
    final FTableDescriptor ftd = getFTableDescriptor(NUM_OF_SPLITS, index);
    final FTable table = createFTable(tableName, ftd);
    assertNotNull(table);
    vm0.invoke(() -> {
      verifyTableOnServer(tableName, ftd);
    });
    vm1.invoke(() -> {
      verifyTableOnServer(tableName, ftd);
    });
    vm2.invoke(() -> {
      verifyTableOnServer(tableName, ftd);
    });

    Record record = new Record();
    for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
    }

    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    long startTimestamp = TimestampUtil.getCurrentTime();
    Thread.sleep(1);
    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }
    Thread.sleep(1);
    long stopTimestamp = TimestampUtil.getCurrentTime();

    if (isOverflowEnabled) {
      final EvictionTrigger trigger = FTableTestHelper.createEvictionTrigger(tableName);
      vm0.invoke(() -> {
        trigger.call();
      });
      vm1.invoke(() -> {
        trigger.call();
      });
      vm2.invoke(() -> {
        trigger.call();
      });
    }

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS * 2, index);

    // scan test
    Scan scan = new Scan();


    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            CompareOp.GREATER, startTimestamp);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            CompareOp.LESS, stopTimestamp);

    FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2);

    scan.setFilter(list);

    try {
      runAtServerAndVerifyCount(tableName, scan, NUM_ROWS, null, ftd);
    } catch (Exception e) {
      fail("Not expected any exception");
    }

    deleteFTable(tableName);
  }

  /**
   * Verify scan. With column value filter Using as row key is currently timestamp is key
   *
   */
  @Test
  public void testFTableScanColumnValueFilter() {
    testFTableScanColumnValueFilterInternal(0);
  }

  @Test
  public void testFTableScanColumnValueFilterR() {
    testFTableScanColumnValueFilterInternal(1);
  }

  private void testFTableScanColumnValueFilterInternal(int index) {
    String tableName = getTestMethodName() + index;
    FTableDescriptor ftd = getFTableDescriptor(NUM_OF_SPLITS, index);
    final FTable table = createFTable(tableName, ftd);
    assertNotNull(table);
    vm0.invoke(() -> {
      verifyTableOnServer(tableName, ftd);
    });
    vm1.invoke(() -> {
      verifyTableOnServer(tableName, ftd);
    });
    vm2.invoke(() -> {
      verifyTableOnServer(tableName, ftd);
    });

    Record record = new Record();
    for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }

    if (isOverflowEnabled) {
      final EvictionTrigger trigger = FTableTestHelper.createEvictionTrigger(tableName);
      vm0.invoke(() -> {
        trigger.call();
      });
      vm1.invoke(() -> {
        trigger.call();
      });
      vm2.invoke(() -> {
        trigger.call();
      });
    }

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS, index);

    // scan test
    Scan scan = new Scan();


    // MFilter filter = new RowFilter(CompareOp.LESS_OR_EQUAL,Bytes.toBytes(System.nanoTime()));
    Filter filter = new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.EQUAL,
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    scan.setFilter(filter);

    try {
      runAtServerAndVerifyCount(tableName, scan, NUM_ROWS, null, ftd);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Not expected any exception");
    }


    filter = new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.GREATER,
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    scan.setFilter(filter);
    // after this number of records from scan should be zero rows
    try {
      runAtServerAndVerifyCount(tableName, scan, 0, null, ftd);
    } catch (Exception e) {
      fail("Not expected any exception");
    }

    filter = new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.GREATER_OR_EQUAL,
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    scan.setFilter(filter);
    // after this number of records from scan should be zero rows
    try {
      runAtServerAndVerifyCount(tableName, scan, NUM_ROWS, null, ftd);
    } catch (Exception e) {
      fail("Not expected any exception");
    }

    filter = new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.LESS,
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    scan.setFilter(filter);
    // after this number of records from scan should be zero rows

    try {
      runAtServerAndVerifyCount(tableName, scan, 0, null, ftd);
    } catch (Exception e) {
      fail("Not expected any exception");
    }

    filter = new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.LESS_OR_EQUAL,
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    scan.setFilter(filter);
    // after this number of records from scan should be zero rows

    try {
      runAtServerAndVerifyCount(tableName, scan, NUM_ROWS, null, ftd);
    } catch (Exception e) {
      fail("Not expected any exception");
    }

    filter = new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.NOT_EQUAL,
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    scan.setFilter(filter);
    // after this number of records from scan should be zero rows
    try {
      runAtServerAndVerifyCount(tableName, scan, 0, null, ftd);
    } catch (Exception e) {
      fail("Not expected any exception");
    }

    deleteFTable(tableName);
  }

  /**
   * Verify scan. With key only filter Using as row key is currently timestamp is key
   *
   */
  @Test
  public void testFTableScanKeyOnlyFilter() {
    testFTableScanKeyOnlyFilterInternal(0);
  }

  @Test
  public void testFTableScanKeyOnlyFilterR() {
    testFTableScanKeyOnlyFilterInternal(1);
  }

  private void testFTableScanKeyOnlyFilterInternal(int index) {
    String tableName = getTestMethodName() + index;
    FTableDescriptor ftd = getFTableDescriptor(NUM_OF_SPLITS, index);
    final FTable table = createFTable(tableName, ftd);
    assertNotNull(table);
    vm0.invoke(() -> {
      verifyTableOnServer(tableName, ftd);
    });
    vm1.invoke(() -> {
      verifyTableOnServer(tableName, ftd);
    });
    vm2.invoke(() -> {
      verifyTableOnServer(tableName, ftd);
    });

    Record record = new Record();
    for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }

    if (isOverflowEnabled) {
      final EvictionTrigger trigger = FTableTestHelper.createEvictionTrigger(tableName);
      vm0.invoke(() -> {
        trigger.call();
      });
      vm1.invoke(() -> {
        trigger.call();
      });
      vm2.invoke(() -> {
        trigger.call();
      });
    }

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS, index);

    // scan test
    Scan scan = new Scan();


    // MFilter filter = new RowFilter(CompareOp.LESS_OR_EQUAL,Bytes.toBytes(System.nanoTime()));
    Filter filter = new KeyOnlyFilter();
    scan.setFilter(filter);
    try {
      runAtServerAndVerifyCount(tableName, scan, NUM_ROWS, null, ftd);
    } catch (Exception ex) {
      System.out.println(ex.getCause().getMessage());
      assertEquals(unsupportedFilterMessage, ex.getCause().getMessage());
    }

    deleteFTable(tableName);
  }



  // ---------------------- NO MORE TESTS BELOW
  // ---------------------------------------------------------------------------

  private void runAtServerAndVerifyCount(final String tableName, final Scan scan, int expectedCount,
                                         final ArrayList<byte[]> selCols, final FTableDescriptor ftd) throws Exception {
    final List<Integer> recordCounts = new ArrayList<>();
    try {
      recordCounts.add(vm0.invoke("t1", new SerializableCallable<Integer>() {
        @Override
        public Integer call() throws Exception {
          try {
            return runScanAtServer(tableName, scan, selCols, ftd);
          } catch (Throwable t) {
            throw t;
          }
        }
      }));

      recordCounts.add(vm1.invoke("t1", new SerializableCallable<Integer>() {
        @Override
        public Integer call() throws Exception {
          return runScanAtServer(tableName, scan, selCols, ftd);
        }
      }));

      recordCounts.add(vm2.invoke("t1", new SerializableCallable<Integer>() {
        @Override
        public Integer call() throws Exception {
          return runScanAtServer(tableName, scan, selCols, ftd);
        }
      }));
    } catch (Throwable t) {
      throw t;
    }
    final int count = recordCounts.stream().mapToInt(i -> i).sum();
    assertEquals(expectedCount, count);
  }


  private int runScanAtServer(final String tableName, Scan mscan, List<byte[]> selCols,
                              FTableDescriptor ftd) {
    System.out.println("FTableScanTests.runScanAtServer :: " + "got table name: " + tableName);
    final MCache cache = MCacheFactory.getAnyInstance();
    final FTable table = cache.getFTable(tableName);
    final Scanner scanner = table.getScanner(mscan);
    int count = 0;
    Row res = scanner.next();
    System.out.println("FTableScanTests.runScanAtServer :: " + "scan result: " + res);
    while (res != null) {
      if (selCols != null && ftd != null) {
        verifyScannedValue(res, count, selCols, ftd);
      } else {
        verifyScannedValue(res, count);
      }
      count++;
      res = scanner.next();
    }
    System.out.println("FTableScanTests.runScanAtServer :: " + "ct: " + count);
    return count;
  }

  private void verifyScannedValue(final Row res, final int index) {
    final SingleVersionRow row = res.getLatestRow();
    final Iterator<Cell> itr = row.getCells().iterator();
    System.out.println(
            "FTableServerScanTests.verifyScannedValue :: " + "cell size: " + row.getCells().size());
    row.getCells().forEach((cell) -> {
      System.out.println("FTableServerScanTests.verifyScannedValue :: " + "NAME: "
              + Bytes.toString(cell.getColumnName()));
    });
    int colIndex = 0;
    while (itr.hasNext()) {
      final Cell cell = itr.next();
      if (!Bytes.toString(cell.getColumnName())
              .equalsIgnoreCase(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) {
        assertEquals(Bytes.toString(cell.getColumnName()), COLUMN_NAME_PREFIX + colIndex);
        System.out.println("FTableScanDUnitTest.verifyScannedValue :: " + "CNAME: "
                + Bytes.toString(cell.getColumnName()));
        System.out.println("FTableScanDUnitTest.verifyScannedValue :: " + "VALUE: "
                + Arrays.toString(cell.getValueArray()));
        assertTrue("Incorrect column value for: " + cell,
                Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                        Bytes.toBytes(COLUMN_NAME_PREFIX + (colIndex)), 0, cell.getValueLength()) == 0);
        // System.out.println("FTableScanDUnitTest.verifyScannedValue :: " + "VALUE: "+
        // (String)cell.getColumnValue());
        colIndex++;
      }
      System.out.println(
              "---------------------------------------------------------------------------------------------------------------------------------");
    }
  }

  private void verifyScannedValue(final Row res, final int index, List<byte[]> selCols,
                                  FTableDescriptor ftd) {
    final SingleVersionRow row = res.getLatestRow();
    assertEquals(selCols.size(), row.getCells().size());
    System.out.println("FTableServerScanTests.verifyScannedValue :: " + "selCols: " + selCols);
    final Iterator<Cell> itr = row.getCells().iterator();
    while (itr.hasNext()) {
      final Cell cell = itr.next();
      for (int i = 0; i < selCols.size(); i++) {
        if (!Bytes.toString(cell.getColumnName())
                .equalsIgnoreCase(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) {
          final Integer positionIndex = ftd.getColumnDescriptorsMap()
                  .get(ftd.getColumnsByName().get(new ByteArrayKey(cell.getColumnName())));
          System.out.println(
                  "FTableServerScanTests.verifyScannedValue :: " + "posIndex: " + positionIndex);
          if (Bytes.compareTo(selCols.get(i), cell.getColumnName()) == 0) {
            // check for this col
            assertEquals(Bytes.toString(cell.getColumnName()),
                    COLUMN_NAME_PREFIX + (positionIndex));
            System.out.println("FTableScanDUnitTest.verifyScannedValue :: " + "CNAME: "
                    + Bytes.toString(cell.getColumnName()));
            System.out.println("FTableScanDUnitTest.verifyScannedValue :: " + "VALUE: "
                    + Arrays.toString(cell.getValueArray()) + " string: " + Bytes
                    .toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            assertTrue("Incorrect column value for: " + cell,
                    Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                            Bytes.toBytes(COLUMN_NAME_PREFIX + (positionIndex)), 0,
                            cell.getValueLength()) == 0);
          }
        }
      }
      System.out.println(
              "---------------------------------------------------------------------------------------------------------------------------------");
    }
  }


  protected void verifyValuesOnAllVMs(String tableName, int expectedRows, int index) {
    final boolean doScanAll = index > 0;
    if (doScanAll) {
      expectedRows *= (redundancy + 1);
    }
    final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
    for (int i = 0; i < vmList.size(); i++) {
      final int res = (int) vmList.get(i).invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          return verifyValues(tableName, doScanAll);
        }
      });
      actualRows += res;
    }
    assertEquals(expectedRows, actualRows);
  }

  private Iterator<BucketRegion> getBucketsIterator(Region region, boolean doScanAll) {
    return doScanAll ////
            ? ((PartitionedRegion) region).getDataStore().getAllLocalBucketRegions().iterator()
            : ((PartitionedRegion) region).getDataStore().getAllLocalPrimaryBucketRegions().iterator();
  }

  protected int verifyValues(String tableName, final boolean doScanAll) {
    int entriesCount = 0;
    final Region<Object, Object> region = MCacheFactory.getAnyInstance().getRegion(tableName);
    assertNotNull(region);
    final Iterator<BucketRegion> itr = getBucketsIterator(region, doScanAll);
    while (itr.hasNext()) {
      final BucketRegion bucketRegion = itr.next();
      final RowTupleConcurrentSkipListMap internalMap =
              (RowTupleConcurrentSkipListMap) bucketRegion.entries.getInternalMap();
      final Map concurrentSkipListMap = internalMap.getInternalMap();
      final Iterator<Entry> iterator = concurrentSkipListMap.entrySet().iterator();
      while (iterator.hasNext()) {
        final Entry entry = iterator.next();
        if (((RegionEntry) entry.getValue())._getValue() instanceof Token) {
          continue;
        }
        RegionEntry value1 = (RegionEntry) entry.getValue();
        Object value = value1._getValue();
        if (value instanceof VMCachedDeserializable) {
          value = ((VMCachedDeserializable) value).getDeserializedForReading();
        }
        final BlockValue blockValue = (BlockValue) value;
        final Iterator objectIterator = blockValue.iterator();
        while (objectIterator.hasNext()) {
          objectIterator.next();
          entriesCount++;
        }
      }
      // System.out.println("Bucket Region Name : " + bucketRegion.getName() + " Size: " +
      // bucketRegion.size());
    }
    return entriesCount;
  }

  protected void stopServer(VM vm) {
    System.out.println("CreateMTableDUnitTest.stopServer :: " + "Stopping server....");
    try {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          MCacheFactory.getAnyInstance().close();
          return null;
        }
      });
    } catch (Exception ex) {
      fail(ex.getMessage());
    }
  }

  private static FTable createFTable(final String tableName,
                                     final FTableDescriptor tableDescriptor) {
    // System.out.println("CreateMTableDUnitTest.createFTable :: " + "Creating mtable:---- " +
    // tableDescriptor);
    int numberOfKeysPerBucket = 20;
    int splits = 10;
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    // System.out.println("CreateMTableDUnitTest.createFTable :: " + "isCacheClosed: " +
    // clientCache.isClosed());
    // MTableDescriptor tableDescriptor = getFTableDescriptor(splits);

    FTable mtable = null;
    try {
      mtable = clientCache.getAdmin().createFTable(tableName, tableDescriptor);
    } catch (Exception e) {
      System.out.println("CreateMTableDUnitTest.createFTable :: " + "Throwing from test");
      throw e;
    }
    System.out.println("CreateMTableDUnitTest.createFTable :: " + "mtable is " + mtable);
    return mtable;

  }

  private static void deleteFTable(final String tableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    clientCache.getAdmin().deleteFTable(tableName);
  }

  private static FTableDescriptor getFTableDescriptor(final int splits, int descriptorIndex) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    if (descriptorIndex == 0) {
      tableDescriptor.setTotalNumOfSplits(splits);
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        // sets a partitioningColumn
        if (colmnIndex == 0) {
          tableDescriptor = tableDescriptor.addColumn(
                  Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex)/* , MBasicObjectType.INT */);
          tableDescriptor =
                  tableDescriptor.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
        } else {
          tableDescriptor =
                  tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
        }
      }
      tableDescriptor.setRedundantCopies(0);
    } else {
      tableDescriptor.setTotalNumOfSplits(splits);
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        // sets a partitioningColumn
        if (colmnIndex == 0) {
          tableDescriptor.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
        }
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
      tableDescriptor.setRedundantCopies(redundancy);
    }
    return tableDescriptor;
  }

  private static void verifyTableOnServer(final String tableName,
                                          final FTableDescriptor tableDescriptor) {
    final MCache serverCache = MCacheFactory.getAnyInstance();
    assertNotNull(serverCache);
    FTable mtable = null;
    int retries = 0;
    do {
      mtable = serverCache.getFTable(tableName);
      if (retries > 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      retries++;
      // if (retries > 1) {
      // System.out.println("CreateMTableDUnitTest.verifyTableOnServer :: " + "Attempting to fetch
      // table... Attempt " + retries);
      // }
    } while (mtable == null && retries < 500);
    assertNotNull(mtable);
    final Region<Object, Object> mregion = ((FTableImpl) mtable).getTableRegion();
    String path = mregion.getFullPath();
    assertTrue(path.contains(tableName));
    // To verify disk persistence
    assertEquals(tableDescriptor.isDiskPersistenceEnabled(),
            mregion.getAttributes().getDataPolicy().withPersistence());
  }

  protected void setEvictionPercetangeOnAllVMs(float percetange) {
    // final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
    // vmList.forEach(vm -> {
    // vm.invoke(new SerializableCallable() {
    // @Override public Object call() throws Exception {
    // MCacheFactory.getAnyInstance().getResourceManager().setEvictionHeapPercentage(percetange);
    // return null;
    // }
    // });
    // });
    isOverflowEnabled = true;
  }
}
