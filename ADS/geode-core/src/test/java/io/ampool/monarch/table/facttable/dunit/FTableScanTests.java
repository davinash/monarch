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

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;
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
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.monarch.table.ftable.utils.FTableUtils;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.MClientScannerUsingGetAll;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.TypeHelper;
import io.ampool.store.StoreHandler;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.internal.DefaultStore;
import io.ampool.tierstore.wal.WriteAheadLog;
import io.ampool.utils.ReflectionUtils;
import io.ampool.utils.TimestampUtil;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.ServerLocation;
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
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(FTableTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class FTableScanTests extends MTableDUnitHelper {

  public static String INSERTION_TIMESTAMP = "INSERTION_TIMESTAMP";

  private static final int NUM_OF_COLUMNS = 5;
  private static final int NUM_OF_SPLITS = 113;
  private static final int NUM_ROWS = 113;
  private static final String COLUMN_NAME_PREFIX = "COL";

  private int actualRows;

  private static boolean isOverflowEnabled = false;

  private static boolean flushWAL = false;

  private static boolean flushTier1 = false;

  private static boolean flushTier2 = false;


  @Parameterized.Parameter
  public static FTableDescriptor.BlockFormat blockFormat;

  @Parameterized.Parameters(name = "BlockFormat: {0}")
  public static Collection<FTableDescriptor.BlockFormat> data() {
    return Arrays.asList(FTableDescriptor.BlockFormat.values());
  }

  /**
   * Return the method-name. In case of Parameterized tests, the parameters are appended to the
   * method-name (like testMethod[0]) and hence need to be stripped off.
   *
   * @return the test method name
   */
  public static String getMethodName() {
    final String methodName = getTestMethodName();
    final int index = methodName.indexOf('[');
    return index > 0 ? methodName.substring(0, index) : methodName;
  }

  @After
  public void cleanUpMethod() {
    final Admin admin = MClientCacheFactory.getAnyInstance().getAdmin();
    final String tableName = getMethodName();
    if (admin.existsFTable(tableName)) {
      admin.deleteFTable(tableName);
    }
  }

  private String unsupportedFilterMessage =
      "Unsupported filters : " + "[class io.ampool.monarch.table.filter.RowFilter, "
          + "class io.ampool.monarch.table.filter.KeyOnlyFilter]";

  private Host host = null;
  public VM vm0 = null;
  public VM vm1 = null;
  public VM vm2 = null;
  public VM vm3 = null;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    int port = (int) startServerOn(vm0, DUnitLauncher.getLocatorString());
    System.out.println("FTableScanTests.postSetUp :: " + "VM0: " + vm0 + " port: " + port);
    port = (int) startServerOn(vm1, DUnitLauncher.getLocatorString());
    System.out.println("FTableScanTests.postSetUp :: " + "VM1: " + vm1 + " port: " + port);
    port = (int) startServerOn(vm2, DUnitLauncher.getLocatorString());
    System.out.println("FTableScanTests.postSetUp :: " + "VM3: " + vm3 + " port: " + port);
    createClientCache(vm3);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(vm3);
    stopServerOn(vm0);
    stopServerOn(vm1);
    stopServerOn(vm2);
    super.tearDown2();
  }


  /**
   * Verify if FTable.append api is working
   */
  @Test
  public void testFTableScanBasic() {
    String tableName = getMethodName();
    int actualRows = 0;
    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    fTableDescriptor.setTotalNumOfSplits(10);
    fTableDescriptor.setRedundantCopies(1);
    fTableDescriptor.setBlockSize(3);
    fTableDescriptor.setBlockFormat(blockFormat);
    Schema.Builder sb = new Schema.Builder();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      sb.column(COLUMN_NAME_PREFIX + colmnIndex);
    }
    fTableDescriptor.setSchema(sb.build());
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
    for (int i = 0; i < NUM_ROWS; i++) {
      /* only to get data distributed across buckets randomly.. */
      record.getValueMap().put(
          new ByteArrayKey(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME.getBytes()), (long) i);
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
    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS);
    flushWALOnAllVMs(tableName);
    verifyWALCountOnAllVMs(tableName, 0);
    flushTier1OnAllVMs();
    verifyTier1CountOnAllVMs(tableName, 0);


    // scan test
    final Scanner scanner = table.getScanner(new Scan());
    int count = 0;
    Row res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count);
      count++;
      res = scanner.next();

    }
    assertEquals(NUM_ROWS, count);

    // scan test by selecting all columns manually
    Scan scan = new Scan();
    scan.setColumns(Arrays.asList(0, 1, 2, 3, 4, 5));
    final Scanner scanner1 = table.getScanner(scan);
    int count1 = 0;
    Row res1 = scanner1.next();
    while (res1 != null) {
      verifyScannedValue(res1, count1);
      count1++;
      res1 = scanner1.next();

    }
    assertEquals(NUM_ROWS, count1);

    // flush tier 2
    flushTier2OnAllVMs();
    verifyTier2CountOnAllVMs(tableName, 0);
  }

  @Test
  public void testFTableScanBasicWithBatchAppend() {
    String tableName = getMethodName();
    int actualRows = 0;
    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    fTableDescriptor.setTotalNumOfSplits(10);
    fTableDescriptor.setRedundantCopies(1);
    fTableDescriptor.setBlockSize(3);
    fTableDescriptor.setBlockFormat(blockFormat);
    Schema.Builder sb = new Schema.Builder();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      sb.column(COLUMN_NAME_PREFIX + colmnIndex);
    }
    fTableDescriptor.setSchema(sb.build());
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

    Record[] records = new Record[2];
    for (int i = 0, j = 0; i < NUM_ROWS; i++) {
      Record record = new Record();
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
      }
      /* only to get data distributed across buckets randomly.. */
      record.getValueMap().put(
          new ByteArrayKey(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME.getBytes()), (long) i);
      records[j++] = record;
      if (j == 2) {
        table.append(records);
        j = 0;
      }
    }
    table.append(records[0]);

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
    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS);
    flushWALOnAllVMs(tableName);
    verifyWALCountOnAllVMs(tableName, 0);
    flushTier1OnAllVMs();
    verifyTier1CountOnAllVMs(tableName, 0);


    // scan test
    final Scanner scanner = table.getScanner(new Scan());
    int count = 0;
    Row res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count);
      count++;
      res = scanner.next();

    }
    assertEquals(NUM_ROWS, count);

    // scan test by selecting all columns manually
    Scan scan = new Scan();
    scan.setColumns(Arrays.asList(0, 1, 2, 3, 4, 5));
    final Scanner scanner1 = table.getScanner(scan);
    int count1 = 0;
    Row res1 = scanner1.next();
    while (res1 != null) {
      verifyScannedValue(res1, count1);
      count1++;
      res1 = scanner1.next();

    }
    assertEquals(NUM_ROWS, count1);

    // flush tier 2
    flushTier2OnAllVMs();
    verifyTier2CountOnAllVMs(tableName, 0);
  }


  /**
   * Verify if FTable.scan with selected columns
   */
  @Test
  public void testFTableScanSelectedColumns() {
    String tableName = getMethodName();
    int actualRows = 0;
    FTableDescriptor ftd = getFTableDescriptor(NUM_OF_SPLITS, 0);
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

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS);
    flushWALOnAllVMs(tableName);
    verifyWALCountOnAllVMs(tableName, 0);
    flushTier1OnAllVMs();
    verifyTier1CountOnAllVMs(tableName, 0);



    // scan test
    Scan scan = new Scan();
    scan.setColumns(Arrays.asList(2));

    ArrayList<byte[]> selCols = new ArrayList<byte[]>();
    // add above added positions to selCols
    selCols.add(Bytes.toBytes(COLUMN_NAME_PREFIX + 2));

    Scanner scanner = table.getScanner(scan);
    int count = 0;
    Row res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count, selCols, ftd);
      count++;
      res = scanner.next();

    }
    assertEquals(NUM_ROWS, count);

    // selected cols by addColumn method

    scan = new Scan();
    scan = scan.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 1));
    selCols = new ArrayList<byte[]>();
    // add above added positions to selCols
    // note zeroth col is INSERTION_TIMESTAMP
    selCols.add(Bytes.toBytes(COLUMN_NAME_PREFIX + 1));

    scanner = table.getScanner(scan);
    count = 0;
    res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count, selCols, ftd);
      count++;
      res = scanner.next();

    }
    assertEquals(NUM_ROWS, count);

    // TODO Handle error condition when both addCol and setCol methods are used

    // flush tier 2
    flushTier2OnAllVMs();
    verifyTier2CountOnAllVMs(tableName, 0);
  }

  /**
   * Ref. GEN-2082: Incorrect ordering for the records inside the bucket. Inside buckets records
   * should be ordered
   *
   */
  @Test
  public void testFTableScan_OrderingInsideBucket() {
    String tableName = getMethodName();
    int numBuckets = 4;
    String[] columnNames = {"NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ"};
    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    fTableDescriptor.setBlockFormat(blockFormat);
    fTableDescriptor.setBlockSize(1);
    final Schema schema = new Schema.Builder().column(columnNames[0], BasicTypes.STRING)
        .column(columnNames[1], BasicTypes.STRING).column(columnNames[2], BasicTypes.INT)
        .column(columnNames[3], BasicTypes.LONG).column(columnNames[4], BasicTypes.STRING)
        .column(columnNames[5], BasicTypes.DATE).build();
    fTableDescriptor.setSchema(schema);

    fTableDescriptor.setTotalNumOfSplits(numBuckets);

    // set the partitioning column
    fTableDescriptor.setPartitioningColumn(Bytes.toBytes(columnNames[1]));
    final FTable fTable = createFTable(tableName, fTableDescriptor);

    int numOfEntries = 10; // Number of Records
    int repeatDataLoop = 1; // Repeat Data loop

    // ingest records using batch append
    int batchAppendSize = numOfEntries;

    Record[] records = new Record[batchAppendSize];
    int count = 0;
    long startTimeStamp = System.currentTimeMillis();
    int totalAppendedRecords = 0;
    for (int r = 0; r < repeatDataLoop; r++) {
      int base = (r + 1) * 100;

      // Ingest records using Batch Append
      for (int i = 0; i < batchAppendSize; i++) {
        int idVal = base + i;
        count++;
        int age = count;
        long sal = 10000 + count;

        Record record = new Record();
        record.add(columnNames[0], "NAME" + i);
        record.add(columnNames[1], "ID" + (base + i));
        record.add(columnNames[2], 10 + i);
        record.add(columnNames[3], new Long(10000 * i));
        record.add(columnNames[4], "DEPT");
        record.add(columnNames[5], new Date(2000, 11, 21));
        records[i] = record;
        System.out.println("ID " + (base + i) + "\t" + "Bucket "
            + Arrays.hashCode(Bytes.toBytes("ID" + (base + i))) % numBuckets + "\t" + "NAME "
            + ("NAME" + i));
      }

      fTable.append(records);
      totalAppendedRecords += records.length;
      assertEquals(batchAppendSize, records.length);
    }

    System.out.println("Total records " + totalAppendedRecords);
    assertEquals(numOfEntries * repeatDataLoop, totalAppendedRecords);

    // --- scan
    Scan scan = new Scan();
    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, startTimeStamp);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, System.currentTimeMillis());
    FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2);
    scan.setFilter(list);

    final Scanner scanner = fTable.getScanner(scan);
    final Iterator<Row> iterator = scanner.iterator();

    Map<Integer, List<Row>> bucketRowsMap = new HashMap<>();

    int recordCount = 0;
    while (iterator.hasNext()) {
      recordCount++;
      final Row result = iterator.next();
      System.out.println("-----------------------------------");
      // read the columns
      final List<Cell> cells = result.getCells();
      // NAME
      int bucketId =
          Arrays.hashCode(Bytes.toBytes((String) cells.get(1).getColumnValue())) % numBuckets;
      if (bucketRowsMap.containsKey(bucketId)) {
        bucketRowsMap.get(bucketId).add(result);
      } else {
        LinkedList<Row> rows = new LinkedList<>();
        rows.add(result);
        bucketRowsMap.put(bucketId, rows);
      }

      System.out.println("Record " + recordCount + "\t"
          + Bytes.toString(cells.get(0).getColumnName()) + " : " + cells.get(0).getColumnValue()
          + "\t" + Bytes.toString(cells.get(1).getColumnName()) + " : "
          + cells.get(1).getColumnValue() + "\t" + "Bucket = " + bucketId + "\t"
          + Bytes.toString(cells.get(2).getColumnName()) + " : " + cells.get(2).getColumnValue()
          + "\t" + Bytes.toString(cells.get(3).getColumnName()) + " : "
          + cells.get(3).getColumnValue() + "\t" + " Insert TS : " + cells.get(6).getColumnValue());
      System.out.println();
    }
    System.out.println("============================================");

    // verify that records are in order of insertion
    bucketRowsMap.forEach((k, v) -> {
      AtomicLong prevTS = new AtomicLong(Long.MIN_VALUE);
      v.forEach(row -> {
        assertTrue(row.getRowTimeStamp() >= prevTS.get());
        prevTS.set(row.getRowTimeStamp());
      });
    });
    System.out.println("Successfully scanned " + recordCount + "records.");
  }


  /**
   * Till now batch mode is not enabled.
   */
  @Test
  public void testFTableScanBatchModeEnabled() {
    String tableName = getMethodName();
    int actualRows = 0;
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
      // System.out.println("FTableScanDUnitTest.testFTableScanBasic :: index: " + colIndex + "
      // Putting byte[] "+Arrays.toString(Bytes.toBytes(COLUMN_NAME_PREFIX+colIndex)));
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

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS);
    flushWALOnAllVMs(tableName);
    verifyWALCountOnAllVMs(tableName, 0);
    flushTier1OnAllVMs();
    verifyTier1CountOnAllVMs(tableName, 0);

    // flush tier 2
    flushTier2OnAllVMs();
    verifyTier2CountOnAllVMs(tableName, 0);

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
    String tableName = getMethodName();
    int actualRows = 0;
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
      System.out.println("FTableScanDUnitTest.testFTableScanBasic :: index: " + colIndex
          + " Putting byte[] " + Arrays.toString(Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex)));
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
    }
    long insertionStartTime = TimestampUtil.getCurrentTime();
    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }

    // get all keys from server
    final Set<Object> keys = ((ProxyFTableRegion) table).getTableRegion().keySetOnServer();

    final Iterator<Object> iterator = keys.iterator();
    Set<Object> sortedKeySet = new TreeSet();
    while (iterator.hasNext()) {
      final BlockValue blockValue =
          (BlockValue) ((ProxyFTableRegion) table).getTableRegion().get(iterator.next());
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

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS);
    flushWALOnAllVMs(tableName);
    verifyWALCountOnAllVMs(tableName, 0);
    flushTier1OnAllVMs();
    verifyTier1CountOnAllVMs(tableName, 0);

    // scan test
    Scan scan = new Scan();


    Filter filter =
        new RowFilter(CompareOp.LESS_OR_EQUAL, Bytes.toBytes(TimestampUtil.getCurrentTime()));
    scan.setFilter(filter);
    try {
      // after this number of records from scan should be all rows
      Scanner scanner = table.getScanner(scan);
      fail("Expected unsupported filter exception");
    } catch (MException ex) {
      System.out.println(ex.getMessage());
      assertEquals(unsupportedFilterMessage, ex.getMessage());
    }

    // flush tier 2
    flushTier2OnAllVMs();
    verifyTier2CountOnAllVMs(tableName, 0);
  }


  @Test
  public void testAppendAndScanWithMultipleClients() throws InterruptedException {
    String tableName = getMethodName();
    final FTableDescriptor ftd = getFTableDescriptor(NUM_OF_SPLITS, 0);
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

    Thread.sleep(1000l);
    long client1StartTimestamp = TimestampUtil.getCurrentTime();
    long client2StartTimestamp = (long) vm3.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return TimestampUtil.getCurrentTime();
      }
    });

    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }

    Thread.sleep(1000l);
    long client1StopTimestamp = TimestampUtil.getCurrentTime();
    long client2StopTimestamp = (long) vm3.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return TimestampUtil.getCurrentTime();
      }
    });

    verifyValuesOnAllVMs(tableName, NUM_ROWS * 2);

    // scan test
    Scan scan = new Scan();
    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER_OR_EQUAL, client1StartTimestamp);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS_OR_EQUAL, client1StopTimestamp);
    FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2);
    scan.setFilter(list);

    // after this number of records from scan should be NUM_ROWS
    Scanner scanner = table.getScanner(scan);
    int count = 0;
    Row res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count);
      count++;
      res = scanner.next();
    }
    assertEquals(NUM_ROWS, count); /* Total rows are NUM_ROWS * 2 */

    // scan test
    scan = new Scan();
    filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER_OR_EQUAL, client2StartTimestamp);
    filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS_OR_EQUAL, client2StopTimestamp);
    list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2);
    scan.setFilter(list);

    // after this number of records from scan should be NUM_ROWS
    scanner = table.getScanner(scan);
    count = 0;
    res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count);
      count++;
      res = scanner.next();
    }
    assertEquals(NUM_ROWS, count); /* Total rows are NUM_ROWS * 2 */

    vm3.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        FTable fTable = MClientCacheFactory.getAnyInstance().getFTable(tableName);
        // scan test
        Scan scan = new Scan();
        Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            CompareOp.GREATER_OR_EQUAL, client1StartTimestamp);
        Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            CompareOp.LESS_OR_EQUAL, client1StopTimestamp);
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2);
        scan.setFilter(list);

        // after this number of records from scan should be NUM_ROWS
        Scanner scanner = fTable.getScanner(scan);
        int count = 0;
        Row res = scanner.next();
        while (res != null) {
          verifyScannedValue(res, count);
          count++;
          res = scanner.next();
        }
        assertEquals(NUM_ROWS, count); /* Total rows are NUM_ROWS * 2 */

        // scan test
        scan = new Scan();
        filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            CompareOp.GREATER_OR_EQUAL, client2StartTimestamp);
        filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            CompareOp.LESS_OR_EQUAL, client2StopTimestamp);
        list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2);
        scan.setFilter(list);

        // after this number of records from scan should be NUM_ROWS
        scanner = fTable.getScanner(scan);
        count = 0;
        res = scanner.next();
        while (res != null) {
          verifyScannedValue(res, count);
          count++;
          res = scanner.next();
        }
        assertEquals(NUM_ROWS, count); /* Total rows are NUM_ROWS * 2 */
        return null;
      }
    });

    MClientCacheFactory.getAnyInstance().getAdmin().deleteFTable(tableName);

  }



  /**
   * Verify scan. With row key filter Using as row key is currently timestamp is key
   */
  @Test
  public void testFTableScanFilterList() {
    String tableName = getMethodName();
    final FTableDescriptor ftd = getFTableDescriptor(NUM_OF_SPLITS, 0);
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

    long startTimestamp = TimestampUtil.getCurrentTime();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }


    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }
    // sleep for 1 seconds for to get stop timestamp
    try {
      Thread.sleep(1000l);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

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

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS * 2);
    flushWALOnAllVMs(tableName);
    verifyWALCountOnAllVMs(tableName, 0);
    flushTier1OnAllVMs();
    verifyTier1CountOnAllVMs(tableName, 0);

    // scan test
    Scan scan = new Scan();


    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, startTimestamp);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, stopTimestamp);

    FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2);

    scan.setFilter(list);
    // after this number of records from scan should be NUM_ROWS
    Scanner scanner = table.getScanner(scan);
    int count = 0;
    Row res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count);
      count++;
      res = scanner.next();
    }
    assertEquals(NUM_ROWS, count); /* Total rows are NUM_ROWS * 2 */

    // flush tier 2
    flushTier2OnAllVMs();
    verifyTier2CountOnAllVMs(tableName, 0);
  }

  /**
   * Verify scan. With row key filter Using as row key is currently timestamp is key
   */
  @Test
  public void testFTableScanFilterList2() throws InterruptedException {
    String tableName = getMethodName();
    final FTableDescriptor ftd = getFTableDescriptor(NUM_OF_SPLITS, 0);
    ftd.setBlockSize(113);
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
    Thread.sleep(1000l);
    long startTimestamp = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);
    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }
    Thread.sleep(1000l);
    long stopTimestamp = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);

    long startTimestamp1 = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);
    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }

    Thread.sleep(1000l);
    long stopTimestamp1 = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);


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

    Thread.sleep(1000l);

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS * 3);
    flushWALOnAllVMs(tableName);
    verifyWALCountOnAllVMs(tableName, 0);
    flushTier1OnAllVMs();
    verifyTier1CountOnAllVMs(tableName, 0);

    System.out.println("FTableScanTests.testFTableScanFilterList :: "
        + "----------------------------------------------------------------------------------");
    // scan test
    Scan scan = new Scan();


    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, startTimestamp);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, stopTimestamp);

    FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2);

    scan.setFilter(list);
    // after this number of records from scan should be NUM_ROWS
    Scanner scanner = table.getScanner(scan);
    int count = 0;
    Row res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count);
      count++;
      res = scanner.next();
    }
    assertEquals(NUM_ROWS, count); /* Total rows are NUM_ROWS * 2 */

    System.out.println("FTableScanTests.testFTableScanFilterList :: "
        + "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
    scan = new Scan();

    System.out.println("FTableScanTests.testFTableScanFilterList :: " + "T1: " + startTimestamp
        + " T2: " + startTimestamp + " T3: " + startTimestamp1 + " T4: " + stopTimestamp1);

    Filter filter3 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER_OR_EQUAL, startTimestamp1);
    Filter filter4 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS_OR_EQUAL, stopTimestamp1);

    FilterList list1 = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter3, filter4);

    FilterList list2 = new FilterList(FilterList.Operator.MUST_PASS_ONE, list, list1);

    scan.setFilter(list2);
    // after this number of records from scan should be NUM_ROWS
    scanner = table.getScanner(scan);
    count = 0;
    res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count);
      count++;
      res = scanner.next();
    }
    assertEquals(NUM_ROWS * 2, count); /* Total rows are NUM_ROWS * 3 */

    // flush tier 2
    flushTier2OnAllVMs();
    verifyTier2CountOnAllVMs(tableName, 0);

  }

  /**
   * Verify scan. With column value filter Using as row key is currently timestamp is key
   *
   */
  @Test
  public void testFTableScanColumnValueFilter() {
    String tableName = getMethodName();
    int actualRows = 0;
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
      // System.out.println("FTableScanDUnitTest.testFTableScanBasic :: index: " + colIndex + "
      // Putting byte[] "+Arrays.toString(Bytes.toBytes(COLUMN_NAME_PREFIX+colIndex)));
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
    }
    long insertionStartTime = TimestampUtil.getCurrentTime();
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

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS);
    flushWALOnAllVMs(tableName);
    verifyWALCountOnAllVMs(tableName, 0);
    flushTier1OnAllVMs();
    verifyTier1CountOnAllVMs(tableName, 0);

    // scan test
    Scan scan = new Scan();
    // MFilter filter = new RowFilter(CompareOp.LESS_OR_EQUAL,Bytes.toBytes(System.nanoTime()));
    Filter filter = new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.EQUAL,
        Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    scan.setFilter(filter);
    // after this number of records from scan should be all rows
    Scanner scanner = table.getScanner(scan);
    int count = 0;
    Row res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count);
      count++;
      res = scanner.next();

    }
    assertEquals(NUM_ROWS, count);
    filter = new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.GREATER,
        Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    scan.setFilter(filter);
    // after this number of records from scan should be zero rows
    scanner = table.getScanner(scan);
    count = 0;
    res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count);
      count++;
      res = scanner.next();

    }
    assertEquals(0, count);

    filter = new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.GREATER_OR_EQUAL,
        Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    scan.setFilter(filter);
    // after this number of records from scan should be zero rows
    scanner = table.getScanner(scan);
    count = 0;
    res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count);
      count++;
      res = scanner.next();

    }
    assertEquals(NUM_ROWS, count);

    filter = new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.LESS,
        Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    scan.setFilter(filter);
    // after this number of records from scan should be zero rows
    scanner = table.getScanner(scan);
    count = 0;
    res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count);
      count++;
      res = scanner.next();

    }
    assertEquals(0, count);

    filter = new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.LESS_OR_EQUAL,
        Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    scan.setFilter(filter);
    // after this number of records from scan should be zero rows
    scanner = table.getScanner(scan);
    count = 0;
    res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count);
      count++;
      res = scanner.next();

    }
    assertEquals(NUM_ROWS, count);

    filter = new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.NOT_EQUAL,
        Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    scan.setFilter(filter);
    // after this number of records from scan should be zero rows
    scanner = table.getScanner(scan);
    count = 0;
    res = scanner.next();
    while (res != null) {
      verifyScannedValue(res, count);
      count++;
      res = scanner.next();

    }
    assertEquals(0, count);

    // flush tier 2
    flushTier2OnAllVMs();
    verifyTier2CountOnAllVMs(tableName, 0);

  }

  /**
   * Verify scan. With key only filter Using as row key is currently timestamp is key
   *
   */
  @Test
  public void testFTableScanKeyOnlyFilter() {
    String tableName = getMethodName();
    int actualRows = 0;
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
      // System.out.println("FTableScanDUnitTest.testFTableScanBasic :: index: " + colIndex + "
      // Putting byte[] "+Arrays.toString(Bytes.toBytes(COLUMN_NAME_PREFIX+colIndex)));
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
    }
    long insertionStartTime = TimestampUtil.getCurrentTime();
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

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS);
    flushWALOnAllVMs(tableName);
    verifyWALCountOnAllVMs(tableName, 0);
    flushTier1OnAllVMs();
    verifyTier1CountOnAllVMs(tableName, 0);

    // scan test
    Scan scan = new Scan();


    // MFilter filter = new RowFilter(CompareOp.LESS_OR_EQUAL,Bytes.toBytes(System.nanoTime()));
    Filter filter = new KeyOnlyFilter();
    scan.setFilter(filter);
    try {
      // after this number of records from scan should be all rows
      Scanner scanner = table.getScanner(scan);
      fail("Expected unsupported filter exception");
    } catch (MException ex) {
      System.out.println(ex.getMessage());
      assertEquals(unsupportedFilterMessage, ex.getMessage());
    }

    // flush tier 2
    flushTier2OnAllVMs();
    verifyTier2CountOnAllVMs(tableName, 0);
  }



  /**
   * Verify if FTable.append api is working
   */
  @Test
  public void testFTableScanServerLocationAPI() {
    String tableName = getMethodName();
    int actualRows = 0;
    FTableDescriptor fTableDescriptor = getFTableDescriptor(NUM_OF_SPLITS, 0);
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

    verifyValuesOnAllVMs(tableName, isOverflowEnabled ? 0 : NUM_ROWS);
    flushWALOnAllVMs(tableName);
    verifyWALCountOnAllVMs(tableName, 0);
    flushTier1OnAllVMs();
    verifyTier1CountOnAllVMs(tableName, 0);

    // scan test
    Set<Integer> bucketIdSet = new TreeSet<Integer>();

    int totalBuckets = fTableDescriptor.getTotalNumOfSplits();
    for (int i = 0; i < totalBuckets; i++) {
      bucketIdSet.add(i);
    }
    Map<Integer, Set<ServerLocation>> bucketToServerMap =
        FTableUtils.getBucketToServerMap(tableName, bucketIdSet);
    bucketToServerMap.forEach((k, v) -> {
      System.out.println("Bucket ID: " + k);
      int count = 0;
      final Iterator<ServerLocation> itr = v.iterator();
      while (itr.hasNext()) {
        final ServerLocation location = itr.next();
        count++;
        System.out.println("Location: " + location.getHostName() + ":" + location.getPort());
      }
      // check if number of buckets are equal no of redundant copies + 1
      assertEquals(fTableDescriptor.getRedundantCopies() + 1, count);
      System.out.println(
          "--------------------------------------------------------------------------------------------");
    });

    // flush tier 2
    flushTier2OnAllVMs();
    verifyTier2CountOnAllVMs(tableName, 0);
  }

  private void verifyScannedValue(final Row row, final int index) {
    // final SingleVersionRow row = res.getLatestRow();
    final Iterator<Cell> itr = row.getCells().iterator();
    int colIndex = 0;
    while (itr.hasNext()) {
      final Cell cell = itr.next();
      if (!Bytes.toString(cell.getColumnName())
          .equalsIgnoreCase(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) {
        assertEquals(Bytes.toString(cell.getColumnName()), COLUMN_NAME_PREFIX + colIndex);
        System.out.println("FTableScanDUnitTest.verifyScannedValue :: " + "CNAME: "
            + Bytes.toString(cell.getColumnName()));
        assertTrue("Incorrect column value for: " + cell.toString(),
            Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex), 0, cell.getValueLength()) == 0);
        // System.out.println("FTableScanDUnitTest.verifyScannedValue :: " + "VALUE: "+
        // (String)cell.getColumnValue());
        colIndex++;
      }
      System.out.println(
          "---------------------------------------------------------------------------------------------------------------------------------");
    }
  }

  private void verifyScannedValue(final Row row, final int index, List<byte[]> selCols,
      FTableDescriptor ftd) {
    // final SingleVersionRow row = res.getLatestRow();
    assertEquals(selCols.size(), row.getCells().size());

    final Iterator<Cell> itr = row.getCells().iterator();
    while (itr.hasNext()) {
      final Cell cell = itr.next();
      for (int i = 0; i < selCols.size(); i++) {
        final Integer positionIndex = ftd.getColumnDescriptorsMap()
            .get(ftd.getColumnsByName().get(new ByteArrayKey(cell.getColumnName())));
        if (Bytes.compareTo(selCols.get(i), cell.getColumnName()) == 0) {
          // check for this col
          assertEquals(Bytes.toString(cell.getColumnName()), COLUMN_NAME_PREFIX + (positionIndex));
          System.out.println("FTableScanDUnitTest.verifyScannedValue :: " + "CNAME: "
              + Bytes.toString(cell.getColumnName()));
          System.out.println("FTableScanDUnitTest.verifyScannedValue :: " + "VALUE: "
              + TypeHelper.deepToString(cell.getColumnValue()));
          assertEquals("Incorrect column value for: " + cell,
              Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()),
              COLUMN_NAME_PREFIX + (positionIndex));
        }
      }
      System.out.println(
          "---------------------------------------------------------------------------------------------------------------------------------");
    }
  }

  private void flushWALOnAllVMs(String tableName) {
    if (flushWAL) {
      final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
      for (int i = 0; i < vmList.size(); i++) {
        vmList.get(i).invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            for (int j = 0; j < NUM_OF_SPLITS; j++) {
              StoreHandler.getInstance().flushWriteAheadLog(tableName, j);
            }
            return null;
          }
        });
      }
    }
  }

  private void flushTier1OnAllVMs() {
    if (flushTier1) {
      final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
      for (int i = 0; i < vmList.size(); i++) {
        vmList.get(i).invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            /* use the test hook to setup dummy expiry intervals for tier-1 and tier-2 */
            final Map<String, Long> TIER_EVICT_INTERVAL = new HashMap<>(2);
            TIER_EVICT_INTERVAL.put(DefaultStore.STORE_NAME, 0L);
            ReflectionUtils.setStaticFieldValue(
                Class.forName("io.ampool.tierstore.internal.TierEvictorThread"),
                "fixedWaitTimeSecs", 1);
            ReflectionUtils.setStaticFieldValue(
                Class.forName("io.ampool.tierstore.internal.TierEvictorThread"),
                "TEST_TIER_EVICT_INTERVAL", TIER_EVICT_INTERVAL);
            ReflectionUtils.setStaticFieldValue(
                Class.forName("io.ampool.tierstore.internal.TierEvictorThread"), "TEST_EVICT",
                true);
            return null;
          }
        });
      }
      // sleep for some time to get the eviction started
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void flushTier2OnAllVMs() {
    if (flushTier2) {
      final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
      for (int i = 0; i < vmList.size(); i++) {
        vmList.get(i).invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            /* use the test hook to setup dummy expiry intervals for tier-1 and tier-2 */
            final Map<String, Long> TIER_EVICT_INTERVAL = new HashMap<>(2);
            TIER_EVICT_INTERVAL.put("hdfsStore", 0L);
            ReflectionUtils.setStaticFieldValue(
                Class.forName("io.ampool.tierstore.internal.TierEvictorThread"),
                "fixedWaitTimeSecs", 1);
            ReflectionUtils.setStaticFieldValue(
                Class.forName("io.ampool.tierstore.internal.TierEvictorThread"),
                "TEST_TIER_EVICT_INTERVAL", TIER_EVICT_INTERVAL);
            ReflectionUtils.setStaticFieldValue(
                Class.forName("io.ampool.tierstore.internal.TierEvictorThread"), "TEST_EVICT",
                true);
            return null;
          }
        });
      }
    }
  }

  protected void verifyValuesOnAllVMs(String tableName, int expectedRows) {
    final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
    for (int i = 0; i < vmList.size(); i++) {
      final int res = (int) vmList.get(i).invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          return verifyValues(tableName);
        }
      });
      actualRows += res;
    }
    assertEquals(expectedRows, actualRows);
  }

  protected int verifyValues(String tableName) {
    int entriesCount = 0;
    final Region<Object, Object> region = MCacheFactory.getAnyInstance().getRegion(tableName);
    assertNotNull(region);
    final Iterator<BucketRegion> allLocalPrimaryBucketRegions =
        ((PartitionedRegion) region).getDataStore().getAllLocalPrimaryBucketRegions().iterator();
    while (allLocalPrimaryBucketRegions.hasNext()) {
      final BucketRegion bucketRegion = allLocalPrimaryBucketRegions.next();
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

  protected void verifyWALCountOnAllVMs(String tableName, int expectedRows) {
    if (flushWAL) {
      final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
      for (int i = 0; i < vmList.size(); i++) {
        final int res = (int) vmList.get(i).invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            AtomicInteger walRecordsCount = new AtomicInteger(0);
            for (int j = 0; j < NUM_OF_SPLITS; j++) {
              WriteAheadLog.getInstance().getScanner(tableName, j).forEach((WALRECORD) -> {
                final Iterator iterator = WALRECORD.getBlockValue().iterator();
                while (iterator.hasNext()) {
                  walRecordsCount.incrementAndGet();
                  iterator.next();
                }
              });
            }
            return walRecordsCount.get();
          }
        });
        actualRows += res;
      }
      assertEquals(expectedRows, actualRows);
    }
  }

  protected void verifyTier1CountOnAllVMs(String tableName, int expectedRows) {
    if (flushTier1) {
      final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
      for (int i = 0; i < vmList.size(); i++) {
        final int res = (int) vmList.get(i).invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            AtomicInteger tier1recordsCount = new AtomicInteger(0);
            final TierStore tierStore = StoreHandler.getInstance().getTierStore(tableName, 1);
            final ConverterDescriptor converterDescriptor =
                tierStore.getConverterDescriptor(tableName);
            for (int j = 0; j < NUM_OF_SPLITS; j++) {
              final TierStoreReader reader = tierStore.getReader(tableName, j);
              reader.setConverterDescriptor(converterDescriptor);
              reader.forEach((SR) -> tier1recordsCount.incrementAndGet());
            }
            return tier1recordsCount.get();
          }
        });
        actualRows += res;
      }
      assertEquals(expectedRows, actualRows);
    }
  }

  protected void verifyTier2CountOnAllVMs(String tableName, int expectedRows) {
    if (flushTier2) {
      final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
      for (int i = 0; i < vmList.size(); i++) {
        final int res = (int) vmList.get(i).invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            AtomicInteger tier2recordsCount = new AtomicInteger(0);
            final TierStore tierStore = StoreHandler.getInstance().getTierStore(tableName, 2);
            final ConverterDescriptor converterDescriptor =
                tierStore.getConverterDescriptor(tableName);
            for (int j = 0; j < NUM_OF_SPLITS; j++) {
              final TierStoreReader reader = tierStore.getReader(tableName, j);
              reader.setConverterDescriptor(converterDescriptor);
              reader.forEach((SR) -> tier2recordsCount.incrementAndGet());
            }
            return tier2recordsCount.get();
          }
        });
        actualRows += res;
      }
      assertEquals(expectedRows, actualRows);
    }
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

  public FTable createFTable(final String tableName, final FTableDescriptor tableDescriptor) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    FTable ftable = null;
    try {
      ftable = clientCache.getAdmin().createFTable(tableName, tableDescriptor);
    } catch (Exception e) {
      System.out.println("CreateMTableDUnitTest.createFTable :: " + "Throwing from test");
      throw e;
    }
    System.out.println("CreateMTableDUnitTest.createFTable :: " + "mtable is " + ftable);
    return ftable;

  }

  private static FTableDescriptor getFTableDescriptor(final int splits, int descriptorIndex) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    Schema.Builder sb = new Schema.Builder();
    if (descriptorIndex == 0) {
      tableDescriptor.setTotalNumOfSplits(splits);
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        // sets a partitioningColumn
        if (colmnIndex == 0) {
          tableDescriptor.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
        }
        sb.column(COLUMN_NAME_PREFIX + colmnIndex);
        // tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX +
        // colmnIndex));
      }
      tableDescriptor.setRedundantCopies(1);
    } else {
      tableDescriptor.setTotalNumOfSplits(splits);
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        // sets a partitioningColumn
        if (colmnIndex == 0) {
          tableDescriptor.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
        }
        sb.column(COLUMN_NAME_PREFIX + colmnIndex);
        // tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX +
        // colmnIndex));
      }
      tableDescriptor.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
      tableDescriptor.setRedundantCopies(2);
    }
    tableDescriptor.setBlockFormat(blockFormat);
    tableDescriptor.setSchema(sb.build());
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
    final Region<Object, Object> mregion = ((ProxyFTableRegion) mtable).getTableRegion();
    String path = mregion.getFullPath();
    assertTrue(path.contains(tableName));
    // To verify disk persistence
    assertEquals(tableDescriptor.isDiskPersistenceEnabled(),
        mregion.getAttributes().getDataPolicy().withPersistence());
  }

  public void setWALFlush(boolean flushwal) {
    flushWAL = flushwal;
  }

  protected void setEvictionPercetangeOnAllVMs(float percetange) {
    isOverflowEnabled = true;
  }

  public void setTier1Flush(boolean flush) {
    flushTier1 = flush;
  }

  public void setTier2Flush(boolean flush) {
    flushTier2 = flush;
  }


  /**
   * Test for scan with filter such that only partial block is qualified/returned.
   */
  @Test
  public void testFilterWithinBlock() {
    /** create the table and insert data **/
    String tableName = getMethodName();
    FTableDescriptor td = getFTableDescriptor(1, 0);
    td.setBlockSize(31);
    final FTable table = createFTable(tableName, td);
    final int numBuckets = table.getTableDescriptor().getTotalNumOfSplits();
    assertNotNull(table);

    Record record = new Record();
    for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
    }
    ByteArrayKey bak = new ByteArrayKey(Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    for (int i = 0; i < NUM_ROWS; i++) {
      record.getValueMap().put(bak, new byte[] {(byte) i});
      table.append(record);
    }

    /** basic test.. no filters **/
    assertEquals("Incorrect count without filters.", NUM_ROWS,
        getScanCount(table, new Scan(), numBuckets));

    Scan scan = new Scan();
    scan.setColumns(Arrays.asList(0, 1));
    scan.setFilter(
        new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.EQUAL, new byte[] {50}));
    assertEquals("Incorrect count with SingleColumnValueFilter.", 1,
        getScanCount(table, scan, numBuckets));

    // flush tier 2
    flushTier2OnAllVMs();
    verifyTier2CountOnAllVMs(tableName, 0);
  }

  /**
   * Test for scan with/without filters using partitioning column.
   */
  @Test
  public void testFilterWithPartitioningColumn() {
    /** create the table and insert data **/
    String tableName = getMethodName();
    final FTable table = createFTable(tableName, getFTableDescriptor(NUM_OF_SPLITS, 0));
    final int numBuckets = table.getTableDescriptor().getTotalNumOfSplits();
    assertNotNull(table);

    Record record = new Record();
    for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }

    /** basic test.. no filters **/
    assertEquals("Incorrect count without filters.", NUM_ROWS,
        getScanCount(table, new Scan(), numBuckets));

    /** a filter with partitioning-column with unknown value **/
    Scan scan = new Scan();
    scan.setFilter(new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.EQUAL, null));
    assertEquals("Incorrect count with SingleColumnValueFilter with `null` value.", 0,
        getScanCount(table, scan, 1));

    /** a filter with partitioning-column as filter with valid value **/
    scan = new Scan();
    scan.setFilter(new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.EQUAL,
        Bytes.toBytes(COLUMN_NAME_PREFIX + 0)));
    assertEquals("Incorrect count with partitioning-column with valid value.", NUM_ROWS,
        getScanCount(table, scan, 1));

    /** a filter with column other than partitioning-column with valid value **/
    scan = new Scan();
    scan.setFilter(new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 1, CompareOp.EQUAL,
        Bytes.toBytes(COLUMN_NAME_PREFIX + 1)));
    assertEquals("Incorrect count with column other than partitioning-column.", NUM_ROWS,
        getScanCount(table, scan, numBuckets));

    /** a filter with partitioning-column OR other columns **/
    scan = new Scan();
    scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE)
        .addFilter(new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.EQUAL,
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0)))
        .addFilter(new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 1, CompareOp.EQUAL,
            Bytes.toBytes(COLUMN_NAME_PREFIX + 1))));

    assertEquals("Incorrect count with partitioning-column OR other columns.", NUM_ROWS,
        getScanCount(table, scan, numBuckets));

    /** a filter with partitioning-column AND other columns **/
    scan = new Scan();
    scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL)
        .addFilter(new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.EQUAL,
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0)))
        .addFilter(new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 1, CompareOp.EQUAL,
            Bytes.toBytes(COLUMN_NAME_PREFIX + 1))));

    assertEquals("Incorrect count with partitioning-column AND other columns.", NUM_ROWS,
        getScanCount(table, scan, 1));

    /** a filter with partitioning-column AND insertion-timestamp-column **/
    scan = new Scan();
    scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL)
        .addFilter(new SingleColumnValueFilter(COLUMN_NAME_PREFIX + 0, CompareOp.EQUAL,
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0)))
        .addFilter(new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            CompareOp.GREATER_OR_EQUAL, 0L))
        .addFilter(new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            CompareOp.LESS_OR_EQUAL, Long.MAX_VALUE)));

    assertEquals("Incorrect count with partitioning-column AND insertion-timestamp-column.",
        NUM_ROWS, getScanCount(table, scan, 1));

    // flush tier 2
    flushTier2OnAllVMs();
    verifyTier2CountOnAllVMs(tableName, 0);

  }

  @SuppressWarnings("unchecked")
  private long getScanCount(final FTable table, final Scan scan, int expectedBuckets) {
    long count = 0;
    try (Scanner scanner = table.getScanner(scan)) {
      if (scanner instanceof MClientScannerUsingGetAll) {
        Set<Integer> bucketIds =
            (Set<Integer>) ReflectionUtils.getFieldValue(scanner, "bucketIdSet");
        assertEquals("Incorrect number of buckets scanned.", expectedBuckets, bucketIds.size());
      }
      for (Row ignored : scanner) {
        ++count;
      }
    }
    return count;
  }
}
