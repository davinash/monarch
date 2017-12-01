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
package io.ampool.monarch.table.scan.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.KeyOnlyFilter;
import io.ampool.monarch.table.filter.RandomRowFilter;
import io.ampool.monarch.table.filter.RowFilter;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.filter.ValueFilter;
import io.ampool.monarch.table.filter.internal.TimestampFilter;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class MTableFilterImplDUnitTest extends MTableFilterTestBase {

  public MTableFilterImplDUnitTest() {
    super();
  }

  private void runTestTimestampFilterMultiVersionTable(boolean ordered) {
    String tableName = getTestMethodName();
    int numRows = 10;
    int numOfVersions = 5;
    long startTimestamp = System.nanoTime();

    System.out
        .println("MTableFilterImplDUnitTest.testTimestampFilter :: 585" + "STS " + startTimestamp);

    MTableDescriptor mts =
        new MTableDescriptor(ordered ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
    for (int i = 0; i < QUALIFIERS.length; i++) {
      mts.addColumn(QUALIFIERS[i]);
    }
    mts.setMaxVersions(numOfVersions);
    MTable table = getmClientCache().getAdmin().createTable(tableName, mts);

    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numOfVersions; j++) {
        Put put = new Put(Bytes.add(ROW, new byte[] {(byte) i}));
        for (int k = 0; k < QUALIFIERS.length; k++) {
          put.addColumn(QUALIFIERS[k], Bytes.toBytes(VALUE + i + j + k));
        }
        table.put(put);
      }
    }

    for (int i = 0; i < numRows; i++) {
      Get get = new Get(Bytes.add(ROW, new byte[] {(byte) i}));
      assertNotNull(table.get(get));
      get.setMaxVersionsToFetch(numOfVersions);
      Row row = table.get(get);
      assertEquals(numOfVersions, row.getAllVersions().size());
    }

    long stopTimestamp = System.nanoTime();
    System.out
        .println("MTableFilterImplDUnitTest.testTimestampFilter :: 588" + "ETS " + stopTimestamp);

    Scan scan = new Scan();
    Filter filter = new TimestampFilter(CompareOp.GREATER_OR_EQUAL, startTimestamp);// Bytes.add(base,
    // new
    // byte[]{(byte)
    // i});
    scan.setFilter(filter);
    Scanner scanner = table.getScanner(scan);
    int resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(numRows, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.LESS_OR_EQUAL, stopTimestamp);// Bytes.add(base, new
    // byte[]{(byte) i});
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(numRows, resultCount);
    scanner.close();

    long equalTS = 0l;

    // gather all timstamps for point quereis
    scan = new Scan();
    scanner = table.getScanner(scan);
    Iterator<Row> iterator = scanner.iterator();
    TreeMap<Long, Row> recordsMap = new TreeMap<>();
    while (iterator.hasNext()) {
      Row next = iterator.next();
      recordsMap.put(next.getRowTimeStamp(), next);
    }
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.EQUAL, recordsMap.firstKey());
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(recordsMap.firstKey(), result.getRowTimeStamp());
      assertEquals(10, result.getCells().size() - 1);
      assertEquals(0, result.compareTo(recordsMap.get(recordsMap.firstKey())));
      resultCount++;
    }
    assertEquals(1, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.LESS, recordsMap.lastKey());
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(10, result.getCells().size() - 1);

      assertTrue(recordsMap.containsKey(result.getRowTimeStamp()));
      assertEquals(0, result.compareTo(recordsMap.get(result.getRowTimeStamp())));
      resultCount++;
    }
    assertEquals(9, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.LESS, recordsMap.firstKey());
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      resultCount++;
    }
    assertEquals(0, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.GREATER, recordsMap.firstKey());
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(10, result.getCells().size() - 1);

      assertTrue(recordsMap.containsKey(result.getRowTimeStamp()));
      assertEquals(0, result.compareTo(recordsMap.get(result.getRowTimeStamp())));
      resultCount++;
    }
    assertEquals(9, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.GREATER, recordsMap.lastKey());
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      resultCount++;
    }
    assertEquals(0, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.NOT_EQUAL, startTimestamp);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(10, result.getCells().size() - 1);

      assertTrue(recordsMap.containsKey(result.getRowTimeStamp()));
      assertEquals(0, result.compareTo(recordsMap.get(result.getRowTimeStamp())));
      resultCount++;
    }
    assertEquals(10, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.NOT_EQUAL, recordsMap.firstKey());
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(10, result.getCells().size() - 1);

      assertTrue(recordsMap.containsKey(result.getRowTimeStamp()));
      assertEquals(0, result.compareTo(recordsMap.get(result.getRowTimeStamp())));
      resultCount++;
    }
    assertEquals(9, resultCount);
    scanner.close();

    /* multiple versions with filter on latest version */
    scan = new Scan();
    filter = new TimestampFilter(CompareOp.NOT_EQUAL, recordsMap.firstKey());
    scan.setFilter(filter);
    scan.setMaxVersions(3, true);
    scan.setFilterOnLatestVersionOnly(true);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertEquals("Incorrect number of cells.", 10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(10, resultCount);
    scanner.close();

    deleteTable(tableName);
  }

  private void runTestAllversionsFilterImplBasic(boolean ordered) {
    String tableName = getTestMethodName();
    int NUM_ROWS = 10;
    int NUM_VERSIONS = 5;
    int NUM_COLS = 5;
    String COL_NAME_PREFIX = "COL_";
    String VALUE_PREFIX = "VAL_";
    String KEY_PREFIX = "KEY_";

    MTableDescriptor mts =
        new MTableDescriptor(ordered ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
    for (int i = 0; i < NUM_COLS; i++) {
      mts.addColumn(COL_NAME_PREFIX + i);
    }
    mts.setMaxVersions(NUM_VERSIONS);

    MTable table = getmClientCache().getAdmin().createTable(tableName, mts);

    long startTimestamp = System.nanoTime();
    System.out
        .println("MTableFilterImplDUnitTest.testTimestampFilter :: 585" + "STS " + startTimestamp);

    for (int i = 0; i < NUM_ROWS; i++) {
      for (int j = 0; j < NUM_VERSIONS; j++) {
        Put put = new Put(Bytes.toBytes(KEY_PREFIX + i));
        for (int k = 0; k < NUM_COLS; k++) {
          put.addColumn(COL_NAME_PREFIX + k, Bytes.toBytes(VALUE_PREFIX + i + k + j));
        }
        table.put(put);
      }
    }

    long stopTimestamp = System.nanoTime();
    System.out
        .println("MTableFilterImplDUnitTest.testTimestampFilter :: 588" + "ETS " + stopTimestamp);

    Map<Integer, List<Long>> timestamps = new HashMap<>();

    for (int i = 0; i < NUM_ROWS; i++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + i));
      assertNotNull(table.get(get));
      get.setMaxVersionsToFetch(NUM_VERSIONS);
      Row row = table.get(get);
      assertEquals(NUM_VERSIONS, row.getAllVersions().size());
      ArrayList<Long> longs = new ArrayList<>(row.getAllVersions().keySet());
      Collections.sort(longs);
      timestamps.put(i, longs);
    }

    int count = 0;
    Scanner scanner = null;
    Scan scan = null;

    // Only one version returned with default scan props
    count = 0;
    scan = new Scan();
    scanner = table.getScanner(scan);
    for (Row row : scanner) {
      assertEquals(1, row.getAllVersions().size());
      count++;
    }
    assertEquals(NUM_ROWS, count);



    // set number of versions to fetch to 3
    count = 0;
    scan = new Scan();
    scan.setMaxVersions(3, false);
    scanner = table.getScanner(scan);
    for (Row row : scanner) {
      assertEquals(3, row.getAllVersions().size());
      count++;
    }
    assertEquals(NUM_ROWS, count);

    System.out.println("-----------------------------------------------------------------------");


    // set timestamp filter
    // this filter should get applied to each versions
    List<Long> tsList = timestamps.get(0);
    count = 0;
    scan = new Scan();
    TimestampFilter tsFilter1 = new TimestampFilter(CompareOp.GREATER_OR_EQUAL, tsList.get(0));
    TimestampFilter tsFilter2 =
        new TimestampFilter(CompareOp.LESS_OR_EQUAL, tsList.get(tsList.size() - 1));
    FilterList filterList = new FilterList();
    filterList.addFilter(tsFilter1);
    filterList.addFilter(tsFilter2);
    scan.setFilter(filterList);
    scan.setMaxVersions(3, true);
    scanner = table.getScanner(scan);
    for (Row row : scanner) {
      assertEquals(3, row.getAllVersions().size());
      Long prevTS = Long.MIN_VALUE;
      Iterator<Long> iterator = row.getAllVersions().keySet().iterator();
      while (iterator.hasNext()) {
        Long ts = iterator.next();
        // System.out.println("MTableFilterImplDUnitTest.testAllVersionFilterImpl :: 1055 " + "curTS
        // "+ts + " prevTS "+prevTS);
        assertTrue(ts > prevTS);
        prevTS = ts;
      }
      count++;
    }
    assertEquals(1, count);

    tsList = timestamps.get(0);
    count = 0;
    scan = new Scan();
    tsFilter1 = new TimestampFilter(CompareOp.GREATER_OR_EQUAL, tsList.get(0));
    tsFilter2 = new TimestampFilter(CompareOp.LESS_OR_EQUAL, tsList.get(tsList.size() - 1));
    filterList = new FilterList();
    filterList.addFilter(tsFilter1);
    filterList.addFilter(tsFilter2);
    scan.setFilter(filterList);
    scan.setMaxVersions(3, false);
    scanner = table.getScanner(scan);
    for (Row row : scanner) {
      assertEquals(3, row.getAllVersions().size());
      Long prevTS = Long.MAX_VALUE;
      Iterator<Long> iterator = row.getAllVersions().keySet().iterator();
      while (iterator.hasNext()) {
        Long ts = iterator.next();
        // System.out.println("MTableFilterImplDUnitTest.testAllVersionFilterImpl :: 1055 " + "curTS
        // "+ts + " prevTS "+prevTS);
        assertTrue(ts < prevTS);
        prevTS = ts;
      }
      count++;
    }
    assertEquals(1, count);

    tsList = timestamps.get(0);
    count = 0;
    scan = new Scan();
    tsFilter1 = new TimestampFilter(CompareOp.GREATER_OR_EQUAL, tsList.get(0));
    tsFilter2 = new TimestampFilter(CompareOp.LESS_OR_EQUAL, tsList.get(tsList.size() - 1));
    filterList = new FilterList();
    filterList.addFilter(tsFilter1);
    filterList.addFilter(tsFilter2);
    scan.setFilter(filterList);
    scan.setMaxVersions(10, false);
    scanner = table.getScanner(scan);
    for (Row row : scanner) {
      assertEquals(NUM_VERSIONS, row.getAllVersions().size());
      Long prevTS = Long.MAX_VALUE;
      Iterator<Long> iterator = row.getAllVersions().keySet().iterator();
      while (iterator.hasNext()) {
        Long ts = iterator.next();
        // System.out.println("MTableFilterImplDUnitTest.testAllVersionFilterImpl :: 1055 " + "curTS
        // "+ts + " prevTS "+prevTS);
        assertTrue(ts < prevTS);
        prevTS = ts;
      }
      count++;
    }
    assertEquals(1, count);
    deleteTable(tableName);
  }

  private void runTestAllVersionFilterImpl(boolean ordered) {

    String tableName = getTestMethodName();
    int NUM_ROWS = 10;
    int NUM_VERSIONS = 5;
    int NUM_COLS = NUM_VERSIONS;
    String COL_NAME_PREFIX = "COL_";
    String VALUE_PREFIX = "VAL_";
    String KEY_PREFIX = "KEY_";

    MTableDescriptor mts = new MTableDescriptor();
    for (int i = 0; i < NUM_COLS; i++) {
      mts.addColumn(COL_NAME_PREFIX + i, BasicTypes.INT);
    }
    mts.setMaxVersions(NUM_VERSIONS);

    MTable table = getmClientCache().getAdmin().createTable(tableName, mts);

    long startTimestamp = System.nanoTime();
    System.out
        .println("MTableFilterImplDUnitTest.testTimestampFilter :: 585" + "STS " + startTimestamp);

    for (int i = 0; i < NUM_ROWS; i++) {
      for (int j = 0; j < NUM_VERSIONS; j++) {
        Put put = new Put(Bytes.toBytes(KEY_PREFIX + i));
        put.addColumn(COL_NAME_PREFIX + j, j);
        table.put(put);
      }
    }

    long stopTimestamp = System.nanoTime();
    System.out
        .println("MTableFilterImplDUnitTest.testTimestampFilter :: 588" + "ETS " + stopTimestamp);


    Map<Integer, List<Long>> timestamps = new HashMap<>();

    for (int i = 0; i < NUM_ROWS; i++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + i));
      assertNotNull(table.get(get));
      get.setMaxVersionsToFetch(NUM_VERSIONS);
      Row row = table.get(get);
      assertEquals(NUM_VERSIONS, row.getAllVersions().size());
      timestamps.put(i, new ArrayList<>(row.getAllVersions().keySet()));
    }

    int count = 0;
    Scanner scanner = null;
    Scan scan = null;

    count = 0;
    scan = new Scan();
    // get all matching versions
    scan.setMaxVersions(false);
    scan.setFilterOnLatestVersionOnly(false);

    // Oldest version of each row
    Filter scvf1 = new SingleColumnValueFilter(COL_NAME_PREFIX + 0, CompareOp.EQUAL, 0);

    // Latest versions of each row
    Filter scvf2 = new SingleColumnValueFilter(COL_NAME_PREFIX + (NUM_VERSIONS - 1),
        CompareOp.EQUAL, (NUM_VERSIONS - 1));

    FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    list.addFilter(scvf1);
    list.addFilter(scvf2);

    scan.setFilter(list);

    scanner = table.getScanner(scan);
    for (Row row : scanner) {
      assertEquals(1, row.getAllVersions().size());
      Iterator<Long> iterator = row.getAllVersions().keySet().iterator();
      count++;
    }
    assertEquals(NUM_ROWS, count);

    count = 0;
    scan = new Scan();
    // get all matching versions
    scan.setMaxVersions(true);
    scan.setFilterOnLatestVersionOnly(false);

    // Oldest version of each row
    scvf1 = new SingleColumnValueFilter(COL_NAME_PREFIX + 0, CompareOp.EQUAL, 0);

    // Latest versions of each row
    scvf2 = new SingleColumnValueFilter(COL_NAME_PREFIX + (NUM_VERSIONS - 1), CompareOp.EQUAL,
        (NUM_VERSIONS - 1));

    list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    list.addFilter(scvf1);
    list.addFilter(scvf2);

    scan.setFilter(list);

    scanner = table.getScanner(scan);
    for (Row row : scanner) {
      assertEquals(1, row.getAllVersions().size());
      count++;
    }
    assertEquals(NUM_ROWS, count);
    //
    count = 0;
    scan = new Scan();
    // get all matching versions
    scan.setMaxVersions(false);
    scan.setFilterOnLatestVersionOnly(false);

    // Oldest version of each row
    scvf1 = new SingleColumnValueFilter(COL_NAME_PREFIX + 0, CompareOp.EQUAL, 0);

    // Latest versions of each row
    scvf2 = new SingleColumnValueFilter(COL_NAME_PREFIX + (NUM_VERSIONS - 1), CompareOp.EQUAL,
        (NUM_VERSIONS - 1));

    list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    list.addFilter(scvf1);
    list.addFilter(scvf2);

    scan.setFilter(list);

    scanner = table.getScanner(scan);
    for (Row row : scanner) {
      assertEquals(5, row.getAllVersions().size());
      Long prevTS = Long.MAX_VALUE;
      Iterator<Long> iterator = row.getAllVersions().keySet().iterator();
      while (iterator.hasNext()) {
        Long ts = iterator.next();
        // System.out.println("MTableFilterImplDUnitTest.testAllVersionFilterImpl :: 1055 " + "curTS
        // "+ts + " prevTS "+prevTS);
        assertTrue(ts < prevTS);
        prevTS = ts;
      }
      count++;
    }
    assertEquals(NUM_ROWS, count);
    //
    count = 0;
    scan = new Scan();
    // get all matching versions
    scan.setMaxVersions(true);
    scan.setFilterOnLatestVersionOnly(false);

    // Oldest version of each row
    scvf1 = new SingleColumnValueFilter(COL_NAME_PREFIX + 0, CompareOp.EQUAL, 0);

    // Latest versions of each row
    scvf2 = new SingleColumnValueFilter(COL_NAME_PREFIX + (NUM_VERSIONS - 1), CompareOp.EQUAL,
        (NUM_VERSIONS - 1));

    list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    list.addFilter(scvf1);
    list.addFilter(scvf2);

    scan.setFilter(list);

    scanner = table.getScanner(scan);
    for (Row row : scanner) {
      assertEquals(5, row.getAllVersions().size());
      Long prevTS = Long.MIN_VALUE;
      Iterator<Long> iterator = row.getAllVersions().keySet().iterator();
      while (iterator.hasNext()) {
        Long ts = iterator.next();
        // System.out.println(
        // "MTableFilterImplDUnitTest.testAllVersionFilterImpl :: 1055 " + "curTS " + ts
        // + " prevTS " + prevTS);
        assertTrue(ts > prevTS);
        prevTS = ts;
      }
      count++;
    }
    assertEquals(NUM_ROWS, count);
    System.out.println(
        "----------------------------------------------------------------------------------------------------");
    // particular TS
    count = 0;
    scan = new Scan();
    // get all matching versions
    scan.setMaxVersions(false);
    scan.setFilterOnLatestVersionOnly(false);

    // Oldest version of each row
    scvf1 = new SingleColumnValueFilter(COL_NAME_PREFIX + 0, CompareOp.EQUAL, 0);

    // Latest versions of each row
    scvf2 = new SingleColumnValueFilter(COL_NAME_PREFIX + (NUM_VERSIONS - 1), CompareOp.EQUAL,
        (NUM_VERSIONS - 1));

    list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    list.addFilter(scvf1);
    list.addFilter(scvf2);

    List<Long> firstRowVersionsTS = timestamps.get(0);
    // second oldest row
    Filter tsFilter1 = new TimestampFilter(CompareOp.EQUAL, firstRowVersionsTS.get(3));
    FilterList list2 = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    list2.addFilter(tsFilter1);

    FilterList list3 = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    list3.addFilter(list);
    list3.addFilter(list2);

    scan.setFilter(list3);

    scanner = table.getScanner(scan);
    for (Row row : scanner) {
      assertEquals(1, row.getAllVersions().size());
      count++;
    }
    assertEquals(1, count);
    deleteTable(tableName);
  }


  @Test
  public void testKeyOnlyFilter() throws Exception {
    int numRows = 10;
    MTable mt = createTableAndPutRows("testKeyOnlyFilter", numRows);

    Scan scan = new Scan();
    Filter filter = new KeyOnlyFilter(true);
    scan.setFilter(filter);
    Scanner scanner = mt.getScanner(scan);
    int count = 0;
    for (Row result : mt.getScanner(scan)) {
      assertEquals(0, result.size());
      count++;
    }
    assertEquals(numRows, count);
    scanner.close();
    deleteTable("testKeyOnlyFilter");
  }

  @Test
  public void testRandomRowKeyFilter() {
    int numRows = 100;
    MTable mt = createTableAndPutRows("testRandomRowKeyFilter", numRows);

    float chance = 0.25f;
    Scan scan = new Scan();
    Filter filter = new RandomRowFilter(chance);
    scan.setFilter(filter);
    Scanner scanner = mt.getScanner(scan);

    int included = 0;
    for (Row result : mt.getScanner(scan)) {
      included++;
    }

    int expectedRecords = (int) (numRows * chance);

    int epsilon = numRows / 10;

    assertTrue("Roughly 25% should pass the filter, Expected epsilon: " + epsilon
        + ", Actual diff: " + (included - expectedRecords), (included - expectedRecords) < epsilon);
    scanner.close();
    deleteTable("testRandomRowKeyFilter");
  }

  @Test
  public void testSingleColumnValueFilter() {
    int numRows = 10;
    MTable table = createTableAndPutRows("testSingleColumnValueFilter", numRows);
    Scan scan = new Scan();
    Filter filter =
        new SingleColumnValueFilter(QUALIFIERS[1], CompareOp.EQUAL, Bytes.toBytes(VALUE + 1));
    scan.setFilter(filter);
    Scanner scanner = table.getScanner(scan);
    int resultCount = 0;
    for (Row result : table.getScanner(scan)) {
      assertTrue("Expecting same value", Bytes.compareTo(Bytes.toBytes(VALUE + 1),
          (byte[]) result.getCells().get(1).getColumnValue()) == 0);
      assertEquals("Expecting all cells", QUALIFIERS.length, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals("Expecting all records", numRows, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new SingleColumnValueFilter(QUALIFIERS[1], CompareOp.EQUAL, Bytes.toBytes(VALUE + 2));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : table.getScanner(scan)) {
      resultCount++;
    }
    assertEquals("Expecting no records", 0, resultCount);
    scanner.close();
    deleteTable("testSingleColumnValueFilter");

    table = createTable("testSingleColumnValueFilter");
    for (int index = 0; index < numRows; index++) {
      Put put = new Put(Bytes.toBytes("rowkey" + index));
      int rvalue = (numRows % (index + 1));
      System.out.println(
          "MTableFilterImplDUnitTest.testSingleColumnValueFilter :: " + "Adding value: " + rvalue);
      put.addColumn(QUALIFIERS[0], Bytes.toBytes(VALUE + rvalue));
      put.addColumn(QUALIFIERS[1], Bytes.toBytes(VALUE + 1));
      put.addColumn(QUALIFIERS[2], Bytes.toBytes(VALUE + 2));
      table.put(put);
    }

    scan = new Scan();
    filter = new SingleColumnValueFilter(QUALIFIERS[0], CompareOp.LESS, Bytes.toBytes(VALUE + 1));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : table.getScanner(scan)) {
      byte[] columnValue = (byte[]) result.getCells().get(0).getColumnValue();
      System.out.println("MTableFilterImplDUnitTest.testSingleColumnValueFilter : value:: "
          + Bytes.toString(columnValue));
      assertTrue("Expecting same value",
          Bytes.compareTo(columnValue, Bytes.toBytes(VALUE + 1)) < 0);
      assertEquals("Expecting cells", 10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals("Expecting 4 records", 4, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new SingleColumnValueFilter(QUALIFIERS[0], CompareOp.LESS_OR_EQUAL,
        Bytes.toBytes(VALUE + 1));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : table.getScanner(scan)) {
      assertTrue("Expecting same value", Bytes.compareTo(Bytes.toBytes(VALUE + 1),
          (byte[]) result.getCells().get(0).getColumnValue()) >= 0);
      assertEquals("Expecting all cells", 10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals("Expecting 6 records", 6, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new SingleColumnValueFilter(QUALIFIERS[0], CompareOp.EQUAL, Bytes.toBytes(VALUE + 0));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : table.getScanner(scan)) {
      assertTrue("Expecting same value", Bytes.compareTo(Bytes.toBytes(VALUE + 0),
          (byte[]) result.getCells().get(0).getColumnValue()) == 0);
      assertEquals("Expecting all cells", 10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals("Expecting 4 records", 4, resultCount);
    scanner.close();

    scan = new Scan();
    filter =
        new SingleColumnValueFilter(QUALIFIERS[0], CompareOp.NOT_EQUAL, Bytes.toBytes(VALUE + 0));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : table.getScanner(scan)) {
      assertTrue("Expecting same value", Bytes.compareTo(Bytes.toBytes(VALUE + 0),
          (byte[]) result.getCells().get(0).getColumnValue()) < 0);
      assertEquals("Expecting all cells", 10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals("Expecting 6 records", 6, resultCount);
    scanner.close();

    scan = new Scan();
    filter =
        new SingleColumnValueFilter(QUALIFIERS[0], CompareOp.GREATER, Bytes.toBytes(VALUE + 1));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertTrue("Expecting same value", Bytes.compareTo(Bytes.toBytes(VALUE + 1),
          (byte[]) result.getCells().get(0).getColumnValue()) < 0);
      assertEquals("Expecting all cells", 10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals("Expecting 4 records", 4, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new SingleColumnValueFilter(QUALIFIERS[0], CompareOp.GREATER_OR_EQUAL,
        Bytes.toBytes(VALUE + 1));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : table.getScanner(scan)) {
      assertTrue("Expecting same value", Bytes.compareTo(Bytes.toBytes(VALUE + 1),
          (byte[]) result.getCells().get(0).getColumnValue()) <= 0);
      assertEquals("Expecting all cells", 10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals("Expecting 6 records", 6, resultCount);
    scanner.close();

    deleteTable("testSingleColumnValueFilter");
  }

  @Test
  public void testFilterList() {
    int numRows = 10;
    String rowKeyPrefix = "rowkey";
    MTable table = createTable("testFilterList");
    for (int index = 0; index < numRows; index++) {
      Put put = new Put(Bytes.toBytes(rowKeyPrefix + index));
      int rvalue = (numRows % (index + 1));
      System.out.println(
          "MTableFilterImplDUnitTest.testSingleColumnValueFilter :: " + "Adding value: " + rvalue);
      put.addColumn(QUALIFIERS[0], Bytes.toBytes(VALUE + rvalue));
      put.addColumn(QUALIFIERS[1], Bytes.toBytes(VALUE + 1));
      put.addColumn(QUALIFIERS[2], Bytes.toBytes(VALUE + 2));
      table.put(put);
    }

    Scan scan = new Scan();
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    Filter singleColumnValueFilter =
        new SingleColumnValueFilter(QUALIFIERS[0], CompareOp.GREATER, Bytes.toBytes(VALUE + 0));
    filterList.addFilter(singleColumnValueFilter);
    scan.setFilter(filterList);
    Scanner scanner = table.getScanner(scan);
    int resultCount = 0;
    for (Row result : table.getScanner(scan)) {
      System.out.println("MTableFilterImplDUnitTest.testFilterList :: RowKey: "
          + Bytes.toString(result.getRowId()));
      assertTrue("Row key must not be 0",
          Bytes.compareTo(Bytes.toBytes(rowKeyPrefix + 0), result.getRowId()) < 0);
      assertEquals("Expecting 10 cells", 10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals("Expecting 6 records", 6, resultCount);

    Filter keyOnlyFilter = new KeyOnlyFilter();
    filterList.addFilter(keyOnlyFilter);
    scan.setFilter(filterList);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : table.getScanner(scan)) {
      System.out.println("MTableFilterImplDUnitTest.testFilterList :: RowKey: "
          + Bytes.toString(result.getRowId()));
      assertTrue("Row key must not be 0",
          Bytes.compareTo(Bytes.toBytes(rowKeyPrefix + 0), result.getRowId()) < 0);
      assertEquals("Expecting 0 cells", 0, result.getCells().size());
      resultCount++;
    }
    assertEquals("Expecting 6 records", 6, resultCount);
    scanner.close();

  }

  @Test
  public void testValueFilter() {
    int numRows = 10;
    MTable table = createTableAndPutRows("testValueFilter", numRows);
    Scan scan = new Scan();
    Filter filter = new ValueFilter(CompareOp.EQUAL, Bytes.toBytes(VALUE + 1));
    scan.setFilter(filter);
    Scanner scanner = table.getScanner(scan);
    int resultCount = 0;
    for (Row result : scanner) {
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value:"
          + Bytes.toString((byte[]) result.getCells().get(0).getColumnValue()));
      assertEquals(1, result.getCells().size());
      assertEquals(Bytes.toString((byte[]) result.getCells().get(0).getColumnValue()), (VALUE + 1));
      resultCount++;
    }
    assertEquals("Expecting all records", numRows, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new ValueFilter(CompareOp.EQUAL, Bytes.toBytes(VALUE + 2));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value:"
          + Bytes.toString((byte[]) result.getCells().get(0).getColumnValue()));
      assertEquals(1, result.getCells().size());
      assertEquals(Bytes.toString((byte[]) result.getCells().get(0).getColumnValue()), (VALUE + 2));
      resultCount++;
    }
    assertEquals("Expecting all records", numRows, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new ValueFilter(CompareOp.LESS, Bytes.toBytes(VALUE + 2));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value0:"
          + Bytes.toString((byte[]) result.getCells().get(0).getColumnValue()));
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value1:"
          + Bytes.toString((byte[]) result.getCells().get(1).getColumnValue()));
      assertEquals(2, result.getCells().size() - 1);
      assertEquals(Bytes.toString((byte[]) result.getCells().get(0).getColumnValue()), (VALUE + 0));
      assertEquals(Bytes.toString((byte[]) result.getCells().get(1).getColumnValue()), (VALUE + 1));
      resultCount++;
    }
    assertEquals("Expecting all records", numRows, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new ValueFilter(CompareOp.LESS_OR_EQUAL, Bytes.toBytes(VALUE + 2));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value0:"
          + Bytes.toString((byte[]) result.getCells().get(0).getColumnValue()));
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value1:"
          + Bytes.toString((byte[]) result.getCells().get(1).getColumnValue()));
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value2:"
          + Bytes.toString((byte[]) result.getCells().get(2).getColumnValue()));
      assertEquals(3, result.getCells().size() - 1);
      assertEquals(Bytes.toString((byte[]) result.getCells().get(0).getColumnValue()), (VALUE + 0));
      assertEquals(Bytes.toString((byte[]) result.getCells().get(1).getColumnValue()), (VALUE + 1));
      assertEquals(Bytes.toString((byte[]) result.getCells().get(2).getColumnValue()), (VALUE + 2));
      resultCount++;
    }
    assertEquals("Expecting all records", numRows, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new ValueFilter(CompareOp.GREATER, Bytes.toBytes(VALUE + 2));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertEquals(7, result.getCells().size());
      resultCount++;
    }
    assertEquals("Expecting all records", numRows, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new ValueFilter(CompareOp.GREATER, Bytes.toBytes(VALUE + 9));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertEquals(0, result.getCells().size());
      resultCount++;
    }
    assertEquals("Expecting all records", numRows, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new ValueFilter(CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(VALUE + 9));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value0:"
          + Bytes.toString((byte[]) result.getCells().get(0).getColumnValue()));
      assertEquals(1, result.getCells().size());
      assertEquals(Bytes.toString((byte[]) result.getCells().get(0).getColumnValue()), (VALUE + 9));
      resultCount++;
    }
    assertEquals("Expecting all records", numRows, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new ValueFilter(CompareOp.NOT_EQUAL, Bytes.toBytes(VALUE + 5));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value0:"
          + Bytes.toString((byte[]) result.getCells().get(0).getColumnValue()));
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value1:"
          + Bytes.toString((byte[]) result.getCells().get(1).getColumnValue()));
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value2:"
          + Bytes.toString((byte[]) result.getCells().get(2).getColumnValue()));
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value3:"
          + Bytes.toString((byte[]) result.getCells().get(3).getColumnValue()));
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value4:"
          + Bytes.toString((byte[]) result.getCells().get(4).getColumnValue()));
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value5:"
          + Bytes.toString((byte[]) result.getCells().get(5).getColumnValue()));
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value6:"
          + Bytes.toString((byte[]) result.getCells().get(6).getColumnValue()));
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value7:"
          + Bytes.toString((byte[]) result.getCells().get(7).getColumnValue()));
      System.out.println("MTableFilterImplDUnitTest.testValueFilter :: " + "Value8:"
          + Bytes.toString((byte[]) result.getCells().get(8).getColumnValue()));
      assertEquals(9, result.getCells().size() - 1);
      assertEquals(Bytes.toString((byte[]) result.getCells().get(0).getColumnValue()), (VALUE + 0));
      assertEquals(Bytes.toString((byte[]) result.getCells().get(1).getColumnValue()), (VALUE + 1));
      assertEquals(Bytes.toString((byte[]) result.getCells().get(2).getColumnValue()), (VALUE + 2));
      assertEquals(Bytes.toString((byte[]) result.getCells().get(3).getColumnValue()), (VALUE + 3));
      assertEquals(Bytes.toString((byte[]) result.getCells().get(4).getColumnValue()), (VALUE + 4));
      assertEquals(Bytes.toString((byte[]) result.getCells().get(5).getColumnValue()), (VALUE + 6));
      assertEquals(Bytes.toString((byte[]) result.getCells().get(6).getColumnValue()), (VALUE + 7));
      assertEquals(Bytes.toString((byte[]) result.getCells().get(7).getColumnValue()), (VALUE + 8));
      assertEquals(Bytes.toString((byte[]) result.getCells().get(8).getColumnValue()), (VALUE + 9));
      resultCount++;
    }
    assertEquals("Expecting all records", numRows, resultCount);
    scanner.close();

    deleteTable("testValueFilter");
  }

  @Test
  public void testRowFilter() {
    int numRows = 10;
    byte[] rowId = Bytes.add(ROW, new byte[] {(byte) 1});
    MTable table = createTableAndPutRows("testRowFilter", numRows);
    Scan scan = new Scan();
    Filter filter = new RowFilter(CompareOp.EQUAL, rowId);// Bytes.add(base, new byte[]{(byte) i});
    scan.setFilter(filter);
    Scanner scanner = table.getScanner(scan);
    int resultCount = 0;
    for (Row result : scanner) {
      System.out.println("MTableFilterImplDUnitTest.testRowFilter :: " + "RowId:"
          + Bytes.toString(result.getRowId()));
      assertEquals(10, result.getCells().size() - 1);
      assertEquals(0, Bytes.compareTo(result.getRowId(), rowId));
      resultCount++;
    }
    assertEquals(1, resultCount);
    scanner.close();

    scan = new Scan();
    rowId = Bytes.add(ROW, new byte[] {(byte) 2});
    filter = new RowFilter(CompareOp.EQUAL, rowId);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      System.out.println("MTableFilterImplDUnitTest.testRowFilter :: " + "RowId:"
          + Bytes.toString(result.getRowId()));
      assertEquals(10, result.getCells().size() - 1);
      assertEquals(Bytes.toString(result.getRowId()), Bytes.toString(rowId));
      resultCount++;
    }
    assertEquals(1, resultCount);
    scanner.close();

    scan = new Scan();
    rowId = Bytes.add(ROW, new byte[] {(byte) 2});
    byte[][] expected =
        {Bytes.add(ROW, new byte[] {(byte) 0}), Bytes.add(ROW, new byte[] {(byte) 1})};
    filter = new RowFilter(CompareOp.LESS, rowId);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      System.out.println("MTableFilterImplDUnitTest.testRowFilter :: " + "RowId:"
          + Bytes.toString(result.getRowId()));
      assertEquals(10, result.getCells().size() - 1);
      assertEquals(Bytes.toString(result.getRowId()), Bytes.toString(expected[resultCount]));
      resultCount++;
    }
    assertEquals(2, resultCount);
    scanner.close();

    scan = new Scan();
    rowId = Bytes.add(ROW, new byte[] {(byte) 2});
    byte[][] expected2 = {Bytes.add(ROW, new byte[] {(byte) 0}),
        Bytes.add(ROW, new byte[] {(byte) 1}), Bytes.add(ROW, new byte[] {(byte) 2})};
    filter = new RowFilter(CompareOp.LESS_OR_EQUAL, rowId);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      System.out.println("MTableFilterImplDUnitTest.testRowFilter :: " + "RowId:"
          + Bytes.toString(result.getRowId()));
      assertEquals(10, result.getCells().size() - 1);
      assertEquals(Bytes.toString(result.getRowId()), Bytes.toString(expected2[resultCount]));
      resultCount++;
    }
    assertEquals(3, resultCount);
    scanner.close();

    scan = new Scan();
    rowId = Bytes.add(ROW, new byte[] {(byte) 5});
    filter = new RowFilter(CompareOp.NOT_EQUAL, rowId);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      System.out.println("MTableFilterImplDUnitTest.testRowFilter :: " + "RowId:"
          + Bytes.toString(result.getRowId()));
      assertEquals(10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(9, resultCount);
    scanner.close();

    scan = new Scan();
    rowId = Bytes.add(ROW, new byte[] {(byte) 9});
    filter = new RowFilter(CompareOp.GREATER_OR_EQUAL, rowId);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      System.out.println("MTableFilterImplDUnitTest.testRowFilter :: " + "RowId:"
          + Bytes.toString(result.getRowId()));
      assertEquals(10, result.getCells().size() - 1);
      assertEquals(Bytes.toString(result.getRowId()), Bytes.toString(rowId));
      resultCount++;
    }
    assertEquals(1, resultCount);
    scanner.close();

    scan = new Scan();
    rowId = Bytes.add(ROW, new byte[] {(byte) 9});
    filter = new RowFilter(CompareOp.GREATER, rowId);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      System.out.println("MTableFilterImplDUnitTest.testRowFilter :: " + "RowId:"
          + Bytes.toString(result.getRowId()));
      assertEquals(10, result.getCells().size() - 1);
      assertEquals(Bytes.toString(result.getRowId()), Bytes.toString(rowId));
      resultCount++;
    }
    assertEquals(0, resultCount);
    scanner.close();

    deleteTable("testRowFilter");
  }

  /**
   * This will test timestamp filter for MTable.
   */
  @Test
  public void testTimestampFilter() {
    String tableName = getTestMethodName();
    int numRows = 10;
    byte[] rowId = Bytes.add(ROW, new byte[] {(byte) 1});
    long startTimestamp = System.nanoTime();
    System.out
        .println("MTableFilterImplDUnitTest.testTimestampFilter :: 585" + "STS " + startTimestamp);

    MTable table = createTableAndPutRows(tableName, numRows);
    long stopTimestamp = System.nanoTime();
    System.out
        .println("MTableFilterImplDUnitTest.testTimestampFilter :: 588" + "ETS " + stopTimestamp);

    Scan scan = new Scan();
    Filter filter = new TimestampFilter(CompareOp.GREATER_OR_EQUAL, startTimestamp);// Bytes.add(base,
    // new
    // byte[]{(byte)
    // i});
    scan.setFilter(filter);
    Scanner scanner = table.getScanner(scan);
    int resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(numRows, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.LESS_OR_EQUAL, stopTimestamp);// Bytes.add(base, new
    // byte[]{(byte) i});
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(numRows, resultCount);
    scanner.close();

    long equalTS = 0l;

    // gather all timstamps for point quereis
    scan = new Scan();
    scanner = table.getScanner(scan);
    Iterator<Row> iterator = scanner.iterator();
    TreeMap<Long, Row> recordsMap = new TreeMap<>();
    while (iterator.hasNext()) {
      Row next = iterator.next();
      recordsMap.put(next.getRowTimeStamp(), next);
    }
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.EQUAL, recordsMap.firstKey());
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(recordsMap.firstKey(), result.getRowTimeStamp());
      assertEquals(10, result.getCells().size() - 1);
      assertEquals(0, result.compareTo(recordsMap.get(recordsMap.firstKey())));
      resultCount++;
    }
    assertEquals(1, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.LESS, recordsMap.lastKey());
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(10, result.getCells().size() - 1);

      assertTrue(recordsMap.containsKey(result.getRowTimeStamp()));
      assertEquals(0, result.compareTo(recordsMap.get(result.getRowTimeStamp())));
      resultCount++;
    }
    assertEquals(9, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.LESS, recordsMap.firstKey());
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      resultCount++;
    }
    assertEquals(0, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.GREATER, recordsMap.firstKey());
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(10, result.getCells().size() - 1);

      assertTrue(recordsMap.containsKey(result.getRowTimeStamp()));
      assertEquals(0, result.compareTo(recordsMap.get(result.getRowTimeStamp())));
      resultCount++;
    }
    assertEquals(9, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.GREATER, recordsMap.lastKey());
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      resultCount++;
    }
    assertEquals(0, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.NOT_EQUAL, startTimestamp);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(10, result.getCells().size() - 1);

      assertTrue(recordsMap.containsKey(result.getRowTimeStamp()));
      assertEquals(0, result.compareTo(recordsMap.get(result.getRowTimeStamp())));
      resultCount++;
    }
    assertEquals(10, resultCount);
    scanner.close();

    scan = new Scan();
    filter = new TimestampFilter(CompareOp.NOT_EQUAL, recordsMap.firstKey());
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertTrue(result.getRowTimeStamp() > startTimestamp);
      assertTrue(result.getRowTimeStamp() < stopTimestamp);
      assertEquals(10, result.getCells().size() - 1);

      assertTrue(recordsMap.containsKey(result.getRowTimeStamp()));
      assertEquals(0, result.compareTo(recordsMap.get(result.getRowTimeStamp())));
      resultCount++;
    }
    assertEquals(9, resultCount);
    scanner.close();

    deleteTable(tableName);
  }

  /**
   * This will test timestamp filter for multiversion MTable.
   */
  @Test
  public void testTimestampFilterMultiVersionTable() {
    runTestTimestampFilterMultiVersionTable(true);
  }

  /**
   * This will test timestamp filter for multiversion MTable.
   */
  @Test
  public void testTimestampFilterMultiVersionTableUnordered() {
    runTestTimestampFilterMultiVersionTable(true);
  }


  @Test
  public void testAllVersionFilterImplBasic() {
    runTestAllversionsFilterImplBasic(true);
  }

  @Test
  public void testAllVersionFilterImplBasicUnordered() {
    runTestAllversionsFilterImplBasic(false);
  }


  @Test
  public void testAllVersionFilterImpl() {
    runTestAllVersionFilterImpl(true);
  }

  @Test
  public void testAllVersionFilterImplUnordered() {
    runTestAllVersionFilterImpl(true);
  }

  @Test
  public void testRowFilterWithRegex() {
    MTable table = createTable("testRowFilterWithRegex");
    int numRows = 50;

    for (int i = 0; i < numRows; i++) {
      Put put = new Put("abc" + i);
      for (int j = 0; j < QUALIFIERS.length; j++) {
        put.addColumn(QUALIFIERS[j], Bytes.toBytes(VALUE + j));
      }
      table.put(put);
    }

    for (int i = 0; i < numRows; i++) {
      Put put = new Put("xyz" + i);
      for (int j = 0; j < QUALIFIERS.length; j++) {
        put.addColumn(QUALIFIERS[j], Bytes.toBytes(VALUE + j));
      }
      table.put(put);
    }

    for (int i = 0; i < numRows; i++) {
      Put put = new Put("abc" + i + "xyz" + (i + 1));
      for (int j = 0; j < QUALIFIERS.length; j++) {
        put.addColumn(QUALIFIERS[j], Bytes.toBytes(VALUE + j));
      }
      table.put(put);
    }

    for (int i = 0; i < numRows; i++) {
      Put put = new Put("xyz" + (i + 1) + "abc" + i);
      for (int j = 0; j < QUALIFIERS.length; j++) {
        put.addColumn(QUALIFIERS[j], Bytes.toBytes(VALUE + j));
      }
      table.put(put);
    }
    Scan scan = null;
    String regex = "";
    Filter filter = null;
    Scanner scanner = null;
    int resultCount = 0;

    scan = new Scan();
    regex = "abc.*";
    filter = new RowFilter(CompareOp.REGEX, regex);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertEquals(true, Bytes.toString(result.getRowId()).matches(regex));
      assertEquals(10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(100, resultCount);
    scanner.close();

    scan = new Scan();
    regex = "xyz.*";
    filter = new RowFilter(CompareOp.REGEX, regex);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertEquals(true, Bytes.toString(result.getRowId()).matches(regex));
      assertEquals(10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(100, resultCount);
    scanner.close();

    scan = new Scan();
    regex = Pattern.compile(".*xyz.*").toString();
    filter = new RowFilter(CompareOp.REGEX, regex);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertEquals(true, Bytes.toString(result.getRowId()).matches(regex));
      assertEquals(10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(150, resultCount);
    scanner.close();

    scan = new Scan();
    regex = ".*abc.*";
    filter = new RowFilter(CompareOp.REGEX, regex);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertEquals(true, Bytes.toString(result.getRowId()).matches(regex));
      assertEquals(10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(150, resultCount);
    scanner.close();

    scan = new Scan();
    regex = "abc.*xyz.*";
    filter = new RowFilter(CompareOp.REGEX, Bytes.toBytes(regex));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertEquals(true, Bytes.toString(result.getRowId()).matches(regex));
      assertEquals(10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(50, resultCount);
    scanner.close();

    scan = new Scan();
    regex = "xyz.*abc.*";
    filter = new RowFilter(CompareOp.REGEX, regex);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertEquals(true, Bytes.toString(result.getRowId()).matches(regex));
      assertEquals(10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(50, resultCount);
    scanner.close();

    scan = new Scan();
    regex = "abc1xyz.*";
    filter = new RowFilter(CompareOp.REGEX, regex);
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertEquals(true, Bytes.toString(result.getRowId()).matches(regex));
      assertEquals(10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(1, resultCount);
    scanner.close();

    scan = new Scan();
    regex = "abc.*xyz1";
    filter = new RowFilter(CompareOp.REGEX, Bytes.toBytes(regex));
    scan.setFilter(filter);
    scanner = table.getScanner(scan);
    resultCount = 0;
    for (Row result : scanner) {
      assertEquals(true, Bytes.toString(result.getRowId()).matches(regex));
      assertEquals(10, result.getCells().size() - 1);
      resultCount++;
    }
    assertEquals(1, resultCount);
    scanner.close();

    deleteTable("testRowFilterWithRegex");
  }

  @Test
  public void testRowFilterWithRange() {
    byte[] RowIDKeySpaceStart = Bytes.toBytes("000");
    byte[] RowIDKeySpaceEnd = Bytes.toBytes("100");
    byte[] scanStartKey = Bytes.toBytes("020");
    byte[] scanStopKey = Bytes.toBytes("080");
    String tableName = "EmployeeTable";
    MTable table = null;
    int TABLE_MAX_VERSIONS = 5;
    int NUM_OF_COLUMNS = 6;
    List<String> columnNames = Arrays.asList("NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ");
    int NUM_REPLICAS = 2;
    int NUM_SPLITS = 10;
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(Bytes.toBytes(columnNames.get(0)))
        .addColumn(Bytes.toBytes(columnNames.get(1))).addColumn(Bytes.toBytes(columnNames.get(2)))
        .addColumn(Bytes.toBytes(columnNames.get(3))).addColumn(Bytes.toBytes(columnNames.get(4)))
        .addColumn(Bytes.toBytes(columnNames.get(5))).setMaxVersions(TABLE_MAX_VERSIONS)
        .setRedundantCopies(NUM_REPLICAS).setTotalNumOfSplits(NUM_SPLITS)
        .setStartStopRangeKey(RowIDKeySpaceStart, RowIDKeySpaceEnd);

    table = getmClientCache().getAdmin().createTable(tableName, tableDescriptor);
    System.out.println("Table [EmployeeTable] is created successfully!\n");

    byte[] row78 = Bytes.toBytes("078");
    System.out.println(
        "STEP:3 Inserting Row ID [078][byte:" + row78 + "] w/ version timestamps 100 - 600]");
    Put record = new Put(row78);
    for (int versionIndex = 0; versionIndex < TABLE_MAX_VERSIONS + 1; versionIndex++) {
      long timestamp = (versionIndex + 1) * 100;
      record.setTimeStamp(timestamp);
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        record.addColumn(Bytes.toBytes(columnNames.get(colIndex)),
            Bytes.toBytes("val" + versionIndex + "" + colIndex));
      }
      table.put(record);
    }

    System.out.println(
        "STEP:4 Updating Row [ID:078][byte:" + row78 + "][version:600] w/ UpdateVal600{0-5}");
    try {
      Put newRecord = new Put(row78);
      newRecord.setTimeStamp(600L);
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        newRecord.addColumn(Bytes.toBytes(columnNames.get(colIndex)),
            Bytes.toBytes("UpdateVal" + "600" + colIndex));
      }
      table.checkAndPut(row78, Bytes.toBytes(columnNames.get(0)), Bytes.toBytes("val50"),
          newRecord);
    } catch (Exception e) {
      e.printStackTrace();
    }

    System.out.println("STEP:5 Inserting Batch of Rows at timestamp [100] - ");
    long version = 100;
    int batchSize = 10;
    Random r = new Random();
    Set<Integer> randomIds = new HashSet<>();
    List<Put> rows = new ArrayList<Put>(2 * batchSize);
    for (int i = 0; i < 2 * batchSize; i++) {
      int j = 0;
      do {
        j = r.nextInt(100);
      } while (!randomIds.add(j));
      String rowID = Strings.padStart(String.valueOf(j), 3, '0');
      if (rowID.equals("078")) {
        continue;
      }
      Put row = new Put(Bytes.toBytes(rowID));
      row.setTimeStamp(version);
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        row.addColumn(Bytes.toBytes(columnNames.get(colIndex)),
            Bytes.toBytes("val-" + rowID + "-" + version + "-" + colIndex));
      }
      rows.add(row);
    }
    table.put(rows);

    System.out.println("Expecting only key 78");
    Scan scanner = new Scan();
    scanner.setBatchSize(20);
    scanner.setStartRow(scanStartKey);
    scanner.setStopRow(scanStopKey);
    Filter filter = new RowFilter(CompareOp.EQUAL, row78);

    scanner.setFilter(filter);
    Scanner results = table.getScanner(scanner);

    Iterator itr = results.iterator();
    int count = 0;
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      System.out.println("Row ID [" + Bytes.toString(res.getRowId()) + "]");
      assertEquals(0, Bytes.compareTo(res.getRowId(), row78));
      count++;
    }
    assertEquals(1, count);

    getmClientCache().getAdmin().deleteTable(tableName);
    System.out.println("Table is deleted successfully!");
  }

}
