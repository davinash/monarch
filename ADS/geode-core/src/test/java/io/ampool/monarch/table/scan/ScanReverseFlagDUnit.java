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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.TypeHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Category(MonarchTest.class)
public class ScanReverseFlagDUnit extends MTableDUnitHelper {
  public ScanReverseFlagDUnit() {
    super();
  }


  private static final String COL1 = "NAME";
  private static final String COL2 = "ID";
  private static final String COL3 = "AGE";
  private static final String COL4 = "SALARY";

  final static int tabMaxVersions = 3;
  final static int numBuckets = 6; // Number of Buckets
  final static int numOfEntries = 5; // Number of Records

  protected static final Logger logger = LogService.getLogger();
  Host host = Host.getHost(0);
  private VM vm0 = host.getVM(0);
  private VM vm1 = host.getVM(1);
  private VM vm2 = host.getVM(2);
  private VM client1 = host.getVM(3);

  private final int NUM_OF_COLUMNS = 5;
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

  private void insertRows(MTable mtable, Map<Integer, List<byte[]>> bucketToKeysForAllBuckets) {

    bucketToKeysForAllBuckets.forEach((bucketId, list) -> {
      final AtomicInteger keyIndex = new AtomicInteger(0);
      list.forEach(keyBytes -> {
        Put myput1 = new Put(keyBytes);
        String strName = "Name-" + keyIndex.get();
        myput1.addColumn(COL1, strName); // Name
        myput1.addColumn(COL2, bucketId * 10 + keyIndex.get()); // ID
        myput1.addColumn(COL3, bucketId); // Age
        myput1.addColumn(COL4, keyIndex.getAndIncrement() * 1000); // Salary
        mtable.put(myput1);
      });
    });
  }

  private void insertRows(MTable mtable) {

    for (int keyIndex = 0; keyIndex < numOfEntries; keyIndex++) {
      Put myput1 = new Put(Bytes.toBytes(padWithZero(keyIndex, 3)));
      String strName = "Name-" + keyIndex;
      myput1.addColumn(COL1, strName); // Name
      myput1.addColumn(COL2, keyIndex); // ID
      myput1.addColumn(COL3, keyIndex); // Age
      myput1.addColumn(COL4, keyIndex * 1000); // Salary
      mtable.put(myput1);
    }

  }

  /***** PAD DATA WITH LEADING ZEROs ************/

  private String padWithZero(final int value, final int maxSize) {
    String valueString = String.valueOf(value);
    for (int index = valueString.length(); index <= maxSize; index++) {
      valueString = "0" + valueString;
    }
    return valueString;

  }

  private void rowFormatter(byte[] key, Long timestamp, List<Cell> cells) {

    StringBuffer outputStr = new StringBuffer();

    outputStr.append("KEY").append(" : ").append(TypeHelper.deepToString(key)).append("\t\t")
        .append("VERSION_ID").append(" : ").append(timestamp).append("\t");

    cells.stream().filter(
        cell -> !(Bytes.toString(cell.getColumnName()).equalsIgnoreCase("__ROW__KEY__COLUMN__")))
        .collect(Collectors.toList()).forEach(cell -> {
          outputStr.append(Bytes.toString(cell.getColumnName())).append(" : ")
              .append(TypeHelper.deepToString(cell.getColumnValue())).append("\t");
        });
    System.out.println(outputStr.toString());
  }

  /***** CREATE M-TABLE ************/

  private MTable createTable(String tableName) {

    // MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    Schema schema = new Schema.Builder().column(COL1, BasicTypes.STRING.toString())
        .column(COL2, BasicTypes.INT.toString()).column(COL3, BasicTypes.INT.toString())
        .column(COL4, BasicTypes.INT.toString()).build();

    tableDescriptor.setSchema(schema);
    tableDescriptor.setMaxVersions(tabMaxVersions);
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

  @Test
  public void serverSideScanWithReverseFlag() {
    String tableName = getTestMethodName();
    MTable mtable = createTable(tableName);
    Map<Integer, List<byte[]>> bucketToKeysForAllBuckets =
        getBucketToKeysForAllBuckets(numBuckets, numOfEntries);
    insertRows(mtable, bucketToKeysForAllBuckets);

    Function<MTable, List<Row>> executeReverseScan =
        (Function<MTable, List<Row>> & Serializable) mTable -> {
          System.out.println("----------------------REVERSE FLAG = TRUE-------------------\n");
          Scan scanObj = new Scan();
          scanObj.setReversed(true);
          Scanner results = mTable.getScanner(scanObj);
          Iterator iterator = results.iterator();
          List<Row> rows = new ArrayList<>();
          long prevTS = Long.MAX_VALUE;
          // Iterate Over all Records
          while (iterator.hasNext()) {
            Row res = (Row) iterator.next();
            rowFormatter(res.getRowId(), res.getRowTimeStamp(), res.getCells());
            // as values are reversed so timestamp of next record must be smaller than this..
            // as we have inserted one after other in this test
            assertTrue(res.getRowTimeStamp() < prevTS);
            prevTS = res.getRowTimeStamp();
            rows.add(res);
          }
          System.out.println("----------------------------------------------------\n");
          return rows;
        };

    Function<MTable, List<Row>> executeScan =
        (Function<MTable, List<Row>> & Serializable) mTable -> {
          System.out.println("----------------------REVERSE FLAG = FALSE-------------------\n");
          Scan scanObj = new Scan();
          scanObj.setReversed(false);
          Scanner results = mTable.getScanner(scanObj);
          Iterator iterator = results.iterator();
          List<Row> rows = new ArrayList<>();
          long prevTS = Long.MIN_VALUE;
          // Iterate Over all Records
          while (iterator.hasNext()) {
            Row res = (Row) iterator.next();
            rowFormatter(res.getRowId(), res.getRowTimeStamp(), res.getCells());
            // as values are reversed so timestamp of next record must be smaller than this..
            // as we have inserted one after other in this test
            assertTrue(res.getRowTimeStamp() > prevTS);
            prevTS = res.getRowTimeStamp();
            rows.add(res);
          }
          System.out.println("----------------------------------------------------\n");
          return rows;
        };

    SerializableCallable<List<Row>> serializableCallable = new SerializableCallable<List<Row>>() {
      @Override
      public List<Row> call() throws Exception {
        MCache mCache = MCacheFactory.getAnyInstance();
        MTable mTable = mCache.getMTable(tableName);
        return executeScan.apply(mTable);
      }
    };

    SerializableCallable<List<Row>> reverseSerializableCallable =
        new SerializableCallable<List<Row>>() {
          @Override
          public List<Row> call() throws Exception {
            MCache mCache = MCacheFactory.getAnyInstance();
            MTable mTable = mCache.getMTable(tableName);
            return executeReverseScan.apply(mTable);
          }
        };

    List<Row> rows0 = vm0.invoke("Server1", serializableCallable);
    List<Row> rows1 = vm1.invoke("Server2", serializableCallable);
    List<Row> rows2 = vm2.invoke("Server3", serializableCallable);

    List<Row> reverseRows0 = vm0.invoke("Server1", reverseSerializableCallable);
    List<Row> reverseRows1 = vm1.invoke("Server2", reverseSerializableCallable);
    List<Row> reverseRows2 = vm2.invoke("Server3", reverseSerializableCallable);

    // reverse way comparision of rows should match
    boolean valid1 = compareRows(rows0, reverseRows0);
    boolean valid2 = compareRows(rows1, reverseRows1);
    boolean valid3 = compareRows(rows2, reverseRows2);
    assertTrue(valid1 && valid2 && valid3);

  }

  @Test
  public void serverSideScanWithReverseFlagKeyRange() {
    String tableName = getTestMethodName();
    MTable mtable = createTable(tableName);
    Map<Integer, List<byte[]>> bucketToKeysForAllBuckets =
        getBucketToKeysForAllBuckets(numBuckets, numOfEntries);
    // List<byte[]> keys = new ArrayList<>();
    // for (int i = 0; i < numOfEntries; i++) {
    // keys.add(Bytes.toBytes(i));
    // }
    // bucketToKeysForAllBuckets.put(0,keys);

    TreeMap<Integer, List<byte[]>> sortedMap = new TreeMap(bucketToKeysForAllBuckets);
    List<byte[]> collect =
        sortedMap.values().stream().flatMap(List::stream).collect(Collectors.toList());

    byte[] prevByteArr = null;
    for (int i = 0; i < collect.size(); i++) {
      if (prevByteArr != null) {
        assertTrue(Bytes.compareTo(prevByteArr, collect.get(i)) < 0);
      }
      prevByteArr = collect.get(i);
    }

    Random random = new Random();
    int startKeyIndex = random.nextInt(collect.size());
    int stopKeyIndex = startKeyIndex + random.nextInt(collect.size() - startKeyIndex);
    /*
     * int startKeyIndex = 0; int stopKeyIndex = collect.size() -1;
     */
    System.out
        .println("ScanReverseFlagDUnit.clientSideScanWithReverseFlagKeyRange :: 296 startIndex "
            + startKeyIndex + " stopKeyIndex " + stopKeyIndex);
    System.out.println("StartKey " + TypeHelper.deepToString(collect.get(startKeyIndex))
        + " stopKey " + TypeHelper.deepToString(collect.get(stopKeyIndex)));

    insertRows(mtable, bucketToKeysForAllBuckets);

    Function<MTable, List<Row>> executeReverseScan =
        (Function<MTable, List<Row>> & Serializable) mTable -> {
          System.out.println("----------------------REVERSE FLAG = TRUE-------------------\n");
          Scan scanObj = new Scan();
          scanObj.setReversed(true);
          scanObj.setStartRow(collect.get(startKeyIndex));
          scanObj.setStopRow(collect.get(stopKeyIndex));

          Scanner results = mTable.getScanner(scanObj);
          Iterator iterator = results.iterator();
          List<Row> rows = new ArrayList<>();
          long prevTS = Long.MAX_VALUE;
          // Iterate Over all Records
          while (iterator.hasNext()) {
            Row res = (Row) iterator.next();
            rowFormatter(res.getRowId(), res.getRowTimeStamp(), res.getCells());
            // as values are reversed so timestamp of next record must be smaller than this..
            // as we have inserted one after other in this test
            assertTrue(res.getRowTimeStamp() < prevTS);
            prevTS = res.getRowTimeStamp();
            rows.add(res);
          }
          System.out.println("----------------------------------------------------\n");
          return rows;
        };

    Function<MTable, List<Row>> executeScan =
        (Function<MTable, List<Row>> & Serializable) mTable -> {
          System.out.println("----------------------REVERSE FLAG = FALSE-------------------\n");
          Scan scanObj = new Scan();
          scanObj.setReversed(false);
          scanObj.setStartRow(collect.get(startKeyIndex));
          scanObj.setStopRow(collect.get(stopKeyIndex));
          Scanner results = mTable.getScanner(scanObj);
          Iterator iterator = results.iterator();
          List<Row> rows = new ArrayList<>();
          long prevTS = Long.MIN_VALUE;
          // Iterate Over all Records
          while (iterator.hasNext()) {
            Row res = (Row) iterator.next();
            rowFormatter(res.getRowId(), res.getRowTimeStamp(), res.getCells());
            // as values are reversed so timestamp of next record must be smaller than this..
            // as we have inserted one after other in this test
            assertTrue(res.getRowTimeStamp() > prevTS);
            prevTS = res.getRowTimeStamp();
            rows.add(res);
          }
          System.out.println("----------------------------------------------------\n");
          return rows;
        };

    SerializableCallable<List<Row>> serializableCallable = new SerializableCallable<List<Row>>() {
      @Override
      public List<Row> call() throws Exception {
        MCache mCache = MCacheFactory.getAnyInstance();
        MTable mTable = mCache.getMTable(tableName);
        return executeScan.apply(mTable);
      }
    };

    SerializableCallable<List<Row>> reverseSerializableCallable =
        new SerializableCallable<List<Row>>() {
          @Override
          public List<Row> call() throws Exception {
            MCache mCache = MCacheFactory.getAnyInstance();
            MTable mTable = mCache.getMTable(tableName);
            return executeReverseScan.apply(mTable);
          }
        };

    List<Row> rows0 = vm0.invoke("Server1", serializableCallable);
    List<Row> rows1 = vm1.invoke("Server2", serializableCallable);
    List<Row> rows2 = vm2.invoke("Server3", serializableCallable);

    List<Row> reverseRows0 = vm0.invoke("Server1", reverseSerializableCallable);
    List<Row> reverseRows1 = vm1.invoke("Server2", reverseSerializableCallable);
    List<Row> reverseRows2 = vm2.invoke("Server3", reverseSerializableCallable);

    // reverse way comparision of rows should match
    boolean valid1 = compareRows(rows0, reverseRows0);
    boolean valid2 = compareRows(rows1, reverseRows1);
    boolean valid3 = compareRows(rows2, reverseRows2);
    assertTrue(valid1 && valid2 && valid3);

  }

  @Test
  public void clientSideScanWithReverseFlagKeyRange() {
    String tableName = getTestMethodName();
    MTable mtable = createTable(tableName);
    Map<Integer, List<byte[]>> bucketToKeysForAllBuckets =
        getBucketToKeysForAllBuckets(numBuckets, numOfEntries);
    List<byte[]> collect = bucketToKeysForAllBuckets.values().stream().flatMap(List::stream)
        .collect(Collectors.toList());
    Random random = new Random();
    int startKeyIndex = random.nextInt(collect.size());
    int stopKeyIndex = startKeyIndex + random.nextInt(collect.size() - startKeyIndex);
    // int startKeyIndex = 22;
    // int stopKeyIndex = 26;
    System.out
        .println("ScanReverseFlagDUnit.clientSideScanWithReverseFlagKeyRange :: 296 startIndex "
            + startKeyIndex + " stopKeyIndex " + stopKeyIndex);
    System.out.println("ScanReverseFlagDUnit.clientSideScanWithReverseFlagKeyRange :: 429 startKey "
        + TypeHelper.deepToString(collect.get(startKeyIndex)) + " stopKey "
        + collect.get(stopKeyIndex));
    insertRows(mtable, bucketToKeysForAllBuckets);

    Function<MTable, List<Row>> executeReverseScan =
        (Function<MTable, List<Row>> & Serializable) mTable -> {
          System.out.println("----------------------REVERSE FLAG = TRUE-------------------\n");
          Scan scanObj = new Scan();
          scanObj.setReversed(true);
          scanObj.setStartRow(collect.get(startKeyIndex));
          scanObj.setStopRow(collect.get(stopKeyIndex));
          Scanner results = mTable.getScanner(scanObj);
          Iterator iterator = results.iterator();
          List<Row> rows = new ArrayList<>();
          long prevTS = Long.MAX_VALUE;
          // Iterate Over all Records
          while (iterator.hasNext()) {
            Row res = (Row) iterator.next();
            rowFormatter(res.getRowId(), res.getRowTimeStamp(), res.getCells());
            // as values are reversed so timestamp of next record must be smaller than this..
            // as we have inserted one after other in this test
            assertTrue(res.getRowTimeStamp() < prevTS);
            prevTS = res.getRowTimeStamp();
            rows.add(res);
          }
          System.out.println("----------------------------------------------------\n");
          return rows;
        };

    Function<MTable, List<Row>> executeScan =
        (Function<MTable, List<Row>> & Serializable) mTable -> {
          System.out.println("----------------------REVERSE FLAG = FALSE-------------------\n");
          Scan scanObj = new Scan();
          scanObj.setReversed(false);
          scanObj.setStartRow(collect.get(startKeyIndex));
          scanObj.setStopRow(collect.get(stopKeyIndex));
          Scanner results = mTable.getScanner(scanObj);
          Iterator iterator = results.iterator();
          List<Row> rows = new ArrayList<>();
          long prevTS = Long.MIN_VALUE;
          // Iterate Over all Records
          while (iterator.hasNext()) {
            Row res = (Row) iterator.next();
            rowFormatter(res.getRowId(), res.getRowTimeStamp(), res.getCells());
            // as values are reversed so timestamp of next record must be smaller than this..
            // as we have inserted one after other in this test
            assertTrue(res.getRowTimeStamp() > prevTS);
            prevTS = res.getRowTimeStamp();
            rows.add(res);
          }
          System.out.println("----------------------------------------------------\n");
          return rows;
        };

    List<Row> rows = executeScan.apply(mtable);
    List<Row> reverseRows = executeReverseScan.apply(mtable);

    // reverse way comparision of rows should match
    boolean valid = compareRows(rows, reverseRows);
    assertTrue(valid);

  }

  @Test
  public void clientSideScanWithReverseFlag() {
    String tableName = getTestMethodName();
    MTable mtable = createTable(tableName);
    Map<Integer, List<byte[]>> bucketToKeysForAllBuckets =
        getBucketToKeysForAllBuckets(numBuckets, numOfEntries);
    insertRows(mtable, bucketToKeysForAllBuckets);

    Function<MTable, List<Row>> executeReverseScan =
        (Function<MTable, List<Row>> & Serializable) mTable -> {
          System.out.println("----------------------REVERSE FLAG = TRUE-------------------\n");
          Scan scanObj = new Scan();
          scanObj.setReversed(true);
          Scanner results = mTable.getScanner(scanObj);
          Iterator iterator = results.iterator();
          List<Row> rows = new ArrayList<>();
          long prevTS = Long.MAX_VALUE;
          // Iterate Over all Records
          while (iterator.hasNext()) {
            Row res = (Row) iterator.next();
            rowFormatter(res.getRowId(), res.getRowTimeStamp(), res.getCells());
            // as values are reversed so timestamp of next record must be smaller than this..
            // as we have inserted one after other in this test
            assertTrue(res.getRowTimeStamp() < prevTS);
            prevTS = res.getRowTimeStamp();
            rows.add(res);
          }
          System.out.println("----------------------------------------------------\n");
          return rows;
        };

    Function<MTable, List<Row>> executeScan =
        (Function<MTable, List<Row>> & Serializable) mTable -> {
          System.out.println("----------------------REVERSE FLAG = FALSE-------------------\n");
          Scan scanObj = new Scan();
          scanObj.setReversed(false);
          Scanner results = mTable.getScanner(scanObj);
          Iterator iterator = results.iterator();
          List<Row> rows = new ArrayList<>();
          long prevTS = Long.MIN_VALUE;
          // Iterate Over all Records
          while (iterator.hasNext()) {
            Row res = (Row) iterator.next();
            rowFormatter(res.getRowId(), res.getRowTimeStamp(), res.getCells());
            // as values are reversed so timestamp of next record must be smaller than this..
            // as we have inserted one after other in this test
            assertTrue(res.getRowTimeStamp() > prevTS);
            prevTS = res.getRowTimeStamp();
            rows.add(res);
          }
          System.out.println("----------------------------------------------------\n");
          return rows;
        };

    List<Row> rows = executeScan.apply(mtable);
    List<Row> reverseRows = executeReverseScan.apply(mtable);

    // reverse way comparision of rows should match
    boolean valid = compareRows(rows, reverseRows);
    assertTrue(valid);

  }

  private boolean compareRows(List<Row> naturalWay, List<Row> reverseWay) {
    assertEquals(naturalWay.size(), reverseWay.size());
    int maxIndex = reverseWay.size() - 1;
    for (int index = 0; index < naturalWay.size(); index++) {
      Row row1 = naturalWay.get(index);
      Row row2 = reverseWay.get(maxIndex > 0 ? maxIndex - index : maxIndex);

      // row key matching
      assertArrayEquals(row1.getRowId(), row2.getRowId());

      // timestamp matching
      assertEquals(row1.getRowTimeStamp(), row2.getRowTimeStamp());

      // cells matching
      List<Cell> row1Cells = row1.getCells();
      List<Cell> row2Cells = row2.getCells();
      assertEquals(row1Cells.size(), row2Cells.size());

      for (int i = 0; i < row1Cells.size(); i++) {
        Cell cell1 = row1Cells.get(i);
        Cell cell2 = row2Cells.get(i);
        // column name
        assertArrayEquals(cell1.getColumnName(), cell2.getColumnName());
        // column value
        assertArrayEquals(cell1.getColumnName(), cell2.getColumnName());
      }
    }
    return true;
  }

}
