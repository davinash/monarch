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
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.filter.internal.TimestampFilter;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.TypeHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

@Category(MonarchTest.class)
public class ScanWithMaxVersionsAndOrderCombinationDUnit extends MTableDUnitHelper {
  public ScanWithMaxVersionsAndOrderCombinationDUnit() {
    super();
  }


  private static final String COL1 = "NAME";
  private static final String COL2 = "ID";
  private static final String COL3 = "AGE";
  private static final String COL4 = "SALARY";

  final static int tabMaxVersions = 3;
  final static int excessVersions = 5; // For Roll Over of Versions
  final static int numBuckets = 3; // Number of Buckets
  final static int numOfEntries = 15; // Number of Records

  final static int startEntry = 2;
  final static int stopEntry = 46;
  final static int maxSz = 3; // Padding Size

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

  private void insertRows(MTable mtable) {

    Random random = new Random(0L);

    int maxInputVersions = tabMaxVersions + excessVersions;
    for (int ver = 1; ver <= maxInputVersions; ver++) {
      for (int keyIndex = 0; keyIndex < numOfEntries; keyIndex++) {
        Put myput1 = new Put(Bytes.toBytes("row-" + padWithZero(keyIndex, maxSz)));
        // System.out.println("Current VERSION = " + ver);
        myput1.setTimeStamp(ver);
        // myput1.setTimeStamp(maxInputVersions - ver);
        // myput1.setTimeStamp(0);
        int m = keyIndex % maxInputVersions;
        if ((m == 0) && (ver == 1)) {
          myput1.addColumn(COL1, "dummy-val"); // Name
          myput1.addColumn(COL2, keyIndex); // ID
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

    outputStr.append("KEY").append(" : ").append(Bytes.toString(key)).append("\t")
        .append("VERSION_ID").append(" : ").append(timestamp).append("\t");

    cells.forEach(cell -> {
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
  public void timestampFilterOnMultiversionTable() {
    String tableName = getTestMethodName();
    MTable mtable = createTable(tableName);
    insertRows(mtable);

    System.out.println("\n---------------ALL VERSIONS SCAN [ RECENT -> OLDER ] -----------\n");
    Scan scan = new Scan();
    // scan.setReversed(true);
    // scan.setMaxVersions(tabMaxVersions, false);
    scan.setMaxVersions();
    Scanner scanner = mtable.getScanner(scan);
    Iterator<Row> itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        rowFormatter(res.getRowId(), k, v.getCells());
      });
      System.out.println();
    }
    System.out.println("----------------------------------------------------\n");

    System.out.println("------------------------TS >3 ----------------------\n");
    scan.setMaxVersions(tabMaxVersions, false);
    scan.setFilterOnLatestVersionOnly(false);
    scan.setFilter(new TimestampFilter(CompareOp.GREATER, 3l));
    scanner = mtable.getScanner(scan);
    itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        Assert.assertTrue("Timestamp must be greater than 3 ", k > 3);
        Assert.assertTrue("Timestamp must be greater than 3 ", v.getTimestamp() > 3);
        rowFormatter(res.getRowId(), k, v.getCells());
      });
      System.out.println();
    }
    System.out.println("----------------------------------------------------\n");

    System.out.println("------------------------TS >3 ----------------------\n");
    scan.setMaxVersions(tabMaxVersions, true);
    scan.setFilterOnLatestVersionOnly(false);
    scan.setFilter(new TimestampFilter(CompareOp.GREATER, 3l));
    scanner = mtable.getScanner(scan);
    itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        Assert.assertTrue("Timestamp must be greater than 3 ", k > 3);
        Assert.assertTrue("Timestamp must be greater than 3 ", v.getTimestamp() > 3);
        rowFormatter(res.getRowId(), k, v.getCells());
      });
      System.out.println();
    }
    System.out.println("----------------------------------------------------\n");

    System.out.println("------------------------AGE >5 ----------------------\n");
    scan.setMaxVersions(tabMaxVersions, false);
    scan.setFilterOnLatestVersionOnly(false);
    scan.setFilter(new SingleColumnValueFilter("AGE", CompareOp.GREATER, 5));
    scanner = mtable.getScanner(scan);
    itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        List<Cell> cells = v.getCells();
        Cell cell = cells.get(2);
        // System.out.println(
        // "ScanWithMaxVersionsAndOrderCombinationDUnit.timestampFilterOnMultiversionTable :: 247
        // name "
        // + Bytes.toString(cell.getColumnName()));
        Assert.assertTrue("Age must be greater than 5 ", ((int) cell.getColumnValue()) > 5);
        rowFormatter(res.getRowId(), k, cells);
      });
      System.out.println();
    }
    System.out.println("----------------------------------------------------\n");

    System.out.println("------------------------AGE >5 ----------------------\n");
    scan.setMaxVersions(tabMaxVersions, true);
    scan.setFilterOnLatestVersionOnly(false);
    scan.setFilter(new SingleColumnValueFilter("AGE", CompareOp.GREATER, 5));
    scanner = mtable.getScanner(scan);
    itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        List<Cell> cells = v.getCells();
        Cell cell = cells.get(2);
        // System.out.println(
        // "ScanWithMaxVersionsAndOrderCombinationDUnit.timestampFilterOnMultiversionTable :: 247
        // name "
        // + Bytes.toString(cell.getColumnName()));
        Assert.assertTrue("Age must be greater than 5 ", ((int) cell.getColumnValue()) > 5);
        rowFormatter(res.getRowId(), k, cells);
      });
      System.out.println();
    }
    System.out.println("----------------------------------------------------\n");

    System.out.println("----------------------AGE == 4-------------------\n");

    Scan scanObj = new Scan();
    scanObj.addColumn(Bytes.toBytes("AGE"));

    scanObj.setMaxVersions(tabMaxVersions, false);
    scanObj.setFilterOnLatestVersionOnly(false);
    scanObj.setFilter(new TimestampFilter(CompareOp.EQUAL, 4l));

    long sum0 = 0L;
    long sumSq0 = 0L;
    long rowCount0 = 0L;
    long minVal0 = Long.MAX_VALUE;
    long maxVal0 = Long.MIN_VALUE;

    // Use the same Scanner Object as Co-Processor.

    scanObj.setBatchSize(10);

    Scanner results = mtable.getScanner(scanObj);
    Iterator itr0 = results.iterator();

    int count = 0;
    // Iterate Over all Records
    while (itr0.hasNext()) {
      long val = 0L;
      Row res = (Row) itr0.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      allVersions.forEach((k, v) -> {
        Assert.assertTrue("Timestamp must be equal to 4 ", k == 4);
        Assert.assertTrue("Timestamp must be equal to 4 ", v.getTimestamp() == 4);
        rowFormatter(res.getRowId(), k, v.getCells());
      });
      System.out.println();
    }
    System.out.println("----------------------------------------------------\n");

  }

  @Test
  public void timestampFilterOnMultiversionTableServerSide() {
    String tableName = getTestMethodName();
    MTable mtable = createTable(tableName);
    insertRows(mtable);

    Function<MTable, List<Row>> executeScan =
        (Function<MTable, List<Row>> & Serializable) mTable -> {
          System.out.println("----------------------AGE == 4-------------------\n");
          Scan scanObj = new Scan();
          scanObj.addColumn(Bytes.toBytes("AGE"));

          scanObj.setMaxVersions(tabMaxVersions, false);
          scanObj.setFilterOnLatestVersionOnly(false);
          scanObj.setFilter(new TimestampFilter(CompareOp.EQUAL, 4l));

          Scanner results = mTable.getScanner(scanObj);
          Iterator iterator = results.iterator();
          List<Row> rows = new ArrayList<>();
          // Iterate Over all Records
          while (iterator.hasNext()) {
            Row res = (Row) iterator.next();
            Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
            allVersions.forEach((k, v) -> {
              rowFormatter(res.getRowId(), k, v.getCells());
              Assert.assertTrue("Timestamp must be equal to 4 ", k == 4);
              Assert.assertTrue("Timestamp must be equal to 4 ", v.getTimestamp() == 4);
            });
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

    List<Row> rows0 = vm0.invoke("Server1", serializableCallable);
    List<Row> rows1 = vm1.invoke("Server2", serializableCallable);
    List<Row> rows2 = vm2.invoke("Server3", serializableCallable);

    List<Row> rows = new ArrayList<Row>();
    rows.addAll(rows0);
    rows.addAll(rows1);
    rows.addAll(rows2);

    List<Row> rowList = executeScan.apply(mtable);

    System.out.println(
        "ScanWithMaxVersionsAndOrderCombinationDUnit.timestampFilterOnMultiversionTableServerSide :: 419 clientRowCount "
            + rowList.size() + " server row count " + rows.size());

    Assert.assertEquals(rowList.size(), rows.size());


  }


}
