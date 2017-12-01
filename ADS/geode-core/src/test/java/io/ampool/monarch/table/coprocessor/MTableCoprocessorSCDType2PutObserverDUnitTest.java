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
package io.ampool.monarch.table.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Date;
import java.util.Iterator;
import java.util.Map;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.MKeyBase;
import io.ampool.monarch.table.internal.ProxyMTableRegion;
import io.ampool.monarch.table.internal.MultiVersionValue;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.table.internal.ThinRow;
import io.ampool.monarch.types.BasicTypes;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class MTableCoprocessorSCDType2PutObserverDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();
  private final int numRecords = 100;
  private final int numberOfVersions = 2;

  public MTableCoprocessorSCDType2PutObserverDUnitTest() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    super.tearDown2();
  }

  private Object[] getParameters() {
    long timeInMilis = System.currentTimeMillis();
    return new Object[][] {{BasicTypes.LONG, 1000l, 100l, 2000l, 200l, 1999l},
        {BasicTypes.LONG, 1000l, 100l, 2000l, null, 1999l},
        {BasicTypes.LONG, 1000l, null, 2000l, null, 1999l},
        {BasicTypes.DATE, new Date(100, 1, 1), new Date(100, 1, 1), new Date(200, 1, 1),
            new Date(100, 1, 1), new Date(101, 12, 31)},
        {BasicTypes.DATE, new Date(100, 1, 1), new Date(100, 1, 1), new Date(2002, 1, 1), null,
            new Date(101, 12, 31)},
        {BasicTypes.DATE, new Date(100, 1, 1), null, new Date(102, 1, 1), null,
            new Date(101, 12, 31)},
        {BasicTypes.STRING, "1000", "100", "2000", "100", "1999"},
        {BasicTypes.STRING, "1000", "100", "2000", null, "1999"},
        {BasicTypes.STRING, "1000", null, "2000", null, "1999"}};
  }

  /*
   * Create the table with this schema customer_id customer_name location start_date end_date
   * ------------------------------------------------------------------ 1 Marston Illions
   * 01-Mar-2010 20-Fdb-2011 1 Marston Seattle 21-Feb-2011 NULL
   * ------------------------------------------------------------------
   */
  private MTableDescriptor getTableDescriptor(final String tableName, BasicTypes dataType) {
    MTableDescriptor mTableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    mTableDescriptor.setTableName(tableName);
    mTableDescriptor.addColumn("customer_id", BasicTypes.INT);
    mTableDescriptor.addColumn("customer_name", BasicTypes.STRING);
    mTableDescriptor.addColumn("location", BasicTypes.STRING);
    mTableDescriptor.addColumn("start_date", dataType);
    mTableDescriptor.addColumn("end_date", dataType);
    mTableDescriptor.addCoprocessor("io.ampool.monarch.table.coprocessor.MTableSCDType2Observer");
    mTableDescriptor.setMaxVersions(5);
    return mTableDescriptor;
  }


  private void createMTableWithCoprocessor(final String tableName, BasicTypes dataType) {
    MClientCache anyInstance = MClientCacheFactory.getAnyInstance();
    Admin admin = anyInstance.getAdmin();
    MTable mTable = admin.createMTable(tableName, getTableDescriptor(tableName, dataType));
    assertNotNull(mTable);
    assertTrue(admin.existsMTable(tableName));
  }

  private void ingestRecords(final String tableName, Object startDate1, Object endDate1,
      Object startDate2, Object endDate2) {
    MTable mTable = MClientCacheFactory.getAnyInstance().getMTable(tableName);
    for (int i = 0; i < numRecords; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn("customer_id", i);
      put.addColumn("customer_name", "ABC" + i);
      put.addColumn("location", "Location" + i);
      put.addColumn("start_date", startDate1);
      put.addColumn("end_date", endDate1);
      mTable.put(put);
    }
    for (int i = 0; i < numRecords; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn("customer_id", i);
      put.addColumn("customer_name", "ABC" + i);
      put.addColumn("location", "Location" + i + 1);
      put.addColumn("start_date", startDate2);
      put.addColumn("end_date", endDate2);
      mTable.put(put);
    }
  }

  private void scanRecords(final String tableName, Object expectedEndDate) {
    MTable mTable = MClientCacheFactory.getAnyInstance().getMTable(tableName);
    Scan scan = new Scan();
    scan.setMaxVersions(true);
    Iterator<Row> iterator = mTable.getScanner(scan).iterator();
    while (iterator.hasNext()) {
      Row res = iterator.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      assertEquals(numberOfVersions, allVersions.size());
      allVersions.forEach((k, v) -> {
        v.getCells().forEach((C) -> System.out
            .println(Bytes.toString(C.getColumnName()) + "=>" + C.getColumnValue()));
      });
      System.out.println();
    }

    // verify end date
    while (iterator.hasNext()) {
      Row row = iterator.next();
      Cell end_date = row.getCells().stream()
          .filter(cell -> cell.getColumnName().equals("end_date")).findFirst().get();
      System.out.println("End_Date is " + end_date.getColumnValue());
      assertEquals(end_date.getColumnValue(), expectedEndDate);
    }
  }

  private void verifyValueOnServer(final String tableName, final Object expectedEndDate) {
    ProxyMTableRegion mTable =
        (ProxyMTableRegion) MCacheFactory.getAnyInstance().getMTable(tableName);
    MTableDescriptor tableDescriptor = mTable.getTableDescriptor();
    Region<Object, Object> region = mTable.getInternalRegion();
    ThinRow row = ThinRow.create(tableDescriptor, ThinRow.RowFormat.M_FULL_ROW);
    for (int i = 0; i < numRecords; i++) {
      Object key = new MKeyBase(Bytes.toBytes(i));
      MultiVersionValue multiVersionValue = (MultiVersionValue) region.get(key);
      System.out.println(multiVersionValue.getVersions().length);
      row.reset(null, multiVersionValue.getVersions()[multiVersionValue.getVersions().length - 2]);
      Iterator<Cell> iterator = row.getCells().iterator();
      while (iterator.hasNext()) {
        Cell cell = iterator.next();
        System.out.println(Bytes.toString(cell.getColumnName()) + " => " + cell.getColumnValue());
      }
    }
  }


  private void verifyValuesOnAllServers(final String tableName, Object expectedEndDate) {
    vm0.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        verifyValueOnServer(tableName, expectedEndDate);
      }
    });

    vm1.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        verifyValueOnServer(tableName, expectedEndDate);
      }
    });

    vm2.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        verifyValueOnServer(tableName, expectedEndDate);
      }
    });
  }

  @Test
  @Parameters(method = "getParameters")
  public void testSCDType2UpdateInObserver(final BasicTypes dataType, final Object startDate1,
      final Object endDate1, final Object startDate2, final Object endDate2,
      final Object expectedEndDate) {
    final String tableName = getTestMethodName();
    createMTableWithCoprocessor(tableName, dataType);
    ingestRecords(tableName, startDate1, endDate1, startDate2, endDate2);
    scanRecords(tableName, expectedEndDate);
    // verifyValuesOnAllServers(tableName, expectedEndDate);
    deleteMTable(tableName);
  }
}
