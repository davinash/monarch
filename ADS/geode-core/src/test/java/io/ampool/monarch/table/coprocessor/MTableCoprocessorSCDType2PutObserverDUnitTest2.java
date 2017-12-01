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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.Map;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class MTableCoprocessorSCDType2PutObserverDUnitTest2 extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();
  private final int numRecords = 100;

  public MTableCoprocessorSCDType2PutObserverDUnitTest2() {
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
    return new Object[][] {{1}, {2}, {3}, {4}, {5}};
  }

  /**
   * Create the table with this schema customer_id customer_name location start_date end_date
   * ------------------------------------------------------------------ 1 Marston Illions
   * 01-Mar-2010 20-Fdb-2011 1 Marston Seattle 21-Feb-2011 NULL
   * ------------------------------------------------------------------
   */
  private MTableDescriptor getTableDescriptor(final String tableName) {
    MTableDescriptor mTableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    mTableDescriptor.setTableName(tableName);
    mTableDescriptor.addColumn("customer_id", BasicTypes.INT);
    mTableDescriptor.addColumn("customer_name", BasicTypes.STRING);
    mTableDescriptor.addColumn("location", BasicTypes.STRING);
    mTableDescriptor.addColumn("start_date", BasicTypes.STRING);
    mTableDescriptor.addColumn("end_date", BasicTypes.STRING);
    mTableDescriptor.addCoprocessor("io.ampool.monarch.table.coprocessor.MTableSCDType2Observer2");
    mTableDescriptor.setMaxVersions(5);
    return mTableDescriptor;
  }


  private void createMTableWithCoprocessor(final String tableName) {
    MClientCache anyInstance = MClientCacheFactory.getAnyInstance();
    Admin admin = anyInstance.getAdmin();
    MTable mTable = admin.createMTable(tableName, getTableDescriptor(tableName));
    assertNotNull(mTable);
    assertTrue(admin.existsMTable(tableName));
  }

  private void ingestRecords(final String tableName, final int numberOfVersions) {
    MTable mTable = MClientCacheFactory.getAnyInstance().getMTable(tableName);
    for (int j = 1; j <= numberOfVersions; j++) {
      for (int i = 0; i < numRecords; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn("customer_id", i);
        put.addColumn("customer_name", "ABC" + i);
        put.addColumn("location", "Location" + (j));
        put.addColumn("start_date", String.valueOf(10000 + (j * 1000)));
        put.addColumn("end_date", null);
        put.setTimeStamp(j);
        mTable.put(put);
      }
    }
  }

  private void scanRecords(final String tableName, final int numberOfVersions) {
    MTable mTable = MClientCacheFactory.getAnyInstance().getMTable(tableName);
    Scan scan = new Scan();
    scan.setMaxVersions();
    Iterator<Row> iterator = mTable.getScanner(scan).iterator();
    int records = 0;
    while (iterator.hasNext()) {
      records++;
      Row res = iterator.next();
      Map<Long, SingleVersionRow> allVersions = res.getAllVersions();
      assertEquals(numberOfVersions, allVersions.size());
      String startdate = null;
      for (long i = numberOfVersions; i >= 1; i--) {
        SingleVersionRow singleVersionRow = allVersions.get(i);
        if (i == numberOfVersions) {
          startdate = assertlatestVersion(singleVersionRow, i);
        } else {
          String expectedEndDate = String.valueOf(Long.parseLong(startdate) - 1);
          startdate = assertNextVersion(singleVersionRow, i, expectedEndDate);
        }
        System.out.println(singleVersionRow.getCells());
      }
    }
    assertEquals(numRecords, records);
  }

  private String assertlatestVersion(SingleVersionRow singleVersionRow, long version) {
    final StringBuilder sb = new StringBuilder();
    assertNotNull(singleVersionRow);
    assertFalse(singleVersionRow.getCells().isEmpty());
    singleVersionRow.getCells().forEach((C) -> {
      if (Bytes.toString(C.getColumnName()).equals("end_date")) {
        assertNull(C.getColumnValue());
      }
      if (Bytes.toString(C.getColumnName()).equals("start_date")) {
        assertNotNull(C.getColumnValue());
        assertEquals(C.getColumnValue(), String.valueOf(10000 + (version * 1000)));
        sb.append(C.getColumnValue());
      }
    });
    return sb.toString();
  }

  private String assertNextVersion(SingleVersionRow singleVersionRow, long version,
      String expectedEndDate) {
    final StringBuilder sb = new StringBuilder();
    assertNotNull(singleVersionRow);
    assertFalse(singleVersionRow.getCells().isEmpty());
    singleVersionRow.getCells().forEach((C) -> {
      if (Bytes.toString(C.getColumnName()).equals("end_date")) {
        assertNotNull(C.getColumnValue());
        assertEquals(C.getColumnValue(), expectedEndDate);
      }
      if (Bytes.toString(C.getColumnName()).equals("start_date")) {
        assertNotNull(C.getColumnValue());
        assertEquals(C.getColumnValue(), String.valueOf(10000 + (version * 1000)));
        sb.append(C.getColumnValue());
      }
    });
    return sb.toString();
  }

  @Test
  @Parameters(method = "getParameters")
  public void testSCDType2UpdateInObserver(final int numberOfVersions) {
    final String tableName = getTestMethodName();
    createMTableWithCoprocessor(tableName);
    ingestRecords(tableName, numberOfVersions);
    scanRecords(tableName, numberOfVersions);
    deleteMTable(tableName);
  }
}
