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
package io.ampool.monarch.table;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class MTableDeleteWithFilterFailoverDUnitTest extends MTableDUnitHelper {
  public MTableDeleteWithFilterFailoverDUnitTest() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    super.tearDown2();
  }

  private void createTable(final int maxVersions, final MTableType mTableType) {
    Admin admin = MCacheFactory.getAnyInstance().getAdmin();
    final String tableName = "test";
    if (admin.existsMTable(tableName)) {
      admin.deleteMTable(tableName);
    }
    admin.createMTable(tableName, getMTableDescriptor(maxVersions, mTableType));
  }

  private MTableDescriptor getMTableDescriptor(final int maxVersions, final MTableType mTableType) {
    MTableDescriptor mTableDescriptor = new MTableDescriptor(mTableType);
    mTableDescriptor.setMaxVersions(maxVersions);
    mTableDescriptor.addColumn("Id", BasicTypes.INT);
    mTableDescriptor.addColumn("Name", BasicTypes.STRING);
    mTableDescriptor.addColumn("Age", BasicTypes.INT);
    mTableDescriptor.addColumn("Salary", BasicTypes.LONG);
    mTableDescriptor.setRedundantCopies(1);
    return mTableDescriptor;
  }

  private void ingestData(final String tableName, final int numRows) {
    MTable mTable = MCacheFactory.getAnyInstance().getMTable(tableName);
    for (int version = 0; version < mTable.getTableDescriptor().getMaxVersions(); version++) {
      for (int i = 0; i < numRows; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn("Id", i);
        put.addColumn("Name", "ABC" + String.valueOf(i + version));
        put.addColumn("Age", i + version);
        put.addColumn("Salary", (long) (100 + i));
        mTable.put(put);
      }
    }
  }

  private void scanAndVerifyData(final String tableName, final int expectedRows,
      final int insertedRows, boolean afterdelete) {
    MTable mTable = MCacheFactory.getAnyInstance().getMTable(tableName);
    int numRows = 0;
    Scan scan = new Scan();
    scan.setMaxVersions();
    Iterator<Row> iterator = mTable.getScanner(scan).iterator();
    while (iterator.hasNext()) {
      Row row = iterator.next();
      assertNotNull(row);
      Map<Long, SingleVersionRow> allVersions = row.getAllVersions();
      assertNotNull(allVersions);
      assertEquals(mTable.getTableDescriptor().getMaxVersions(), allVersions.size());
      numRows += allVersions.entrySet().size();
    }
    if (afterdelete) {
      assertEquals(numRows, expectedRows);
    } else {
      assertEquals(numRows, insertedRows * mTable.getTableDescriptor().getMaxVersions());
    }
  }

  private void deleteValuesWithFilter(final String tableName, final Filter filter,
      final int numRows) {
    MTable mTable = MCacheFactory.getAnyInstance().getMTable(tableName);
    Map<Delete, Filter> deleteFilterMap = new HashMap<>();
    for (int i = 0; i < numRows; i++) {
      Delete delete = new Delete(Bytes.toBytes(i));
      deleteFilterMap.put(delete, filter);
    }
    mTable.delete(deleteFilterMap);
  }

  private void deleteTable(final String tableName) {
    MCacheFactory.getAnyInstance().getAdmin().deleteMTable(tableName);
  }

  // numberofversions,tabletype, filter, numofrows, expectedrows
  private Object[] getTestData() {
    return new Object[][] {{1, MTableType.ORDERED_VERSIONED, null, 100, 100},
        {2, MTableType.ORDERED_VERSIONED, null, 100, 200},
        {5, MTableType.ORDERED_VERSIONED, null, 100, 500},
        {200, MTableType.ORDERED_VERSIONED, null, 100, 20000},
        {1, MTableType.UNORDERED, null, 100, 100}, {2, MTableType.UNORDERED, null, 100, 200},
        {5, MTableType.UNORDERED, null, 100, 500}, {200, MTableType.UNORDERED, null, 100, 20000},
        {1, MTableType.ORDERED_VERSIONED, new SingleColumnValueFilter("Id", CompareOp.LESS, 50),
            100, 50},
        {2, MTableType.ORDERED_VERSIONED, new SingleColumnValueFilter("Id", CompareOp.LESS, 50),
            100, 100},
        {5, MTableType.ORDERED_VERSIONED, new SingleColumnValueFilter("Id", CompareOp.LESS, 50),
            100, 250},
        {200, MTableType.ORDERED_VERSIONED, new SingleColumnValueFilter("Id", CompareOp.LESS, 50),
            100, 10000},
        {1, MTableType.UNORDERED, new SingleColumnValueFilter("Id", CompareOp.LESS, 50), 100, 50},
        {2, MTableType.UNORDERED, new SingleColumnValueFilter("Id", CompareOp.LESS, 50), 100, 100},
        {5, MTableType.UNORDERED, new SingleColumnValueFilter("Id", CompareOp.LESS, 50), 100, 250},
        {200, MTableType.UNORDERED, new SingleColumnValueFilter("Id", CompareOp.LESS, 50), 100,
            10000}

    };
  }

  @Test
  @Parameters(method = "getTestData")
  public void testRunDeleteWithFilter(final int maxVersions, final MTableType mTableType,
      final Filter filter, final int numRows, final int expectedRows) throws Exception {
    startServerOn(vm0, DUnitLauncher.getLocatorString());
    final String tableName = "test";
    createTable(maxVersions, mTableType);
    ingestData(tableName, numRows);
    scanAndVerifyData(tableName, expectedRows, numRows, false);
    deleteValuesWithFilter(tableName, filter, numRows);
    scanAndVerifyData(tableName, expectedRows, numRows, true);
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    Thread.sleep(10000);
    stopServerOn(vm0);
    Thread.sleep(10000);
    scanAndVerifyData(tableName, expectedRows, numRows, true);
    deleteTable(tableName);
    stopServerOn(vm1);
  }
}
