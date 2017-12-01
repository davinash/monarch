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

package io.ampool.internal.functions;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.filter.internal.TimestampFilter;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class DeleteWithFilterFunctionJUnitTest {

  private Properties createLonerProperties() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    return props;
  }


  @Before
  public void before() {
    MCache cache = new MCacheFactory(this.createLonerProperties()).create();
  }

  @After
  public void after() {
    MCacheFactory.getAnyInstance().close();
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
    mTableDescriptor.addColumn("Version", BasicTypes.INT);
    mTableDescriptor.enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS);
    return mTableDescriptor;
  }

  private void ingestData(final String tableName, final int numRows) {
    MTable mTable = MCacheFactory.getAnyInstance().getMTable(tableName);
    for (int version = 1; version <= mTable.getTableDescriptor().getMaxVersions(); version++) {
      for (int i = 0; i < numRows; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn("Id", i);
        put.addColumn("Name", "ABC" + String.valueOf(i + version));
        put.addColumn("Age", i + version);
        put.addColumn("Salary", (long) (10000 + version));
        put.addColumn("Version", version);
        put.setTimeStamp(version);
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
    Map<Delete, Filter> deleteFilterMap = new HashMap<>();
    for (int i = 0; i < numRows; i++) {
      Delete delete = new Delete(Bytes.toBytes(i));
      deleteFilterMap.put(delete, filter);
    }
    DeleteWithFilterFunction deleteWithFilterFunction = new DeleteWithFilterFunction();
    deleteWithFilterFunction.deleteWithFilter(tableName, deleteFilterMap);
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
        {1, MTableType.ORDERED_VERSIONED,
            new SingleColumnValueFilter("Id", CompareOp.GREATER_OR_EQUAL, 50), 100, 50},
        {2, MTableType.ORDERED_VERSIONED,
            new SingleColumnValueFilter("Id", CompareOp.GREATER_OR_EQUAL, 50), 100, 100},
        {5, MTableType.ORDERED_VERSIONED,
            new SingleColumnValueFilter("Id", CompareOp.GREATER_OR_EQUAL, 50), 100, 250},
        {200, MTableType.ORDERED_VERSIONED,
            new SingleColumnValueFilter("Id", CompareOp.GREATER_OR_EQUAL, 50), 100, 10000},
        {1, MTableType.ORDERED_VERSIONED,
            new SingleColumnValueFilter("Id", CompareOp.GREATER_OR_EQUAL, 0), 100, 0},
        {2, MTableType.ORDERED_VERSIONED,
            new SingleColumnValueFilter("Id", CompareOp.GREATER_OR_EQUAL, 0), 100, 0},
        {5, MTableType.ORDERED_VERSIONED,
            new SingleColumnValueFilter("Id", CompareOp.GREATER_OR_EQUAL, 0), 100, 0},
        {200, MTableType.ORDERED_VERSIONED,
            new SingleColumnValueFilter("Id", CompareOp.GREATER_OR_EQUAL, 0), 100, 0},
        {1, MTableType.ORDERED_VERSIONED, new SingleColumnValueFilter("Id", CompareOp.GREATER, 100),
            100, 100},
        {2, MTableType.ORDERED_VERSIONED, new SingleColumnValueFilter("Id", CompareOp.GREATER, 100),
            100, 200},
        {5, MTableType.ORDERED_VERSIONED, new SingleColumnValueFilter("Id", CompareOp.GREATER, 100),
            100, 500},
        {200, MTableType.ORDERED_VERSIONED,
            new SingleColumnValueFilter("Id", CompareOp.GREATER, 100), 100, 20000},
        {1, MTableType.ORDERED_VERSIONED,
            new FilterList(FilterList.Operator.MUST_PASS_ALL)
                .addFilter(new SingleColumnValueFilter("Id", CompareOp.GREATER_OR_EQUAL, 50))
                .addFilter(new SingleColumnValueFilter("Salary", CompareOp.GREATER_OR_EQUAL, 150l)),
            100, 50},
        {2, MTableType.ORDERED_VERSIONED,
            new FilterList(FilterList.Operator.MUST_PASS_ALL)
                .addFilter(new SingleColumnValueFilter("Id", CompareOp.GREATER_OR_EQUAL, 50))
                .addFilter(new SingleColumnValueFilter("Salary", CompareOp.GREATER_OR_EQUAL, 150l)),
            100, 100},
        {5, MTableType.ORDERED_VERSIONED,
            new FilterList(FilterList.Operator.MUST_PASS_ALL)
                .addFilter(new SingleColumnValueFilter("Id", CompareOp.GREATER_OR_EQUAL, 50))
                .addFilter(new SingleColumnValueFilter("Salary", CompareOp.GREATER_OR_EQUAL, 150l)),
            100, 250},
        {200, MTableType.ORDERED_VERSIONED,
            new FilterList(FilterList.Operator.MUST_PASS_ALL)
                .addFilter(new SingleColumnValueFilter("Id", CompareOp.GREATER_OR_EQUAL, 50))
                .addFilter(new SingleColumnValueFilter("Salary", CompareOp.GREATER_OR_EQUAL, 150l)),
            100, 10000},
        {1, MTableType.ORDERED_VERSIONED,
            new SingleColumnValueFilter("Version", CompareOp.EQUAL, 1), 100, 0},
        {2, MTableType.ORDERED_VERSIONED,
            new SingleColumnValueFilter("Version", CompareOp.EQUAL, 1), 100, 100},
        {5, MTableType.ORDERED_VERSIONED,
            new SingleColumnValueFilter("Version", CompareOp.EQUAL, 1), 100, 400},
        {200, MTableType.ORDERED_VERSIONED,
            new SingleColumnValueFilter("Version", CompareOp.LESS_OR_EQUAL, 100l), 100, 10000},
        //
        {1, MTableType.ORDERED_VERSIONED, new TimestampFilter(CompareOp.EQUAL, 1l), 100, 0},
        {2, MTableType.ORDERED_VERSIONED, new TimestampFilter(CompareOp.EQUAL, 1l), 100, 100},
        {5, MTableType.ORDERED_VERSIONED, new TimestampFilter(CompareOp.GREATER_OR_EQUAL, 3l), 100,
            200},
        {200, MTableType.ORDERED_VERSIONED, new TimestampFilter(CompareOp.LESS_OR_EQUAL, 100l), 100,
            10000},};
  }


  @Test
  @Parameters(method = "getTestData")
  public void testRunDeleteWithFilter(final int maxVersions, final MTableType mTableType,
      final Filter filter, final int numRows, final int expectedRows) throws Exception {
    final String tableName = "test";
    createTable(maxVersions, mTableType);
    ingestData(tableName, numRows);
    scanAndVerifyData(tableName, expectedRows, numRows, false);
    deleteValuesWithFilter(tableName, filter, numRows);
    scanAndVerifyData(tableName, expectedRows, numRows, true);
    deleteTable(tableName);
  }

  @Test
  @Parameters(method = "getTestData")
  public void testRunDeleteWithFilterWithRecovery(final int maxVersions,
      final MTableType mTableType, final Filter filter, final int numRows, final int expectedRows)
      throws Exception {
    final String tableName = "test";
    createTable(maxVersions, mTableType);
    ingestData(tableName, numRows);
    scanAndVerifyData(tableName, expectedRows, numRows, false);
    deleteValuesWithFilter(tableName, filter, numRows);
    scanAndVerifyData(tableName, expectedRows, numRows, true);
    MCacheFactory.getAnyInstance().close();

    MCache cache = new MCacheFactory(this.createLonerProperties()).create();

    scanAndVerifyData(tableName, expectedRows, numRows, true);
    deleteTable(tableName);
  }
}
