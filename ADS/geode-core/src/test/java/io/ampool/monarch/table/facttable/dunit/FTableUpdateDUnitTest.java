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
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTableColumnType;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.AdminImpl;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.CompareOp;
import io.ampool.utils.TimestampUtil;
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

@Category(FTableTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class FTableUpdateDUnitTest extends MTableDUnitHelper {
  private static final int NUM_COLS = 10;
  List<VM> allServers = null;

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
    createClientCache(client1);
    createClientCache();
    allServers = Arrays.asList(vm0, vm1, vm2);
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);
    allServers.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    }));

    super.tearDown2();
  }

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

  private FTable createFtable(String tableName) {
    FTableDescriptor fd = new FTableDescriptor();
    fd.setBlockSize(1000);

    for (int i = 0; i < NUM_COLS; i++) {
      fd.addColumn("COL_" + i);
    }

    fd.setRedundantCopies(2);
    fd.setBlockFormat(blockFormat);

    FTable ftable = MClientCacheFactory.getAnyInstance().getAdmin().createFTable(tableName, fd);

    /*
     * Make sure tbale is created
     */
    checkTableOnServers(tableName);

    return ftable;
  }

  private void checkTableOnServers(String tableName) {
    for (VM vm : allServers) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          TableDescriptor td = MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
          assertNotNull(td);
          return null;
        }
      });
    }
  }

  private void verifyRecordCountOnClient(String tableName, int expCount) {
    FTable table = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    int actualCount = 0;
    while (itr.hasNext()) {
      Row row = (Row) itr.next();
      List<Cell> cells = row.getCells();
      // StringBuilder builder = new StringBuilder(1024);
      // for (Cell cell : cells) {
      // builder.append(Bytes.toString(cell.getColumnName()) + " : ");
      // if (cell.getColumnValue() instanceof byte[]) {
      // builder.append(Bytes.toString((byte[]) cell.getColumnValue()) + " : ");
      // } else {
      // builder.append(cell.getColumnValue());
      // }
      // }
      // System.out.println("ROW: " + builder.toString());
      actualCount++;
    }
    assertEquals(expCount, actualCount);
  }

  private void verifyRecordCountOnServers(String tableName, int expectedCount) {
    for (VM vm : allServers) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          int actualCount = 0;
          MCache cache = MCacheFactory.getAnyInstance();
          TableDescriptor td = MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
          int numSplits = td.getTotalNumOfSplits();
          PartitionedRegion pr = (PartitionedRegion) cache.getRegion(tableName);
          for (int i = 0; i < numSplits; i++) {
            BucketRegion br = pr.getDataStore().getLocalBucketById(i);
            if (br != null) {
              RowTupleConcurrentSkipListMap internalMap =
                  (RowTupleConcurrentSkipListMap) br.getRegionMap().getInternalMap();
              Map realMap = internalMap.getInternalMap();
              Iterator<Map.Entry<IMKey, RegionEntry>> itr = realMap.entrySet().iterator();
              while (itr.hasNext()) {
                Map.Entry<IMKey, RegionEntry> entry = itr.next();
                Object value = entry.getValue()._getValue();
                if (value == null || Token.isInvalidOrRemoved(value)) {
                  continue;
                }
                if (value instanceof VMCachedDeserializable) {
                  value = ((VMCachedDeserializable) value).getDeserializedForReading();
                }
                BlockValue blockValue = (BlockValue) value;
                actualCount += blockValue.getCurrentIndex();
              }
            }
          }
          assertEquals(expectedCount, actualCount);
          return null;
        }
      });
    }
  }

  /**
   * Update whole block
   */
  @Test
  public void testUpdateFullBlockWithInsertionTimeFilter() throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1);
    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }
    Thread.sleep(1);
    long end = TimestampUtil.getCurrentTime();

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    Filter filter = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Map<byte[], Object> newColValues = new LinkedHashMap<>();
    newColValues.put(Bytes.toBytes("COL_0"), Bytes.toBytes("UPDATED_COL0"));
    newColValues.put(Bytes.toBytes("COL_9"), Bytes.toBytes("UPDATED_COL9"));
    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).updateFTable(tableName, filter,
          newColValues);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 2000);
    verifyUpdatedRowsOnClient(table, filter, 1000);
    verifyNonUpdatedRowsOnClient(table, new SingleColumnValueFilter(
        FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, CompareOp.GREATER_OR_EQUAL, end), 1000);
  }

  private void verifyNonUpdatedRowsOnClient(FTable table, Filter filter, int expectedCount) {
    Scan scan = new Scan();
    scan.setFilter(filter);
    Scanner scanner = table.getScanner(scan);
    int actualCount = 0;
    while (true) {
      Row row = scanner.next();
      if (row == null) {
        break;
      }
      actualCount++;
      List<Cell> cells = row.getCells();
      for (Cell cell : cells) {
        if (Bytes.compareTo(cell.getColumnName(), Bytes.toBytes("COL_0")) == 0) {
          assertEquals(0,
              Bytes.compareTo((byte[]) cell.getColumnValue(), Bytes.toBytes("COL_AFTER0")));
        }
        if (Bytes.compareTo(cell.getColumnName(), Bytes.toBytes("COL_9")) == 0) {
          assertEquals(0,
              Bytes.compareTo((byte[]) cell.getColumnValue(), Bytes.toBytes("COL_AFTER9")));
        }
      }
    }
    scanner.close();
    assertEquals(expectedCount, actualCount);
  }

  private void verifyUpdatedRowsOnClient(FTable table, Filter filter, int expectedCount) {
    Scan scan = new Scan();
    scan.setFilter(filter);
    Scanner scanner = table.getScanner(scan);
    int actualCount = 0;
    while (true) {
      Row row = scanner.next();
      if (row == null) {
        break;
      }
      actualCount++;
      List<Cell> cells = row.getCells();
      for (Cell cell : cells) {
        if (Bytes.compareTo(cell.getColumnName(), Bytes.toBytes("COL_0")) == 0) {
          assertEquals("Incorrect value for COL_0.", "UPDATED_COL0",
              Bytes.toString((byte[]) cell.getColumnValue()));
        }
        if (Bytes.compareTo(cell.getColumnName(), Bytes.toBytes("COL_9")) == 0) {
          assertEquals("Incorrect value for COL_9.", "UPDATED_COL9",
              Bytes.toString((byte[]) cell.getColumnValue()));
        }
      }
    }
    scanner.close();
    assertEquals(expectedCount, actualCount);
  }

  /**
   * Update from beginning of the block
   */
  @Test
  public void testUpdatePartialBlockBeginWithInsertionTimeFilter() throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1);
    for (int j = 0; j < 500; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }
    Thread.sleep(1);
    long end = TimestampUtil.getCurrentTime();

    for (int j = 0; j < 1500; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    Filter filter = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Map<byte[], Object> newColValues = new LinkedHashMap<>();
    newColValues.put(Bytes.toBytes("COL_0"), Bytes.toBytes("UPDATED_COL0"));
    newColValues.put(Bytes.toBytes("COL_9"), Bytes.toBytes("UPDATED_COL9"));
    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).updateFTable(tableName, filter,
          newColValues);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 2000);
    verifyUpdatedRowsOnClient(table, filter, 500);
    verifyNonUpdatedRowsOnClient(table, new SingleColumnValueFilter(
        FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, CompareOp.GREATER_OR_EQUAL, end), 1500);
  }

  @Test
  public void testUpdatePartialBlockMiddleWithInsertionTimeFilter() throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);

    for (int j = 0; j < 500; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1);

    for (int j = 0; j < 250; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    Thread.sleep(1);
    long end = TimestampUtil.getCurrentTime();

    for (int j = 0; j < 1250; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    FilterList filterList = new FilterList();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ALL);
    Map<byte[], Object> newColValues = new LinkedHashMap<>();
    newColValues.put(Bytes.toBytes("COL_0"), Bytes.toBytes("UPDATED_COL0"));
    newColValues.put(Bytes.toBytes("COL_9"), Bytes.toBytes("UPDATED_COL9"));
    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).updateFTable(tableName,
          filterList, newColValues);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 2000);
    verifyUpdatedRowsOnClient(table, filterList, 250);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ONE);
    verifyNonUpdatedRowsOnClient(table, new SingleColumnValueFilter(
        FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, CompareOp.GREATER_OR_EQUAL, end), 1250);
    verifyNonUpdatedRowsOnClient(table, new SingleColumnValueFilter(
        FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, CompareOp.LESS_OR_EQUAL, start), 500);
  }

  @Test
  public void testUpdatePartialBlockEndWithInsertionTimeFilter() throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);

    for (int j = 0; j < 500; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1);

    for (int j = 0; j < 500; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    Thread.sleep(1);
    long end = TimestampUtil.getCurrentTime();

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    FilterList filterList = new FilterList();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ALL);
    Map<byte[], Object> newColValues = new LinkedHashMap<>();
    newColValues.put(Bytes.toBytes("COL_0"), Bytes.toBytes("UPDATED_COL0"));
    newColValues.put(Bytes.toBytes("COL_9"), Bytes.toBytes("UPDATED_COL9"));
    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).updateFTable(tableName,
          filterList, newColValues);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 2000);
    verifyRecordCountOnServers(tableName, 2000);
    verifyUpdatedRowsOnClient(table, filterList, 500);
    verifyNonUpdatedRowsOnClient(table, new SingleColumnValueFilter(
        FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, CompareOp.GREATER_OR_EQUAL, end), 1000);
    verifyNonUpdatedRowsOnClient(table, new SingleColumnValueFilter(
        FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, CompareOp.LESS_OR_EQUAL, start), 500);
  }

  /**
   * multiple ranges deleted from a block
   */
  @Test
  public void testUpdatePartialBlockMultiRangeWithInsertionTimeFilter()
      throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);

    for (int j = 0; j < 300; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1);

    for (int j = 0; j < 200; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    Thread.sleep(1);
    long end = TimestampUtil.getCurrentTime();

    for (int j = 0; j < 251; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    long start1 = TimestampUtil.getCurrentTime();
    Thread.sleep(1);
    for (int j = 0; j < 249; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }
    Thread.sleep(1);
    long end1 = TimestampUtil.getCurrentTime();


    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);
    verifyRecordCountOnServers(tableName, 2000);

    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    Filter filter3 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end1);
    Filter filter4 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start1);

    FilterList filterList1 = new FilterList();
    FilterList filterList2 = new FilterList();

    filterList1.addFilter(filter1);
    filterList1.addFilter(filter2);
    filterList1.setOperator(FilterList.Operator.MUST_PASS_ALL);

    filterList2.addFilter(filter3);
    filterList2.addFilter(filter4);
    filterList2.setOperator(FilterList.Operator.MUST_PASS_ALL);

    FilterList filterList = new FilterList();
    filterList.addFilter(filterList1);
    filterList.addFilter(filterList2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ONE);

    Map<byte[], Object> newColValues = new LinkedHashMap<>();
    newColValues.put(Bytes.toBytes("COL_0"), Bytes.toBytes("UPDATED_COL0"));
    newColValues.put(Bytes.toBytes("COL_9"), Bytes.toBytes("UPDATED_COL9"));

    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).updateFTable(tableName,
          filterList, newColValues);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 2000);
    verifyRecordCountOnServers(tableName, 2000);

    verifyUpdatedRowsOnClient(table, filterList, 449);

    Filter filter5 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS_OR_EQUAL, start);
    verifyNonUpdatedRowsOnClient(table, filter5, 300);

    FilterList filterList3 = new FilterList();
    filterList3.addFilter(new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS_OR_EQUAL, start1));
    filterList3.addFilter(new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER_OR_EQUAL, end));
    filterList3.setOperator(FilterList.Operator.MUST_PASS_ALL);
    verifyNonUpdatedRowsOnClient(table, filterList3, 251);
    verifyNonUpdatedRowsOnClient(table, new SingleColumnValueFilter(
        FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, CompareOp.GREATER_OR_EQUAL, end1), 1000);
  }

  /**
   * Verify that the updated block is replicated to secondary and the update happens only on primary
   */
  @Test
  public void testUpdatePartialBlockEndMultiNodeSingleBlock() throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);

    for (int j = 0; j < 399; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1);


    for (int j = 0; j < 601; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    Thread.sleep(1);
    long end = TimestampUtil.getCurrentTime();


    verifyRecordCountOnClient(tableName, 1000);

    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    FilterList filterList = new FilterList();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ALL);
    verifyRecordCountOnServers(tableName, 1000);

    Map<byte[], Object> newColValues = new LinkedHashMap<>();
    newColValues.put(Bytes.toBytes("COL_0"), Bytes.toBytes("UPDATED_COL0"));
    newColValues.put(Bytes.toBytes("COL_9"), Bytes.toBytes("UPDATED_COL9"));

    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).updateFTable(tableName,
          filterList, newColValues);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 1000);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    verifyRecordCountOnServers(tableName, 1000);
    verifyRecordCountOnClient(tableName, 1000);

    verifyUpdatedRowsOnClient(table, filterList, 601);
    verifyNonUpdatedRowsOnClient(table, new SingleColumnValueFilter(
        FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, CompareOp.LESS_OR_EQUAL, start), 399);
  }


  /**
   * Verify that the updated block is (multiple blocks of data, with each buckcet having one block)
   * replicated to secondary and the update happens only on primary
   */
  // @Test
  public void testUpdatePartialBlockEndMultiNodeMultiBlocks() throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);

    for (int j = 0; j < 399; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1);


    for (int j = 0; j < 601; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    Thread.sleep(1);
    long end = TimestampUtil.getCurrentTime();

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    FilterList filterList = new FilterList();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ALL);
    verifyRecordCountOnServers(tableName, 2000);

    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }

    Map<byte[], Object> newColValues = new LinkedHashMap<>();
    newColValues.put(Bytes.toBytes("COL_0"), Bytes.toBytes("UPDATED_COL0"));
    newColValues.put(Bytes.toBytes("COL_9"), Bytes.toBytes("UPDATED_COL9"));

    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).updateFTable(tableName,
          filterList, newColValues);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 2000);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    verifyRecordCountOnServers(tableName, 2000);
    // verifyRecordCountOnClient(tableName, 2000);

    verifyUpdatedRowsOnClient(table, filterList, 601);

    FilterList filterList1 = new FilterList();
    filterList1.addFilter(new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER_OR_EQUAL, end));
    filterList1.addFilter(new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS_OR_EQUAL, start));
    filterList1.setOperator(FilterList.Operator.MUST_PASS_ONE);
    verifyNonUpdatedRowsOnClient(table, filterList1, 1399);
  }

  /**
   * Verify that the updated block is (multiple blocks of data, with each bucket having multiple
   * blocks) replicated to secondary and the update happens only on primary
   */
  // @Test
  public void testUpdatePartialBlockEndMultiNodeMultiBlocksInSingleBucket()
      throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);

    for (int j = 0; j < 399; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1);

    for (int j = 0; j < 601; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    Thread.sleep(1);
    long end = TimestampUtil.getCurrentTime();

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    FilterList filterList = new FilterList();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ALL);
    verifyRecordCountOnServers(tableName, 2000);

    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }

    Map<byte[], Object> newColValues = new LinkedHashMap<>();
    newColValues.put(Bytes.toBytes("COL_0"), Bytes.toBytes("UPDATED_COL0"));
    newColValues.put(Bytes.toBytes("COL_9"), Bytes.toBytes("UPDATED_COL9"));

    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).updateFTable(tableName,
          filterList, newColValues);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 2000);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    verifyRecordCountOnServers(tableName, 2000);
    verifyRecordCountOnClient(tableName, 2000);
    verifyUpdatedRowsOnClient(table, filterList, 601);

    FilterList filterList1 = new FilterList();
    filterList1.addFilter(new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER_OR_EQUAL, end));
    filterList1.addFilter(new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS_OR_EQUAL, start));
    filterList1.setOperator(FilterList.Operator.MUST_PASS_ONE);
    verifyNonUpdatedRowsOnClient(table, filterList1, 1399);
  }

  /**
   * Verify that the changes are persisted on all nodes and server recovery has no issues
   */
  // @Test
  public void testUpdateFullBlockMultiNodeWithRecovery() throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);

    for (int j = 0; j < 399; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1);


    for (int j = 0; j < 601; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    Thread.sleep(1);
    long end = TimestampUtil.getCurrentTime();

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    FilterList filterList = new FilterList();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ALL);
    verifyRecordCountOnServers(tableName, 2000);

    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }

    Map<byte[], Object> newColValues = new LinkedHashMap<>();
    newColValues.put(Bytes.toBytes("COL_0"), Bytes.toBytes("UPDATED_COL0"));
    newColValues.put(Bytes.toBytes("COL_9"), Bytes.toBytes("UPDATED_COL9"));

    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).updateFTable(tableName,
          filterList, newColValues);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 2000);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    verifyRecordCountOnServers(tableName, 2000);
    verifyRecordCountOnClient(tableName, 2000);

    verifyUpdatedRowsOnClient(table, filterList, 601);

    FilterList filterList1 = new FilterList();
    filterList1.addFilter(new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER_OR_EQUAL, end));
    filterList1.addFilter(new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS_OR_EQUAL, start));
    filterList1.setOperator(FilterList.Operator.MUST_PASS_ONE);
    verifyNonUpdatedRowsOnClient(table, filterList1, 1399);


    // restart all servers
    for (VM vm : allServers) {
      stopServerOn(vm);
    }
    for (VM vm : allServers) {
      startServerOn(vm, DUnitLauncher.getLocatorString());
    }

    verifyRecordCountOnClient(tableName, 2000);
    verifyRecordCountOnServers(tableName, 2000);

    verifyUpdatedRowsOnClient(table, filterList, 601);
    verifyNonUpdatedRowsOnClient(table, filterList1, 1399);
  }

  /**
   * Test with any column value filter
   */
  @Test
  public void testUpdateWithColumnValueFilter() {
    String tableName = getMethodName();
    Exception e = null;

    MClientCache cache = MClientCacheFactory.getAnyInstance();
    FTableDescriptor ftd = new FTableDescriptor();
    ftd.addColumn("COL1", new MTableColumnType("INT"));
    ftd.setRedundantCopies(2);
    ftd.setBlockFormat(blockFormat);
    FTable table = cache.getAdmin().createFTable(tableName, ftd);

    for (int j = 0; j < 399; j++) {
      Record record = new Record();
      record.add("COL1", 0);
      table.append(record);
    }

    Record record = new Record();
    record.add("COL1", 1);
    table.append(record);

    for (int j = 0; j < 600; j++) {
      record = new Record();
      record.add("COL1", 2);
      table.append(record);
    }

    for (int j = 0; j < 1000; j++) {
      record = new Record();
      record.add("COL1", 3);
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    Filter filter = new SingleColumnValueFilter("COL1", CompareOp.EQUAL, 1);

    verifyRecordCountOnServers(tableName, 2000);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }

    Map<byte[], Object> newColValues = new LinkedHashMap<>();
    newColValues.put(Bytes.toBytes("COL1"), 10);

    try {
      MClientCacheFactory.getAnyInstance().getAdmin().updateFTable(tableName, filter, newColValues);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 2000);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    verifyRecordCountOnServers(tableName, 2000);
    verifyRecordCountOnClient(tableName, 2000);

    Scan scan = new Scan();
    scan.setFilter(filter);
    Scanner scanner = table.getScanner(scan);
    int cnt = 0;
    while (true) {
      Row row = scanner.next();
      if (row == null) {
        break;
      }
      cnt++;
    }
    assertEquals(0, cnt);
    scanner.close();

    scan.setFilter(new SingleColumnValueFilter("COL1", CompareOp.EQUAL, 10));
    scanner = table.getScanner(scan);
    cnt = 0;
    while (true) {
      Row row = scanner.next();
      if (row == null) {
        break;
      }
      cnt++;
      assertEquals(10, row.getCells().get(0).getColumnValue());
    }
    assertEquals(1, cnt);
    scanner.close();
  }

  /**
   * Most of the above tests put data in a single bucket(same records hash) this will verify the
   * functionality on multi bucket case.
   */
  @Test
  public void testUpdatePartialBlockMultiNodeMultiBucket() throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    Random r = new Random();

    for (int j = 0; j < 399; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + r.nextInt(i + 1)));
      }
      table.append(record);
    }

    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1);


    for (int j = 0; j < 601; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + r.nextInt(i + 1)));
      }
      table.append(record);
    }

    Thread.sleep(1);
    long end = TimestampUtil.getCurrentTime();

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE_CHANGE" + r.nextInt(i + 1)));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    FilterList filterList = new FilterList();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ALL);
    verifyRecordCountOnServers(tableName, 2000);

    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    Map<byte[], Object> newColValues = new LinkedHashMap<>();
    newColValues.put(Bytes.toBytes("COL_1"), Bytes.toBytes("COL_UPDATED"));

    try {
      MClientCacheFactory.getAnyInstance().getAdmin().updateFTable(tableName, filterList,
          newColValues);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 2000);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    // TODO: check
    verifyRecordCountOnServers(tableName, 2000);
    verifyRecordCountOnClient(tableName, 2000);
    Scan scan = new Scan();
    scan.setFilter(filterList);
    Scanner scanner = table.getScanner(scan);

    while (true) {
      Row row = scanner.next();
      if (row == null) {
        break;
      }
      assertEquals(0, Bytes.compareTo(Bytes.toBytes("COL_UPDATED"),
          (byte[]) row.getCells().get(1).getColumnValue()));
    }
    scanner.close();
  }
}
