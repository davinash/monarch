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

import java.sql.Date;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableColumnType;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Pair;
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
import io.ampool.monarch.types.BasicTypes;
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
public class FTableTruncateDUnitTest extends MTableDUnitHelper {
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

  protected MTable createMTable(MTableType tableType, String tableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor(tableType);
    tableDescriptor.setTotalNumOfSplits(2);
    tableDescriptor.setRedundantCopies(1);

    // set version for ordered table
    if (tableType == MTableType.ORDERED_VERSIONED) {
      tableDescriptor.setMaxVersions(3);
    }

    // add columns
    for (int colmnIndex = 0; colmnIndex < NUM_COLS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes("COL_" + colmnIndex));
    }

    // create table
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createMTable(tableName, tableDescriptor);
    assertEquals(table.getName(), tableName);
    assertNotNull(table);

    return table;
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
      itr.next();
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
   * Truncate whole block
   */
  @Test
  public void testTruncateFullBlockWithInsertionTimeFilter() throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    long start = TimestampUtil.getCurrentTime();
    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    Thread.sleep(1000l);
    long end = TimestampUtil.getCurrentTime();

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_AFTER" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    // truncate the table
    Filter filter = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).truncateFTable(tableName,
          filter);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 1000);
  }

  /**
   * Truncate from beginning of the block
   */
  @Test
  public void testTruncatePartialBlockBeginWithInsertionTimeFilter() throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    long start = TimestampUtil.getCurrentTime();
    for (int j = 0; j < 500; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }
    Thread.sleep(1000l);
    long end = TimestampUtil.getCurrentTime();

    for (int j = 0; j < 1500; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    // truncate the table
    Filter filter = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).truncateFTable(tableName,
          filter);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 1500);
  }

  @Test
  public void testTruncatePartialBlockMiddleWithInsertionTimeFilter() throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);

    for (int j = 0; j < 500; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    Thread.sleep(1000l);
    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);


    for (int j = 0; j < 250; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    Thread.sleep(1000l);
    long end = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);

    for (int j = 0; j < 1250; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    // truncate the table
    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    FilterList filterList = new FilterList();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ALL);

    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).truncateFTable(tableName,
          filterList);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 1750);
  }

  @Test
  public void testTruncatePartialBlockEndWithInsertionTimeFilter() throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);

    for (int j = 0; j < 500; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    Thread.sleep(1000l);
    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);


    for (int j = 0; j < 500; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    Thread.sleep(1000l);
    long end = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    // truncate the table
    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    FilterList filterList = new FilterList();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ALL);

    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).truncateFTable(tableName,
          filterList);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 1500);
    verifyRecordCountOnServers(tableName, 1500);
  }

  /**
   * multiple ranges deleted from a block i.e multiple holes in block
   */
  @Test
  public void testTruncatePartialBlockMultiRangeWithInsertionTimeFilter()
      throws InterruptedException {
    String tableName = getMethodName();
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);

    for (int j = 0; j < 300; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    Thread.sleep(1);
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
    Thread.sleep(1);

    for (int j = 0; j < 251; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    long start1 = TimestampUtil.getCurrentTime();
    Thread.sleep(1);
    for (int j = 0; j < 249; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }
    Thread.sleep(1);
    long end1 = TimestampUtil.getCurrentTime();
    Thread.sleep(1);

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }
    Thread.sleep(1000l);
    verifyRecordCountOnClient(tableName, 2000);
    verifyRecordCountOnServers(tableName, 2000);

    // truncate the table
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

    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).truncateFTable(tableName,
          filterList);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 1551);
    verifyRecordCountOnServers(tableName, 1551);
  }

  /**
   * Verify that the updated block is replicated to secondary and the update happens only on primary
   */
  @Test
  public void testTruncatePartialBlockEndMultiNodeSingleBlock() throws InterruptedException {
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

    Thread.sleep(1000l);
    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);

    for (int j = 0; j < 601; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    Thread.sleep(1000l);
    long end = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);


    verifyRecordCountOnClient(tableName, 1000);

    // truncate the table
    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    FilterList filterList = new FilterList();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ALL);
    verifyRecordCountOnServers(tableName, 1000);

    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).truncateFTable(tableName,
          filterList);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 399);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    verifyRecordCountOnServers(tableName, 399);
    verifyRecordCountOnClient(tableName, 399);
  }


  /**
   * Verify that the updated block is (multiple blocks of data, with each buckcet having one block)
   * replicated to secondary and the update happens only on primary
   */
  @Test
  public void testTruncatePartialBlockEndMultiNodeMultiBlocks() throws InterruptedException {
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

    Thread.sleep(1000l);
    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);


    for (int j = 0; j < 601; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    Thread.sleep(1000l);
    long end = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE_CHANGE" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    // truncate the table
    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    FilterList filterList = new FilterList();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ALL);
    verifyRecordCountOnServers(tableName, 2000);

    Thread.sleep(1000l);

    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).truncateFTable(tableName,
          filterList);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 1399);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    verifyRecordCountOnServers(tableName, 1399);
    verifyRecordCountOnClient(tableName, 1399);
  }

  /**
   * Verify that the updated block is (multiple blocks of data, with each bucket having multiple
   * blocks) replicated to secondary and the update happens only on primary
   */
  @Test
  public void testTruncatePartialBlockEndMultiNodeMultiBlocksInSingleBucket()
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

    Thread.sleep(1000l);
    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);

    for (int j = 0; j < 601; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }
    Thread.sleep(1000l);
    long end = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    // truncate the table
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

    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).truncateFTable(tableName,
          filterList);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 1399);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    verifyRecordCountOnServers(tableName, 1399);
    verifyRecordCountOnClient(tableName, 1399);
  }



  /*
   * Create MTable and FTable Pass MTable to truncateF and vice-versa The api's should fail with
   * exception
   */

  @Test
  public void testMandFTableTruncatewithWrongTypes() {
    MTable table = createMTable(MTableType.ORDERED_VERSIONED, "ORDERED");
    Exception e = null;
    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).truncateFTable(table.getName(),
          null);
    } catch (Exception e1) {
      e = e1;
    }
    assertNotNull(e);
    assertEquals(e.getMessage(), "Table " + table.getName() + " is not an Immutable Table");


    MTable unordered = createMTable(MTableType.UNORDERED, "UNORDERED");
    Exception une = null;
    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin())
          .truncateFTable(unordered.getName(), null);
    } catch (Exception e1) {
      une = e1;
    }
    assertNotNull(une);
    assertEquals(une.getMessage(), "Table " + unordered.getName() + " is not an Immutable Table");

    FTable ftable = createFtable("FTable");

    Exception fe = null;
    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).truncateMTable(ftable.getName(),
          null, false);
    } catch (Exception e1) {
      fe = e1;
    }
    assertNotNull(fe);
    assertEquals(fe.getMessage(), "Table " + ftable.getName() + " is not a MTable");

    Exception fe1 = null;
    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin())
          .truncateMTable(ftable.getName());
    } catch (Exception e1) {
      fe1 = e1;
    }
    assertNotNull(fe);
    assertEquals(fe1.getMessage(), "Table " + ftable.getName() + " is not a MTable");

    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).deleteMTable(table.getName());
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin())
          .deleteMTable(unordered.getName());
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).deleteFTable(ftable.getName());
    } catch (Exception e1) {
      e.printStackTrace();
    }

  }


  /**
   * Verify that the changes are persisted on all nodes and server recovery has no issues
   */
  @Test
  public void testTruncateFullBlockMultiNodeWithRecovery() throws InterruptedException {
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

    Thread.sleep(1000l);
    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);


    for (int j = 0; j < 601; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    Thread.sleep(1000l);
    long end = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    // truncate the table
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

    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).truncateFTable(tableName,
          filterList);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 1399);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    verifyRecordCountOnServers(tableName, 1399);
    verifyRecordCountOnClient(tableName, 1399);

    // restart all servers
    for (VM vm : allServers) {
      stopServerOn(vm);
    }
    for (VM vm : allServers) {
      asyncStartServerOn(vm, DUnitLauncher.getLocatorString());
    }
    Thread.sleep(5000l);

    verifyRecordCountOnClient(tableName, 1399);
    verifyRecordCountOnServers(tableName, 1399);
  }

  /**
   * Test with any column value filter
   */
  @Test
  public void testTruncateWithColumnValueFilter() {
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

    // truncate the table
    Filter filter = new SingleColumnValueFilter("COL1", CompareOp.EQUAL, 1);

    verifyRecordCountOnServers(tableName, 2000);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }

    try {
      MClientCacheFactory.getAnyInstance().getAdmin().truncateFTable(tableName, filter);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 1999);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    verifyRecordCountOnServers(tableName, 1999);
    verifyRecordCountOnClient(tableName, 1999);
  }

  /**
   * Most of the above tests put data in a single bucket(same records hash) this will verify the
   * functionality on multi bucket case.
   */
  @Test
  public void testTruncatePartialBlockMultiNodeMultiBucket() throws InterruptedException {
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

    Thread.sleep(1000l);
    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);


    for (int j = 0; j < 601; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + r.nextInt(i + 1)));
      }
      table.append(record);
    }

    Thread.sleep(1000l);
    long end = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE_CHANGE" + r.nextInt(i + 1)));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    // truncate the table
    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    FilterList filterList = new FilterList();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ALL);
    verifyRecordCountOnServers(tableName, 2000);

    Thread.sleep(1000l);

    try {
      MClientCacheFactory.getAnyInstance().getAdmin().truncateFTable(tableName, filterList);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 1399);
    Thread.sleep(1000l);

    // TODO: check
    verifyRecordCountOnServers(tableName, 1399);
    verifyRecordCountOnClient(tableName, 1399);
  }

  /**
   * Most of the above tests put data in a single bucket(same records hash) this will verify the
   * functionality on multi bucket case with restart.
   */
  // TODO @Test
  public void testTruncatePartialBlockMultiNodeMultiBucketWithRestart()
      throws InterruptedException {
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

    Thread.sleep(1000l);
    long start = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);


    for (int j = 0; j < 601; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + r.nextInt(i + 1)));
      }
      table.append(record);
    }

    Thread.sleep(1000l);
    long end = TimestampUtil.getCurrentTime();
    Thread.sleep(1000l);

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE_CHANGE" + r.nextInt(i + 1)));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);

    // truncate the table
    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, end);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, start);

    FilterList filterList = new FilterList();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    filterList.setOperator(FilterList.Operator.MUST_PASS_ALL);
    verifyRecordCountOnServers(tableName, 2000);

    Thread.sleep(1000l);

    try {
      MClientCacheFactory.getAnyInstance().getAdmin().truncateFTable(tableName, filterList);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);

    verifyRecordCountOnClient(tableName, 1399);
    Thread.sleep(1000l);
    // TODO: check
    // verifyRecordCountOnServers(tableName, 1399);
    verifyRecordCountOnClient(tableName, 1399);

    for (VM vm : allServers) {
      stopServerOn(vm);
    }

    Collections.reverse(allServers);
    for (VM vm : allServers) {
      startServerOn(vm, DUnitLauncher.getLocatorString());
    }
    Collections.reverse(allServers);

    // TODO: check
    // verifyRecordCountOnServers(tableName, 1399);
    verifyRecordCountOnClient(tableName, 1399);
  }

  @Test
  public void testTruncateWithColumnValueFilter2() {
    String tableName = getMethodName();
    Pair<String, BasicTypes>[] schema =
        new Pair[] {new Pair("NAME", BasicTypes.STRING), new Pair("ID", BasicTypes.STRING),
            new Pair("AGE", BasicTypes.INT), new Pair("SALARY", BasicTypes.LONG),
            new Pair("DEPT", BasicTypes.STRING), new Pair("DOJ", BasicTypes.DATE),};
    Long startTimeStamp;

    Integer blockSize = 10;
    Integer numBuckets = 4;

    Integer partitionColIdx = 1;
    Integer numOfEntries = 20;
    Integer repeatDataLoop = 2;

    FTableDescriptor ftd = new FTableDescriptor();
    for (Pair<String, BasicTypes> colSchema : schema) {
      ftd.addColumn(colSchema.getFirst(), colSchema.getSecond());
    }


    ftd.setTotalNumOfSplits(numBuckets);
    ftd.setBlockSize(blockSize);
    ftd.setBlockFormat(blockFormat);

    ftd.setPartitioningColumn(Bytes.toBytes(schema[partitionColIdx].getFirst()));

    FTable fTable = MClientCacheFactory.getAnyInstance().getAdmin().createFTable(tableName, ftd);

    startTimeStamp = System.currentTimeMillis();
    appendRecords(schema, numBuckets, numOfEntries, repeatDataLoop, fTable);

    long startTimeStamp2 = System.currentTimeMillis();

    scanRecordsAfterTS(startTimeStamp, numBuckets, fTable, 40);

    appendRecords(schema, numBuckets, numOfEntries, repeatDataLoop, fTable);

    scanRecordsAfterTS(startTimeStamp, numBuckets, fTable, 80);

    deleteRecords2(tableName, "AGE", 11);

    scanRecordsAfterTS(startTimeStamp, numBuckets, fTable, 20);
    scanRecordsAfterTS(startTimeStamp2, numBuckets, fTable, 10);
  }

  public void deleteRecords2(String tableName, String colName, int min) {
    Filter filter1;
    filter1 = new SingleColumnValueFilter(colName, CompareOp.GREATER_OR_EQUAL, min);
    try {
      MClientCacheFactory.getAnyInstance().getAdmin().truncateFTable(tableName, filter1);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void scanRecordsAfterTS(Long startTimeStamp, Integer numBuckets, FTable fTable,
      int expRecords) {
    Scan scan = new Scan();
    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, startTimeStamp);
    scan.setFilter(filter1);

    Scanner scanner = fTable.getScanner(scan);
    Iterator<Row> iterator = scanner.iterator();

    int recordCount = 0;

    while (iterator.hasNext()) {
      recordCount++;
      iterator.next();
    }
    assertEquals(expRecords, recordCount);
  }

  public void appendRecords(Pair<String, BasicTypes>[] schema, Integer numBuckets,
      Integer numOfEntries, Integer repeatDataLoop, FTable fTable) {
    int batchAppendSize = numOfEntries / 2;

    Record[] batchRecords = new Record[batchAppendSize];
    int count = 0;

    int stLoop1 = 0;
    int endLoop1 = stLoop1 + batchAppendSize;
    // endLoop1 = stLoop1;
    int stLoop2 = endLoop1;
    int endLoop2 = numOfEntries;

    for (int r = 0; r < repeatDataLoop; r++) {
      int base = (r + 1) * 100;

      // Ingest records using Batch Append
      for (int i = stLoop1; i < endLoop1; i++) {
        int idVal = base + i;
        count++;
        int age = count;
        long sal = 10000 + count;

        Record record = new Record();
        record.add(schema[0].getFirst(), "NAME" + i); // NAME
        record.add(schema[1].getFirst(), "ID" + idVal); // ID
        record.add(schema[2].getFirst(), age); // AGE
        record.add(schema[3].getFirst(), sal); // SALARY
        record.add(schema[4].getFirst(), "DEPT"); // DEPT
        record.add(schema[5].getFirst(), new Date(System.currentTimeMillis())); // DOJ
        batchRecords[i] = record;
      }

      if ((endLoop1 - stLoop1) > 0)
        fTable.append(batchRecords);

      // Ingest records using Append
      for (int i = stLoop2; i < endLoop2; i++) {
        int idVal = base + i;
        count++;
        int age = count;
        long sal = 10000 + count;
        Record record = new Record();
        record.add(schema[0].getFirst(), "NAME_" + (i + 1)); // NAME
        record.add(schema[1].getFirst(), "ID" + idVal); // ID
        record.add(schema[2].getFirst(), age); // AGE
        record.add(schema[3].getFirst(), sal); // SALARY

        record.add(schema[4].getFirst(), "DEPT"); // DEPT
        record.add(schema[5].getFirst(), new Date(System.currentTimeMillis())); // DOJ

        fTable.append(record);
      }
    }
  }
}
