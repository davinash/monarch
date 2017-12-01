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

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.client.coprocessor.AggregationClient;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.logging.log4j.Logger;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;

@Category(MonarchTest.class)
public class AggregationClientCPTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();
  private static final String TABLE_NAME = "EmployeeTable";
  private static final String COL1 = "NAME";
  private static final String COL2 = "ID";
  private static final String COL3 = "AGE";
  private static final String COL4 = "SALARY";

  final int numOfEntries = 10;
  final int numBuckets = 10;
  private String coprocessor = "io.ampool.monarch.table.coprocessor.impl.RowCountCoProcessor";

  public AggregationClientCPTest() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();

    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());

    createClientCache(vm3);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(this.vm3);
    super.tearDown2();
  }

  private void doPutOperationFromClient(VM vm, final int locatorPort, final boolean order) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          MTable mtable = createTable(TABLE_NAME);
          putDataInEachBucket(mtable);
          AggregationClient ac = new AggregationClient();
          long count = ac.rowCount(TABLE_NAME, new Scan());
          assertEquals(new Long(numOfEntries * numBuckets).longValue(), count);
        } catch (CacheClosedException cce) {
        }

        return null;
      }
    });

  }

  private void doPutOperationFromClientWithFilter(VM vm, final int locatorPort,
      final boolean order) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          MTable mtable = createTable(TABLE_NAME);
          Map<Integer, Pair<byte[], byte[]>> keysInRange = putDataInEachBucket(mtable);
          byte[] sampleKey1 = keysInRange.get(new Integer(1)).getFirst();
          byte[] sampleKey2 = keysInRange.get(new Integer(2)).getSecond();
          System.out.println(
              "AggregationClientCPTest.call: " + "First Key: " + Arrays.toString(sampleKey1));
          System.out.println(
              "AggregationClientCPTest.call: " + "Second Key: " + Arrays.toString(sampleKey2));
          Scan scan = new Scan(sampleKey1, sampleKey2);
          AggregationClient ac = new AggregationClient();
          long count = ac.rowCount(TABLE_NAME, scan);
          System.out.println("AggregationClientCPTest.call: " + "Filterred rows: " + count);
          assertEquals(new Long(numOfEntries * 2).longValue(), count);
        } catch (CacheClosedException cce) {
        }

        return null;
      }
    });

  }

  private Map<Integer, Pair<byte[], byte[]>> putDataInEachBucket(MTable mtable) {
    Map<Integer, Pair<byte[], byte[]>> splitsMap = MTableUtils.getUniformKeySpaceSplit(numBuckets);
    for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
      Pair<byte[], byte[]> pair = splitsMap.get(bucketId);
      System.out.println("AggregationClientCPTest.putDataInEachBucket: " + " Pair: "
          + Arrays.toString(pair.getFirst()) + " : " + Arrays.toString(pair.getSecond()));
      List<byte[]> keysInRange = getKeysInRange(pair.getFirst(), pair.getSecond(), numOfEntries);
      putKeysInRange(mtable, keysInRange);
    }
    return splitsMap;
  }

  private MTable createTable(String tableName) {
    MTableDescriptor tableDescriptor = new MTableDescriptor();

    tableDescriptor.addColumn(Bytes.toBytes(COL1)).addColumn(Bytes.toBytes(COL2))
        .addColumn(Bytes.toBytes(COL3)).addColumn(Bytes.toBytes(COL4));
    /*
     * if (order == false) { tableDescriptor.setTableType(MTableType.UNORDERED); }
     */
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setTotalNumOfSplits(numBuckets);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createTable(tableName, tableDescriptor);
    assertEquals(mtable.getName(), tableName);
    return mtable;
  }

  private void putKeysInRange(MTable table, List<byte[]> keysInRange) {
    int i = 0;
    for (byte[] key : keysInRange) {
      Put myput1 = new Put(key);
      myput1.addColumn(Bytes.toBytes(COL1), Bytes.toBytes("Deepak" + i));
      myput1.addColumn(Bytes.toBytes(COL2), Bytes.toBytes(i + 10));
      myput1.addColumn(Bytes.toBytes(COL3), Bytes.toBytes(i + 10));
      myput1.addColumn(Bytes.toBytes(COL4), Bytes.toBytes(i + 10));
      table.put(myput1);
      i++;
    }
  }

  private void doTestAggregationClient(final boolean order) throws Exception {
    doPutOperationFromClient(vm3, getLocatorPort(), order);
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  private void doTestAggregationClientWithFilter(final boolean order) throws Exception {
    doPutOperationFromClientWithFilter(vm3, getLocatorPort(), order);
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testAggregationClientCP() throws Exception {
    doTestAggregationClient(true);
  }

  @Test
  public void testAggregationClientCPFilter() throws Exception {
    doTestAggregationClientWithFilter(true);
  }
}
