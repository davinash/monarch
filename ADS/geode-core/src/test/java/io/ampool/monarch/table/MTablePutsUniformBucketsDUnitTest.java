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

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.MTableUtils;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.io.IOException;
import java.util.*;
import org.junit.Test;

@Category(MonarchTest.class)
public class MTablePutsUniformBucketsDUnitTest extends MTableDUnitHelper {
  public MTablePutsUniformBucketsDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();
  private List<VM> allServerVms = null;

  private final int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "MTablePutsUniformBucketsDUnitTest";
  private final String KEY_PREFIX = "KEY";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 100;
  private final String COLUMN_NAME_PREFIX = "COLUMN";
  private final int LATEST_TIMESTAMP = 300;
  private final int MAX_VERSIONS = 5;
  private final int TABLE_MAX_VERSIONS = 7;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    allServerVms = new ArrayList<>(Arrays.asList(this.vm0, this.vm1, this.vm2));
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
    super.tearDown2();
  }

  private List<byte[]> doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);

    List<byte[]> allKeys = new ArrayList<byte[]>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> {
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    return allKeys;
  }

  private Object doPutFrom(VM vm) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return doPuts();
      }
    });
  }

  protected void createTable() {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  protected void createTableFrom(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable();
        return null;
      }
    });
  }

  private void doGets(List<byte[]> keys) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    keys.forEach((K) -> {
      Get get = new Get(K);
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out
              .println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
          System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
          System.out.println(
              "actualValue    => " + Arrays.toString((byte[]) row.get(i).getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex++;
      }
    });
  }

  private void verifyBucketsInEachServer() {
    this.allServerVms.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        Region r = cache.getRegion(TABLE_NAME);
        assertNotNull(r);
        PartitionedRegion pr = (PartitionedRegion) r;
        assertNotNull(pr);
        PartitionedRegionDataStore dataStore = pr.getDataStore();
        assertNotNull(dataStore);
        Set<BucketRegion> allLocalBucketRegions = dataStore.getAllLocalBucketRegions();
        assertNotNull(allLocalBucketRegions);
        assertFalse(allLocalBucketRegions.isEmpty());
        allLocalBucketRegions.forEach((BUCKET) -> assertEquals(BUCKET.size(), NUM_OF_ROWS));
        return null;
      }
    }));
  }

  private Object getAllLocalBuckets(VM vm) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        Region r = cache.getRegion(TABLE_NAME);
        assertNotNull(r);
        PartitionedRegion pr = (PartitionedRegion) r;
        assertNotNull(pr);
        PartitionedRegionDataStore dataStore = pr.getDataStore();
        assertNotNull(dataStore);
        Set<BucketRegion> allLocalBucketRegions = dataStore.getAllLocalPrimaryBucketRegions();
        return allLocalBucketRegions.size();
      }
    });

  }

  private void verifyTotalNumberOfBuckets(int totalNumOfSplits) {
    int totalNumOfBucket = 0;
    totalNumOfBucket += (int) getAllLocalBuckets(this.vm0);
    totalNumOfBucket += (int) getAllLocalBuckets(this.vm1);
    totalNumOfBucket += (int) getAllLocalBuckets(this.vm2);

    assertEquals(totalNumOfSplits, totalNumOfBucket);
  }


  @Test
  public void testPutsInAllBuckets() {
    createTableFrom(this.client1);
    List<byte[]> keys = doPuts();
    doGets(keys);

    verifyBucketsInEachServer();

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    MTable table = clientCache.getTable(TABLE_NAME);
    verifyTotalNumberOfBuckets(table.getTableDescriptor().getTotalNumOfSplits());

  }

  @Test
  public void testGetBucketIdSet() {
    createTableFrom(this.client1);

    // get table descriptor
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);
    MTableDescriptor tableDescriptor = table.getTableDescriptor();

    Map<Integer, List<byte[]>> bIdToKeysMap = getKeysForAllBuckets(113, 10);

    // Testcase-1:: passing null table Descriptor
    Exception expectedException = null;
    try {
      MTableUtils.getBucketIdSet(null, null, null);
    } catch (IllegalArgumentException e) {
      expectedException = e;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    // Testcase-2:: startKey = null, StopKey = null, valid tableDescriptor.
    Set<Integer> bucketIdSet = MTableUtils.getBucketIdSet(null, null, tableDescriptor);
    assertEquals(bucketIdSet.size(), 113);

    // Testcase-3:: startKey = null, StopKey = validKey, valid tableDescriptor.
    int bIndex = 1;
    for (Map.Entry<Integer, List<byte[]>> entry : bIdToKeysMap.entrySet()) {
      Integer bucketId = entry.getKey(); // bucketId
      List<byte[]> keyList = entry.getValue(); // List<byte[]>
      byte[] key = keyList.get(new Random().nextInt(keyList.size()));
      Set<Integer> bIds1 = MTableUtils.getBucketIdSet(null, key, tableDescriptor);
      assertEquals(bIds1.size(), bIndex);
      bIndex++;
    }

    // Testcase-3:: startKey = random validkey from list, StopKey = null, valid tableDescriptor.
    bIndex = 0;
    byte[] keyFromFirstBucket = null;
    byte[] keyFromLastBucket = null;

    for (Map.Entry<Integer, List<byte[]>> entry : bIdToKeysMap.entrySet()) {
      Integer bucketId = entry.getKey();
      List<byte[]> keyList = entry.getValue();
      byte[] key = keyList.get(new Random().nextInt(keyList.size()));
      if (bIndex == 0) {
        keyFromFirstBucket = key;
      }

      Set<Integer> bIds2 = MTableUtils.getBucketIdSet(key, null, tableDescriptor);
      assertEquals(bIds2.size(), 113 - bIndex);
      // System.out.println("Nilkanth bIds2 = "+ bIds2.size());

      if (bIndex == bIdToKeysMap.size() - 1) {
        keyFromLastBucket = key;
      }
      bIndex++;
    }

    // Testcase-4:: valid startkey and endKey
    Set<Integer> bIdsSet =
        MTableUtils.getBucketIdSet(keyFromFirstBucket, keyFromLastBucket, tableDescriptor);
    assertEquals(bIdsSet.size(), 113);
  }

  @Test
  public void testClientScanner() throws IOException {
    createTableFrom(this.client1);
    List<byte[]> keys = doPuts();
    doGets(keys);

    verifyBucketsInEachServer();

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    MTable table = clientCache.getTable(TABLE_NAME);

    Scanner scanner = table.getScanner(new Scan());
    assertNotNull(scanner);

    Row next = scanner.next();
    int counter = 0;
    List<byte[]> keysReturnedByScanner = new ArrayList<>(keys.size());
    while (next != null) {
      // System.out.println("MTablePutsUniformBucketsDUnitTest.testClientScanner " +
      // Arrays.toString(next.getRowId()));
      keysReturnedByScanner.add(next.getRowId());
      counter++;

      int columnIndex = 0;
      List<Cell> row = next.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out
              .println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
          System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
          System.out.println(
              "actualValue    => " + Arrays.toString((byte[]) row.get(i).getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex++;
      }
      next = scanner.next();
    }
    scanner.close();

    assertEquals(keys.size(), counter);

    byte[] prev = null;
    for (byte[] elem : keysReturnedByScanner) {
      if (prev != null && Bytes.compareTo(prev, elem) > 0) {
        fail("Order is not mainted by the scanner");
      }
      prev = elem;
    }


  }

}
