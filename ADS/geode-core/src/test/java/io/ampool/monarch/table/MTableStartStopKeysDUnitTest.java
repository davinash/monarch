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
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.MTableLocationInfo;
import io.ampool.monarch.table.internal.MTableUtils;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.junit.Assert;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.io.IOException;
import java.util.*;
import org.junit.Test;

@Category(MonarchTest.class)
public class MTableStartStopKeysDUnitTest extends MTableDUnitHelper {
  public MTableStartStopKeysDUnitTest() {
    super();
  }

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "MTableStartStopKeysDUnitTest";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 10000;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";

  private String getRandomString() {
    String CHARACTER_RANGE = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxya";
    StringBuffer sb = new StringBuffer();
    Random rnd = new Random();
    while (sb.length() < 18) {
      int index = (int) (rnd.nextFloat() * CHARACTER_RANGE.length());
      sb.append(CHARACTER_RANGE.charAt(index));
    }
    String saltStr = sb.toString();
    return saltStr;

  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
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

  private void createTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    MTableDescriptor tableDescriptor = new MTableDescriptor();

    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);

    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertNotNull(table);
    assertEquals(table.getName(), TABLE_NAME);

  }

  private void createTableOn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable();
        return null;
      }
    });
  }

  private void doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      /*
       * String key = getRandomString(); //System.out.println(key);
       */
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }
  }

  private void doPutFrom(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPuts();
        return null;
      }
    });
  }

  private void doGets() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
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
    }
  }

  private void verifyNoDuplicatesInStartKeys() throws IOException {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    Set<ByteArrayKey> keySetStart = new HashSet<>();
    byte[][] startKeys = table.getMTableLocationInfo().getStartKeys();
    assertNotNull(startKeys);

    for (byte[] startKey : startKeys) {
      if (startKey != null) {
        if (keySetStart.contains(new ByteArrayKey(startKey))) {
          Assert.fail("Duplicate key is returned by getStartKeys");
        }
        keySetStart.add(new ByteArrayKey(startKey));
      }
    }
  }

  private void verifyNoDuplicatesInEndKeys() throws IOException {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    Set<ByteArrayKey> keySetEnd = new HashSet<>();
    byte[][] endKeys = table.getMTableLocationInfo().getEndKeys();
    assertNotNull(endKeys);

    for (byte[] endKey : endKeys) {
      if (endKey != null) {
        if (keySetEnd.contains(new ByteArrayKey(endKey))) {
          Assert.fail("Duplicate key is returned by getEndKeys");
        }
        keySetEnd.add(new ByteArrayKey(endKey));
      }
    }
  }

  private void verifyStartAndStopKeysFrom(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyNoDuplicatesInStartKeys();
        verifyNoDuplicatesInEndKeys();
        return null;
      }
    });
  }

  private void verifyStartAndStopKeysForEmptyTable() throws IOException {
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(Bytes.toBytes("Column1"));
    tableDescriptor.addColumn(Bytes.toBytes("Column2"));
    tableDescriptor.addColumn(Bytes.toBytes("Column3"));
    tableDescriptor.addColumn(Bytes.toBytes("Column4"));

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable("EMPTY_TABLE", tableDescriptor);
    assertNotNull(table);

    byte[][] startKeys = table.getMTableLocationInfo().getStartKeys();
    assertNotNull(startKeys);
    int count = 0;
    for (int i = 0; i < startKeys.length; i++) {
      if (startKeys[i] != null)
        count++;
    }
    assertTrue(count == 0);
  }

  private Object getTotalNumberOfBuckets(VM vm, final String regionName) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        Region r = cache.getRegion(regionName);
        PartitionedRegion pr = (PartitionedRegion) r;
        return pr.getTotalNumberOfBuckets();
      }
    });
  }

  private void verifyActualStartAndStopKeys() throws IOException {
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(Bytes.toBytes("Column1"));
    tableDescriptor.addColumn(Bytes.toBytes("Column2"));
    tableDescriptor.addColumn(Bytes.toBytes("Column3"));
    tableDescriptor.addColumn(Bytes.toBytes("Column4"));
    tableDescriptor.setTotalNumOfSplits(1);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable("TABLE_WITH_SOME_DATA", tableDescriptor);

    assertNotNull(table);
    Put putRecord1 = new Put(Bytes.toBytes("1"));
    putRecord1.addColumn(Bytes.toBytes("Column1"), new byte[10]);
    Put putRecord2 = new Put(Bytes.toBytes("2"));
    putRecord2.addColumn(Bytes.toBytes("Column2"), new byte[10]);
    Put putRecord3 = new Put(Bytes.toBytes("3"));
    putRecord3.addColumn(Bytes.toBytes("Column3"), new byte[10]);
    Put putRecord4 = new Put(Bytes.toBytes("4"));
    putRecord4.addColumn(Bytes.toBytes("Column4"), new byte[10]);

    table.put(putRecord1);
    table.put(putRecord2);
    table.put(putRecord3);
    table.put(putRecord4);

    MTableLocationInfo mTableLocationInfo = table.getMTableLocationInfo();

    byte[][] startKeys = mTableLocationInfo.getStartKeys();
    assertNotNull(startKeys);
    assertTrue(startKeys.length == 1);

    byte[][] endKeys = mTableLocationInfo.getEndKeys();
    assertNotNull(endKeys);
    assertTrue(endKeys.length == 1);

    Pair<byte[][], byte[][]> startEndKeys = mTableLocationInfo.getStartEndKeys();
    assertNotNull(startEndKeys);

    if (Bytes.compareTo(Bytes.toBytes("1"), startKeys[0]) != 0) {
      Assert.fail("Invalid Start Key returned");
    }

    if (Bytes.compareTo(Bytes.toBytes("4"), endKeys[0]) != 0) {
      Assert.fail("Invalid End Key returned");
    }

    if (Bytes.compareTo(Bytes.toBytes("1"), startEndKeys.getFirst()[0]) != 0) {
      Assert.fail("Invalid Start Key returned by getStartEndKeysOverKeySpace");
    }

    if (Bytes.compareTo(Bytes.toBytes("4"), startEndKeys.getSecond()[0]) != 0) {
      Assert.fail("Invalid End Key returned by getStartEndKeysOverKeySpace");
    }
  }

  private void verifyActualStartAndStopKeysForAllBuckets(int numBuckets, int numKeysPerBucket)
      throws IOException {

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(Bytes.toBytes("Column1"));
    tableDescriptor.addColumn(Bytes.toBytes("Column2"));
    tableDescriptor.addColumn(Bytes.toBytes("Column3"));
    tableDescriptor.addColumn(Bytes.toBytes("Column4"));
    tableDescriptor.setTotalNumOfSplits(1);
    tableDescriptor.setTotalNumOfSplits(numBuckets);
    String tableName = "TABLE_WITH_DATA_FOR_ALL_BUCKETS";

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(tableName, tableDescriptor);

    assertNotNull(table);

    Map<Integer, Pair<byte[], byte[]>> splitsMap = MTableUtils.getUniformKeySpaceSplit(numBuckets);

    Set<ByteArrayKey> inputStartKeys = new HashSet<>();
    Set<ByteArrayKey> inputEndKeys = new HashSet<>();

    for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
      Pair<byte[], byte[]> pair = splitsMap.get(bucketId);
      List<byte[]> keysInRange =
          getKeysInRange(pair.getFirst(), pair.getSecond(), numKeysPerBucket);
      Pair<byte[], byte[]> minMaxPair = getMinMaxPair(keysInRange);
      inputStartKeys.add(new ByteArrayKey(minMaxPair.getFirst()));
      inputEndKeys.add(new ByteArrayKey(minMaxPair.getSecond()));
      putKeysInRange(table, keysInRange);
    }

    Assert.assertTrue(inputStartKeys.size() == numBuckets);
    Assert.assertTrue(inputEndKeys.size() == numBuckets);

    MTableLocationInfo mTableLocationInfo = table.getMTableLocationInfo();

    byte[][] startKeys = mTableLocationInfo.getStartKeys();
    assertNotNull(startKeys);
    Assert.assertTrue(startKeys.length == numBuckets);

    byte[][] endKeys = mTableLocationInfo.getEndKeys();
    assertNotNull(endKeys);
    Assert.assertTrue(endKeys.length == numBuckets);

    for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
      Assert.assertTrue("Incorrect key in startkeys:" + Arrays.toString(startKeys[bucketId]),
          inputStartKeys.contains(new ByteArrayKey(startKeys[bucketId])));
      Assert.assertTrue("Incorrect key in endKeys:" + Arrays.toString(endKeys[bucketId]),
          inputEndKeys.contains(new ByteArrayKey(endKeys[bucketId])));
    }

    Pair<byte[][], byte[][]> startEndKeys = mTableLocationInfo.getStartEndKeys();

    startKeys = startEndKeys.getFirst();
    assertNotNull(startKeys);
    Assert.assertTrue(startKeys.length == numBuckets);

    endKeys = startEndKeys.getSecond();
    assertNotNull(endKeys);
    Assert.assertTrue(endKeys.length == numBuckets);

    for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
      Assert.assertTrue("Incorrect key in startkeys:" + Arrays.toString(startKeys[bucketId]),
          inputStartKeys.contains(new ByteArrayKey(startKeys[bucketId])));
      Assert.assertTrue("Incorrect key in endKeys:" + Arrays.toString(endKeys[bucketId]),
          inputEndKeys.contains(new ByteArrayKey(endKeys[bucketId])));
    }

    admin.deleteTable(tableName);
  }

  private void putKeysInRange(MTable table, List<byte[]> keysInRange) {
    for (byte[] key : keysInRange) {
      Put putRecord = new Put(key);
      putRecord.addColumn(Bytes.toBytes("Column1"), new byte[10]);
      table.put(putRecord);
    }
  }

  private Pair<byte[], byte[]> getMinMaxPair(List<byte[]> keysInRange) {
    byte[] min = null;
    byte[] max = null;
    for (byte[] currKey : keysInRange) {
      if (min == null || Bytes.compareTo(currKey, min) < 0) {
        min = currKey;
      }
      if (max == null || Bytes.compareTo(currKey, max) > 0) {
        max = currKey;
      }
    }
    return new Pair<byte[], byte[]>(min, max);
  }

  @Test
  public void testStartStopKeys() throws IOException {
    createTableOn(this.client1);
    doPutFrom(this.client1);
    doGets();

    verifyStartAndStopKeysFrom(this.client1);
    verifyNoDuplicatesInStartKeys();

    verifyStartAndStopKeysForEmptyTable();

    verifyActualStartAndStopKeys();
    verifyActualStartAndStopKeysForAllBuckets(113, 100);
  }
}
