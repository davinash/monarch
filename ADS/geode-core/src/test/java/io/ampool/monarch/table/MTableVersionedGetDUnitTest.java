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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MTableExistsException;
import io.ampool.monarch.table.internal.CellRef;
import io.ampool.monarch.table.internal.SingleVersionRow;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.MTableUtils;

import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MTableVersionedGetDUnitTest extends MTableDUnitHelper {

  private static final Logger logger = LogService.getLogger();

  private Host host = Host.getHost(0);
  private VM vm0 = host.getVM(0);
  private VM vm1 = host.getVM(1);
  private VM vm2 = host.getVM(2);
  private VM vm3 = host.getVM(3);
  final int numOfEntries = 3;

  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();

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

  public MTableVersionedGetDUnitTest() {

  }

  /**
   * Create table with default schema | COL1 | COL2 | COL3 | COL4 | COL5 |
   *
   * @param tableName
   * @return
   */
  public MTable createMTable(String tableName, boolean ordered) {
    String COL_PREFIX = "COL";
    MTableDescriptor tableDescriptor =
        new MTableDescriptor(ordered ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);

    for (int i = 1; i <= 5; i++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COL_PREFIX + i));
    }
    tableDescriptor.setMaxVersions(5);
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable mtable = null;
    try {
      mtable = admin.createTable(tableName, tableDescriptor);
    } catch (MTableExistsException e) {
      e.printStackTrace();
    }
    assertNotNull(mtable);
    assertEquals(mtable.getName(), tableName);
    return mtable;
  }

  /**
   * Simply puts 5 versions and checks each version by doing get
   * 
   * @throws Exception
   */
  @Test
  public void testVersionedGet() throws Exception {
    final MTable mTable = createMTable(getTestMethodName(), true);
    final List<byte[]> keys = putDataInEachBucket(mTable, 1);
    // verify for both ascending as well as descending
    verifyVersionedGets(mTable, keys, 5, true);
    verifyVersionedGets(mTable, keys, 5, false);
  }

  /**
   * Simply puts 5 versions and checks get using maxVersion as 3
   * 
   * @throws Exception
   */
  @Test
  public void testVersionedGetLessThanMaxVersions() throws Exception {
    final MTable mTable = createMTable(getTestMethodName(), true);
    final List<byte[]> keys = putDataInEachBucket(mTable, 1);
    // verify for both ascending as well as descending
    verifyVersionedGets(mTable, keys, 3, true);
    verifyVersionedGets(mTable, keys, 3, false);
  }

  /**
   * Test MGet added API's for error checking
   * 
   * @throws Exception
   */
  @Test
  public void testVersionedGetAPIForErrorChecking() throws Exception {
    final MTable mTable = createMTable(getTestMethodName(), true);
    final List<byte[]> keys = putDataInEachBucket(mTable, 1);

    Get get = new Get(keys.get(0));
    try {
      // negative value not allowed
      get.setMaxVersionsToFetch(-1);
    } catch (IllegalArgumentException ex) {
      ex.printStackTrace();
    }

    get = new Get(keys.get(0));
    try {
      // setting versions more than table max version
      get.setMaxVersionsToFetch(10);
      mTable.get(get);
    } catch (IllegalArgumentException ex) {
      ex.printStackTrace();
      return;
    }

    fail("Exception is expected");
    // verifyVersionedGets(mTable,keys,5);
  }

  /**
   * Simply puts 5 versions and checks each version by doing get
   *
   * @throws Exception
   */
  @Test
  public void testVersionedGetUnordered() throws Exception {
    final MTable mTable = createMTable(getTestMethodName(), false);
    final List<byte[]> keys = putDataInEachBucket(mTable, 1);
    // verify for both ascending as well as descending
    verifyVersionedGets(mTable, keys, 5, true);
    verifyVersionedGets(mTable, keys, 5, false);
  }

  /**
   * Simply puts 5 versions and checks get using maxVersion as 3
   *
   * @throws Exception
   */
  @Test
  public void testVersionedGetLessThanMaxVersionsUnordered() throws Exception {
    final MTable mTable = createMTable(getTestMethodName(), false);
    final List<byte[]> keys = putDataInEachBucket(mTable, 1);
    // verify for both ascending as well as descending
    verifyVersionedGets(mTable, keys, 3, true);
    verifyVersionedGets(mTable, keys, 3, false);
  }

  /**
   * Test MGet added API's for error checking
   *
   * @throws Exception
   */
  @Test
  public void testVersionedGetAPIForErrorCheckingUnordered() throws Exception {
    final MTable mTable = createMTable(getTestMethodName(), false);
    final List<byte[]> keys = putDataInEachBucket(mTable, 1);

    Get get = new Get(keys.get(0));
    try {
      // negative value not allowed
      get.setMaxVersionsToFetch(-1);
    } catch (IllegalArgumentException ex) {
      ex.printStackTrace();
    }

    get = new Get(keys.get(0));
    try {
      // setting versions more than table max version
      get.setMaxVersionsToFetch(10);
      mTable.get(get);
    } catch (IllegalArgumentException ex) {
      ex.printStackTrace();
      return;
    }

    fail("Exception is expected");
    // verifyVersionedGets(mTable,keys,5);
  }

  private void verifyVersionedGets(final MTable mTable, final List<byte[]> keys,
      final int maxVersions, final boolean isNewerFirst) {
    int keyIndex = 0;
    for (byte[] key : keys) {
      System.out.println(
          "-----------------------------------------------------------------------------------------------------------------");
      System.out.println("MTableVersionedGetDUnitTest.verifyVersionedGets :: " + "KeyNo. "
          + keyIndex + " key data: " + Arrays.toString(key) + " isNewerFirst: " + isNewerFirst);
      Put put = new Put(key);

      Get get = new Get(key);
      // set max value to get all versions
      get.setMaxVersionsToFetch(maxVersions);
      if (!isNewerFirst) {
        get.setMaxVersionsToFetch(maxVersions, true);
      }

      final Row result = mTable.get(get);
      final Map<Long, SingleVersionRow> allVersions = result.getAllVersions();

      assertNotNull(result);
      // as this depends on number of versions put
      assertTrue(allVersions.size() <= maxVersions);

      final Iterator<Entry<Long, SingleVersionRow>> itr = allVersions.entrySet().iterator();

      Long preTimeStamp = -1l;

      int versionIndex = 1;

      if (isNewerFirst) {
        versionIndex = mTable.getTableDescriptor().getMaxVersions();
      }

      while (itr.hasNext()) {
        final Entry<Long, SingleVersionRow> entry = itr.next();
        Long timeStamp = entry.getKey();
        SingleVersionRow row = entry.getValue();
        assertNotNull(row);

        put.addColumn(Bytes.toBytes("COL1"), Bytes.toBytes("User" + keyIndex + versionIndex));
        put.addColumn(Bytes.toBytes("COL2"), Bytes.toBytes(keyIndex * 2 * versionIndex));
        put.addColumn(Bytes.toBytes("COL3"), Bytes.toBytes(keyIndex * 3 * versionIndex));
        put.addColumn(Bytes.toBytes("COL4"), Bytes.toBytes(keyIndex * 4 * versionIndex));
        put.addColumn(Bytes.toBytes("COL5"), Bytes.toBytes(keyIndex * 5 * versionIndex));
        final Map<ByteArrayKey, Object> cvMap = put.getColumnValueMap();

        // based on order of versions in get do checking

        // first check timestamp
        if (preTimeStamp >= 0) {
          // that is atleast one row is traversed
          // now verify that this timestamp is smaller than previous one
          // System.out.println("MTableVersionedGetDUnitTest.verifyVersionedGets :: " +
          // "preTimestamp: " + preTimeStamp + " current Timestamp: " + timeStamp);
          if (isNewerFirst) {
            assertTrue(timeStamp < preTimeStamp);
          } else {
            assertTrue(timeStamp > preTimeStamp);
          }
        }
        preTimeStamp = timeStamp;

        // check key data
        assertEquals("Key comparison failed", 0, Bytes.compareTo(key, row.getRowId()));

        // check columns
        assertTrue(row.size() - 1 == mTable.getTableDescriptor().getMaxVersions());
        assertEquals(mTable.getTableDescriptor().getMaxVersions(), row.getCells().size() - 1);

        final List<Cell> cells = row.getCells();

        for (int k = 1; k <= cells.size() - 1; k++) {
          final Cell cell = cells.get(k - 1);
          assertNotNull(cell);

          System.out.println("Expected Column Name: " + ("COL" + k) + " Actual Column Name: "
              + Bytes.toString(cell.getColumnName()));
          assertTrue(Bytes.equals(cell.getColumnName(), Bytes.toBytes("COL" + k)));

          final byte[] expectedColVal =
              (byte[]) cvMap.get(new ByteArrayKey(Bytes.toBytes("COL" + k)));
          System.out.println(
              "Expected Column Value: "
                  + (k == 1
                      ? Bytes
                          .toString((byte[]) cvMap.get(new ByteArrayKey(Bytes.toBytes("COL" + k))))
                      : Bytes.toInt((byte[]) cvMap.get(new ByteArrayKey(Bytes.toBytes("COL" + k)))))
                  + " Actual Column Value: "
                  + (k == 1 ? (Bytes.toString((byte[]) cell.getColumnValue()))
                      : (Bytes.toInt((byte[]) cell.getColumnValue()))));
          // check value
          assertTrue(Bytes.equals(expectedColVal, ((CellRef) cell).getValueArrayCopy()));
        }

        if (isNewerFirst) {
          versionIndex--;
        } else {
          versionIndex++;
        }
      }
      keyIndex++;
    }
  }

  /**
   * Puts rowsPerBucket rows into each bucket
   * 
   * @param mtable
   * @param rowsPerBucket
   */
  public List<byte[]> putDataInEachBucket(MTable mtable, int rowsPerBucket) {
    final MTableDescriptor mtableDesc = mtable.getTableDescriptor();
    Map<Integer, Pair<byte[], byte[]>> splitsMap =
        MTableUtils.getUniformKeySpaceSplit(mtableDesc.getTotalNumOfSplits());
    List<byte[]> keysInRange = new ArrayList<>();
    for (int bucketId = 0; bucketId < mtableDesc.getTotalNumOfSplits(); bucketId++) {
      Pair<byte[], byte[]> pair = splitsMap.get(bucketId);
      keysInRange.addAll(getKeysInRange(pair.getFirst(), pair.getSecond(), rowsPerBucket));
    }
    putKeysInRange(mtable, keysInRange, mtableDesc.getMaxVersions());
    return keysInRange;
  }

  private void putKeysInRange(MTable table, List<byte[]> keysInRange, final int maxVersions) {
    int i = 0;
    List<Put> puts = new ArrayList<>();
    for (byte[] key : keysInRange) {
      Put myput1 = new Put(key);
      for (int j = 1; j <= maxVersions; j++) {
        // myput1.setTimeStamp(j);
        myput1.addColumn(Bytes.toBytes("COL1"), Bytes.toBytes("User" + i + j));
        myput1.addColumn(Bytes.toBytes("COL2"), Bytes.toBytes(i * 2 * j));
        myput1.addColumn(Bytes.toBytes("COL3"), Bytes.toBytes(i * 3 * j));
        myput1.addColumn(Bytes.toBytes("COL4"), Bytes.toBytes(i * 4 * j));
        myput1.addColumn(Bytes.toBytes("COL5"), Bytes.toBytes(i * 5 * j));
        table.put(myput1);
      }
      i++;
    }

  }
}
