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

package io.ampool.monarch.table.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitConfigFramework;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.filter.KeyOnlyFilter;
import io.ampool.monarch.table.internal.MTableKey;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class MTableClientScannerAllFiltersDUnit extends MTableDUnitConfigFramework {

  private static int NO_OF_KEYS_PER_BUCKET = 1;

  Map<Integer, List<byte[]>> keysPerBucket = new LinkedHashMap<>();

  public MTableClientScannerAllFiltersDUnit() {
    super();
  }

  /**
   * This test case should scan only keys by using KeyOnlyFilter with ignoring lenAsValue
   */
  @Test
  public void testScanWithKeyValueFilterIgnoringLenAsValue() {
    final FrameworkRunnable runnable = new FrameworkRunnable() {
      @Override
      public void run() {
        keysPerBucket = new LinkedHashMap<>();
        // get the table
        MTable table = getTable();

        // get keys per bucket
        final MTableDescriptor tableDesc = table.getTableDescriptor();
        final Map<Integer, Pair<byte[], byte[]>> keySpace = tableDesc.getKeySpace();
        keySpace.forEach((bucket, keyLimits) -> {
          final List<byte[]> genKeys =
              getKeysInRange(keyLimits.getFirst(), keyLimits.getSecond(), NO_OF_KEYS_PER_BUCKET);
          keysPerBucket.put(bucket, genKeys);
        });
        keysPerBucket = sortKeys(keysPerBucket);
        doPut(table, keysPerBucket);

        Scan scan = new Scan();
        scan.setFilter(new KeyOnlyFilter());
        verifyScan(scan, keysPerBucket, true);

        scan = new Scan();
        scan.setFilter(new KeyOnlyFilter());
        doVerifyScanFromClient(client1, keysPerBucket, scan, true);
      }

      @Override
      public void runAfterRestart() {

        MTable table = getTable();
        assertNotNull(table);
        if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
          final Scan scan = new Scan();
          scan.setFilter(new KeyOnlyFilter());
          final Scanner scanner = table.getScanner(scan);
          verifyScan(scan, keysPerBucket, true);

          doVerifyScanFromClient(client1, keysPerBucket, scan, true);
        }
      }
    };
    runAllConfigs(runnable);
  }

  /**
   * This test case should scan only keys by using KeyOnlyFilter with considering lenAsValue
   */
  @Test
  public void testScanWithKeyValueFilter() {
    final FrameworkRunnable runnable = new FrameworkRunnable() {
      @Override
      public void run() {
        keysPerBucket = new LinkedHashMap<>();
        // get the table
        MTable table = getTable();

        // get keys per bucket
        final MTableDescriptor tableDesc = table.getTableDescriptor();
        final Map<Integer, Pair<byte[], byte[]>> keySpace = tableDesc.getKeySpace();
        keySpace.forEach((bucket, keyLimits) -> {
          final List<byte[]> genKeys =
              getKeysInRange(keyLimits.getFirst(), keyLimits.getSecond(), NO_OF_KEYS_PER_BUCKET);
          keysPerBucket.put(bucket, genKeys);
        });
        keysPerBucket = sortKeys(keysPerBucket);
        doPut(table, keysPerBucket);

        Scan scan = new Scan();
        scan.setFilter(new KeyOnlyFilter(true));
        verifyScan(scan, keysPerBucket, true);

        scan = new Scan();
        scan.setFilter(new KeyOnlyFilter(true));
        doVerifyScanFromClient(client1, keysPerBucket, scan, true);
      }

      @Override
      public void runAfterRestart() {

        MTable table = getTable();
        assertNotNull(table);
        if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
          final Scan scan = new Scan();
          scan.setFilter(new KeyOnlyFilter(true));
          final Scanner scanner = table.getScanner(scan);
          verifyScan(scan, keysPerBucket, true);

          doVerifyScanFromClient(client1, keysPerBucket, scan, true);
        }
      }
    };
    runAllConfigs(runnable);
  }


  // -------------------------------------------------------- No tests below
  // -------------------------------------


  private LinkedHashMap<Integer, List<byte[]>> sortKeys(
      final Map<Integer, List<byte[]>> keysPerBucket) {
    final LinkedHashMap<Integer, List<byte[]>> sortedKeys =
        new LinkedHashMap<Integer, List<byte[]>>();

    keysPerBucket.forEach((bucket, keyList) -> {
      Collections.sort(keyList, Bytes.BYTES_COMPARATOR);
      sortedKeys.put(bucket, keyList);
    });
    return sortedKeys;
  }


  private void doPut(MTable table, Map<Integer, List<byte[]>> keys) {
    keys.forEach((bucketid, rowkeys) -> {
      doPut(table, rowkeys, bucketid);
    });
  }

  private void doPut(MTable table, List<byte[]> keys, final Integer bucketid) {
    // System.out.println("MTableClientScannerAllFiltersDUnit.doPut :: " + "bucketid: " + bucketid);
    int entryAdded = (bucketid * NO_OF_KEYS_PER_BUCKET);
    for (int rowInd = 0; rowInd < keys.size(); rowInd++) {
      final byte[] rowKey = keys.get(rowInd);
      Put put = new Put(rowKey);
      // System.out.println("-------------------------------------------------------------------------------------------------------------------");
      // System.out.println((rowInd + entryAdded)+" :RowKey: "+Arrays.toString(rowKey));
      for (int colInd = 0; colInd < NUM_COLUMNS; colInd++) {
        put.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + colInd),
            Bytes.toBytes(VALUE_PREFIX + (rowInd + entryAdded) + colInd));
        // System.out.println("ColName: "+COLUMNNAME_PREFIX + colInd + " ColValue:
        // "+Arrays.toString(Bytes.toBytes(VALUE_PREFIX + (rowInd + entryAdded) + colInd)));
      }
      table.put(put);
    }
  }

  private void doPut(MTable table, String[] keys) {
    for (int rowInd = 0; rowInd < keys.length; rowInd++) {
      Put put = new Put(Bytes.toBytes(keys[rowInd]));
      for (int colInd = 0; colInd < NUM_COLUMNS; colInd++) {
        put.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + colInd),
            Bytes.toBytes(VALUE_PREFIX + rowInd + colInd));
      }
      table.put(put);
    }
  }

  private void doVerifyScanFromClient(VM vm, Map<Integer, List<byte[]>> keys, final Scan scan,
      final boolean verifyOnlyKeys) {
    // System.out.println("MTableClientScannerAllFiltersDUnit.doVerifyScanFromClient :: " + "Into
    // the client");
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyScan(scan, keys, verifyOnlyKeys);
        return null;
      }
    });
  }

  private void verifyScan(Scan scan, Map<Integer, List<byte[]>> keys,
      final boolean verifyOnlykeys) {
    // System.out.println("MTableClientScannerAllFiltersDUnit.verifyScan :: " + "in the verify
    // scan");
    final MTable mtable = getTableFromClientCache();
    final MTableDescriptor tableDesc = mtable.getTableDescriptor();
    final Scanner scanner = mtable.getScanner(scan);
    // printKeys();
    final MTableType tableType = mtable.getTableDescriptor().getTableType();
    if (tableType == MTableType.ORDERED_VERSIONED) {
      int[] rowIndex = {0};
      keys.forEach((bucket, sortedKeyList) -> {
        sortedKeyList.forEach((expectedKey) -> {
          // System.out.println("-----------------------------------------------------------------------------------------------------------------------------");
          final Row res = scanner.next();
          assertNotNull(res);
          final byte[] actualKey = res.getRowId();
          // System.out.println("MTableClientScannerAllFiltersDUnit.verifyScan :: bucket: " + bucket
          // + " Expected RowKey: " + Arrays.toString(expectedKey));
          // System.out.println("MTableClientScannerAllFiltersDUnit.verifyScan :: bucket: " + bucket
          // + " Actual RowKey: " + Arrays.toString(actualKey));
          assertNotNull(actualKey);
          assertFalse(actualKey.length == 0);
          assertEquals(0, Bytes.compareTo(expectedKey, actualKey));

          if (!verifyOnlykeys) {
            final int columns = tableDesc.getNumOfColumns();
            // Verification only particular to key only filter
            // TODO change rowIndex
            List colToChecks = new LinkedList();
            verifyColumnValues(res, rowIndex[0]++, colToChecks);
          }
        });
      });
    } else {
      final List<MTableKey> keysList = getOnlyKeys();
      Row res = scanner.next();
      while (res != null) {
        assertNotNull(res);
        final byte[] actualKey = res.getRowId();
        assertNotNull(actualKey);
        assertFalse(actualKey.length == 0);
        assertTrue(keysList.contains(new MTableKey(actualKey)));
        // System.out.println("MTableClientScannerAllFiltersDUnit.verifyScan :: " + " Actual RowKey:
        // " + Arrays.toString(actualKey));
        final int keyIndex = keysList.indexOf(new MTableKey(actualKey));
        if (!verifyOnlykeys) {
          // TODO verify values too
          final int columns = tableDesc.getNumOfColumns();
          // Verification only particular to key only filter
          // TODO change rowIndex
          List colToChecks = new LinkedList();
          verifyColumnValues(res, keyIndex, colToChecks);
        }
        res = scanner.next();
      }
    }
  }

  private void verifyColumnValues(final Row row, final int rowIndex, List colToChecks) {
    System.out.println(
        "row = [" + row + "], rowIndex = [" + rowIndex + "], colToChecks = [" + colToChecks + "]");
    // verify column values
    List<Cell> cells = row.getCells();
    // if (colToChecks.size() > 1) {
    int checkColIndex = 0;
    for (int columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex++) {
      // if(colToChecks.size() < columnIndex){
      // break;
      // }
      if (!colToChecks.contains(columnIndex)) {
        continue;
      }
      assertEquals(colToChecks.size(), cells.size());
      final Integer colIndex = (Integer) colToChecks.get(checkColIndex);
      Cell cell = cells.get(checkColIndex++);
      byte[] colNameRec = cell.getColumnName();
      byte[] colNameExp = Bytes.toBytes(COLUMNNAME_PREFIX + colIndex);

      // Verify ColumnNames for the Row
      assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));

      //// Verify Column-Values for the Row
      byte[] colValRec = (byte[]) cell.getColumnValue();
      byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + rowIndex + colIndex);
      System.out.println("MTableClientScannerAllFiltersDUnit.verifyColumnValues :: " + "colValRec: "
          + Arrays.toString(colValRec) + "colValExp: " + Arrays.toString(colValExp));
      assertEquals(0, Bytes.compareTo(colValExp, colValRec));
    }
    // }
    // else {
    // // check particular columns
    // assertEquals(1, cells.size());
    // MCell cell = cells.get(0);
    // byte[] colNameRec = cell.getColumnName();
    // byte[] colNameExp = Bytes.toBytes(COLUMNNAME_PREFIX + colToChecks);
    //
    // //Verify ColumnNames for the Row
    // assertEquals(0, Bytes.compareTo(colNameExp, colNameRec));
    //
    // ////Verify Column-Values for the Row
    // byte[] colValRec = (byte[]) cell.getColumnValue();
    // byte[] colValExp = Bytes.toBytes(VALUE_PREFIX + rowIndex + colToChecks);
    // System.out.println(
    // "MTableClientScannerAllFiltersDUnit.verifyColumnValues :: " + "colValRec: " +
    // Arrays.toString(colValRec) + "colValExp: " + Arrays.toString(colValExp));
    // assertEquals(0, Bytes.compareTo(colValExp, colValRec));
    // }

  }


  private void printKeys() {
    System.out.println(
        "---------------------------------------KEYSET START-------------------------------------------------------------------------------------");
    keysPerBucket.forEach((bucket, keyList) -> {
      keyList.forEach(key -> {
        System.out.println("Bucket: " + bucket + " : key: " + Arrays.toString(key));
      });
    });
    System.out.println(
        "---------------------------------------KEYSET END---------------------------------------------------------------------------------------");
  }

  private List<MTableKey> getOnlyKeys() {
    List<MTableKey> keys = new LinkedList<>();
    keysPerBucket.forEach((bucket, keyList) -> {
      keyList.forEach(key -> {
        keys.add(new MTableKey(key));
      });
    });
    Collections.sort(keys);
    return keys;
  }
}
