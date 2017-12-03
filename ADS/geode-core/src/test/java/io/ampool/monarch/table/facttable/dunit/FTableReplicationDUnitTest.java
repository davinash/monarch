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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.facttable.FTableDescriptorHelper;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.monarch.table.internal.ThinRow;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(FTableTest.class)
public class FTableReplicationDUnitTest extends MTableDUnitHelper {

  private static final int NUM_OF_COLUMNS = 10;
  private static final int NUM_OF_SPLITS = 1;
  private static final String COLUMN_NAME_PREFIX = "COL";
  public static int actualRows;


  public FTableReplicationDUnitTest() {}

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
    closeMClientCache(vm3);
    closeMClientCache();
    super.tearDown2();
  }

  protected void verifyValuesOnAllPrimaryVMs(String tableName, final int numRows,
      Map<String, String> map) {
    final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
    for (int i = 0; i < vmList.size(); i++) {
      final int res = (int) vmList.get(i).invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          return verifyValuesOnPrimary(tableName, map);
        }
      });
      actualRows += res;
    }
    assertEquals(numRows, actualRows);
  }

  protected void verifyValuesOnAllSecondaryVMs(String tableName, final int numRows,
      Map<String, String> map) {
    actualRows = 0;
    final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
    for (int i = 0; i < vmList.size(); i++) {
      final int res = (int) vmList.get(i).invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          return verifyValuesOnSecondary(tableName, map);
        }
      });
      actualRows += res;
    }
    assertEquals(numRows, actualRows);
  }

  protected int verifyValuesOnPrimary(final String tableName, Map<String, String> map) {
    int entriesCount = 0;
    final Region<Object, Object> region = MCacheFactory.getAnyInstance().getRegion(tableName);
    assertNotNull(region);
    final Iterator<BucketRegion> allLocalPrimaryBucketRegions =
        ((PartitionedRegion) region).getDataStore().getAllLocalPrimaryBucketRegions().iterator();

    TableDescriptor tableDescriptor = MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
    ThinRow thinRow = ThinRow.create(tableDescriptor, ThinRow.RowFormat.F_FULL_ROW);

    while (allLocalPrimaryBucketRegions.hasNext()) {
      final BucketRegion bucketRegion = allLocalPrimaryBucketRegions.next();
      final RowTupleConcurrentSkipListMap internalMap =
          (RowTupleConcurrentSkipListMap) bucketRegion.entries.getInternalMap();
      final Map concurrentSkipListMap = internalMap.getInternalMap();
      final Iterator<Entry> iterator = concurrentSkipListMap.entrySet().iterator();
      while (iterator.hasNext()) {
        final Entry entry = iterator.next();
        RegionEntry value1 = (RegionEntry) entry.getValue();
        Object value = value1._getValue();
        if (value instanceof VMCachedDeserializable) {
          value = ((VMCachedDeserializable) value).getDeserializedForReading();
        }
        final BlockValue blockValue = (BlockValue) value;
        final Iterator objectIterator = blockValue.iterator();
        while (objectIterator.hasNext()) {
          thinRow.reset(null, blockValue.getRecord(entriesCount % 1000));
          List<Cell> cells = thinRow.getCells();
          for (int i = 0; i < cells.size(); i++) {
            Cell cell = cells.get(i);
            byte[] columnName = cell.getColumnName();
            if (Bytes.toString(columnName).equals(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) {
              continue;
            }
            Object columnValue = cell.getColumnValue();
            String s = map.get(Bytes.toString(columnName));
            assertEquals(s, Bytes.toString((byte[]) columnValue));
          }
          objectIterator.next();
          entriesCount++;
        }
      }
      System.out.println(
          "Bucket Region Name : " + bucketRegion.getName() + "   Size: " + bucketRegion.size());
    }
    System.out.println("FTableAppendDUnitTest.verifyValues :: " + "ECount: " + entriesCount);
    return entriesCount;
  }

  protected int verifyValuesOnSecondary(final String tableName, Map<String, String> map) {
    int entriesCount = 0;
    final Region<Object, Object> region = MCacheFactory.getAnyInstance().getRegion(tableName);
    assertNotNull(region);
    final Iterator<BucketRegion> allLocalBucketRegions =
        ((PartitionedRegion) region).getDataStore().getAllLocalBucketRegions().iterator();
    TableDescriptor tableDescriptor = MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
    ThinRow thinRow = ThinRow.create(tableDescriptor, ThinRow.RowFormat.F_FULL_ROW);
    while (allLocalBucketRegions.hasNext()) {
      final BucketRegion bucketRegion = allLocalBucketRegions.next();
      if (!bucketRegion.getBucketAdvisor().isPrimary()
          && bucketRegion.getBucketAdvisor().isHosting()) {
        final RowTupleConcurrentSkipListMap internalMap =
            (RowTupleConcurrentSkipListMap) bucketRegion.entries.getInternalMap();
        final Map concurrentSkipListMap = internalMap.getInternalMap();
        final Iterator<Entry> iterator = concurrentSkipListMap.entrySet().iterator();
        while (iterator.hasNext()) {
          final Entry entry = iterator.next();

          RegionEntry value1 = (RegionEntry) entry.getValue();
          Object value = value1._getValue();
          if (value instanceof VMCachedDeserializable) {
            value = ((VMCachedDeserializable) value).getDeserializedForReading();
          }
          final BlockValue blockValue = (BlockValue) value;
          final Iterator objectIterator = blockValue.iterator();
          while (objectIterator.hasNext()) {
            thinRow.reset(null, blockValue.getRecord(entriesCount % 1000));
            List<Cell> cells = thinRow.getCells();
            for (int i = 0; i < cells.size(); i++) {
              Cell cell = cells.get(i);
              byte[] columnName = cell.getColumnName();
              if (Bytes.toString(columnName)
                  .equals(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) {
                continue;
              }
              Object columnValue = cell.getColumnValue();
              String s = map.get(Bytes.toString(columnName));
              assertEquals(s, Bytes.toString((byte[]) columnValue));
            }
            objectIterator.next();
            entriesCount++;
          }
        }
      }
      System.out.println(
          "Bucket Region Name : " + bucketRegion.getName() + "   Size: " + bucketRegion.size());
    }
    System.out.println("FTableAppendDUnitTest.verifyValues :: " + "ECount: " + entriesCount);
    return entriesCount;
  }

  private void runReplicationTest(final String tableName, final int numRows,
      final int expectedKeys) {
    actualRows = 0;
    final FTableDescriptor fTableDescriptor =
        FTableDescriptorHelper.getFTableDescriptor(NUM_OF_COLUMNS);
    fTableDescriptor.setTotalNumOfSplits(NUM_OF_SPLITS);
    fTableDescriptor.setRedundantCopies(1);
    final FTable table =
        MClientCacheFactory.getAnyInstance().getAdmin().createFTable(tableName, fTableDescriptor);
    assertNotNull(table);

    Record record = new Record();

    Map<String, String> map = new LinkedMap();


    for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
      map.put(COLUMN_NAME_PREFIX + colIndex, COLUMN_NAME_PREFIX + colIndex);
    }

    for (int i = 0; i < numRows; i++) {
      table.append(record);
    }

    final int size = ((ProxyFTableRegion) table).getTableRegion().keySetOnServer().size();
    System.out.println("Keys on  server " + size);
    assertEquals(expectedKeys, size);

    verifyValuesOnAllPrimaryVMs(tableName, numRows, map);
    verifyValuesOnAllSecondaryVMs(tableName, numRows, map);
  }

  @Test
  public void testFTableRelicationSingleRow() {
    runReplicationTest(getTestMethodName(), 1, 1);
  }

  @Test
  public void testFTableRelicationSingleBlock() {
    runReplicationTest(getTestMethodName(), 1000, 1);
  }

  @Test
  public void testFTableRelicationSingleBlock2() {
    runReplicationTest(getTestMethodName(), 1999, 2);
  }

  @Test
  public void testFTableRelicationTwoBlocks() {
    runReplicationTest(getTestMethodName(), 2000, 2);
  }
}
