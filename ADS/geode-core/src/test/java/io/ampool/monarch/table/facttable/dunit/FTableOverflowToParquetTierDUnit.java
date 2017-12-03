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

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.facttable.junit.FTableOverflowToTierTest;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.types.TypeUtils;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.experimental.categories.Category;

import java.io.Serializable;
import java.util.LinkedHashMap;


@Category(FTableTest.class)
public class FTableOverflowToParquetTierDUnit extends FTableOverflowToTierTest
    implements Serializable {

  private static final String SCHEMA = "{'c1':'INT','c2':'array<LONG>','c3':'STRING'}";
  private static final int NUM_OF_COLUMNS = 10;
  private static final String COLUMN_NAME_PREFIX = "OFLOW";

  public static final String STORE_NAME = "DefaultParquetStore";

  @Override
  protected void createFTableWithBlockSizeOne() {

    FTableDescriptor ftd = new FTableDescriptor();

    ftd.setRedundantCopies(0);
    ftd.setTotalNumOfSplits(1);
    ftd.setBlockSize(1);
    ftd.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    Schema.Builder sb = new Schema.Builder();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      sb.column(COLUMN_NAME_PREFIX + colmnIndex);
    }
    ftd.setSchema(sb.build());
    ftd.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    FTable table = clientCache.getAdmin().createFTable(getTableName(), ftd);
    assertEquals(table.getName(), getTableName());
    assertNotNull(table);
  }

  @Override
  public void createFTable(int numSplits) {
    // create parquet store
    createParquetStore();
    LinkedHashMap<String, TierStoreConfiguration> stores = new LinkedHashMap();
    stores.put(STORE_NAME, new TierStoreConfiguration());
    FTableDescriptor ftd = new FTableDescriptor();

    ftd.setRedundantCopies(0);
    ftd.setTotalNumOfSplits(numSplits);

    ftd.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    ftd.addTierStores(stores);

    Schema.Builder sb = new Schema.Builder();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      sb.column(COLUMN_NAME_PREFIX + colmnIndex);
    }
    ftd.setSchema(sb.build());
    ftd.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    FTable table = clientCache.getAdmin().createFTable(getTableName(), ftd);
    assertEquals(table.getName(), getTableName());
    assertNotNull(table);

  }

  @Override
  public void appendFTableData() {

    FTable table = clientCache.getFTable(getTableName());
    final int totalCount = 113 * 4;

    for (int i = 0; i < totalCount; i++) {
      Record record = new Record();
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
      }
      table.append(record);
      if (debug) {
        System.out.println("MOverflowToTierUnorderedTableDUnit.appendFTableData XXXXXXXXXX-------->"
            + i + "    " + record.toString());
      }
    }
  }

  @Override
  protected void appendOneRecordinFTable() {

    FTable table = clientCache.getFTable(getTableName());
    Record record = new Record();
    for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
      if (debug) {
        System.out.println("MOverflowToTierUnorderedTableDUnit.appendFTableData XXXXXXXXXX-------->"
            + record.toString());
      }
    }
    table.append(record);
  }

  @Override
  public void appendFTableDataforORCStore() {

    FTable table = clientCache.getFTable(getTableName());
    final int totalCount = 113 * 4 * 1000;

    for (int i = 0; i < totalCount; i++) {
      Record record = new Record();
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
      }
      table.append(record);
      if (debug) {
        System.out.println("MOverflowToTierUnorderedTableDUnit.appendFTableData XXXXXXXXXX-------->"
            + i + "    " + record.toString());
      }
    }
  }

  @Override
  public void appendFTableforWAL() {
    FTableDescriptor ftd = new FTableDescriptor();

    ftd.setRedundantCopies(0);
    ftd.setTotalNumOfSplits(2);
    actOnWal(getTableName(), 2, 0);

    ftd.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    TypeUtils.addColumnsFromJson(SCHEMA, ftd);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      ftd = ftd.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    ftd.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    FTable table = clientCache.getAdmin().createFTable(getTableName(), ftd);
    assertEquals(table.getName(), getTableName());
    assertNotNull(table);

    System.out
        .println("FTableOverflowToTierDUnit.appendFourMillionRowsinFTable   " + getTableName());
    table = clientCache.getFTable(getTableName());
    final int totalCount = 2 * 4 * 1000;

    for (int i = 0; i < totalCount; i++) {
      Record record = new Record();
      for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
        record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
      }
      table.append(record);
      if (i % 500 == 0 && i != 0) {
        if (debug) {
          System.out
              .println("MOverflowToTierUnorderedTableDUnit.appendFTableData XXXXXXXXXX-i------->"
                  + i + " getCountFromScan     " + getCountFromScan(getTableName())
                  + " getinMemoryCount     " + getinMemoryCount(getTableName())
                  + " getCountFromWalScan  " + getCountFromWalScan(getTableName()));
          System.out.println("FTableOverflowToTierDUnit.appendFTableforWAL Forcing Eviction");
        }
        if (i == 500) {
          int scanCount = getCountFromScan(getTableName());
          long memoryCount = getinMemoryCount(getTableName());
          assertEquals("Expecting all the entries to be seen from scan  ", i + 1, scanCount);
          // assertEquals("WAL should not see any entries ", 0,
          // getCountFromWalScan(getTableName()));
          // assertEquals("Total count should be same as sum of wal count and mem count", scanCount,
          // memoryCount);
        } else {
          assertEquals("Expecting all the entries to be seen from scan  ", i + 1,
              getCountFromScan(getTableName()));
          // assertEquals("Total count should be same as wal count",
          // getCountFromScan(getTableName()), getCountFromWalScan(getTableName()));
        }

        forceEvictionTask(getTableName());
        if (debug) {
          System.out
              .println("MOverflowToTierUnorderedTableDUnit.appendFTableData XXXXXXXXXX-i------->"
                  + i + " getCountFromScan     " + getCountFromScan(getTableName())
                  + " getinMemoryCount     " + getinMemoryCount(getTableName())
                  + " getCountFromWalScan  " + getCountFromWalScan(getTableName()));
        }

        try {
          System.out.println("FTableOverflowToTierDUnit.appendFTableforWAL Sleeping");
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        assertEquals("Expecting no entries to be seen from complete region  ", 0,
            getinMemoryCount(getTableName()));
        assertEquals("Expecting all the entries to be seen from scan  ", i + 1,
            getScanCount(getTableName(), i + 1));
        assertEquals("Total count should be equal to WAL entries + in memory entries",
            getCountFromScan(getTableName()), getCountFromWalScan(getTableName())
                + getCountFromScan(getTableName()) - getCountFromWalScan(getTableName()));
      }
    }
  }
}
