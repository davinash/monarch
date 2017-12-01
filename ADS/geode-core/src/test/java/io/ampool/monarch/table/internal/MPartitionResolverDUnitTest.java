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
package io.ampool.monarch.table.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.test.junit.categories.FTableTest;
import io.ampool.monarch.AbstractTestDUnitBase;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.types.TypeUtils;
import io.ampool.monarch.types.TypeHelper;
import org.junit.experimental.categories.Category;

@Category(FTableTest.class)
public class MPartitionResolverDUnitTest extends AbstractTestDUnitBase {
  private static final String SCHEMA = "{'c1':'INT','c2':'array<LONG>','c3':'STRING'}";
  private static final String TABLE_NAME = "MPartitionResolverTable";
  private static final int BUCKET_COUNT = 8;
  private static boolean usePartitioningColumn = true;
  private static int[] rowHashCodes;

  /**
   * Create an FTable.
   *
   * @return the table with specified inputs
   */
  private static FTable createTable(final String partitionCol) {
    FTableDescriptor td = new FTableDescriptor();
    td.setRedundantCopies(1);
    td.setTotalNumOfSplits(BUCKET_COUNT);
    TypeUtils.addColumnsFromJson(SCHEMA, td);
    td.setPartitionResolver(MPartitionResolver.DEFAULT_RESOLVER);
    if (usePartitioningColumn) {
      td.setPartitioningColumn(partitionCol);
    }
    if (clientCache.getAdmin().existsFTable(TABLE_NAME)) {
      clientCache.getAdmin().deleteFTable(TABLE_NAME);
    }
    return clientCache.getAdmin().createFTable(TABLE_NAME, td);
  }

  /**
   * Append data to the table and return the data that was inserted. Append some data as single
   * record and rest as a batch.
   *
   * @return the data that was inserted/appended to the table
   */
  public static Object[][] appendData() {
    FTable table = clientCache.getFTable(TABLE_NAME);
    final int totalCount = BUCKET_COUNT * 4;
    Object[][] data = new Object[totalCount][];

    List<MColumnDescriptor> columns = table.getTableDescriptor().getAllColumnDescriptors();
    rowHashCodes = new int[totalCount];
    Record[] records = new Record[BUCKET_COUNT * 3];
    for (int i = 0; i < totalCount; i++) {
      data[i] = new Object[columns.size()];
      Record record = new Record();
      for (int j = 0; j < columns.size(); j++) {
        if (!FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME
            .equals(columns.get(j).getColumnNameAsString())) {
          data[i][j] = TypeUtils.getRandomValue(columns.get(j).getColumnType());
          record.add(columns.get(j).getColumnNameAsString(), data[i][j]);
        }
      }
      rowHashCodes[i] = record.getValueMap().hashCode();
      if (i < BUCKET_COUNT) {
        table.append(record);
      } else {
        records[i - BUCKET_COUNT] = record;
      }
    }
    table.append(records);
    return data;
  }

  /**
   * Do a full-table scan and verify that the total count is as expected.
   *
   * @param data the data that was inserted
   */
  private static void scanAndVerify(final Object[][] data) {
    FTable table = clientCache.getFTable(TABLE_NAME);

    Scan scan = new Scan();
    Scanner scanner = table.getScanner(scan);

    int count = 0;
    for (final Row ignored : scanner) {
      count++;
    }
    assertEquals("Incorrect number of results from scan.", data.length, count);
  }

  /**
   * Verify that the key distribution has happened as expected.
   *
   * @param data the data that was inserted
   */
  private void verifyKeyDistribution(final Object[][] data, final String partitionCol) {
    FTable table = clientCache.getFTable(TABLE_NAME);

    int partColId = -1;
    if (usePartitioningColumn) {
      int i = 0;
      for (MColumnDescriptor cd : table.getTableDescriptor().getAllColumnDescriptors()) {
        if (partitionCol.equals(cd.getColumnNameAsString())) {
          partColId = i;
          break;
        }
        i++;
      }
      assertNotEquals("Incorrect partitioning column.", -1, partColId);
    }

    Map<Integer, List<String>> bucketIdToRowMap = new HashMap<>(BUCKET_COUNT);
    int bid;
    List<String> list;
    int i = 0;
    for (Object[] row : data) {
      bid = PartitionedRegionHelper.getHashKey(
          usePartitioningColumn ? MTableUtils.getDeepHashCode(row[partColId]) : rowHashCodes[i++],
          BUCKET_COUNT);
      list = bucketIdToRowMap.get(bid);
      if (list == null) {
        list = new ArrayList<>(10);
        bucketIdToRowMap.put(bid, list);
      }
      list.add(TypeHelper.deepToString(row));
    }

    Map<Integer, Pair<ServerLocation, Long>> primaryMap = new HashMap<>(100);
    MTableUtils.getLocationAndCount(table, primaryMap, null);
    for (Map.Entry<Integer, List<String>> entry : bucketIdToRowMap.entrySet()) {
      assertEquals("Incorrect number of records in bucketId=" + entry.getKey(),
          entry.getValue().size(), primaryMap.get(entry.getKey()).getSecond().intValue());
    }

    /**
     * verification of actual data.. since scan on FTable with specific buckets is not working yet,
     * no verification on data is there..
     */
    Map<Integer, Set<ServerLocation>> bucketMap = new HashMap<>(BUCKET_COUNT);
    MTableUtils.getSplits(table.getName(), BUCKET_COUNT, BUCKET_COUNT, bucketMap);

    System.out.println("bucketMap = " + bucketMap);
    Map<Integer, Set<ServerLocation>> primaryBucketMap = new LinkedHashMap<>(BUCKET_COUNT);
    bucketMap.entrySet().forEach(entry -> {
      primaryBucketMap.put(entry.getKey(),
          entry.getValue() == null || entry.getValue().isEmpty() ? null : entry.getValue());
    });

    Scan scan;
    List<Cell> cells;
    List<String> actualData = new ArrayList<>(data.length);
    List<String> expectedData;
    Object[] values = new Object[data[0].length];
    final byte[] INSERTION_COLUMN = FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME.getBytes();
    for (Map.Entry<Integer, List<String>> entry : bucketIdToRowMap.entrySet()) {
      actualData.clear();
      bid = entry.getKey();
      expectedData = entry.getValue();

      scan = new Scan();
      scan.setBucketIds(Collections.singleton(bid));
      scan.setBucketToServerMap(primaryBucketMap);
      for (Row result : table.getScanner(scan)) {
        cells = result.getCells();
        for (int j = 0; j < cells.size(); j++) {
          /** specific handling for insertion-timestamp.. i.e. column-0; don't compare **/
          if (Arrays.equals(cells.get(j).getColumnName(), INSERTION_COLUMN)) {
            continue;
          }
          values[j] = cells.get(j).getColumnValue();
        }
        actualData.add(TypeHelper.deepToString(values));
      }
      assertEquals("Incorrect number of records found for bucketId=" + bid, expectedData.size(),
          actualData.size());
      Collections.sort(actualData);
      Collections.sort(expectedData);
      assertEquals("Incorrect data found for bucketId=" + bid, expectedData.toString(),
          actualData.toString());
    }

  }

  // @Test
  public void testPartitionResolverBasicWithPartitioningColumn() {
    System.out
        .println("MPartitionResolverDUnitTest.testPartitionResolverBasicWithPartitioningColumn");
    usePartitioningColumn = true;

    createTable("c1");

    Object[][] data = appendData();

    scanAndVerify(data);

    verifyKeyDistribution(data, "c1");
  }

  // @Test
  public void testPartitionResolverBasicWithNoPartitioningColumn() {
    System.out
        .println("MPartitionResolverDUnitTest.testPartitionResolverBasicWithNoPartitioningColumn");
    usePartitioningColumn = false;

    createTable("c1");

    Object[][] data = appendData();

    scanAndVerify(data);

    verifyKeyDistribution(data, "c2");
  }

  // @Test
  public void testPartitionResolverBasicWithPartitioningColumnArray() {
    System.out.println(
        "MPartitionResolverDUnitTest.testPartitionResolverBasicWithPartitioningColumnArray");
    usePartitioningColumn = true;

    createTable("c2");

    Object[][] data = appendData();

    scanAndVerify(data);

    verifyKeyDistribution(data, "c2");
  }
}
