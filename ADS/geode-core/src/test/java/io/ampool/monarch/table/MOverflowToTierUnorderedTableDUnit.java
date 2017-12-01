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

import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.types.TypeUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;



public class MOverflowToTierUnorderedTableDUnit extends MTableOverflowToTierTest {
  private static final String SCHEMA = "{'c1':'INT','c2':'array<LONG>','c3':'STRING'}";
  private static final int NUM_OF_COLUMNS = 10;
  private static final String COLUMN_NAME_PREFIX = "OFLOW";

  @Override
  public void createMTable() {

    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    td.setRedundantCopies(0);
    td.setTotalNumOfSplits(99);
    td.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    TypeUtils.addColumnsFromJson(SCHEMA, td);
    MTable table = clientCache.getAdmin().createMTable(TABLE_NAME, td);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }


  @Override
  public void createFTable() {

    FTableDescriptor ftd = new FTableDescriptor();

    ftd.setRedundantCopies(0);
    ftd.setTotalNumOfSplits(99);

    ftd.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    TypeUtils.addColumnsFromJson(SCHEMA, ftd);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      ftd = ftd.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    FTable table = clientCache.getAdmin().createFTable(FTABLE_NAME, ftd);
    assertEquals(table.getName(), FTABLE_NAME);
    assertNotNull(table);

  }

  @Override
  public Map<Object, Object[]> putAndVerifyMTableData() {
    MTable table = clientCache.getMTable(TABLE_NAME);
    final int totalCount = 113 * 4;
    Map<Object, Object[]> data = new HashMap<>(totalCount);

    List<MColumnDescriptor> columns = table.getTableDescriptor().getAllColumnDescriptors();
    String key;
    Object[] value;
    for (int i = 0; i < totalCount; i++) {
      key = "key_" + i;
      value = new Object[columns.size()];
      Put put = new Put(key);
      data.put(key, value);
      for (int j = 0; j < columns.size(); j++) {
        value[j] = TypeUtils.getRandomValue(columns.get(j).getColumnType());
        put.addColumn(columns.get(j).getColumnNameAsString(), value[j]);
      }
      table.put(put);

    }
    return data;
  }


  @Override
  public void appendFTableData() {

    FTable table = clientCache.getFTable(FTABLE_NAME);
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
  public void verifyMTableDataUsingGet(final Map<Object, Object[]> data) {
    MTable table = clientCache.getMTable(TABLE_NAME);
    assertNotNull(table);

    Object[] value;
    for (Map.Entry<Object, Object[]> entry : data.entrySet()) {
      Row result = table.get(new Get((String) entry.getKey()));
      assertFalse(result.isEmpty());
      value = entry.getValue();
      assertNotNull(value);
      assertEquals(value.length, result.size());
      for (int j = 0; j < result.getCells().size(); j++) {
        assertEquals("Invalid data for row= " + entry.getKey() + ", column: " + j, value[j],
            result.getCells().get(j).getColumnValue());
      }
    }
  }



}
