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

import io.ampool.monarch.types.BasicTypes;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class MTableTransactionJUnitTest {
  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {}

  @After
  public void tearDown() throws Exception {}

  private MCache createCache() {
    return new MCacheFactory(createLonerProperties()).create();
  }

  private Properties createLonerProperties() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    return props;
  }

  private MTableType[] tableType() {
    return new MTableType[] {MTableType.ORDERED_VERSIONED, MTableType.UNORDERED};
  }

  @Test
  @Parameters(method = "tableType")
  public void testMTableTransaction(MTableType tableType) throws Exception {

    String tableName = "MTABLE_TX_TEST";
    if (tableType == MTableType.UNORDERED) {
      tableName += "_UNORDERED";
    }
    MCache geodeCache = createCache();
    MCache ampoolCache = MCacheFactory.getAnyInstance();
    final int NUM_OF_ROWS = 1000;

    MTableDescriptor tableDescriptor = new MTableDescriptor(tableType);
    for (int colIdx = 0; colIdx < 5; colIdx++) {
      tableDescriptor.addColumn("Column-" + colIdx, BasicTypes.STRING);
    }
    assertNotNull(MCacheFactory.getAnyInstance());
    MTable table = ampoolCache.getAdmin().createMTable(tableName, tableDescriptor);

    CacheTransactionManager cacheTransactionManager = ampoolCache.getCacheTransactionManager();
    cacheTransactionManager.begin();
    for (int rowKey = 0; rowKey < NUM_OF_ROWS; rowKey++) {
      Put row = new Put(Bytes.toBytes(rowKey));
      for (int colIdx = 0; colIdx < 5; colIdx++) {
        row.addColumn("Column-" + colIdx, Bytes.toBytes("Value-" + colIdx));
      }
      table.put(row);
    }
    cacheTransactionManager.commit();

    for (int rowKey = 0; rowKey < NUM_OF_ROWS; rowKey++) {
      Get get = new Get(Bytes.toBytes(rowKey));
      Row row = table.get(get);
      assertFalse(row.isEmpty());
      List<Cell> cells = row.getCells();
      assertEquals(6, cells.size());

      for (int colIdx = 0; colIdx < cells.size() - 1; colIdx++) {
        Cell cell = cells.get(colIdx);
        assertEquals(0, Bytes.compareTo(cell.getColumnName(), Bytes.toBytes("Column-" + colIdx)));
        assertEquals(cell.getColumnValue(), "Value-" + colIdx);
      }
    }
    ampoolCache.getAdmin().deleteMTable(tableName);
  }
}
