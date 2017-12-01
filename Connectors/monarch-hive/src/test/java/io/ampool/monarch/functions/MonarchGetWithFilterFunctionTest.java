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

package io.ampool.monarch.functions;

import io.ampool.monarch.RMIException;
import io.ampool.monarch.TestBase;
import io.ampool.monarch.hive.MonarchUtils;
import io.ampool.monarch.hive.TestHelper;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.MPredicateHolder;
import io.ampool.monarch.types.interfaces.DataType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;

/**
 * Created on: 2016-01-14
 * Since version: 0.2.0
 */
public class MonarchGetWithFilterFunctionTest extends TestBase {
  final DataType[] dataTypes = new DataType[]{
    BasicTypes.LONG, BasicTypes.STRING, BasicTypes.INT, BasicTypes.STRING
  };
  final String file = "data/sample1.txt";
  final String regionName = "region_get_all_test";
  final String[] columnNames = new String[]{"c0", "c1", "c2", "c3"};
  final Map<String, String> columnMap = new LinkedHashMap<String, String>(4) {{
    put("c0", "bigint");
    put("c1", "string");
    put("c2", "int");
    put("c3", "string");
  }};


  /**
   * Setup method.. Create the region and put data in the region.
   *
   * @throws Exception
   */
  @BeforeClass
  private void setUpMethod() throws Exception {
//    System.out.println("MonarchGetWithFilterFunctionTest.setUpMethod");
    Map<String, String> confMap = new HashMap<String, String>(1) {{ put(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());}};
    MonarchUtils.createConnectionAndTable(regionName, confMap, false, null, columnMap);
    putDataInTable(regionName);
  }

  /**
   * Clean-up method.. Destroy the region on server. It should delete the data as well.
   * @throws Exception
   */
  @AfterClass
  public void cleanUpMethod() throws Exception {
//    System.out.println("MonarchGetWithFilterFunctionTest.cleanUpMethod");
    MonarchUtils.destroyTable(regionName, Collections.emptyMap(), false, true);
  }

  /**
   * Put the specified data in region as 2-D byte array (byte[][]).
   *
   * @throws IOException
   * @throws RMIException
   */
  private void putDataInTable(final String tableName) throws IOException, RMIException {
    List<String> lines = TestHelper.getResourceAsString(file);
    List<Put> putList = new ArrayList<>(lines.size());

    for (int rowId = 0; rowId < lines.size(); rowId++) {
      putList.add(new Put("row__" + rowId));
      int i = 0;
      for (String col : lines.get(rowId).split(",")) {
        putList.get(rowId).addColumn(columnNames[i], getObjectFromString(dataTypes[i++], col));
      }
    }
    MCacheFactory.getAnyInstance().getMTable(tableName).put(putList);
  }

  /**
   * Convert the column value from string to correct type and return.
   *
   * @param dataType the data type of the column
   * @param colValue the column value as string
   * @return the column value converted to correct type
   */
  private Object getObjectFromString(final DataType dataType, final String colValue) {
    if (BasicTypes.INT.equals(dataType)) {
      return Integer.valueOf(colValue);
    } else if (BasicTypes.LONG.equals(dataType)) {
      return Long.valueOf(colValue);
    } else {
      return colValue;
    }
  }

  @DataProvider
  public Object[][] getData() {
    return new Object[][]{
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.GREATER_OR_EQUAL, 0), 31},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.LESS_OR_EQUAL, 0), 0},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.GREATER, Integer.MAX_VALUE), 0},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.LESS, Integer.MAX_VALUE), 31},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.LESS_OR_EQUAL, 4096), 11},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.EQUAL, 4096), 12},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.NOT_EQUAL, 4096), 19},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.LESS_OR_EQUAL, 4096), 23},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.GREATER_OR_EQUAL, 0L), 31},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.LESS_OR_EQUAL, 0L), 0},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.GREATER, Long.MAX_VALUE), 0},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.LESS, Long.MAX_VALUE), 31},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.EQUAL, 12978724L), 1},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.NOT_EQUAL, 12978724L), 30},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.EQUAL, 12978724123L), 0},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.NOT_EQUAL, 12978724123L), 31},
      {new MPredicateHolder(3, BasicTypes.STRING, CompareOp.EQUAL, "target/warehouse"), 1},
      {new MPredicateHolder(3, BasicTypes.STRING, CompareOp.NOT_EQUAL, "target/warehouse"), 30},
      {new MPredicateHolder(1, BasicTypes.STRING, CompareOp.EQUAL, "-rw-rw-r--"), 19},
      {new MPredicateHolder(1, BasicTypes.STRING, CompareOp.NOT_EQUAL, "-rw-rw-r--"), 12},
      {new MPredicateHolder(1, BasicTypes.STRING, CompareOp.EQUAL, "drwxrwxr-x"), 12},
      {new MPredicateHolder(1, BasicTypes.STRING, CompareOp.REGEX, "d.wxr[xw]{2}[r]-x"), 12},
      {new MPredicateHolder(3, BasicTypes.STRING, CompareOp.REGEX, ".*spark.*"), 6},
    };
  }

  /**
   * Test various predicate-operations with pre-existing data-set. The data-provider
   *    provides the data as well as the output count to assert on.
   * The predicate contains:
   *    - the column-id on which to execute the predicate
   *    - the argument type {@link BasicTypes}
   *    - the predicate operation {@link CompareOp} like <, <=, >, >=, == !=
   *    - the initial value to be compared with other values
   *
   * @param ph the {@link MPredicateHolder} containing the required predicate data
   * @param expectedResultCount the expected result count after executing the specified predicate
   * @throws RMIException
   * @throws IOException
   */
  @Test(dataProvider = "getData")
  public void test_Predicates(final MPredicateHolder ph,
                              final int expectedResultCount) throws RMIException, IOException {
//    System.out.println("MonarchGetWithFilterFunctionTest.test_Predicates :: " + ph.toString()
//      + " -- expectedResultCount= " + expectedResultCount);
    MTable mTable = MCacheFactory.getAnyInstance().getMTable(regionName);
    final Object[] args = new Object[]{mTable.getName(), new MPredicateHolder[]{ph}, null};
    Collection o = MonarchUtils.getWithFilter(mTable, Collections.emptySet(), args);
    assertEquals(o.size(), expectedResultCount);
  }

  /**
   * Test for function execution with filter and single predicate.
   * @throws RMIException
   * @throws IOException
   */
  @Test
  public void testFunctionWithFilter() throws RMIException, IOException {
//    System.out.println("MonarchGetWithFilterFunctionTest.testFunctionWithFilter");
    MPredicateHolder ph = new MPredicateHolder(2, BasicTypes.INT, CompareOp.GREATER_OR_EQUAL, 0);
    MTable mTable = MCacheFactory.getAnyInstance().getMTable(regionName);
    Set<MTableKey> keySet = new HashSet<MTableKey>(2) {{
      add(new MTableKey("row__19".getBytes()));
      add(new MTableKey("row__29".getBytes()));
    }};
    final Object[] args = new Object[]{mTable.getName(), new MPredicateHolder[]{ph}, null};
    Collection o = MonarchUtils.getWithFilter(mTable, keySet, args);
    assertEquals(o.size(), 2);
  }

  /**
   * Test for function execution with filter but without predicates.
   * @throws RMIException
   * @throws IOException
   */
  @Test
  public void testFunctionWithFilter_1() throws RMIException, IOException {
//    System.out.println("MonarchGetWithFilterFunctionTest.testFunctionWithFilter");
    MTable mTable = MCacheFactory.getAnyInstance().getMTable(regionName);
    Set<MTableKey> keySet = new HashSet<MTableKey>(2) {{
      add(new MTableKey("row__19".getBytes()));
      add(new MTableKey("row__29".getBytes()));
    }};
    final Object[] args = new Object[]{mTable.getName(), null, null};
    Collection o = MonarchUtils.getWithFilter(mTable, keySet, args);
    assertEquals(o.size(), 2);
  }
}
