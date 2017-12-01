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

package io.ampool.monarch.types;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;

import io.ampool.monarch.types.interfaces.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for Struct type..
 * <p>
 */
public class StructTypeTest {
  /**
   * Provide the variety of inputs required for testing Map type..
   *
   * @return the inputs required for {@link MapObjectTypeTest#testMapObject}
   * @see MapObjectTypeTest#testMapObject
   */
  @DataProvider
  public static Object[][] getDataStruct() {
    return new Object[][] {
        /** test Struct **/
        {new String[] {"c1", "c2"},
            new DataType[] {BasicTypes.INT, new MapType(BasicTypes.STRING, BasicTypes.FLOAT)},
            null},
        {new String[] {"c1", "c2"},
            new DataType[] {BasicTypes.INT, new MapType(BasicTypes.STRING, BasicTypes.FLOAT)},
            new Object[] {11, new HashMap<String, Float>(2) {
              {
                put("String_2", 222.222f);
                put("String_1", 111.111f);
              }
            }},},
        {new String[] {"c1", "c2", "c3"},
            new DataType[] {BasicTypes.INT, BasicTypes.BIG_DECIMAL, BasicTypes.FLOAT},
            new Object[] {11, new BigDecimal("12.345678"), 111.111f}},
        {new String[] {"c1", "c2", "c3"},
            new DataType[] {BasicTypes.INT, BasicTypes.STRING, BasicTypes.FLOAT},
            new Object[] {11, "String_1", 111.111f}},
        {new String[] {"c1", "c2", "c3"},
            new DataType[] {BasicTypes.INT, BasicTypes.STRING, new ListType(BasicTypes.FLOAT)},
            new Object[] {11, "String_1", new Object[] {111.111f, 222.222f}}},
        {new String[] {"c1", "c2"},
            new DataType[] {BasicTypes.STRING,
                new ListType(new MapType(BasicTypes.STRING, BasicTypes.DOUBLE))},
            new Object[] {"String_1", new Object[] {new HashMap<String, Double>(2) {
              {
                put("String_11", 111.111);
                put("String_12", 111.222);
              }
            }, new HashMap<String, Double>(2) {
              {
                put("String_21", 222.111);
                put("String_22", 222.222);
              }
            },}}},};
  }

  /**
   * Test for Struct object type..
   *
   * @param columnNames an array of column names
   * @param columnTypes an array of column types
   * @param values an array of values corresponding to the column types
   */
  @Test(dataProvider = "getDataStruct")
  public void testStructObject(final String[] columnNames, final DataType[] columnTypes,
      final Object[] values) {
    final StructType type = new StructType(columnNames, columnTypes);
    assertEquals(columnNames, type.getColumnNames());
    System.out.println("MStructObjectTypeTest.testStructObject :: " + type);
    final Object[] outputStruct = (Object[]) type.deserialize(type.serialize(values));
    if (outputStruct != null && values != null) {
      assertEquals(outputStruct.length, values.length);
    }
    assertEquals(Arrays.deepToString(outputStruct), Arrays.deepToString(values));
  }

  @DataProvider
  public static Object[][] getDataToString() {
    return new Object[][] {{
        new StructType(new String[] {"c1", "c2", "c3"},
            new DataType[] {BasicTypes.INT, BasicTypes.STRING, BasicTypes.FLOAT}),
        "struct<c1:INT,c2:STRING,c3:FLOAT>"}, {
            new StructType(new String[] {"c1", "c2", "c3"}, new DataType[] {BasicTypes.INT,
                new MapType(BasicTypes.INT, BasicTypes.STRING), BasicTypes.FLOAT}),
            "struct<c1:INT,c2:map<INT,STRING>,c3:FLOAT>"},};
  }

  @Test(dataProvider = "getDataToString")
  public void testToString(final StructType structObjectType, final String typeStr) {
    assertEquals(structObjectType.toString(), typeStr);
  }

  @DataProvider
  public static Object[][] getInvalidData() {
    return new Object[][] {{new String[0], new DataType[0]},
        {new String[0], new DataType[] {BasicTypes.INT}},
        {new String[] {"c1", "c2"}, new DataType[] {BasicTypes.INT}},
        {new String[] {"c1"}, new DataType[] {BasicTypes.INT, BasicTypes.STRING}},};
  }

  /**
   * Test for invalid inputs..
   *
   * @param columnNames an array of column names
   * @param columnTypes an array of respective column types
   */
  @Test(expectedExceptions = IllegalArgumentException.class, dataProvider = "getInvalidData")
  public void testIncorrectArguments(final String[] columnNames, final DataType[] columnTypes) {
    new StructType(columnNames, columnTypes);
  }

  /**
   * Test for category..
   */
  @Test
  public void testGetCategory() {
    StructType t = new StructType(new String[] {"c1"}, new DataType[] {BasicTypes.INT});
    assertEquals(DataType.Category.Struct, t.getCategory());
  }

  /**
   * Null predicate at the moment..
   */
  @Test
  public void testGetPredicate() {
    final DataType[] types = new DataType[] {BasicTypes.INT};
    final StructType type = new StructType(new String[] {"c1"}, types);
    assertNull(type.getPredicate(CompareOp.EQUAL, new Object[0]));
  }

  /**
   * Tests for equality..
   */
  @Test
  public void testEquals() {
    final String[] names = new String[] {"c1", "c2"};
    final String[] names1 = new String[] {"c2", "c1"};
    final DataType[] types = new DataType[] {BasicTypes.INT, BasicTypes.STRING};
    final DataType[] types1 = new DataType[] {BasicTypes.STRING, BasicTypes.INT};
    final StructType o1 = new StructType(names, types);

    StructType o2 = new StructType(names, types);

    assertTrue(o1.equals(o1));
    assertTrue(o1.equals(o2));

    assertFalse(o1.equals(null));
    assertFalse(o1.equals("test"));
    assertFalse(o1.equals(BasicTypes.BINARY));

    o2 = new StructType(Arrays.copyOfRange(names, 0, 1), Arrays.copyOfRange(types, 0, 1));
    assertFalse(o1.equals(o2));
    o2 = new StructType(names1, types);
    assertFalse(o1.equals(o2));
    o2 = new StructType(names, types1);
    assertFalse(o1.equals(o2));
  }
}
