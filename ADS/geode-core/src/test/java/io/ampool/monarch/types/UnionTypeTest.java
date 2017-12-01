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

import java.util.Arrays;
import java.util.HashMap;

import io.ampool.monarch.types.interfaces.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class UnionTypeTest {
  /**
   * Provide the variety of inputs required for testing Map type..
   *
   * @return the inputs required for {@link UnionTypeTest#testUnionObject(DataType[], Object[])}
   * @see UnionTypeTest#testUnionObject
   */
  @DataProvider
  public static Object[][] getDataUnion() {
    return new Object[][] {
        /** test Union **/
        {new DataType[] {BasicTypes.INT, new MapType(BasicTypes.STRING, BasicTypes.FLOAT)}, null},
        {new DataType[] {BasicTypes.INT, new MapType(BasicTypes.STRING, BasicTypes.FLOAT)},
            new Object[] {(byte) 1, new HashMap<String, Float>(2) {
              {
                put("String_2", 222.222f);
                put("String_1", 111.111f);
              }
            }},},
        {new DataType[] {BasicTypes.INT, BasicTypes.STRING, BasicTypes.FLOAT},
            new Object[] {(byte) 2, 111.111f}},
        {new DataType[] {BasicTypes.INT, BasicTypes.STRING, new ListType(BasicTypes.FLOAT)},
            new Object[] {(byte) 2, new Object[] {111.111f, 222.222f}}},
        {new DataType[] {BasicTypes.STRING,
            new ListType(new StructType(new String[] {"c1", "c2"},
                new DataType[] {BasicTypes.STRING, BasicTypes.DOUBLE}))},
            new Object[] {(byte) 1, new Object[] {new Object[] {"string_11", 11.111}}}},};
  }

  /**
   * Test for Union object type..
   *
   * @param columnTypes an array of column types
   * @param values an array of values corresponding to the column types
   */
  @Test(dataProvider = "getDataUnion")
  public void testUnionObject(final DataType[] columnTypes, final Object[] values) {
    final UnionType type = new UnionType(columnTypes);
    System.out.println("MUnionObjectTypeTest.testUnionObject :: " + type.toString());
    assertEquals(type.getColumnTypes(), columnTypes);
    final Object[] outputUnion = (Object[]) type.deserialize(type.serialize(values));
    if (outputUnion != null && values != null) {
      assertEquals(outputUnion.length, values.length);
    }

    /** convert input and output to deep string.. **/
    StringBuilder sbi = new StringBuilder(32);
    TypeHelper.deepToString(values, sbi);
    StringBuilder sbo = new StringBuilder(32);
    TypeHelper.deepToString(outputUnion, sbo);
    assertEquals(sbo.toString(), sbi.toString());
  }

  @DataProvider
  public static Object[][] getDataToString() {
    return new Object[][] {
        {new UnionType(new DataType[] {BasicTypes.INT, BasicTypes.STRING, BasicTypes.FLOAT}),
            "union<INT,STRING,FLOAT>"},
        {new UnionType(new DataType[] {BasicTypes.INT,
            new MapType(BasicTypes.INT, BasicTypes.STRING), BasicTypes.FLOAT}),
            "union<INT,map<INT,STRING>,FLOAT>"},
        {new UnionType(new DataType[] {BasicTypes.INT,
            new MapType(BasicTypes.INT, new ListType(BasicTypes.STRING)), BasicTypes.FLOAT}),
            "union<INT,map<INT,array<STRING>>,FLOAT>"},};
  }

  @Test(dataProvider = "getDataToString")
  public void testToString(final UnionType unionObjectType, final String typeStr) {
    assertEquals(unionObjectType.toString(), typeStr);
  }

  @DataProvider
  public static Object[][] getInvalidData() {
    return new Object[][] {{null}, {new DataType[0]},};
  }

  @DataProvider
  public static Object[][] getInvalidLength() {
    return new Object[][] {{new Object[] {(byte) 0}}, {new Object[] {(byte) 0, 10, 20}},
        {new Object[] {(byte) 2, 10}},};
  }

  @Test(expectedExceptions = AssertionError.class, dataProvider = "getInvalidLength")
  public void testSerializeIncorrectLength(final Object[] values) {
    DataType[] subTypes = new DataType[] {BasicTypes.INT, BasicTypes.STRING};
    UnionType type = new UnionType(subTypes);
    type.serialize(values);
  }

  /**
   * Test for invalid inputs..
   *
   * @param columnTypes an array of respective column types
   */
  @Test(expectedExceptions = IllegalArgumentException.class, dataProvider = "getInvalidData")
  public void testIncorrectArguments(final DataType[] columnTypes) {
    new UnionType(columnTypes);
  }

  /**
   * Test for category..
   */
  @Test
  public void testGetCategory() {
    UnionType t = new UnionType(new DataType[] {BasicTypes.INT});
    assertEquals(DataType.Category.Union, t.getCategory());
  }

  /**
   * Null predicate at the moment..
   */
  @Test
  public void testGetPredicate() {
    final DataType[] types = new DataType[] {BasicTypes.INT};
    final UnionType type = new UnionType(types);
    assertNull(type.getPredicate(CompareOp.EQUAL, new Object[0]));
  }

  /**
   * Tests for equality..
   */
  @Test
  public void testEquals() {
    final DataType[] types = new DataType[] {BasicTypes.INT, BasicTypes.STRING};
    final DataType[] types1 = new DataType[] {BasicTypes.STRING, BasicTypes.INT};
    final UnionType o1 = new UnionType(types);

    UnionType o2 = new UnionType(types);

    assertTrue(o1.equals(o1));
    assertTrue(o1.equals(o2));

    assertFalse(o1.equals(null));
    assertFalse(o1.equals("test"));
    assertFalse(o1.equals(BasicTypes.BINARY));

    o2 = new UnionType(Arrays.copyOfRange(types, 0, 1));
    assertFalse(o1.equals(o2));
    o2 = new UnionType(types1);
    assertFalse(o1.equals(o2));
  }
}
