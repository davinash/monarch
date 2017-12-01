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
import java.util.List;

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.types.interfaces.DataType;
import org.junit.experimental.categories.Category;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for List (Array) type.
 *
 */
@Category(MonarchTest.class)
public class ListTypeTest {

  @DataProvider
  public static Object[][] getObjectList() {
    return new Object[][] {
        /** test LIST using List **/
        {BasicTypes.INT, null, "null"},
        {BasicTypes.INT, Arrays.asList(1, 2, null, 4, 5), "[1, 2, null, 4, 5]"},
        {BasicTypes.BOOLEAN, Arrays.asList(true, false, true, true), "[true, false, true, true]"},
        {BasicTypes.FLOAT, Arrays.asList(10.52f, 1.2f, 3.4f), "[10.52, 1.2, 3.4]"},
        {BasicTypes.DOUBLE, Arrays.asList(10.52, 7.89, 3.4034), "[10.52, 7.89, 3.4034]"},
        {BasicTypes.BINARY,
            Arrays.asList(new byte[] {0, 1, 2}, new byte[0], null, new byte[] {127, 126, 125}),
            "[[0, 1, 2], [], null, [127, 126, 125]]"},
        {BasicTypes.STRING,
            Arrays.asList("One", "Two", null, "TenThousandFiveHundredAndSomething", "", "K"),
            "[One, Two, null, TenThousandFiveHundredAndSomething, , K]"},
        {new ListType(BasicTypes.INT), Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4, 5)),
            "[[1, 2], [3, 4, 5]]"},
        {new ListType(new ListType(BasicTypes.INT)),
            Arrays.asList(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4, 5)),
                Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4, 5))),
            "[[[1, 2], [3, 4, 5]], [[1, 2], [3, 4, 5]]]"},
        /** test LIST using arrays **/
        {BasicTypes.INT, new Integer[] {1, 2, null, 4, 5}, "[1, 2, null, 4, 5]"},
        {new ListType(new ListType(BasicTypes.INT)),
            new Object[] {new Object[] {new Integer[] {1, 2}, new Integer[] {3, 4, 5}},
                new Object[] {new Integer[] {1, 2}, new Integer[] {3, 4, 5}}},
            "[[[1, 2], [3, 4, 5]], [[1, 2], [3, 4, 5]]]"},
        {new StructType(new String[] {"c1", "c2"},
            new DataType[] {BasicTypes.INT, BasicTypes.FLOAT}),
            Arrays.asList(new Object[] {11, 11.111f}, new Object[] {22, 22.222f}),
            "[[11, 11.111], [22, 22.222]]"}};
  }

  /**
   * Test for List object type.
   *
   * @param objectType the object type of the element
   * @param values the list of input objects
   * @param expectedStrOutput expected string representation of the input list
   * @throws Exception
   */
  @Test(dataProvider = "getObjectList")
  public void testSerialize(final DataType objectType, final Object values,
      final String expectedStrOutput) throws Exception {
    ListType listObjectType = new ListType(objectType);
    System.out.println("MListObjectTypeTest.testSerialize :: " + listObjectType.toString());
    assertEquals(listObjectType.getTypeOfElement(), objectType);
    List outputList = (List) listObjectType.deserialize(listObjectType.serialize(values));
    final StringBuilder sb = new StringBuilder(32);
    TypeHelper.deepToString(outputList, sb);
    assertEquals(sb.toString(), expectedStrOutput);
  }

  /**
   * Test for category..
   */
  @Test
  public void testGetCategory() {
    ListType t = new ListType(BasicTypes.INT);
    assertEquals(DataType.Category.List, t.getCategory());
  }

  /**
   * Null predicate at the moment..
   */
  @Test
  public void testGetPredicate() {
    ListType t = new ListType(BasicTypes.INT);
    assertNull(t.getPredicate(CompareOp.EQUAL, new Object[0]));
  }

  /**
   * Tests for equality..
   */
  @Test
  public void testEquals() {
    final ListType o1 = new ListType(BasicTypes.INT);

    ListType o2 = new ListType(BasicTypes.INT);

    assertTrue(o1.equals(o1));
    assertTrue(o1.equals(o2));

    assertFalse(o1.equals(null));
    assertFalse(o1.equals("test"));
    assertFalse(o1.equals(BasicTypes.BINARY));

    o2 = new ListType(BasicTypes.FLOAT);
    assertFalse(o1.equals(o2));
  }
}
