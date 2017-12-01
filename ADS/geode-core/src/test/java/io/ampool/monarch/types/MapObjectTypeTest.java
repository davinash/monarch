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

import io.ampool.monarch.types.interfaces.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.*;

/**
 * Tests for Map object type..
 * <p>
 */
public class MapObjectTypeTest {
  private static void putInMap(final Map<Object, Object> map, final Object[] array) {
    if (array.length % 2 != 0) {
      throw new IllegalArgumentException("Array of incorrect size.");
    }
    for (int i = 0; i < array.length; i += 2) {
      map.put(array[i], array[i + 1]);
    }
  }

  private static Map<Object, Object> getMapFromArray(final Object... array) {
    Map<Object, Object> map = new HashMap<>(array.length / 2);
    putInMap(map, array);
    return map;
  }

  /**
   * Provide the variety of inputs required for testing Map type..
   *
   * @return the inputs required for {@link MapObjectTypeTest#testMapObject}
   * @see MapObjectTypeTest#testMapObject
   */
  @DataProvider
  public static Object[][] getObjectMap() {
    return new Object[][] {
        /** test MAP using List **/
        {BasicTypes.INT, BasicTypes.STRING, null, "null"},
        {BasicTypes.INT, BasicTypes.STRING, getMapFromArray(1, "One", 2, "Two"), "{1=One, 2=Two}"},
        {BasicTypes.INT, new MapType(BasicTypes.INT, BasicTypes.STRING),
            getMapFromArray(1, getMapFromArray(1, "One", 2, "Two"), 2,
                getMapFromArray(1, "One", 2, "Two")),
            "{1={1=One, 2=Two}, 2={1=One, 2=Two}}"},
        {BasicTypes.INT, new ListType(BasicTypes.STRING),
            getMapFromArray(1, Arrays.asList("One", "Two"), 2, Arrays.asList("Three", "Four")),
            "{1=[One, Two], 2=[Three, Four]}"},
        {BasicTypes.STRING, new ListType(BasicTypes.FLOAT),
            getMapFromArray("Key_1", Arrays.asList(1.1f, 2.2f), "Key_2", Arrays.asList(3.3f, 4.4f)),
            "{Key_1=[1.1, 2.2], Key_2=[3.3, 4.4]}"},};
  }

  /**
   * Test for Map object type.
   *
   * @param keyType the object type of the element
   * @param values the list of input objects
   * @param expectedStrOutput expected string representation of the input list
   */
  @Test(dataProvider = "getObjectMap")
  public void testMapObject(final DataType keyType, final DataType valueType, final Object values,
      final String expectedStrOutput) {
    MapType mapObjectType = new MapType(keyType, valueType);
    assertEquals(mapObjectType.getTypeOfKey().toString(), keyType.toString());
    assertEquals(mapObjectType.getTypeOfValue().toString(), valueType.toString());
    System.out.println("MapObjectTypeTest.testMapObject :: " + mapObjectType);
    Map outputMap = (Map) mapObjectType.deserialize(mapObjectType.serialize(values));
    assertEquals(String.valueOf(outputMap), expectedStrOutput);
  }

  /**
   * Test for category..
   */
  @Test
  public void testGetCategory() {
    MapType t = new MapType(BasicTypes.INT, BasicTypes.STRING);
    assertEquals(DataType.Category.Map, t.getCategory());
  }

  /**
   * Null predicate at the moment..
   */
  @Test
  public void testGetPredicate() {
    final MapType t = new MapType(BasicTypes.INT, BasicTypes.STRING);
    assertNull(t.getPredicate(CompareOp.EQUAL, new Object[0]));
  }

  /**
   * Tests for equality..
   */
  @Test
  public void testEquals() {
    final MapType o1 = new MapType(BasicTypes.INT, BasicTypes.STRING);

    MapType o2 = new MapType(BasicTypes.INT, BasicTypes.STRING);

    assertTrue(o1.equals(o1));
    assertTrue(o1.equals(o2));

    assertFalse(o1.equals(null));
    assertFalse(o1.equals("test"));
    assertFalse(o1.equals(BasicTypes.BINARY));

    o2 = new MapType(BasicTypes.STRING, BasicTypes.INT);
    assertFalse(o1.equals(o2));
  }

  /**
   * Test serialization/deserialization of complex object chains.. like map of array of struct
   * having array of map
   */
  @Test
  public void testMapWithArrayAndList() {
    String type = "map<INT,ARRAY<STRUCT<CITY:STRING,PIN:INT,RANDOM:ARRAY<MAP<INT,INT>>>>>";
    DataType type1 = DataTypeFactory.getTypeFromString(type);

    Map<Integer, List<Object[]>> map = new LinkedHashMap<>();
    List<Object[]> list = new ArrayList<>();

    List<Map<Integer, Integer>> innerList1 = new ArrayList<>();
    List<Map<Integer, Integer>> innerList2 = new ArrayList<>();

    Map<Integer, Integer> innerMap1 = new LinkedHashMap<>();
    Map<Integer, Integer> innerMap2 = new LinkedHashMap<>();
    Map<Integer, Integer> innerMap3 = new LinkedHashMap<>();


    Object[] arr = new Object[3];
    Object[] arr1 = new Object[3];

    innerMap1.put(12, 24);
    innerMap1.put(13, 26);

    innerMap2.put(4, 2);
    innerMap2.put(6, 3);


    innerMap3.put(5, 25);
    innerMap3.put(6, 36);

    innerList1.add(innerMap1);
    innerList1.add(innerMap2);

    innerList2.add(innerMap2);
    innerList2.add(innerMap3);

    arr[0] = "PUNE";
    arr[1] = 411001;
    arr[2] = innerList1;

    arr1[0] = "DELHI";
    arr1[1] = 100001;
    arr1[2] = innerList2;

    list.add(arr);
    list.add(arr1);

    map.put(13, list);

    final Map outputMap = (Map) type1.deserialize(type1.serialize(map));

    /** assert on the output.. **/
    assertEquals(outputMap.keySet(), map.keySet());
    for (final Object outputValue : outputMap.values()) {
      assertTrue(outputValue instanceof List);
      int i = 0;
      for (final Object obj : ((List) outputValue)) {
        assertTrue(Arrays.deepEquals((Object[]) obj, list.get(i++)));
      }
    }
  }
}
