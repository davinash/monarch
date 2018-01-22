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

import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableColumnType;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.utils.ReflectionUtils;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static io.ampool.monarch.types.DataTypeFactory.getTypeFromString;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;

@Category(MonarchTest.class)
public class TypeUtilsTest {
  /**
   * Assert that construction cannot be instantiated.. even via reflection.
   */
  @Test
  public void testDummyConstructor() {
    try {
      Constructor<?> c;
      c = Class.forName("io.ampool.monarch.types.TypeUtils").getDeclaredConstructor();
      c.setAccessible(true);
      c.newInstance();
      fail("Expected exception.");
    } catch (InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    } catch (Exception e) {
      fail("This exception was not expected here: " + e.getMessage());
    }
  }

  /**
   * Test for unknown type -- make sure that exception is thrown.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidType() {
    final DataType unknownType = new DataType() {
      @Override
      public Object deserialize(byte[] bytes, Integer offset, Integer length) {
        return null;
      }

      @Override
      public byte[] serialize(Object object) {
        return new byte[0];
      }

      @Override
      public Category getCategory() {
        return null;
      }
    };
    try {
      TypeUtils.jsonToObject("junk", unknownType);
      fail("Not expected here.");
    } catch (JSONException je) {
      fail("No JSONExpected.");
    }
  }

  /**
   * Table schema to be used for populating/testing JSON.
   */
  private static final String[] COLUMN_TYPES = {"BINARY", "INT", "LONG", "FLOAT", "DOUBLE",
          "STRING", "BIG_DECIMAL(13,13)", "BYTE", "SHORT", "BOOLEAN", "CHAR", "VARCHAR", "DATE",
          "TIMESTAMP", "CHARS", "array<INT>", "map<STRING,DOUBLE>", "struct<c1:DATE,c2:SHORT,c3:FLOAT>",
          "union<BOOLEAN,BYTE,BIG_DECIMAL>",};

  /**
   * Helper method to populate the value-map for the specified schema.
   *
   * @param expectedMap the expected values map
   * @param isNull true if the null values is requested; false otherwise
   * @return the column value map
   * @throws JSONException
   */
  private Map<String, Object> populateValues(final Map<String, String> expectedMap,
                                             final boolean isNull) throws JSONException {
    StringBuilder sb = new StringBuilder(100);
    final MTableDescriptor td = new MTableDescriptor();
    sb.append('{');
    String name;
    Object value;
    DataType type;
    for (final String typeStr : COLUMN_TYPES) {
      name = typeStr.replaceAll("\\W+", "_");
      type = getTypeFromString(typeStr);
      value = isNull ? null : TypeUtils.getRandomValue(type);

      td.addColumn(name, new MTableColumnType(type));

      sb.append(JSONObject.quote(name)).append(':');
      TypeHelper.deepToString(value, sb, true);
      sb.append(',');
      expectedMap.put(name, TypeHelper.deepToString(value));
    }
    if (sb.length() > 1) {
      sb.deleteCharAt(sb.length() - 1);
    }
    sb.append('}');

    return TypeUtils.getValueMap(td, sb.toString());
  }

  /**
   * Simple test for validating the inputs without asserts.
   *
   * @throws JSONException
   */
  @Test
  public void testGetValueMap_Simple() throws JSONException {
    final String[][] COLUMNS = {{"ID", "LONG"}, {"MY_MAP", "map<STRING,array<DOUBLE>>"}};
    final String[] VALUES = {"{'ID': '11', 'MY_MAP': {'ABC' : ['11.111', '11.222']}}",
            "{'ID': '22', 'MY_MAP': {'PQR' : ['22.111', '22.222']}}",};
    MTableDescriptor td = new MTableDescriptor();
    TypeUtils.addColumnsFromJson(TypeUtils.getJsonSchema(COLUMNS), td);

    TypeUtils.getValueMap(td, VALUES[0]);
    TypeUtils.getValueMap(td, VALUES[1]);
  }

  /**
   * Simple test for validating the exceptions for invalid data.
   *
   * @throws JSONException
   */
  @Test
  public void testGetValueMap_Invalid() throws JSONException {
    final String[][] COLUMNS = {{"c1", "INT"}, {"c2", "array<LONG>"}, {"c3", "array<INT>"},
            {"c4", "DOUBLE"}, {"c5", "map<STRING,LONG>"},};
    final String[][] DATA = {{"{'c1': 'abc'}", "column=c1"}, {"{'c2': ['abc']}", "column=c2"},
            {"{'c3': 'abc'}", "column=c3"},};
    MTableDescriptor td = new MTableDescriptor();
    TypeUtils.addColumnsFromJson(TypeUtils.getJsonSchema(COLUMNS), td);

    for (final String[] VALUE : DATA) {
      try {
        TypeUtils.getValueMap(td, VALUE[0]);
      } catch (Exception e) {
        assertTrue("Invalid exception.", e.getMessage().contains(VALUE[1]));
      }
    }
  }

  /**
   * Basic test for populate and validate basic.
   *
   * @throws JSONException
   */
  @Test
  public void testGetValueMap_ValidValues() throws JSONException {
    final Map<String, String> expectedMap = new HashMap<>(COLUMN_TYPES.length);
    Map<String, Object> map = populateValues(expectedMap, false);
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      assertEquals(expectedMap.get(entry.getKey()), TypeHelper.deepToString(entry.getValue()));
    }
  }

  /**
   * Assert that null values are returned as expected.
   *
   * @throws JSONException
   */
  @Test
  public void testGetValueMap_NullValues() throws JSONException {
    final Map<String, String> expectedMap = new HashMap<>(COLUMN_TYPES.length);
    Map<String, Object> map = populateValues(expectedMap, true);
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      assertEquals(expectedMap.get(entry.getKey()), TypeHelper.deepToString(entry.getValue()));
    }
  }

  /**
   * Assert that only partial columns could be converted as well.
   *
   * @throws JSONException
   */
  @Test
  public void testGetValueMap_PartialRow() throws JSONException {
    final String[][] COLUMNS = {{"ID", "LONG"}, {"MY_MAP", "map<STRING,array<DOUBLE>>"}};
    final String[] VALUES = {"{'ID': '11', 'MY_MAP': {'ABC' : ['11.111', '11.222']}}",};

    MTableDescriptor td = new MTableDescriptor();
    TypeUtils.addColumnsFromJson(TypeUtils.getJsonSchema(COLUMNS), td);

    /** get and assert on full row **/
    Map<String, Object> ret1 = TypeUtils.getValueMap(td, VALUES[0]);
    assertEquals("Incorrect value for: ID", 11L, ret1.get("ID"));
    assertEquals("Incorrect value for: MY_MAP", "{ABC=[11.111, 11.222]}",
            TypeHelper.deepToString(ret1.get("MY_MAP")));

    /** get and assert on single column **/
    Map<String, Object> ret2 = TypeUtils.getValueMap(td, "{'ID': '33'}");
    assertEquals("Incorrect value for: ID", 33L, ret2.get("ID"));
    assertNull("Expected null for: MY_MAP", ret2.get("MY_MAP"));

    /** get and assert on the other column **/
    Map<String, Object> ret3 =
            TypeUtils.getValueMap(td, "{'MY_MAP': {'PQR' : ['22.111', '22.222']}}");
    assertNull("Expected null for: ID", ret3.get("ID"));
    assertEquals("Incorrect value for: MY_MAP", "{PQR=[22.111, 22.222]}",
            TypeHelper.deepToString(ret3.get("MY_MAP")));
  }

  @Test
  public void testAddColumnsFromJson_Invalid() {
    final String error = TypeUtils.addColumnsFromJson("abc", new MTableDescriptor());
    assertNotNull(error);
    assertTrue("Incorrect error for: addColumnsFromJson", error.startsWith("Invalid schema"));
  }

  /**
   * Test and validate table descriptor creation from JSON schema.
   */
  @Test
  public void testAddColumnsFromJson_1() {
    final String[][] columns = new String[][] {{"c1", "STRING"}, {"c2", "INT"},
            {"c3", "map<INT,struct<A:BINARY,B:array<DOUBLE>,C:BIG_DECIMAL>>"}, {"c4", "DATE"},};
    /** create JSON string for this schema **/
    final String schema = TypeUtils.getJsonSchema(columns);

    /** create and validate descriptor from the JSON **/
    final MTableDescriptor tableDescriptor = new MTableDescriptor();
    TypeUtils.addColumnsFromJson(schema, tableDescriptor);

    int i = 0;
    for (final MColumnDescriptor cd : tableDescriptor.getAllColumnDescriptors()) {
      assertEquals("Incorrect column-name.", columns[i][0], cd.getColumnNameAsString());
      assertEquals("Incorrect column-type.", columns[i][1], cd.getColumnType().toString());
      i++;
    }
  }

  @Test
  public void testTypesWithArgs() {
    final String[] typeStr = {"VARCHAR(10)", "BIG_DECIMAL(10,5)"};
    final DataType[] types = {getTypeFromString(typeStr[0]), getTypeFromString(typeStr[1])};

    final Object[] values =
            {"abcdefghijklmnop", new BigDecimal("12345.6789123456", new MathContext(10))};
    final Object[] expected = {"abcdefghij", new BigDecimal("12345.67891")};

    final Map<DataType, Supplier<Object>> map =
            Collections.unmodifiableMap(new HashMap<DataType, Supplier<Object>>(15) {
              {
                put(BasicTypes.VARCHAR, () -> values[0]);
                put(BasicTypes.BIG_DECIMAL, () -> values[1]);
              }
            });

    /* replace the map that generates random values by this map */
    final String field = "RANDOM_DATA_TYPE_MAP";
    final Object old = ReflectionUtils.setStaticFinalFieldValue(TypeUtils.class, field, map);

    assertEquals("Incorrect value: VARCHAR", expected[0], TypeUtils.getRandomValue(types[0]));
    assertEquals("Incorrect value: BIG_DECIMAL", expected[1], TypeUtils.getRandomValue(types[1]));

    /* reset the map to original value */
    ReflectionUtils.setStaticFieldValue(TypeUtils.class, field, old);

    /* just make sure that original random value generator function are reset */
    assertNotEquals("Incorrect value: VARCHAR", expected[0], TypeUtils.getRandomValue(types[0]));
    assertNotEquals("Incorrect value: BIG_DECIMAL", expected[1],
            TypeUtils.getRandomValue(types[1]));
  }
}
