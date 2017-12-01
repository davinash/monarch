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
import static org.testng.Assert.assertNotNull;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.types.interfaces.DataType;
import org.junit.experimental.categories.Category;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Simple Tests for object types..
 *
 */
@Category(MonarchTest.class)
public class BasicTypesTest {

  /**
   * Test for category..
   */
  @Test
  public void testCategory() {
    BasicTypes type = BasicTypes.BOOLEAN;
    assertEquals(type.getCategory(), DataType.Category.Basic);
  }

  /**
   * Helper method to assert using serialization and deserialization for the object-type.
   *
   * @param type the object type
   * @param value the value to be tested
   * @param size the size (in bits) of the object
   */
  private void assertUsingSerDe(final BasicTypes type, final Object value, final int size) {
    byte[] ret = type.serialize(null);
    assertEquals(ret.length, 0);

    ret = type.serialize(value);
    assertEquals(ret.length, size / Byte.SIZE);

    assertEquals(type.deserialize(ret), value);
  }

  @Test
  public void test_INT() throws Exception {
    assertUsingSerDe(BasicTypes.INT, 10, Integer.SIZE);
  }

  @Test
  public void test_CHARS() throws Exception {
    final String str = "abc";
    BasicTypes type = BasicTypes.CHARS.setArgs("10");
    assertEquals(type.toString(), "CHARS(10)");
    assertUsingSerDe(type, str, str.length() * (Byte.SIZE));
  }

  @Test
  public void test_BYTE() throws Exception {
    assertUsingSerDe(BasicTypes.BYTE, Byte.MIN_VALUE, Byte.SIZE);
    assertUsingSerDe(BasicTypes.BYTE, (byte) 0, Byte.SIZE);
    // assertUsingSerDe(MBasicObjectType.BYTE, (Byte)null, Byte.SIZE);
  }

  @Test
  public void test_CHARACTER() throws Exception {
    assertUsingSerDe(BasicTypes.CHAR, 'a', Character.SIZE);
    assertUsingSerDe(BasicTypes.CHAR, (char) 0, Character.SIZE);
  }

  @Test
  public void test_SHORT() throws Exception {
    assertUsingSerDe(BasicTypes.SHORT, Short.MIN_VALUE, Short.SIZE);
  }

  @Test
  public void test_BOOLEAN() throws Exception {
    assertUsingSerDe(BasicTypes.BOOLEAN, Boolean.TRUE, Byte.SIZE);
    // assertUsingSerDe(MBasicObjectType.BOOLEAN, (Boolean)null, Byte.SIZE);
  }

  @Test
  public void test_DOUBLE() throws Exception {
    assertUsingSerDe(BasicTypes.DOUBLE, 10.0, Double.SIZE);
  }

  @Test
  public void test_FLOAT() throws Exception {
    assertUsingSerDe(BasicTypes.FLOAT, 10.0f, Float.SIZE);
  }

  @Test
  public void test_LONG() throws Exception {
    assertUsingSerDe(BasicTypes.LONG, 10L, Long.SIZE);
  }

  @DataProvider
  public static Object[][] getLongData() {
    return new Object[][] {{10L, 1}, {200L, 2}, {4096L, 3}, {65236L, 3}, {1234567L, 4},
        {12345678L, 4}, {-12345678L, 4}, {65235236L, 5}, {-65235236L, 5}, {999999999L, 5},
        {-999999999L, 5}, {Long.MAX_VALUE - 1, 9}, {Long.MIN_VALUE + 2, 9},};
  }

  @Test(dataProvider = "getLongData", enabled = false)
  public void test_O_LONG(final long value, final int expectedSize) throws Exception {
    DataType type = BasicTypes.O_LONG;

    byte[] ret = type.serialize(null);
    assertEquals(ret.length, 0);

    ret = type.serialize(value);
    assertEquals(ret.length, expectedSize);

    assertEquals(type.deserialize(ret), value);
  }

  @DataProvider
  public static Object[][] getIntData() {
    return new Object[][] {{10, 1}, {200, 2}, {4096, 3}, {65236, 3}, {1234567, 4}, {12345678, 4},
        {-12345678, 4}, {65235236, 5}, {-65235236, 5}, {999999999, 5}, {-999999999, 5},
        {Integer.MAX_VALUE - 1, 5}, {Integer.MIN_VALUE + 2, 5},};
  }

  @Test(dataProvider = "getIntData", enabled = false)
  public void test_O_INT(final int value, final int expectedSize) throws Exception {
    DataType type = BasicTypes.O_INT;
    byte[] ret = type.serialize(null);
    assertEquals(ret.length, 0);

    ret = type.serialize(value);
    assertEquals(ret.length, expectedSize);

    assertEquals(type.deserialize(ret), value);
  }

  @DataProvider
  public static Object[][] getBinaryData() {
    return new Object[][] {{new byte[] {0, 1, 2}}, {null}, {new byte[0]}, {new byte[] {0, 0, 0}},};
  }

  @Test(dataProvider = "getBinaryData")
  public void test_BINARY(final byte[] value) throws Exception {
    DataType type = BasicTypes.BINARY;
    byte[] ret = type.serialize(null);
    assertEquals(ret.length, 0);

    assertEquals(type.deserialize(type.serialize(value)), value);
  }

  @DataProvider
  public static Object[][] getStringData() {
    return new Object[][] {{"abc"}, {null}, {""}, {"AbCdXyZ"},};
  }

  @Test(dataProvider = "getStringData")
  public void test_STRING(final String value) throws Exception {
    DataType type = BasicTypes.STRING;
    byte[] ret = type.serialize(null);
    assertEquals(ret.length, 0);

    assertEquals(type.deserialize(type.serialize(value)), value);
  }

  @Test
  public void test_DATE() throws Exception {
    assertUsingSerDe(BasicTypes.DATE, new Date(2016, 01, 13), Long.SIZE);

    String time = "2009-07-20 05-33";
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh-mm");
    java.util.Date dt = df.parse(time);
    java.sql.Date sqlDate = new java.sql.Date(dt.getTime());
    Long l = dt.getTime();
    assertUsingSerDe(BasicTypes.DATE, sqlDate, Long.SIZE);
  }

  @Test
  public void test_TIMESTAMP() throws Exception {
    assertUsingSerDe(BasicTypes.TIMESTAMP, new Timestamp(System.currentTimeMillis()), Long.SIZE);

    String time = "2009-07-20 05-33";
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh-mm");
    java.util.Date dt = df.parse(time);
    Long l = dt.getTime();
    assertUsingSerDe(BasicTypes.TIMESTAMP, new Timestamp(dt.getTime()), Long.SIZE);

    String time2 = "2011-05-06 07-08-09.1234567";
    SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd hh-mm-ss.SSSSSSS");
    java.util.Date dt2 = df2.parse(time2);
    Long l2 = dt2.getTime();
    assertUsingSerDe(BasicTypes.TIMESTAMP, new Timestamp(l2), Long.SIZE);
  }

  @DataProvider
  public static Object[][] getBigDecimals() {
    return new Object[][] {{new BigDecimal("1234567890")},
        {new BigDecimal(new BigInteger("123456789"), 10)},
        {new BigDecimal("30.000000000000000001")},
        {new BigDecimal(new char[] {'0', '.', '1'}, new MathContext(30))},
        {new BigDecimal("-30.00000000009000000001", new MathContext(30))},};
  }

  @Test(dataProvider = "getBigDecimals")
  public void test_BD(final BigDecimal bigDecimal) {
    DataType objectType = BasicTypes.BIG_DECIMAL.setArgs("10,10");

    assertEquals(objectType.toString(), "BIG_DECIMAL(10,10)");
    assertEquals(objectType.deserialize(objectType.serialize(bigDecimal)), bigDecimal);
  }

  private static final Function<Object, Object> StringToIntFunction =
      (Function<Object, Object> & Serializable) e -> Integer.valueOf((String) e);
  private static final Function<Object, Object> StringToLongFunction =
      (Function<Object, Object> & Serializable) e -> Long.valueOf((String) e);
  private static final Function<Object, Object> IntToStringFunction =
      (Function<Object, Object> & Serializable) String::valueOf;
  private static final Function<Object, Object> LongToStringFunction =
      (Function<Object, Object> & Serializable) String::valueOf;
  /** a function to convert CSV data to an array of ints i.e. "1,2,3" -> int[]{1,2,3} **/
  private static final Function<Object, Object> CsvToIntArrayFunction =
      (Function<Object, Object> & Serializable) e -> Arrays.stream(String.valueOf(e).split(","))
          .mapToInt(Integer::valueOf).toArray();
  /** a function to convert CSV pairs to map<string,int> i.e. a=1,b=2 -> {"a":1, "b":2} **/
  private static final Function<Object, Object> PairsToStrIntMapFunction =
      (Function<Object, Object> & Serializable) e -> Arrays.stream(String.valueOf(e).split(","))
          .map(x -> x.split("=")).collect(Collectors.toMap(y -> y[0], y -> Integer.valueOf(y[1])));
  /** toString function.. **/
  private static final Function<Object, Object> ToStringFunction =
      (Function<Object, Object> & Serializable) Object::toString;

  @DataProvider
  public static Object[][] getPostDeserializeData() {
    return new Object[][] {{"10", BasicTypes.STRING, 10, StringToIntFunction},
        {"10", BasicTypes.STRING, 10L, StringToLongFunction},
        {10, BasicTypes.INT, "10", IntToStringFunction},
        {10L, BasicTypes.LONG, "10", LongToStringFunction},
        {Arrays.asList(1, 2, 3), new ListType(BasicTypes.INT), "[1, 2, 3]", ToStringFunction},
        {new HashMap<String, Integer>(2) {
          {
            put("a", 1);
            put("b", 2);
          }
        }, new MapType(BasicTypes.STRING, BasicTypes.INT), "{a=1, b=2}", ToStringFunction},};
  }

  /**
   * Test for post-deserialize converter.. the specified function will transform result of
   * deserialize.
   *
   * @param input the input object
   * @param objectType the object type corresponding to the input object
   * @param expected expected output object of transformed/converted type
   * @param postDesFunction the post deserialize function
   */
  @Test(dataProvider = "getPostDeserializeData")
  public void testPostDeserialize(final Object input, final DataType objectType,
      final Object expected, final Function<Object, Object> postDesFunction) {
    DataType type = new WrapperType(objectType, null, null, postDesFunction);
    Object output = objectType.deserialize(objectType.serialize(input));
    /** only for basic types.. skip this check for list/map etc. **/
    if (objectType instanceof BasicTypes) {
      assertEquals(output.getClass(), input.getClass());
    }

    output = type.deserialize(objectType.serialize(input));
    assertEquals(output.getClass(), expected.getClass());
    assertEquals(output, expected);
    System.out.println("objectType = " + objectType.toString());
  }

  @DataProvider
  public static Object[][] getPreSerializeData() {
    return new Object[][] {{"10", BasicTypes.INT, 10, StringToIntFunction},
        {"10", BasicTypes.LONG, 10L, StringToLongFunction},
        {10, BasicTypes.STRING, "10", IntToStringFunction},
        {10L, BasicTypes.STRING, "10", LongToStringFunction},
        {"1,2,3", new ListType(BasicTypes.INT), Arrays.asList(1, 2, 3), CsvToIntArrayFunction},
        {"a=1,b=2", new MapType(BasicTypes.STRING, BasicTypes.INT),
            new HashMap<String, Integer>(2) {
              {
                put("a", 1);
                put("b", 2);
              }
            }, PairsToStrIntMapFunction},};
  }

  /**
   * Test for pre-serialize converter.. the specified function will convert the input value and then
   * apply respective serialization (of the type)
   *
   * @param input the input object
   * @param objectType the object type corresponding to the input object
   * @param expected expected output object of transformed/converted type
   * @param preSerFunction the pre-serialization function
   */
  @Test(dataProvider = "getPreSerializeData")
  public void testPreSerialize(final Object input, final DataType objectType, final Object expected,
      final Function<Object, Object> preSerFunction) {
    DataType type = new WrapperType(objectType, null, preSerFunction, null);
    assertEquals(type.serialize(input), objectType.serialize(expected));

    Object output = type.deserialize(objectType.serialize(expected));
    /** only for basic types.. skip this check for list/map etc. **/
    if (objectType instanceof BasicTypes) {
      assertEquals(output.getClass(), expected.getClass());
    }
    assertEquals(output, expected);
    System.out.println("objectType = " + objectType.toString());
  }

  /**
   * Test for Spark VectorUDT like type using wrapper. The pre-serialize function can transform the
   * input (an array) to a struct and then post-deserialize will transform the struct to the
   * respective array depending on type of vector (dense/sparse).
   */
  @Test
  public void testPreSerAndPostDes() {
    String str = "struct<type:INT,size:INT,indices:array<INT>,values:array<DOUBLE>>";
    Object[] input = new Object[] {0, 3, new int[] {-1, -2, -3}, new double[] {1.0, 2.0, 3.0}};

    DataType structType = DataTypeFactory.getTypeFromString(str);
    DataType type = new WrapperType(structType, "VectorUDT", null, null);

    Object[] output = (Object[]) type.deserialize(type.serialize(input));
    assertEquals(Arrays.deepToString(output), Arrays.deepToString(input));
  }

  @DataProvider
  public static Object[][] getData() {
    return new Object[][] {
        {BasicTypes.STRING, "test", new String[] {"EQUAL", "NOT_EQUAL", "REGEX"}},
        {BasicTypes.CHARS, "test", new String[] {"EQUAL", "NOT_EQUAL", "REGEX"}},
        {BasicTypes.VARCHAR, "test", new String[] {"EQUAL", "NOT_EQUAL", "REGEX"}},
        {BasicTypes.INT, 10,
            new String[] {"EQUAL", "NOT_EQUAL", "LESS_OR_EQUAL", "LESS", "GREATER",
                "GREATER_OR_EQUAL"}},
        {BasicTypes.LONG, 10,
            new String[] {"EQUAL", "NOT_EQUAL", "LESS_OR_EQUAL", "LESS", "GREATER",
                "GREATER_OR_EQUAL"}},
        {BasicTypes.SHORT, 10,
            new String[] {"EQUAL", "NOT_EQUAL", "LESS_OR_EQUAL", "LESS", "GREATER",
                "GREATER_OR_EQUAL"}},
        {BasicTypes.FLOAT, 10,
            new String[] {"EQUAL", "NOT_EQUAL", "LESS_OR_EQUAL", "LESS", "GREATER",
                "GREATER_OR_EQUAL"}},
        {BasicTypes.DOUBLE, 10,
            new String[] {"EQUAL", "NOT_EQUAL", "LESS_OR_EQUAL", "LESS", "GREATER",
                "GREATER_OR_EQUAL"}},
        {BasicTypes.BIG_DECIMAL, new BigDecimal("10.0"),
            new String[] {"EQUAL", "NOT_EQUAL", "LESS_OR_EQUAL", "LESS", "GREATER",
                "GREATER_OR_EQUAL"}},
        {BasicTypes.DATE, Date.valueOf("2016-01-31"),
            new String[] {"EQUAL", "NOT_EQUAL", "LESS_OR_EQUAL", "LESS", "GREATER",
                "GREATER_OR_EQUAL"}},
        {BasicTypes.TIMESTAMP, new Timestamp(System.currentTimeMillis()),
            new String[] {"EQUAL", "NOT_EQUAL", "LESS_OR_EQUAL", "LESS", "GREATER",
                "GREATER_OR_EQUAL"}},
        {BasicTypes.BYTE, (byte) 10, new String[] {"EQUAL", "NOT_EQUAL", "LESS_OR_EQUAL", "LESS",
            "GREATER", "GREATER_OR_EQUAL"}},};
  }

  @Test(dataProvider = "getData")
  public void testGetPredicate(final BasicTypes type, final Object value, final String[] ops) {
    assertNotNull(type.getPredicateFunction());
    Arrays.stream(ops).map(CompareOp::valueOf)
        .forEach(e -> assertNotNull(type.getPredicate(e, value)));
  }
}
