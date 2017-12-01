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
import static org.testng.Assert.fail;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import io.ampool.monarch.types.interfaces.TypePredicateOp;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for various helper methods for predicates.
 * <p>
 */
public class MPredicateHelperTest {
  /**
   * Test for constructor.. for code coverage..
   */
  @Test
  public void testDummyConstructor() {
    try {
      Constructor<?> c;
      c = Class.forName("io.ampool.monarch.types.MPredicateHelper").getDeclaredConstructor();
      c.setAccessible(true);
      Object o = c.newInstance();
      assertNotNull(o);
    } catch (Exception e) {
      fail("No exception was expected here..");
    }
  }


  @DataProvider
  public static Object[][] getDataForInvalidRegex() {
    return new Object[][] {{null, NullPointerException.class}, {"*", PatternSyntaxException.class}};
  }

  /**
   * Test for invalid regex.
   *
   * @param regex the regular expression
   * @param exceptionClass the expected class for the exception
   */
  @Test(dataProvider = "getDataForInvalidRegex")
  public void testGetPredicateForString_RegexWithNullRegex(final String regex,
      Class<?> exceptionClass) {
    try {
      MPredicateHelper.getPredicateForString(CompareOp.REGEX, regex);
      fail("Expected the exception.");
    } catch (Exception e) {
      assertEquals(e.getClass(), exceptionClass);
    }
  }

  /**
   * Test for valid regular expressions..
   *
   * @throws Exception
   */
  @Test
  public void testGetPredicateForString_RegexWithNullValues() throws Exception {
    final String[] inputLines =
        {"12978332,-rw-rw-r--,11358,target/maven-shared-archive-resources/META-INF/LICENSE",
            "12978691,drwxrwxr-x,4096,target/tmp", "12978693,drwxrwxr-x,4096,target/tmp/conf",
            "12978708,-rw-rw-r--,1582,target/tmp/conf/hiveserver2-site.xml",
            "12978727,drwxrwxr-x,4096,target/tmp/conf/spark",
            "12978728,-rw-rw-r--,3109,target/tmp/conf/spark/log4j2.xml",
            "12978729,drwxrwxr-x,4096,target/tmp/conf/spark/standalone", null};
    Predicate p = MPredicateHelper.getPredicateForString(CompareOp.REGEX, ".*spark.*");
    final int expectedSize = 3;
    List<Object> list = new ArrayList<>(10);
    Collections.addAll(list, inputLines);

    /** assert that predicate returns only expected results **/
    List<Object> result = assertOnResultSize(list, p, expectedSize);

    /** assert that none of the returned elements/objects contain spark **/
    result.forEach(e -> ((String) e).concat("spark"));
  }

  /**
   * Assert that the specified predicate filters out unwanted data/elements.
   *
   * @param input the list of input objects
   * @param p the predicate to be executed against each object in above list
   * @param expectedSize the number of elements matching the specified predicate
   * @return the list of objects matching the specified predicate
   */
  private List<Object> assertOnResultSize(final List<Object> input, final Predicate p,
      final int expectedSize) {
    /** execute the predicate and get result for regex.. **/
    List<Object> result = (List<Object>) input.stream().filter(p).collect(Collectors.toList());
    assertEquals(result.size(), expectedSize);
    return result;
  }

  /**
   * Assert on the specified operation using the serialization/de-serialization for the provided
   * object-type.
   *
   * @param objectType the object type
   * @param constant the constant value to be tested
   * @param op the operation to be used for testing/condition against constant value
   * @param arg1 the value to be compared against the constant value
   * @param check boolean indicating the result of predicate test for above
   */
  private void assertUsingSerDeForType(BasicTypes objectType, Object constant, TypePredicateOp op,
      Object arg1, boolean check) {
    Predicate p = objectType.getPredicate(op, constant);
    Object arg1Obj = objectType.deserialize((objectType.serialize(arg1)));

    assertEquals(p.test(arg1Obj), check);
  }

  @DataProvider
  public static Object[][] getIntData() {
    return new Object[][] {{11L, CompareOp.LESS, 10, true}, {1L, CompareOp.GREATER, 10, true},
        {1L, CompareOp.EQUAL, 1, true}, {1, CompareOp.EQUAL, 1, true},
        {1, CompareOp.NOT_EQUAL, 1, false}, {1, CompareOp.NOT_EQUAL, null, true},
        {null, CompareOp.EQUAL, 1, false}, {null, CompareOp.EQUAL, null, true},
        {null, CompareOp.NOT_EQUAL, null, false}, {null, CompareOp.LESS, null, false},
        {null, CompareOp.LESS_OR_EQUAL, null, true}, {null, CompareOp.GREATER, null, false},
        {null, CompareOp.GREATER_OR_EQUAL, null, true},};
  }

  /**
   * Test for integer values.
   *
   * @param constant the constant value to be tested
   * @param op the operation to be used for testing/condition against constant value
   * @param arg1 the value to be compared against the constant value
   * @param check boolean indicating the result of predicate test for above
   */
  @Test(dataProvider = "getIntData")
  public void testGetPredicateForInt(final Object constant, final TypePredicateOp op,
      final Integer arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForInt :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.INT, constant, op, arg1, check);
  }

  @Test(dataProvider = "getIntData", enabled = false)
  public void testGetPredicateForInt_O(final Object constant, final TypePredicateOp op,
      final Integer arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForInt :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.O_INT, constant, op, arg1, check);
  }

  @DataProvider
  public static Object[][] getLongData() {
    return new Object[][] {{1, CompareOp.LESS, 10L, false},
        {1, CompareOp.GREATER_OR_EQUAL, 1L, true}, {10, CompareOp.LESS, 1L, true},
        {1L, CompareOp.EQUAL, 1L, true}, {1L, CompareOp.NOT_EQUAL, 1L, false},
        {1L, CompareOp.NOT_EQUAL, null, true}, {null, CompareOp.EQUAL, 1L, false},
        {null, CompareOp.EQUAL, null, true}, {null, CompareOp.NOT_EQUAL, null, false},
        {null, CompareOp.LESS, null, false}, {null, CompareOp.LESS_OR_EQUAL, null, true},
        {null, CompareOp.GREATER, null, false}, {null, CompareOp.GREATER_OR_EQUAL, null, true},};
  }

  @DataProvider
  public static Object[][] getStringData() {
    return new Object[][] {{"abc", CompareOp.EQUAL, "abc", true},
        {"abc", CompareOp.EQUAL, "ABC", false}, {"abc", CompareOp.NOT_EQUAL, "abc", false},
        {null, CompareOp.EQUAL, "abc", false}, {"abc", CompareOp.NOT_EQUAL, null, true},
        {null, CompareOp.EQUAL, null, true}, {null, CompareOp.NOT_EQUAL, null, false},
        {"abc", CompareOp.GREATER, "abcd", true}, {"abc", CompareOp.GREATER_OR_EQUAL, "abcd", true},
        {"abcd", CompareOp.LESS, "abc", true}, {"abcd", CompareOp.LESS_OR_EQUAL, "abc", true},
        {"kabcd", CompareOp.LESS_OR_EQUAL, "abc", true},
        {"1994-03-02", CompareOp.GREATER, "1995-03-02", true},
        {"2000-03-02", CompareOp.GREATER, "1995-03-02", false},
        {"1995-11-02", CompareOp.GREATER, "1995-03-02", false},};
  }

  /**
   * Test for String values.
   *
   * @param constant the constant value to be tested
   * @param op the operation to be used for testing/condition against constant value
   * @param arg1 the value to be compared against the constant value
   * @param check boolean indicating the result of predicate test for above
   */
  @Test(dataProvider = "getStringData")
  public void testGetPredicateForString(final Object constant, final TypePredicateOp op,
      final String arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForString :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.STRING, constant, op, arg1, check);
  }

  /**
   * Test for long values.
   *
   * @param constant the constant value to be tested
   * @param op the operation to be used for testing/condition against constant value
   * @param arg1 the value to be compared against the constant value
   * @param check boolean indicating the result of predicate test for above
   */
  @Test(dataProvider = "getLongData")
  public void testGetPredicateForLong(final Object constant, final TypePredicateOp op,
      final Long arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForLong :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.LONG, constant, op, arg1, check);
  }

  @Test(dataProvider = "getLongData", enabled = false)
  public void testGetPredicateForLong_O(final Object constant, final TypePredicateOp op,
      final Long arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForLong :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.O_LONG, constant, op, arg1, check);
  }

  @DataProvider
  public static Object[][] getFloatData() {
    return new Object[][] {{1.0f, CompareOp.EQUAL, 1.0f, true},
        {1.0f, CompareOp.NOT_EQUAL, 1.0f, false},
        {Float.MAX_VALUE, CompareOp.EQUAL, Float.MAX_VALUE, true},
        {Float.MIN_VALUE, CompareOp.EQUAL, Float.MIN_VALUE, true},
        {Float.MAX_VALUE, CompareOp.NOT_EQUAL, Float.MAX_VALUE, false},
        {Float.MIN_VALUE, CompareOp.NOT_EQUAL, Float.MIN_VALUE, false},
        {Float.MAX_VALUE, CompareOp.LESS_OR_EQUAL, Float.MAX_VALUE, true},
        {Float.MAX_VALUE, CompareOp.LESS, Float.MAX_VALUE, false},
        {Float.MAX_VALUE, CompareOp.GREATER_OR_EQUAL, 0f, false},
        {Float.MIN_VALUE, CompareOp.LESS, 0f, true}, {2f, CompareOp.LESS_OR_EQUAL, 0f, true},
        {Float.MAX_VALUE, CompareOp.LESS_OR_EQUAL, 0f, true},
        {Float.MIN_VALUE, CompareOp.GREATER_OR_EQUAL, 0f, false},
        {Float.MAX_VALUE, CompareOp.GREATER, Float.MAX_VALUE - 1, false},
        {Float.MIN_VALUE, CompareOp.LESS, Float.MIN_VALUE + 1, false},
        {null, CompareOp.LESS, null, false}, {null, CompareOp.GREATER_OR_EQUAL, null, true},};
  }

  /**
   * Test for float values.
   *
   * @param constant the constant value to be tested
   * @param op the operation to be used for testing/condition against constant value
   * @param arg1 the value to be compared against the constant value
   * @param check boolean indicating the result of predicate test for above
   */
  @Test(dataProvider = "getFloatData")
  public void testGetPredicateForFloat(final Object constant, final TypePredicateOp op,
      final Float arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForFloat :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.FLOAT, constant, op, arg1, check);
  }

  @DataProvider
  public static Object[][] getDoubleData() {
    return new Object[][] {{1.0, CompareOp.EQUAL, 1.0, true},
        {1.0, CompareOp.NOT_EQUAL, 1.0, false},
        {Double.MAX_VALUE, CompareOp.EQUAL, Double.MAX_VALUE, true},
        {Double.MIN_VALUE, CompareOp.EQUAL, Double.MIN_VALUE, true},
        {Double.MAX_VALUE, CompareOp.NOT_EQUAL, Double.MAX_VALUE, false},
        {Double.MIN_VALUE, CompareOp.NOT_EQUAL, Double.MIN_VALUE, false},
        {Double.MAX_VALUE, CompareOp.LESS_OR_EQUAL, Double.MAX_VALUE, true},
        {Double.MAX_VALUE, CompareOp.LESS, Double.MAX_VALUE, false},
        {Double.MAX_VALUE, CompareOp.GREATER_OR_EQUAL, 0d, false},
        {Double.MIN_VALUE, CompareOp.LESS, 0d, true}, {2d, CompareOp.LESS_OR_EQUAL, 0d, true},
        {Double.MAX_VALUE, CompareOp.LESS_OR_EQUAL, 0d, true},
        {Double.MIN_VALUE, CompareOp.GREATER_OR_EQUAL, 0d, false},
        {Double.MAX_VALUE, CompareOp.GREATER, Double.MAX_VALUE - 1, false},
        {Double.MIN_VALUE, CompareOp.LESS, Double.MIN_VALUE + 1, false},
        {null, CompareOp.LESS_OR_EQUAL, null, true}, {null, CompareOp.GREATER, null, false},};
  }

  /**
   * Test for double values..
   *
   * @param constant the constant value to be tested
   * @param op the operation to be used for testing/condition against constant value
   * @param arg1 the value to be compared against the constant value
   * @param check boolean indicating the result of predicate test for above
   */
  @Test(dataProvider = "getDoubleData")
  public void testGetPredicateForDouble(final Object constant, final TypePredicateOp op,
      final Double arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForDouble :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.DOUBLE, constant, op, arg1, check);
  }

  @DataProvider
  public static Object[][] getBigDecimalData() {
    return new Object[][] {{new BigDecimal("1.0"), CompareOp.EQUAL, new BigDecimal("1.00"), false},
        {new BigDecimal("1.0"), CompareOp.LESS_OR_EQUAL, new BigDecimal("1.00"), true},
        {new BigDecimal("1.0"), CompareOp.GREATER, new BigDecimal("1.00"), false},
        {new BigDecimal("1.0"), CompareOp.LESS, new BigDecimal("1.00"), false},
        {new BigDecimal("1.0"), CompareOp.LESS_OR_EQUAL, new BigDecimal("1.01"), false},
        {new BigDecimal("1.0"), CompareOp.LESS, new BigDecimal("1.001"), false},
        {new BigDecimal("1.0"), CompareOp.GREATER, new BigDecimal("1.001"), true},
        {new BigDecimal("1.0"), CompareOp.GREATER_OR_EQUAL, new BigDecimal("1.000"), true},
        {new BigDecimal("1.0"), CompareOp.NOT_EQUAL, new BigDecimal("1.000"), true},
        {"1.0", CompareOp.NOT_EQUAL, new BigDecimal("1.000"), true},
        {new BigDecimal("1.0", new MathContext(2)), CompareOp.LESS,
            new BigDecimal("1.000001", new MathContext(2)), false},
        {new BigDecimal("1.0", new MathContext(2)), CompareOp.EQUAL,
            new BigDecimal("1.000001", new MathContext(2)), true},
        {null, CompareOp.EQUAL, null, true}, {null, CompareOp.LESS_OR_EQUAL, null, true},
        {null, CompareOp.GREATER, null, false},};
  }

  /**
   * Test for double values..
   *
   * @param constant the constant value to be tested
   * @param op the operation to be used for testing/condition against constant value
   * @param arg1 the value to be compared against the constant value
   * @param check boolean indicating the result of predicate test for above
   */
  @Test(dataProvider = "getBigDecimalData")
  public void testGetPredicateForBigDecimal(final Object constant, final TypePredicateOp op,
      final BigDecimal arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForBigDecimal :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.BIG_DECIMAL, constant, op, arg1, check);
  }

  @DataProvider
  public static Object[][] getByteData() {
    return new Object[][] {{(byte) 100, CompareOp.EQUAL, (byte) 100, true},
        {(byte) 100, CompareOp.NOT_EQUAL, (byte) 100, false},
        {Byte.MAX_VALUE, CompareOp.EQUAL, Byte.MAX_VALUE, true},
        {Byte.MIN_VALUE, CompareOp.EQUAL, Byte.MIN_VALUE, true},
        {Byte.MAX_VALUE, CompareOp.NOT_EQUAL, Byte.MAX_VALUE, false},
        {Byte.MIN_VALUE, CompareOp.NOT_EQUAL, Byte.MIN_VALUE, false},
        {Byte.MAX_VALUE, CompareOp.LESS_OR_EQUAL, Byte.MAX_VALUE, true},
        {Byte.MAX_VALUE, CompareOp.LESS, Byte.MAX_VALUE, false},
        {Byte.MAX_VALUE, CompareOp.GREATER_OR_EQUAL, (byte) 100, false},
        {Byte.MIN_VALUE, CompareOp.LESS, (byte) 100, false},
        {(byte) 102, CompareOp.LESS_OR_EQUAL, (byte) 100, true},
        {Byte.MAX_VALUE, CompareOp.LESS_OR_EQUAL, (byte) 100, true},
        {Byte.MIN_VALUE, CompareOp.GREATER_OR_EQUAL, (byte) 100, true},
        {Byte.MAX_VALUE, CompareOp.GREATER, (byte) (Byte.MAX_VALUE - 1), false},
        {Byte.MIN_VALUE, CompareOp.LESS, (byte) (Byte.MIN_VALUE + 1), false},
        {(byte) 0, CompareOp.EQUAL, (byte) (0), true},
        {(byte) 0, CompareOp.NOT_EQUAL, (byte) (0), false}, {1, CompareOp.LESS, (byte) 10, false},
        {10, CompareOp.GREATER_OR_EQUAL, (byte) 10, true},};
  }

  /**
   * Test for byte values.
   *
   * @param constant the constant value to be tested
   * @param op the operation to be used for testing/condition against constant value
   * @param arg1 the value to be compared against the constant value
   * @param check boolean indicating the result of predicate test for above
   */
  @Test(dataProvider = "getByteData")
  public void testGetPredicateForByte(final Object constant, final TypePredicateOp op,
      final Byte arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForByte :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.BYTE, constant, op, arg1, check);
  }

  @DataProvider
  public static Object[][] getShortData() {
    return new Object[][] {{100, CompareOp.GREATER, (short) 100, false},
        {(short) 100, CompareOp.EQUAL, (short) 100, true},
        {(short) 100, CompareOp.NOT_EQUAL, (short) 100, false},
        {Short.MAX_VALUE, CompareOp.EQUAL, Short.MAX_VALUE, true},
        {Short.MIN_VALUE, CompareOp.EQUAL, Short.MIN_VALUE, true},
        {Short.MAX_VALUE, CompareOp.NOT_EQUAL, Short.MAX_VALUE, false},
        {Short.MIN_VALUE, CompareOp.NOT_EQUAL, Short.MIN_VALUE, false},
        {Short.MAX_VALUE, CompareOp.LESS_OR_EQUAL, Short.MAX_VALUE, true},
        {Short.MAX_VALUE, CompareOp.LESS, Short.MAX_VALUE, false},
        {Short.MAX_VALUE, CompareOp.GREATER_OR_EQUAL, (short) 100, false},
        {Short.MIN_VALUE, CompareOp.LESS, (short) 100, false},
        {(short) 102, CompareOp.LESS_OR_EQUAL, (short) 100, true},
        {Short.MAX_VALUE, CompareOp.LESS_OR_EQUAL, (short) 100, true},
        {Short.MIN_VALUE, CompareOp.GREATER_OR_EQUAL, (short) 100, true},
        {Short.MAX_VALUE, CompareOp.GREATER, (short) (Short.MAX_VALUE - 1), false},
        {Short.MIN_VALUE, CompareOp.LESS, (short) (Short.MIN_VALUE + 1), false},
        {null, CompareOp.LESS, null, false}, {null, CompareOp.EQUAL, null, true},};
  }

  /**
   * Test for byte values.
   *
   * @param constant the constant value to be tested
   * @param op the operation to be used for testing/condition against constant value
   * @param arg1 the value to be compared against the constant value
   * @param check boolean indicating the result of predicate test for above
   */
  @Test(dataProvider = "getShortData")
  public void testGetPredicateForShort(final Object constant, final TypePredicateOp op,
      final Short arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForShort :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.SHORT, constant, op, arg1, check);
  }

  @DataProvider
  public static Object[][] getDateData() {
    return new Object[][] {{new Date(2016, 01, 13), CompareOp.EQUAL, new Date(2016, 01, 13), true},
        {new Date(2016, 01, 13), CompareOp.NOT_EQUAL, new Date(2016, 01, 13), false},
        {new Date(2016, 01, 13), CompareOp.LESS_OR_EQUAL, new Date(2016, 01, 13), true},
        {new Date(2016, 01, 13), CompareOp.LESS, new Date(2016, 01, 13), false},
        {new Date(2016, 01, 13), CompareOp.GREATER, new Date(2016, 01, 13), false},
        {new Date(2016, 01, 13), CompareOp.GREATER_OR_EQUAL, new Date(2016, 01, 13), true},
        {new Date(2016, 01, 13), CompareOp.GREATER_OR_EQUAL, new Date(2016, 01, 14), true},
        {new Date(2016, 01, 13), CompareOp.GREATER, new Date(2016, 01, 14), true},
        {new Date(2016, 01, 13), CompareOp.LESS_OR_EQUAL, new Date(2016, 01, 14), false},
        {new Date(2016, 01, 13), CompareOp.LESS, new Date(2016, 01, 14), false},
        {"2016-01-28", CompareOp.LESS, Date.valueOf("2016-01-30"), false},
        {"2016-01-28", CompareOp.EQUAL, Date.valueOf("2016-01-28"), true},
        {"2016-01-28", CompareOp.GREATER_OR_EQUAL, Date.valueOf("2016-01-30"), true},
        {"2016-01-28", CompareOp.LESS_OR_EQUAL, Date.valueOf("2016-01-26"), true},};
  }

  /**
   * Test for Date values.
   *
   * @param constant the constant value to be tested
   * @param op the operation to be used for testing/condition against constant value
   * @param arg1 the value to be compared against the constant value
   * @param check boolean indicating the result of predicate test for above
   */
  @Test(dataProvider = "getDateData")
  public void testGetPredicateForDate(final Object constant, final TypePredicateOp op,
      final Date arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForDate :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.DATE, constant, op, arg1, check);
  }

  @DataProvider
  public static Object[][] getTimeStampData() throws Exception {
    String time = "2011-05-06 07-08-09.1234567";
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh-mm-ss");
    java.util.Date dt = df.parse(time);
    java.sql.Date sqlDate = new java.sql.Date(dt.getTime());
    Long l = sqlDate.getTime();

    String time2 = "2012-05-06 07-08-09.1234567";
    SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd hh-mm-ss");
    java.util.Date dt2 = df2.parse(time);
    java.sql.Date sqlDate2 = new java.sql.Date(dt.getTime());
    Long l2 = sqlDate2.getTime();

    return new Object[][] {{new Timestamp(l), CompareOp.EQUAL, new Timestamp(l), true},
        {new Timestamp(l), CompareOp.NOT_EQUAL, new Timestamp(l), false},
        {new Timestamp(l), CompareOp.LESS_OR_EQUAL, new Timestamp(l), true},
        {new Timestamp(l), CompareOp.LESS, new Timestamp(l), false},
        {new Timestamp(l), CompareOp.GREATER, new Timestamp(l), false},
        {new Timestamp(l), CompareOp.GREATER_OR_EQUAL, new Timestamp(l), true},
        {new Timestamp(l), CompareOp.GREATER_OR_EQUAL, new Timestamp(l2), true},
        {new Timestamp(l), CompareOp.GREATER, new Timestamp(l2), false},
        {new Timestamp(l), CompareOp.LESS_OR_EQUAL, new Timestamp(l2), true},
        {new Timestamp(l), CompareOp.LESS, new Timestamp(l2), false}};
  }

  /**
   * Test for timestamp values.
   *
   * @param constant the constant value to be tested
   * @param op the operation to be used for testing/condition against constant value
   * @param arg1 the value to be compared against the constant value
   * @param check boolean indicating the result of predicate test for above
   */
  @Test(dataProvider = "getTimeStampData")
  public void testGetPredicateForTimeStamp(final Timestamp constant, final TypePredicateOp op,
      final Timestamp arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForTimeStamp :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.TIMESTAMP, constant, op, arg1, check);
  }

  @DataProvider
  public static Object[][] getCharacterData() {
    return new Object[][] {{'a', CompareOp.EQUAL, 'a', true},
        {'a', CompareOp.NOT_EQUAL, 'a', false},
        {Character.MAX_VALUE, CompareOp.EQUAL, Character.MAX_VALUE, true},
        {Character.MIN_VALUE, CompareOp.EQUAL, Character.MIN_VALUE, true},
        {Character.MAX_VALUE, CompareOp.NOT_EQUAL, Character.MAX_VALUE, false},
        {Character.MIN_VALUE, CompareOp.NOT_EQUAL, Character.MIN_VALUE, false},
        {Character.MAX_VALUE, CompareOp.LESS_OR_EQUAL, Character.MAX_VALUE, true},
        {Character.MAX_VALUE, CompareOp.LESS, Character.MAX_VALUE, false},
        {Character.MAX_VALUE, CompareOp.GREATER_OR_EQUAL, (char) 100, false},
        {Character.MIN_VALUE, CompareOp.LESS, (char) 100, false},
        {(char) 102, CompareOp.LESS_OR_EQUAL, (char) 100, true},
        {Character.MAX_VALUE, CompareOp.LESS_OR_EQUAL, (char) 100, true},
        {Character.MIN_VALUE, CompareOp.GREATER_OR_EQUAL, (char) 100, true},
        {Character.MAX_VALUE, CompareOp.GREATER, (char) (Character.MAX_VALUE - 1), false},
        {Character.MIN_VALUE, CompareOp.LESS, (char) (Character.MIN_VALUE + 1), false}};
  }

  /**
   * Test for char values.
   *
   * @param constant the constant value to be tested
   * @param op the operation to be used for testing/condition against constant value
   * @param arg1 the value to be compared against the constant value
   * @param check boolean indicating the result of predicate test for above
   */
  @Test(dataProvider = "getCharacterData")
  public void testGetPredicateForCharacter(final char constant, final TypePredicateOp op,
      final char arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForShort :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.CHAR, constant, op, arg1, check);
  }

  @DataProvider
  public static Object[][] getBooleanData() {
    return new Object[][] {{true, CompareOp.EQUAL, true, true},
        {true, CompareOp.NOT_EQUAL, true, false}, {true, CompareOp.EQUAL, false, false},
        {true, CompareOp.NOT_EQUAL, false, true}};
  }

  /**
   * Test for byte values.
   *
   * @param constant the constant value to be tested
   * @param op the operation to be used for testing/condition against constant value
   * @param arg1 the value to be compared against the constant value
   * @param check boolean indicating the result of predicate test for above
   */
  @Test(dataProvider = "getBooleanData")
  public void testGetPredicateForBoolean(final boolean constant, final TypePredicateOp op,
      final boolean arg1, boolean check) {
    System.out.printf("PredicateHelperTest.testGetPredicateForBoolean :: %s -- %s -- %s -- %s\n",
        constant, op, arg1, check);

    assertUsingSerDeForType(BasicTypes.BOOLEAN, constant, op, arg1, check);
  }
}
