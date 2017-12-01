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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.types.interfaces.TypePredicateOp;

/**
 * Helper class to enable the respective predicate operation.
 * <p>
 * Since version: 0.2.0
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class MPredicateHelper {

  /**
   * Get the correct predicate for binary object.
   *
   * @param op the specified predicate operation
   * @param initialValue the initial value, to be checked against, for the predicate
   * @return the predicate with correct operation and specified initial value
   */
  public static Predicate getPredicateForBinary(final TypePredicateOp op,
      final Object initialValue) {
    Predicate predicate = null;
    final byte[] binaryValue = initialValue == null ? null : (byte[]) initialValue;
    switch ((CompareOp) op) {
      case LESS:
        predicate = isLessThan(binaryValue);
        break;
      case LESS_OR_EQUAL:
        predicate = isLessThanOrEqual(binaryValue);
        break;
      case GREATER:
        predicate = isGreaterThan(binaryValue);
        break;
      case GREATER_OR_EQUAL:
        predicate = isGreaterThanOrEqual(binaryValue);
        break;
      case EQUAL:
        predicate = isEqual(binaryValue);
        break;
      case NOT_EQUAL:
        predicate = isNotEqual(binaryValue);
        break;
      case REGEX:
        /** pre-compile the provided regex for better performance.. **/
        predicate =
            isRegexMatchesBinary(Pattern.compile(Bytes.toString(binaryValue)).matcher("dummy"));
        break;
    }
    return predicate;
  }

  /**
   * Get the correct predicate for integer object.
   *
   * @param op the specified predicate operation
   * @param initialValue the initial value, to be checked against, for the predicate
   * @return the predicate with correct operation and specified initial value
   */
  public static Predicate getPredicateForInt(final TypePredicateOp op, final Object initialValue) {
    Predicate predicate = null;
    final Integer intValue = initialValue == null ? null : ((Number) initialValue).intValue();
    switch ((CompareOp) op) {
      case LESS:
        predicate = isLessThan(intValue);
        break;
      case LESS_OR_EQUAL:
        predicate = isLessThanOrEqual(intValue);
        break;
      case GREATER:
        predicate = isGreaterThan(intValue);
        break;
      case GREATER_OR_EQUAL:
        predicate = isGreaterThanOrEqual(intValue);
        break;
      case EQUAL:
        predicate = isEqual(intValue);
        break;
      case NOT_EQUAL:
        predicate = isNotEqual(intValue);
        break;
    }
    return predicate;
  }

  /**
   * Get the correct predicate for byte object.
   *
   * @param op the specified predicate operation
   * @param initialValue the initial value, to be checked against, for the predicate
   * @return the predicate with correct operation and specified initial value
   */
  public static Predicate getPredicateForByte(final TypePredicateOp op, final Object initialValue) {
    Predicate predicate = null;
    final Byte byteValue = initialValue == null ? null : ((Number) initialValue).byteValue();
    switch ((CompareOp) op) {
      case LESS:
        predicate = isLessThan(byteValue);
        break;
      case LESS_OR_EQUAL:
        predicate = isLessThanOrEqual(byteValue);
        break;
      case GREATER:
        predicate = isGreaterThan(byteValue);
        break;
      case GREATER_OR_EQUAL:
        predicate = isGreaterThanOrEqual(byteValue);
        break;
      case EQUAL:
        predicate = isEqual(byteValue);
        break;
      case NOT_EQUAL:
        predicate = isNotEqual(byteValue);
        break;
    }
    return predicate;
  }

  /**
   * Get the correct predicate for char object.
   *
   * @param op the specified predicate operation
   * @param initialValue the initial value, to be checked against, for the predicate
   * @return the predicate with correct operation and specified initial value
   */
  public static Predicate getPredicateForChar(final TypePredicateOp op, final char initialValue) {
    Predicate predicate = null;
    switch ((CompareOp) op) {
      case LESS:
        predicate = isLessThan(initialValue);
        break;
      case LESS_OR_EQUAL:
        predicate = isLessThanOrEqual(initialValue);
        break;
      case GREATER:
        predicate = isGreaterThan(initialValue);
        break;
      case GREATER_OR_EQUAL:
        predicate = isGreaterThanOrEqual(initialValue);
        break;
      case EQUAL:
        predicate = isEqual(initialValue);
        break;
      case NOT_EQUAL:
        predicate = isNotEqual(initialValue);
        break;
    }
    return predicate;
  }

  /**
   * Get the correct predicate for short object.
   *
   * @param op the specified predicate operation
   * @param initialValue the initial value, to be checked against, for the predicate
   * @return the predicate with correct operation and specified initial value
   */
  public static Predicate getPredicateForShort(final TypePredicateOp op,
      final Object initialValue) {
    Predicate predicate = null;
    final Short shortValue = initialValue == null ? null : ((Number) initialValue).shortValue();
    switch ((CompareOp) op) {
      case LESS:
        predicate = isLessThan(shortValue);
        break;
      case LESS_OR_EQUAL:
        predicate = isLessThanOrEqual(shortValue);
        break;
      case GREATER:
        predicate = isGreaterThan(shortValue);
        break;
      case GREATER_OR_EQUAL:
        predicate = isGreaterThanOrEqual(shortValue);
        break;
      case EQUAL:
        predicate = isEqual(shortValue);
        break;
      case NOT_EQUAL:
        predicate = isNotEqual(shortValue);
        break;
    }
    return predicate;
  }

  /**
   * Get the correct predicate for boolean object.
   *
   * @param op the specified predicate operation
   * @param initialValue the initial value, to be checked against, for the predicate
   * @return the predicate with correct operation and specified initial value
   */
  public static Predicate getPredicateForBoolean(final TypePredicateOp op,
      final Object initialValue) {
    Predicate predicate = null;
    switch ((CompareOp) op) {
      case EQUAL:
        predicate = isEqual(initialValue);
        break;
      case NOT_EQUAL:
        predicate = isNotEqual(initialValue);
        break;
    }
    return predicate;
  }

  /**
   * Get the correct predicate for Long objects.
   *
   * @param op the specified predicate operation
   * @param initialValue the initial value, to be checked against, for the predicate
   * @return the predicate with correct operation and specified initial value
   */
  public static Predicate getPredicateForLong(final TypePredicateOp op, final Object initialValue) {
    Predicate predicate = null;
    final Long longValue = initialValue == null ? null : ((Number) initialValue).longValue();
    switch ((CompareOp) op) {
      case LESS:
        predicate = isLessThan(longValue);
        break;
      case LESS_OR_EQUAL:
        predicate = isLessThanOrEqual(longValue);
        break;
      case GREATER:
        predicate = isGreaterThan(longValue);
        break;
      case GREATER_OR_EQUAL:
        predicate = isGreaterThanOrEqual(longValue);
        break;
      case EQUAL:
        predicate = isEqual(longValue);
        break;
      case NOT_EQUAL:
        predicate = isNotEqual(longValue);
        break;
    }
    return predicate;
  }

  /**
   * Get the correct predicate for Date objects.
   *
   * @param op the specified predicate operation
   * @param initialValue the initial value, to be checked against, for the predicate
   * @return the predicate with correct operation and specified initial value
   */
  public static Predicate getPredicateForDate(final TypePredicateOp op, final Object initialValue) {
    Predicate predicate = null;
    Date dateValue = initialValue instanceof Date ? (Date) initialValue
        : Date.valueOf(String.valueOf(initialValue));
    switch ((CompareOp) op) {
      case LESS:
        predicate = isLessThan(dateValue);
        break;
      case LESS_OR_EQUAL:
        predicate = isLessThanOrEqual(dateValue);
        break;
      case GREATER:
        predicate = isGreaterThan(dateValue);
        break;
      case GREATER_OR_EQUAL:
        predicate = isGreaterThanOrEqual(dateValue);
        break;
      case EQUAL:
        predicate = isEqual(dateValue);
        break;
      case NOT_EQUAL:
        predicate = isNotEqual(dateValue);
        break;
    }
    return predicate;
  }

  /**
   * Get the correct predicate for TimeStamp objects.
   *
   * @param op the specified predicate operation
   * @param initialValue the initial value, to be checked against, for the predicate
   * @return the predicate with correct operation and specified initial value
   */
  public static Predicate getPredicateForTimeStamp(final TypePredicateOp op,
      final Object initialValue) {
    Predicate predicate = null;
    Timestamp tsValue = initialValue instanceof Timestamp ? (Timestamp) initialValue
        : Timestamp.valueOf(String.valueOf(initialValue));
    switch ((CompareOp) op) {
      case LESS:
        predicate = isLessThan(tsValue);
        break;
      case LESS_OR_EQUAL:
        predicate = isLessThanOrEqual(tsValue);
        break;
      case GREATER:
        predicate = isGreaterThan(tsValue);
        break;
      case GREATER_OR_EQUAL:
        predicate = isGreaterThanOrEqual(tsValue);
        break;
      case EQUAL:
        predicate = isEqual(tsValue);
        break;
      case NOT_EQUAL:
        predicate = isNotEqual(tsValue);
        break;
    }
    return predicate;
  }

  /**
   * Get the correct predicate for Float objects.
   *
   * @param op the specified predicate operation
   * @param initialValue the initial value, to be checked against, for the predicate
   * @return the predicate with correct operation and specified initial value
   */
  public static Predicate getPredicateForFloat(final TypePredicateOp op,
      final Object initialValue) {
    Predicate predicate = null;
    final Float floatValue = initialValue == null ? null : ((Number) initialValue).floatValue();
    switch ((CompareOp) op) {
      case LESS:
        predicate = isLessThan(floatValue);
        break;
      case LESS_OR_EQUAL:
        predicate = isLessThanOrEqual(floatValue);
        break;
      case GREATER:
        predicate = isGreaterThan(floatValue);
        break;
      case GREATER_OR_EQUAL:
        predicate = isGreaterThanOrEqual(floatValue);
        break;
      case EQUAL:
        predicate = isEqual(floatValue);
        break;
      case NOT_EQUAL:
        predicate = isNotEqual(floatValue);
        break;
    }
    return predicate;
  }

  /**
   * Get the correct predicate for Float objects.
   *
   * @param op the specified predicate operation
   * @param initialValue the initial value, to be checked against, for the predicate
   * @return the predicate with correct operation and specified initial value
   */
  public static Predicate getPredicateForDouble(final TypePredicateOp op,
      final Object initialValue) {
    Predicate predicate = null;
    final Double doubleValue = initialValue == null ? null : ((Number) initialValue).doubleValue();
    switch ((CompareOp) op) {
      case LESS:
        predicate = isLessThan(doubleValue);
        break;
      case LESS_OR_EQUAL:
        predicate = isLessThanOrEqual(doubleValue);
        break;
      case GREATER:
        predicate = isGreaterThan(doubleValue);
        break;
      case GREATER_OR_EQUAL:
        predicate = isGreaterThanOrEqual(doubleValue);
        break;
      case EQUAL:
        predicate = isEqual(doubleValue);
        break;
      case NOT_EQUAL:
        predicate = isNotEqual(doubleValue);
        break;
    }
    return predicate;
  }

  /**
   * Get the correct predicate for String objects.
   *
   * @param op the specified predicate operation
   * @param initialValue the initial value, to be checked against, for the predicate
   * @return the predicate with correct operation and specified initial value
   */
  public static Predicate getPredicateForString(final TypePredicateOp op,
      final Object initialValue) {
    Predicate predicate = null;
    final String stringValue = initialValue == null ? null : initialValue.toString();
    switch ((CompareOp) op) {
      case EQUAL:
        predicate = isEqual(stringValue);
        break;
      case NOT_EQUAL:
        predicate = isNotEqual(stringValue);
        break;
      case REGEX:
        /** pre-compile the provided regex for better performance.. **/
        predicate = isRegexMatches(Pattern.compile(stringValue).matcher("dummy"));
        break;
      case LESS:
        predicate = isLessThan(stringValue);
        break;
      case LESS_OR_EQUAL:
        predicate = isLessThanOrEqual(stringValue);
        break;
      case GREATER:
        predicate = isGreaterThan(stringValue);
        break;
      case GREATER_OR_EQUAL:
        predicate = isGreaterThanOrEqual(stringValue);
        break;
    }
    return predicate;
  }

  /**
   * Get the correct predicate for Float objects.
   *
   * @param op the specified predicate operation
   * @param initialValue the initial value, to be checked against, for the predicate
   * @return the predicate with correct operation and specified initial value
   */
  public static Predicate getPredicateForBigDecimal(final TypePredicateOp op,
      final Object initialValue) {
    Predicate predicate = null;
    final BigDecimal bdValue = initialValue == null ? null
        : (initialValue instanceof BigDecimal ? (BigDecimal) initialValue
            : new BigDecimal(initialValue.toString()));
    switch ((CompareOp) op) {
      case LESS:
        predicate = isLessThan(bdValue);
        break;
      case LESS_OR_EQUAL:
        predicate = isLessThanOrEqual(bdValue);
        break;
      case GREATER:
        predicate = isGreaterThan(bdValue);
        break;
      case GREATER_OR_EQUAL:
        predicate = isGreaterThanOrEqual(bdValue);
        break;
      case EQUAL:
        predicate = isEqual(bdValue);
        break;
      case NOT_EQUAL:
        predicate = isNotEqual(bdValue);
        break;
    }
    return predicate;
  }

  /** helper methods for BigDecimal operations **/
  public static Predicate<BigDecimal> isLessThan(final BigDecimal target) {
    return (null == target) ? Objects::nonNull
        : me -> Objects.nonNull(me) && ((me.compareTo(target)) < 0);
  }

  public static Predicate<BigDecimal> isLessThanOrEqual(final BigDecimal target) {
    return (null == target) ? Objects::isNull
        : me -> Objects.nonNull(me) && ((me.compareTo(target)) <= 0);
  }

  public static Predicate<BigDecimal> isGreaterThan(final BigDecimal target) {
    return (null == target) ? Objects::nonNull
        : me -> Objects.nonNull(me) && ((me.compareTo(target)) > 0);
  }

  public static Predicate<BigDecimal> isGreaterThanOrEqual(final BigDecimal target) {
    return (null == target) ? Objects::isNull
        : me -> Objects.nonNull(me) && ((me.compareTo(target)) >= 0);
  }

  /** helper methods for String operations **/
  public static Predicate<String> isLessThan(final String target) {

    // Predicate<String> p = new Predicate<String>() {
    // @Override
    // public boolean test(String aString) {
    //
    // if (target == null) {
    // return aString == null;
    // }
    // else {
    // int res = aString.compareTo(target);
    // System.out.println("MPredicateHelper.test1 " + res);
    // System.out.println("MPredicateHelper.test2 " + (res < 0));
    // return (res < 0);
    // }
    // }
    // };
    // return p;

    return (null == target) ? Objects::nonNull
        : me -> Objects.nonNull(me) && ((me.compareTo(target)) < 0);
  }

  public static Predicate<String> isLessThanOrEqual(final String target) {
    return (null == target) ? Objects::isNull
        : me -> Objects.nonNull(me) && ((me.compareTo(target)) <= 0);
  }

  public static Predicate<String> isGreaterThan(final String target) {
    return (null == target) ? Objects::nonNull
        : me -> Objects.nonNull(me) && ((me.compareTo(target)) > 0);
  }

  public static Predicate<String> isGreaterThanOrEqual(final String target) {
    return (null == target) ? Objects::isNull
        : me -> Objects.nonNull(me) && ((me.compareTo(target)) >= 0);
  }

  /**
   * helper methods for the supported operations..
   **/
  public static Predicate<Integer> isLessThan(final Integer target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me < target;
  }

  public static Predicate<byte[]> isLessThan(final byte[] target) {
    return (null == target) ? Objects::nonNull
        : me -> me != null && Bytes.compareTo(me, target) < 0;
  }

  public static Predicate<Byte> isLessThan(final Byte target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me < target;
  }

  public static Predicate<Character> isLessThan(final Character target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me < target;
  }

  public static Predicate<Short> isLessThan(final Short target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me < target;
  }

  public static Predicate<Long> isLessThan(final Long target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me < target;
  }

  public static Predicate<Date> isLessThan(final Date target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me.before(target);
  }

  public static Predicate<Timestamp> isLessThan(final Timestamp target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me.before(target);
  }

  public static Predicate<Integer> isLessThanOrEqual(final Integer target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me <= target;
  }

  public static Predicate<byte[]> isLessThanOrEqual(final byte[] target) {
    return (null == target) ? Objects::isNull
        : me -> me != null && Bytes.compareTo(me, target) <= 0;
  }

  public static Predicate<Byte> isLessThanOrEqual(final Byte target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me <= target;
  }

  public static Predicate<Character> isLessThanOrEqual(final Character target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me <= target;
  }

  public static Predicate<Short> isLessThanOrEqual(final Short target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me <= target;
  }

  public static Predicate<Long> isLessThanOrEqual(final Long target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me <= target;
  }

  public static Predicate<Date> isLessThanOrEqual(final Date target) {
    return (null == target) ? Objects::isNull : me -> (me.before(target) || me.equals(target));
  }

  public static Predicate<Timestamp> isLessThanOrEqual(final Timestamp target) {
    return (null == target) ? Objects::isNull : me -> (me.before(target) || me.equals(target));
  }

  public static Predicate<Integer> isGreaterThan(final Integer target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me > target;
  }

  public static Predicate<byte[]> isGreaterThan(final byte[] target) {
    return (null == target) ? Objects::nonNull
        : me -> me != null && Bytes.compareTo(me, target) > 0;
  }

  public static Predicate<Byte> isGreaterThan(final Byte target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me > target;
  }

  public static Predicate<Character> isGreaterThan(final Character target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me > target;
  }

  public static Predicate<Short> isGreaterThan(final Short target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me > target;
  }

  public static Predicate<Long> isGreaterThan(final Long target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me > target;
  }

  public static Predicate<Date> isGreaterThan(final Date target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me.after(target);
  }

  public static Predicate<Timestamp> isGreaterThan(final Timestamp target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me.after(target);
  }

  public static Predicate<Integer> isGreaterThanOrEqual(final Integer target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me >= target;
  }

  public static Predicate<byte[]> isGreaterThanOrEqual(final byte[] target) {
    return (null == target) ? Objects::isNull
        : me -> me != null && Bytes.compareTo(me, target) >= 0;
  }

  public static Predicate<Byte> isGreaterThanOrEqual(final Byte target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me >= target;
  }

  public static Predicate<Character> isGreaterThanOrEqual(final Character target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me >= target;
  }

  public static Predicate<Short> isGreaterThanOrEqual(final Short target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me >= target;
  }

  public static Predicate<Long> isGreaterThanOrEqual(final Long target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me >= target;
  }

  public static Predicate<Date> isGreaterThanOrEqual(final Date target) {
    return (null == target) ? Objects::isNull : me -> (me.after(target) || me.equals(target));
  }

  public static Predicate<Timestamp> isGreaterThanOrEqual(final Timestamp target) {
    return (null == target) ? Objects::isNull : me -> (me.after(target) || me.equals(target));
  }

  public static Predicate isEqual(final Object target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me.equals(target);
  }

  public static Predicate<byte[]> isEqual(final byte[] target) {
    return (null == target) ? Objects::isNull
        : me -> me != null && Bytes.compareTo(me, target) == 0;
  }

  public static Predicate isNotEqual(final Object target) {
    return (null == target) ? Objects::nonNull : me -> Objects.isNull(me) || !me.equals(target);
  }

  public static Predicate<byte[]> isNotEqual(final byte[] target) {
    return (null == target) ? Objects::nonNull
        : me -> me != null && Bytes.compareTo(me, target) != 0;
  }

  public static Predicate<String> isRegexMatches(final Matcher target) {
    return me -> Objects.nonNull(me) && target.reset(me).matches();
  }

  public static Predicate<byte[]> isRegexMatchesBinary(final Matcher target) {
    return me -> Objects.nonNull(me) && target.reset(Bytes.toString(me)).matches();
  }

  /** float functions **/
  public static Predicate<Float> isLessThan(final Float target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me < target;
  }

  public static Predicate<Float> isLessThanOrEqual(final Float target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me <= target;
  }

  public static Predicate<Float> isGreaterThan(final Float target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me > target;
  }

  public static Predicate<Float> isGreaterThanOrEqual(final Float target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me >= target;
  }

  /** double functions **/
  public static Predicate<Double> isLessThan(final Double target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me < target;
  }

  public static Predicate<Double> isLessThanOrEqual(final Double target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me <= target;
  }

  public static Predicate<Double> isGreaterThan(final Double target) {
    return (null == target) ? Objects::nonNull : me -> Objects.nonNull(me) && me > target;
  }

  public static Predicate<Double> isGreaterThanOrEqual(final Double target) {
    return (null == target) ? Objects::isNull : me -> Objects.nonNull(me) && me >= target;
  }
}
