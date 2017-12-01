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

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.interfaces.Function3;
import io.ampool.monarch.types.interfaces.TypePredicate;
import io.ampool.monarch.types.interfaces.TypePredicateOp;

import java.sql.Timestamp;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Helper class to facilitate serialization and de-serialization from/to other types.
 *
 * Since version: 0.2.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum BasicTypes implements DataType, TypePredicate {
  BINARY(TypeHelper.BinaryToBytesFunction, TypeHelper.BytesToBinaryFunction,
      TypeHelper.BinaryPredicateFunction, false, -1, null),
  /**
   * Integer type with respective serialization, deserialization, and predicate functions.
   */
  INT(TypeHelper.IntToBytesFunction, TypeHelper.BytesToIntFunction, TypeHelper.IntPredicateFunction, true, Integer.BYTES, Integer.valueOf(0)),

  /** Optimized/compressed integer and long values.. **/
  O_INT(TypeHelper.OIntToBytesFunction, TypeHelper.BytesToOIntFunction, TypeHelper.IntPredicateFunction, false, -1, null), O_LONG(TypeHelper.OLongToBytesFunction, TypeHelper.BytesToOLongFunction, TypeHelper.LongPredicateFunction, false, -1, null),

  /**
   * Long type with respective serialization, deserialization, and predicate functions.
   */
  LONG(TypeHelper.LongToBytesFunction, TypeHelper.BytesToLongFunction, TypeHelper.LongPredicateFunction, true, Long.BYTES, Long.valueOf(0)),
  /**
   * Float type with respective serialization, deserialization, and predicate functions.
   */
  FLOAT(TypeHelper.FloatToBytesFunction, TypeHelper.BytesToFloatFunction, TypeHelper.FloatPredicateFunction, true, Float.BYTES, Float.valueOf(0)),
  /**
   * Double type with respective serialization, deserialization, and predicate functions.
   */
  DOUBLE(TypeHelper.DoubleToBytesFunction, TypeHelper.BytesToDoubleFunction, TypeHelper.DoublePredicateFunction, true, Double.BYTES, Double.valueOf(0)),
  /**
   * String type with respective serialization, deserialization, and predicate functions.
   */
  STRING(TypeHelper.StringToBytesFunction, TypeHelper.BytesToStringFunction, TypeHelper.StringPredicateFunction, false, -1, null),
  /**
   * BigDecimal type with respective serialization, deserialization, and predicate functions.
   */
  BIG_DECIMAL(TypeHelper.BigDecimalToBytesFunction, TypeHelper.BytesToBigDecimalFunction, TypeHelper.BigDecimalPredicateFunction, false, -1, null),
  /**
   * Byte type with respective serialization, deserialization, and predicate functions.
   */
  BYTE(TypeHelper.ByteToBytesFunction, TypeHelper.BytesToByteFunction, TypeHelper.BytePredicateFunction, true, Byte.BYTES, Byte.valueOf((byte) 0)),
  /**
   * Short type with respective serialization, deserialization, and predicate functions.
   */
  SHORT(TypeHelper.ShortToBytesFunction, TypeHelper.BytesToShortFunction, TypeHelper.ShortPredicateFunction, true, Short.BYTES, Short.valueOf((short) 0)),
  /**
   * Boolean type with respective serialization, deserialization, and predicate functions.
   */
  BOOLEAN(TypeHelper.BooleanToBytesFunction, TypeHelper.BytesToBooleanFunction, TypeHelper.BooleanPredicateFunction, true, 1, Boolean.FALSE),
  /**
   * Char type with respective serialization, deserialization, and predicate functions.
   */
  CHAR(TypeHelper.CharToBytesFunction, TypeHelper.BytesToCharFunction, TypeHelper.CharPredicateFunction, true, Character.BYTES, new Character('0')),
  /**
   * Date type with respective serialization, deserialization, and predicate functions.
   */
  DATE(TypeHelper.DateToBytesFunction, TypeHelper.BytesToDateFunction, TypeHelper.DatePredicateFunction, true, Long.BYTES, new java.sql.Date(0)),
  /**
   * TimeStamp type with respective serialization, deserialization, and predicate functions.
   */
  TIMESTAMP(TypeHelper.TimeStampToBytesFunction, TypeHelper.BytesToTimeStampFunction, TypeHelper.TimeStampPredicateFunction, true, Long.BYTES, new Timestamp(0)),
  /**
   * HiveChar type with respective serialization, deserialization, and predicate functions.
   */
  CHARS(TypeHelper.StringToBytesFunction, TypeHelper.BytesToStringFunction, TypeHelper.StringPredicateFunction, false, -1, null),
  /**
   * HiveVarChar type with respective serialization, deserialization, and predicate functions.
   */
  VARCHAR(TypeHelper.StringToBytesFunction, TypeHelper.BytesToStringFunction, TypeHelper.StringPredicateFunction, false, -1, null);

  private final Function<Object, byte[]> serializeFunction;
  private final BiFunction<TypePredicateOp, Object, Predicate> predicateFunction;
  private final Function3<byte[], Integer, Integer, Object> deserializeFunction;
  private final boolean isFixedLength;
  private final int lengthOfByteArray;
  private final Object defaultValue;

  /**
   * Arguments, if any, provided to the type.. not used, at the moment, except the string
   * representation of the type. For example CHAR(100) or BIG_DECIMAL(10,10)
   *
   * @see BasicTypes#toString()
   */
  private String args = null;

  /**
   * The required details for serialization, de-serialization and predicate for supported types.
   *
   * @param serFunction the serialization function converting an object into byte-array
   * @param desFunction the deserialization function converting a byte-array into respective object
   * @param pFunction the function redirecting to the respective predicate operation
   */
  BasicTypes(final Function<Object, byte[]> serFunction,
      final Function3<byte[], Integer, Integer, Object> desFunction,
      final BiFunction<TypePredicateOp, Object, Predicate> pFunction, final boolean isFixedLength,
      final int lengthOfByteArray, final Object defaultValue) {
    this.serializeFunction = serFunction;
    this.deserializeFunction = desFunction;
    this.predicateFunction = pFunction;
    this.isFixedLength = isFixedLength;
    this.lengthOfByteArray = lengthOfByteArray;
    this.defaultValue = defaultValue;
  }

  /**
   * Construct and get the object with correct type from bytes.
   *
   * @param bytes an array of bytes representing an object
   * @param offset offset in the byte-array
   * @param length number of bytes, from offset, to read from the byte-array
   * @return the actual Java object with the respective type
   */
  @Override
  public Object deserialize(final byte[] bytes, final Integer offset, final Integer length) {
    return length <= 0 ? null : deserializeFunction.apply(bytes, offset, length);
  }

  /**
   * Convert the Java object into bytes correctly for the specified type.
   *
   * @param object the actual Java object
   * @return the bytes representing object
   */
  @Override
  public byte[] serialize(Object object) {
    return object == null ? new byte[0] : serializeFunction.apply(object);
  }

  /**
   * Provide the required predicate for execution. It takes following things into consideration: -
   * the argument type {@link BasicTypes} - the predicate operation {@link CompareOp} - the initial
   * value, all the values shall be compared against this value
   *
   * @param op the predicate operation
   * @param initialValue initial (constant) value to be tested against
   * @return a predicate that tests the argument against initialValue for the specified operation
   */
  @Override
  public Predicate getPredicate(final TypePredicateOp op, final Object initialValue) {
    return predicateFunction.apply(op, initialValue);
  }

  /**
   * Set arguments, if any..
   *
   * @param args the arguments, if any
   */
  public BasicTypes setArgs(final String args) {
    this.args = args;
    return this;
  }

  /**
   * String representation of the type.. If the type was provided arguments, include these within
   * the argument separators else simply provide the name().
   *
   * @return the string representation of type
   */
  @Override
  public String toString() {
    return this.args == null ? this.name()
        : this.name() + DataType.TYPE_ARGS_BEGIN_CHAR + args + DataType.TYPE_ARGS_END_CHAR;
  }


  @Override
  public Category getCategory() {
    return Category.Basic;
  }

  @Override
  public boolean isFixedLength() {
    return this.isFixedLength;
  }

  @Override
  public int lengthOfByteArray() {
    return this.lengthOfByteArray;
  }

  /** getters for serialization, deserialization and predicate functions.. **/
  public Function3<byte[], Integer, Integer, Object> getDeserializeFunction() {
    return this.deserializeFunction;
  }

  public Function<Object, byte[]> getSerializeFunction() {
    return this.serializeFunction;
  }

  public BiFunction<TypePredicateOp, Object, Predicate> getPredicateFunction() {
    return this.predicateFunction;
  }

  @Override
  public Object defaultValue() {
    return this.defaultValue;
  }
}
