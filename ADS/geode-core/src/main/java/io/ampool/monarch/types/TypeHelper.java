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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.types.interfaces.Function3;
import io.ampool.monarch.types.interfaces.TypePredicateOp;
import org.json.JSONObject;

/**
 * Helper class/methods for various utilities..
 * <p>
 * Since version: 0.2.0
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class TypeHelper {
  public static final int IntegerLength = Integer.SIZE / Byte.SIZE;
  public static final byte NULL_BYTE =
      Integer.getInteger("ampool.null.byte", Integer.parseInt("10111111", 2)).byteValue();
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[] {NULL_BYTE};

  public static final boolean IS_EMPTY(final byte[] bytes, final int offset, final int length) {
    return (length == EMPTY_BYTE_ARRAY.length) && bytes[offset] == NULL_BYTE;
  }

  /** utility methods.. **/
  public static final Function3<byte[], Integer, Integer, Integer> BytesToIntFunction1 =
      (e, offset, length) -> ByteBuffer.wrap(e, offset, length).asIntBuffer().get();

  public static int readInt(final byte[] bytes, final int offset) {
    return (int) BytesToIntFunction.apply(bytes, offset, IntegerLength);
  }

  public static void writeInt(final ByteArrayOutputStream bos, final int intValue) {
    try {
      bos.write(IntToBytesFunction.apply(intValue));
    } catch (IOException e) {
    }
  }

  public static void writeBytes(final ByteArrayOutputStream bos, final byte[] bytes) {
    writeInt(bos, bytes.length);
    try {
      bos.write(bytes);
    } catch (IOException e) {
    }
  }

  /** functions to convert from byte-array to native types **/
  public static final Function3<byte[], Integer, Integer, Object> BytesToBinaryFunction =
      (e, o, l) -> IS_EMPTY(e, o, l) ? new byte[0] : Arrays.copyOfRange(e, o, o + l);
  public static final Function3<byte[], Integer, Integer, Object> BytesToCharFunction =
      (e, o, l) -> ByteBuffer.wrap(e, o, l).asCharBuffer().get();
  public static final Function3<byte[], Integer, Integer, Object> BytesToByteFunction =
      (e, o, l) -> e[o];
  public static final Function3<byte[], Integer, Integer, Object> BytesToShortFunction =
      Bytes::toShort;
  public static final Function3<byte[], Integer, Integer, Object> BytesToBooleanFunction =
      (e, o, l) -> e[o] != (byte) 0;
  public static final Function3<byte[], Integer, Integer, Object> BytesToIntFunction = Bytes::toInt;
  public static final Function3<byte[], Integer, Integer, Object> BytesToLongFunction =
      Bytes::toLong;
  public static final Function3<byte[], Integer, Integer, Object> BytesToDateFunction =
      (e, o, l) -> new Date((long) TypeHelper.BytesToLongFunction.apply(e, o, l));
  public static final Function3<byte[], Integer, Integer, Object> BytesToTimeStampFunction =
      (e, o, l) -> new Timestamp((long) TypeHelper.BytesToLongFunction.apply(e, o, l));
  public static final Function3<byte[], Integer, Integer, Object> BytesToFloatFunction =
      (e, o, l) -> Bytes.toFloat(e, o);
  public static final Function3<byte[], Integer, Integer, Object> BytesToDoubleFunction =
      (e, o, l) -> Bytes.toDouble(e, o);
  public static final Function3<byte[], Integer, Integer, Object> BytesToStringFunction =
      (e, o, l) -> {
        try {
          return IS_EMPTY(e, o, l) ? "" : new String(e, o, l, "UTF-8");
        } catch (UnsupportedEncodingException e1) {
          return null;
        }
      };
  /**
   * BigDecimal
   **/
  public static final Function3<byte[], Integer, Integer, Object> BytesToBigDecimalFunction =
      Bytes::toBigDecimal;

  /** functions to convert native objects to byte-array */
  public static final Function<Object, byte[]> BinaryToBytesFunction =
      e -> ((byte[]) e).length == 0 ? EMPTY_BYTE_ARRAY : (byte[]) e;
  public static final Function<Object, byte[]> ByteToBytesFunction = e -> new byte[] {(byte) e};
  public static final Function<Object, byte[]> ShortToBytesFunction = e -> Bytes.toBytes((short) e);
  public static final Function<Object, byte[]> BooleanToBytesFunction =
      e -> Bytes.toBytes((Boolean) e);
  public static final Function<Object, byte[]> CharToBytesFunction =
      e -> ByteBuffer.allocate(Character.SIZE / Byte.SIZE).putChar(((char) e)).array();
  public static final Function<Object, byte[]> IntToBytesFunction = e -> Bytes.toBytes((int) e);
  public static final Function<Object, byte[]> LongToBytesFunction = e -> Bytes.toBytes((long) e);
  public static final Function<Object, byte[]> DateToBytesFunction =
      e -> Bytes.toBytes(((Date) e).getTime());
  public static final Function<Object, byte[]> TimeStampToBytesFunction =
      e -> Bytes.toBytes(((Timestamp) e).getTime());
  public static final Function<Object, byte[]> FloatToBytesFunction = e -> Bytes.toBytes((float) e);
  public static final Function<Object, byte[]> DoubleToBytesFunction =
      e -> Bytes.toBytes((double) e);
  public static final Function<Object, byte[]> StringToBytesFunction =
      (Function<Object, byte[]> & Serializable) (e -> {
        try {
          return e.toString().length() == 0 ? EMPTY_BYTE_ARRAY
              : String.valueOf(e).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e1) {
          return new byte[0];
        }
      });
  /**
   * BIgDecimal
   **/
  public static final Function<Object, byte[]> BigDecimalToBytesFunction =
      (Function<Object, byte[]> & Serializable) (e -> Bytes.toBytes((BigDecimal) e));

  /** functions to provide correct predicate for the respective types **/
  public static final BiFunction<TypePredicateOp, Object, Predicate> BinaryPredicateFunction =
      MPredicateHelper::getPredicateForBinary;
  public static final BiFunction<TypePredicateOp, Object, Predicate> CharPredicateFunction =
      (op, value) -> MPredicateHelper.getPredicateForChar(op, ((Character) value));
  public static final BiFunction<TypePredicateOp, Object, Predicate> BytePredicateFunction =
      MPredicateHelper::getPredicateForByte;
  public static final BiFunction<TypePredicateOp, Object, Predicate> ShortPredicateFunction =
      MPredicateHelper::getPredicateForShort;
  public static final BiFunction<TypePredicateOp, Object, Predicate> BooleanPredicateFunction =
      MPredicateHelper::getPredicateForBoolean;
  public static final BiFunction<TypePredicateOp, Object, Predicate> IntPredicateFunction =
      MPredicateHelper::getPredicateForInt;
  public static final BiFunction<TypePredicateOp, Object, Predicate> LongPredicateFunction =
      MPredicateHelper::getPredicateForLong;
  public static final BiFunction<TypePredicateOp, Object, Predicate> DatePredicateFunction =
      MPredicateHelper::getPredicateForDate;
  public static final BiFunction<TypePredicateOp, Object, Predicate> TimeStampPredicateFunction =
      MPredicateHelper::getPredicateForTimeStamp;
  public static final BiFunction<TypePredicateOp, Object, Predicate> FloatPredicateFunction =
      MPredicateHelper::getPredicateForFloat;
  public static final BiFunction<TypePredicateOp, Object, Predicate> DoublePredicateFunction =
      MPredicateHelper::getPredicateForDouble;
  public static final BiFunction<TypePredicateOp, Object, Predicate> StringPredicateFunction =
      MPredicateHelper::getPredicateForString;

  public static BiFunction<TypePredicateOp, Object, Predicate> BigDecimalPredicateFunction =
      MPredicateHelper::getPredicateForBigDecimal;

  /******************** START -- COMPRESSED INTEGER AND LOG SERIALIZATION ******************/
  /////////// From hive-serde LazyBinaryUtils.. uses hadoop-common WritableUtils ///////////
  /** allocate the bytes per thread and reuse for all subsequent operations **/
  private static ThreadLocal<byte[]> threadLocalBytes = new ThreadLocal<byte[]>() {
    @Override
    public byte[] initialValue() {
      return new byte[9];
    }
  };

  /** respective functions to convert to/from integer and long **/
  public static final Function3<byte[], Integer, Integer, Object> BytesToOIntFunction =
      (e, o, l) -> readVInt(e, o);
  public static final Function3<byte[], Integer, Integer, Object> BytesToOLongFunction =
      (e, o, l) -> readVLong(e, o);

  public static final Function<Object, byte[]> OIntToBytesFunction = e -> {
    byte[] bytes = threadLocalBytes.get();
    int len = writeVLongToByteArray(bytes, 0, (int) e);
    return Arrays.copyOfRange(bytes, 0, len);
  };

  public static final Function<Object, byte[]> OLongToBytesFunction = e -> {
    byte[] vLongBytes = threadLocalBytes.get();
    int len = writeVLongToByteArray(vLongBytes, 0, (long) e);
    return Arrays.copyOfRange(vLongBytes, 0, len);
  };

  /**
   * Read integer from the bytes; first byte contains length so no need to separately provide
   * length.
   *
   * @param bytes bytes to be converted to int
   * @param offset offset in the bytes
   * @return an integer
   */
  public static int readVInt(byte[] bytes, int offset) {
    byte firstByte = bytes[offset];
    // TODO : Merge to look into
    int len = 0;// (byte) WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    int i = 0;
    for (int idx = 0; idx < len - 1; idx++) {
      byte b = bytes[offset + 1 + idx];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    // TODO : Merge to look into
    return 0;// (WritableUtils.isNegativeVInt(firstByte) ? (~i) : i);
  }

  /**
   * Read long from the bytes; first byte contains length so no need to separately provide length.
   *
   * @param bytes bytes to be converted to long
   * @param offset offset in the bytes
   * @return long
   */
  public static long readVLong(final byte[] bytes, int offset) {
    byte firstByte = bytes[offset++];
    // TODO : Merge to look into
    int len = 0;
    // int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len - 1; idx++) {
      byte b = bytes[offset++];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    // TODO : Merge to look into
    return 0;
    // return (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
  }

  /**
   * Convert the integer to bytes and write bytes to the stream.
   *
   * @param bos the byte-array-output-stream
   * @param i an integer value to serialize
   */
  public static void writeVInt(final ByteArrayOutputStream bos, int i) {
    TypeHelper.writeVLong(bos, i);
  }

  /**
   * Convert the long to bytes and write bytes to the stream.
   *
   * @param bos the byte-array-output-stream
   * @param l the long value to serialize
   */
  public static void writeVLong(final ByteArrayOutputStream bos, long l) {
    byte[] vLongBytes = threadLocalBytes.get();
    int len = writeVLongToByteArray(vLongBytes, 0, l);
    bos.write(vLongBytes, 0, len);
  }

  /**
   * Serialize the long value and write the respective bytes to the provided byte-array start at the
   * specified offset. Returns the number of bytes wrote the byte array.
   *
   * @param bytes an array of bytes where serialized value will be stored
   * @param offset the offset in the provided byte-array for writing
   * @param l the long value
   * @return the number of bytes written in the provided byte-array
   */
  public static int writeVLongToByteArray(byte[] bytes, int offset, long l) {
    if (l >= -112 && l <= 127) {
      bytes[offset] = (byte) l;
      return 1;
    }

    int len = -112;
    if (l < 0) {
      l ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = l;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    bytes[offset] = (byte) len;

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      bytes[offset + 1 - (idx - len)] = (byte) ((l & mask) >> shiftbits);
    }
    return 1 + len;
  }

  /******************** END -- COMPRESSED INTEGER AND LOG SERIALIZATION ******************/

  /**
   * Generic method to convert object to string by traversing elements recursively. In case of an
   * array or list all the elements are converted to string recursively.
   *
   * @param object the object to be converted to string representation
   * @param sb the string-builder to append the string representation
   */
  @SuppressWarnings("unchecked")
  public static void deepToString(final Object object, final StringBuilder sb) {
    deepToString(object, sb, false);
  }

  /**
   * Generic method to convert object to string by traversing elements recursively. In case of an
   * array or list all the elements are converted to string recursively. In case quoting is
   * requested, the primitive values are quoted even from the complex types.
   *
   * @param object the object to be converted to string representation
   * @param sb the string-builder to append the string representation
   * @param doQuote whether or not to quote the primitive values
   */
  @SuppressWarnings("unchecked")
  public static void deepToString(final Object object, final StringBuilder sb,
      final boolean doQuote) {
    if (object != null && object.getClass().isArray()) {
      sb.append('[');
      final int length = Array.getLength(object);
      for (int i = 0; i < length; ++i) {
        deepToString(Array.get(object, i), sb, doQuote);
        sb.append(", ");
      }
      /** delete last two characters.. **/
      if (length > 0) {
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(sb.length() - 1);
      }
      sb.append(']');
    } else if (object != null && object instanceof Collection) {
      deepToString(((Collection) object).toArray(), sb, doQuote);
    } else if (object != null && object instanceof Map) {
      Map<Object, Object> map = (Map<Object, Object>) object;
      sb.append('{');
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        deepToString(entry.getKey(), sb, doQuote);
        sb.append('=');
        deepToString(entry.getValue(), sb, doQuote);
        sb.append(", ");
      }
      /** delete last two characters.. **/
      if (map.size() > 0) {
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(sb.length() - 1);
      }
      sb.append('}');
    } else {
      sb.append(object != null && doQuote ? JSONObject.quote(String.valueOf(object))
          : String.valueOf(object));
    }
  }

  public static String deepToString(final Object object) {
    StringBuilder sb = new StringBuilder(32);
    TypeHelper.deepToString(object, sb);
    return sb.toString();
  }
}
