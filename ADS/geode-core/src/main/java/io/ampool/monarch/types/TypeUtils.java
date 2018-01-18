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

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.security.SecureRandom;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableColumnType;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.internal.AbstractTableDescriptor;
import io.ampool.monarch.types.interfaces.DataType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Since version: 1.0.1
 */
public class TypeUtils {
  /**
   * Don't allow the c'tor..
   */
  private TypeUtils() {
    throw new IllegalArgumentException("Must not be instantiated.");
  }

  /********************** START: Utility for generating random data ***************************/
  /**
   * Instantiate the random number generator..
   **/
  private static final SecureRandom RANDOM = new SecureRandom();

  private static final AtomicInteger intCounter = new AtomicInteger(0);

  /**
   * the date-range
   **/
  private static final long BEG_DAY = LocalDate.of(1900, 1, 1).toEpochDay();
  private static final long END_DAY = LocalDate.of(2100, 12, 31).toEpochDay();
  private static final int DAYS_RANGE = (int) (END_DAY - BEG_DAY);
  private static final int DAY_SECONDS = 86400;

  /**
   * maximum length for variable length types
   **/
  private static final int MAX_BYTES_LEN = Integer.getInteger("max.bytes.length", 50);
  private static final int MAX_STRING_LEN = Integer.getInteger("max.string.length", 50);
  private static final int MAX_CHARS_LEN = Integer.getInteger("max.chars.length", 15);
  private static final int MAX_LIST_LEN = Integer.getInteger("max.list.length", 10);
  private static final int MAX_DECIMAL_LEN = Integer.getInteger("max.decimal.length", 8);

  /**
   * random character to pick from
   **/
  private static final String CHARS =
          "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  private static final Map<DataType, Supplier<Object>> RANDOM_DATA_TYPE_MAP =
          Collections.unmodifiableMap(new HashMap<DataType, Supplier<Object>>(15) {
            {
              put(BasicTypes.BINARY, TypeUtils::getRandomBytes);
              put(BasicTypes.INT, RANDOM::nextInt);
              put(BasicTypes.LONG, RANDOM::nextLong);
              put(BasicTypes.SHORT, () -> new Integer(RANDOM.nextInt()).shortValue());
              put(BasicTypes.BYTE, () -> new Integer(RANDOM.nextInt()).byteValue());
              put(BasicTypes.BOOLEAN, RANDOM::nextBoolean);
              put(BasicTypes.DOUBLE, RANDOM::nextDouble);
              put(BasicTypes.FLOAT, RANDOM::nextFloat);
              put(BasicTypes.STRING,
                      () -> new BigInteger(getRandomInt(MAX_STRING_LEN) * 8, RANDOM).toString(32));
              put(BasicTypes.VARCHAR,
                      () -> new BigInteger(getRandomInt(MAX_STRING_LEN) * 8, RANDOM).toString(32));
              put(BasicTypes.BIG_DECIMAL, () -> getBigDecimal(13, 13));
              put(BasicTypes.CHAR, () -> CHARS.charAt(RANDOM.nextInt(CHARS.length())));
              put(BasicTypes.CHARS,
                      () -> new BigInteger(getRandomInt(MAX_CHARS_LEN) * 8, RANDOM).toString(32));
              put(BasicTypes.DATE, () -> new Date(LocalDateTime.of(getRandomDate(), LocalTime.now())
                      .toInstant(ZoneOffset.UTC).toEpochMilli()));
              put(BasicTypes.TIMESTAMP, () -> new Timestamp(LocalDateTime
                      .of(getRandomDate(), getRandomTime()).toInstant(ZoneOffset.UTC).toEpochMilli()));
            }
          });

  private static final Map<DataType, Class> BASIC_TYPE_TO_PRIMITIVE_MAP =
          Collections.unmodifiableMap(new HashMap<DataType, Class>(15) {
            {
              put(BasicTypes.INT, int.class);
              put(BasicTypes.LONG, long.class);
              put(BasicTypes.SHORT, short.class);
              put(BasicTypes.BYTE, byte.class);
              put(BasicTypes.BOOLEAN, boolean.class);
              put(BasicTypes.DOUBLE, double.class);
              put(BasicTypes.FLOAT, float.class);
              put(BasicTypes.STRING, String.class);
              put(BasicTypes.VARCHAR, String.class);
              put(BasicTypes.BIG_DECIMAL, BigDecimal.class);
              put(BasicTypes.CHAR, char.class);
              put(BasicTypes.CHARS, String.class);
              put(BasicTypes.DATE, Date.class);
              put(BasicTypes.TIMESTAMP, Timestamp.class);
            }
          });

  /**
   * Get the random number of bytes..
   *
   * @return the byte array
   */
  public static byte[] getRandomBytes() {
    return getRandomBytes(RANDOM.nextInt(MAX_BYTES_LEN));
  }

  /**
   * Get the random bytes of the specified length.
   *
   * @return the byte array
   */
  public static byte[] getRandomBytes(final int length) {
    final byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  /**
   * Get the positive random integer bounded by the specified value. The value returned will be in
   * in range 0 to bound (excluded).
   *
   * @param bound the required upper bound for the integer
   * @return the random positive integer between 0 and bound
   */
  public static final int getRandomInt(final int bound) {
    return Math.abs(RANDOM.nextInt(bound));
  }

  /**
   * Get the random date that falls between 1-Jan-1900 and 31-Dec-2100.
   *
   * @return the date
   */
  public static final LocalDate getRandomDate() {
    return LocalDate.ofEpochDay(BEG_DAY + Math.abs(RANDOM.nextInt(DAYS_RANGE)));
  }

  /**
   * Get the random time of day in seconds.
   *
   * @return the time in seconds
   */
  public static final LocalTime getRandomTime() {
    return LocalTime.ofSecondOfDay(getRandomInt(DAY_SECONDS));
  }

  /**
   * Get a random big-decimal of specified scale and precision.
   *
   * @param scale the scale
   * @param precision the precision
   * @return the random big-decimal value
   */
  public static BigDecimal getBigDecimal(final int scale, final int precision) {
    final BigInteger bi = new BigInteger(MAX_DECIMAL_LEN * 8, RANDOM);
    return new BigDecimal(new BigDecimal(bi, new MathContext(precision)).unscaledValue(), scale);
  }

  public static Object getRandomValue(final DataType inType, boolean autoincrementInt) {
    if (inType.toString().equals("INT")) {
      return intCounter.getAndIncrement();
    } else {
      return getRandomValue(inType);
    }
  }


  /**
   * Get the random value for the specified type.
   *
   * @param inType the object type
   * @return the random value for the specified type
   */
  public static Object getRandomValue(final DataType inType) {
    final DataType type =
            inType instanceof WrapperType ? ((WrapperType) inType).getBasicObjectType() : inType;
    Object retValue = null;
    if (type instanceof BasicTypes) {
      final Supplier<Object> supplier = RANDOM_DATA_TYPE_MAP.get(type);
      if (supplier == null) {
        throw new IllegalArgumentException("Unsupported type: " + type.toString());
      }
      retValue = supplier.get();
      /* use arguments, if any, to generate the random value. */
      if (inType instanceof WrapperType) {
        final WrapperType wType = (WrapperType) inType;
        if (type.equals(BasicTypes.BIG_DECIMAL)) {
          final String[] s = wType.getArgs().replaceAll("[()\\s]", "").split(",");
          final int precision = Integer.parseInt(s[0]);
          final int scale = Integer.parseInt(s[1]);
          BigDecimal bd = (BigDecimal) retValue;
          if (scale == bd.scale() && precision == bd.precision()) {
            retValue = bd;
          } else {
            retValue = getBigDecimal(scale, precision);
          }
        } else if (type.equals(BasicTypes.VARCHAR)) {
          final int len = Integer.parseInt(wType.getArgs());
          final String s = (String) retValue;
          final int end = s.length() > len ? len : s.length();
          retValue = new String(s.substring(0, end));
        }
      }
    } else if (type instanceof ListType) {
      final int size = getRandomInt(MAX_LIST_LEN);
      List<Object> list = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        list.add(getRandomValue(((ListType) type).getTypeOfElement()));
      }
      retValue = list;
    } else if (type instanceof MapType) {
      final Object kValue = getRandomValue(((MapType) type).getTypeOfKey());
      final Object vValue = getRandomValue(((MapType) type).getTypeOfValue());
      if (kValue != null) {
        retValue = new HashMap<Object, Object>(1) {
          {
            put(kValue, vValue);
          }
        };
      }
    } else if (type instanceof StructType) {
      final DataType[] types = ((StructType) type).getColumnTypes();
      final Object[] values = new Object[types.length];
      for (int i = 0; i < types.length; i++) {
        values[i] = getRandomValue(types[i]);
      }
      retValue = values;
    } else if (type instanceof UnionType) {
      final DataType[] types = ((UnionType) type).getColumnTypes();
      final Object[] values = new Object[2];
      byte tag = (byte) getRandomInt(3);
      values[0] = tag;
      values[1] = getRandomValue(types[tag]);
      retValue = values;
    }
    return retValue;
  }

  /********************** END: Utility for generating random data ***************************/


  /********************** START: JSON to Object conversion related utils ********************/
  /**
   * String to the respective-type conversion for primitive/basic types. These conversion functions
   * convert a string representation to the respective Java primitive type.
   */
  private static final Map<BasicTypes, Function<String, Object>> STRING_TO_TYPE_CONVERT_MAP =
          Collections.unmodifiableMap(new HashMap<BasicTypes, Function<String, Object>>(15) {
            {
              put(BasicTypes.BOOLEAN, Boolean::parseBoolean);
              put(BasicTypes.STRING, e -> e);
              put(BasicTypes.CHARS, e -> e);
              put(BasicTypes.VARCHAR, e -> e);
              put(BasicTypes.CHAR, e -> e.charAt(0));
              put(BasicTypes.INT, Integer::parseInt);
              put(BasicTypes.BYTE, Byte::parseByte);
              put(BasicTypes.SHORT, Short::parseShort);
              put(BasicTypes.LONG, Long::parseLong);
              put(BasicTypes.FLOAT, Float::parseFloat);
              put(BasicTypes.DOUBLE, Double::parseDouble);
              put(BasicTypes.BIG_DECIMAL, BigDecimal::new);
              put(BasicTypes.DATE, Date::valueOf);
              put(BasicTypes.TIMESTAMP, Timestamp::valueOf);
            }
          });
  private static final ListType BINARY_TYPE = new ListType(BasicTypes.BYTE);

  /**
   * Convert the specified complex JSON object to the respective Java types using the specified
   * object type that has the actual type definition.
   *
   * @param jObject the JSON object
   * @param type the object type to be used for conversion
   * @return the respective Java object representation using the specified type
   * @throws JSONException
   */
  public static Object jsonToObject(final Object jObject, final DataType type)
          throws JSONException {
    if (jObject == null || jObject == JSONObject.NULL) {
      return null;
    }

    /** convert from JSON-string to the respective Java object **/
    if (type instanceof WrapperType) {
      return jsonToObject(jObject, ((WrapperType) type).getBasicObjectType());
    } else if (type instanceof BasicTypes) {
      if (BasicTypes.BINARY.equals(type)) {
        return toArray(BINARY_TYPE, (JSONArray) jObject);
      } else {
        final Function<String, Object> function = STRING_TO_TYPE_CONVERT_MAP.get(type);
        if (function == null) {
          throw new IllegalArgumentException("Unsupported type: " + type.toString());
        }
        return function.apply((String) jObject);
      }
    } else if (type instanceof ListType) {
      return toArray((ListType) type, (JSONArray) jObject);
    } else if (type instanceof MapType) {
      return toMap((MapType) type, (JSONObject) jObject);
    } else if (type instanceof StructType) {
      return toStruct((StructType) type, (JSONArray) jObject);
    } else if (type instanceof UnionType) {
      return toUnion((UnionType) type, (JSONArray) jObject);
    } else {
      throw new IllegalArgumentException("Unsupported type: " + type.toString());
    }
  }

  /**
   * Convert JSON array to the respective union representation.The first element in the array is the
   * tag (byte) indicating the type and second is the value.
   *
   * @param type the union object type
   * @param jArray an array of values with tag and actual value
   * @return the union representation as Java objects
   * @throws JSONException
   */
  private static Object[] toUnion(final UnionType type, final JSONArray jArray)
          throws JSONException {
    final DataType[] types = type.getColumnTypes();
    final Object[] array = new Object[2];
    array[0] = jsonToObject(jArray.get(0), BasicTypes.BYTE);
    array[1] = jsonToObject(jArray.get(1), types[(byte) array[0]]);
    return array;
  }

  /**
   * Convert JSON array to the respective struct representation.
   *
   * @param type the struct object type
   * @param jArray the JSON array of values
   * @return an array of Java object values as per the struct-type
   * @throws JSONException
   */
  private static Object[] toStruct(final StructType type, final JSONArray jArray)
          throws JSONException {
    final DataType[] types = type.getColumnTypes();
    final Object[] array = new Object[jArray.length()];
    for (int i = 0; i < jArray.length(); i++) {
      array[i] = jsonToObject(jArray.get(i), types[i]);
    }
    return array;
  }

  /**
   * Convert JSON object (map) to the respective Java types.
   *
   * @param type the map object type
   * @param jMap the JSON object map
   * @return the map of values as Java objects
   * @throws JSONException
   */
  private static Map<Object, Object> toMap(final MapType type, final JSONObject jMap)
          throws JSONException {
    final Map<Object, Object> map = new LinkedHashMap<>(jMap.length());
    Iterator itr = jMap.keys();
    String key;
    while (itr.hasNext()) {
      key = (String) itr.next();
      map.put(jsonToObject(key, type.getTypeOfKey()),
              jsonToObject(jMap.get(key), type.getTypeOfValue()));
    }
    return map;
  }

  /**
   * Convert JSONArray object to Java array/list.
   *
   * @param type the list object type
   * @param jArray the JSON array of values
   * @return an array of values as Java objects
   * @throws JSONException
   */
  private static Object toArray(final ListType type, final JSONArray jArray) throws JSONException {
    Object array;
    Class c;
    if (type.getTypeOfElement() instanceof BasicTypes) {
      c = BASIC_TYPE_TO_PRIMITIVE_MAP.get(type.getTypeOfElement());
    } else {
      c = Object.class;
    }
    array = Array.newInstance(c, jArray.length());
    for (int i = 0; i < jArray.length(); i++) {
      Array.set(array, i, jsonToObject(jArray.get(i), type.getTypeOfElement()));
    }
    return array;
  }

  public static Object convertToType(final BasicTypes type, final String valueStr) {
    return STRING_TO_TYPE_CONVERT_MAP.get(type).apply(valueStr);
  }

  /**
   * Get the column-name to column-value (of respective type) map from the input JSON string.
   *
   * @param td the table descriptor
   * @param valueJson the row value as JSON string
   * @return map of column values with respective types
   * @throws JSONException
   */
  public static Map<String, Object> getValueMap(final AbstractTableDescriptor td,
                                                final String valueJson, final Object... ftable) throws JSONException {

    Map<String, Object> valueMap = null;
    valueMap = new HashMap<>(td.getNumOfColumns());
    JSONObject jsonObject = new JSONObject(valueJson);
    String columnName;
    for (final MColumnDescriptor cd : td.getAllColumnDescriptors()) {
      columnName = cd.getColumnNameAsString();
      /** skip unavailable columns **/
      if (!jsonObject.has(columnName)) {
        continue;
      }
      try {
        valueMap.put(columnName, jsonToObject(jsonObject.get(columnName), cd.getColumnType()));
      } catch (Exception e) {
        throw new JSONException(String.format("Invalid data for column=%s, type=%s: %s", columnName,
                cd.getColumnType().toString(), e.getMessage()));
      }
    }
    return valueMap;
  }

  /**
   * Add columns with their column types from the provided JSON to the specified table descriptor.
   *
   * @param schema the JSON schema
   * @param td the table descriptor either FTable or MTable
   * @return null if successful; error message otherwise
   */
  public static String addColumnsFromJson(final String schema, final TableDescriptor td) {
    try {
      JSONObject jsonObject = new JSONObject(schema);
      Iterator keys = jsonObject.keys();
      String name;
      while (keys.hasNext()) {
        name = (String) keys.next();
        td.addColumn(name, new MTableColumnType(jsonObject.getString(name).toUpperCase()));
      }
    } catch (JSONException e) {
      return "Invalid schema provided: " + e.getMessage();
    }
    return null;
  }

  /**
   * Get table schema as JSON from the provided column name and column types.
   *
   * @param columns column information
   * @return schema in valid JSON format
   */
  public static String getJsonSchema(final String[][] columns) {
    final StringBuilder sb = new StringBuilder(32);
    sb.append('{');
    for (final String[] column : columns) {
      sb.append(JSONObject.quote(column[0])).append(':').append(JSONObject.quote(column[1]))
              .append(',');
    }
    if (sb.length() > 1) {
      sb.deleteCharAt(sb.length() - 1);
    }
    sb.append('}');
    return sb.toString();
  }
  /********************** END: JSON to Object conversion related utils ********************/
}
