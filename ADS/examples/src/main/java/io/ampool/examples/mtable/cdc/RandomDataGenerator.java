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
package io.ampool.examples.mtable.cdc;

import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.ListType;
import io.ampool.monarch.types.MapType;
import io.ampool.monarch.types.StructType;
import io.ampool.monarch.types.UnionType;
import io.ampool.monarch.types.interfaces.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 */
public class RandomDataGenerator {
  /**
   * Instantiate the random number generator..
   **/
  private static final SecureRandom RANDOM = new SecureRandom();

  /**
   * the date-range
   **/
  static final long BEG_DAY = LocalDate.of(1900, 1, 1).toEpochDay();
  static final long END_DAY = LocalDate.of(2100, 12, 31).toEpochDay();
  static final int DAYS_RANGE = (int) (END_DAY - BEG_DAY);
  static final int DAY_SECONDS = 86400;

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
  static final String CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  public static final Map<String, Callable<Object>> RANDOM_DATA_MAP =
      new HashMap<String, Callable<Object>>(15) {
        {
          put("BINARY", RandomDataGenerator::getRandomBytes);
          put("INT", RANDOM::nextInt);
          put("LONG", RANDOM::nextLong);
          put("SHORT", () -> new Integer(RANDOM.nextInt()).shortValue());
          put("BYTE", () -> new Integer(RANDOM.nextInt()).byteValue());
          put("BOOLEAN", RANDOM::nextBoolean);
          put("DOUBLE", RANDOM::nextDouble);
          put("FLOAT", RANDOM::nextFloat);
          put("STRING",
              () -> new BigInteger(getRandomInt(MAX_STRING_LEN) * 8, RANDOM).toString(32));
          put("VARCHAR",
              () -> new BigInteger(getRandomInt(MAX_STRING_LEN) * 8, RANDOM).toString(32));
          put("BIG_DECIMAL", () -> new BigDecimal(new BigInteger(MAX_DECIMAL_LEN * 8, RANDOM)));
          put("CHAR", () -> CHARS.charAt(RANDOM.nextInt(CHARS.length())));
          put("CHARS", () -> new BigInteger(getRandomInt(MAX_CHARS_LEN) * 8, RANDOM).toString(32)
              .toCharArray());
          put("DATE", () -> new Date(LocalDateTime.of(getRandomDate(), LocalTime.now())
              .toInstant(ZoneOffset.UTC).toEpochMilli()));
          put("TIMESTAMP", () -> new Timestamp(LocalDateTime.of(getRandomDate(), getRandomTime())
              .toInstant(ZoneOffset.UTC).toEpochMilli()));
        }
      };

  /**
   * Get the random number of bytes..
   * 
   * @return the byte array
   */
  public static final byte[] getRandomBytes(final int length) {
    final byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  public static final byte[] getRandomBytes() {
    return getRandomBytes(RANDOM.nextInt(MAX_BYTES_LEN));
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
   * Get the random value for the specified type.
   * 
   * @param type the object type
   * @return the random value for the specified type
   */
  public static Object getRandomValue(final DataType type) {
    Object retValue = null;
    try {
      if (type instanceof BasicTypes) {
        retValue = RANDOM_DATA_MAP.get(type.toString()).call();
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
    } catch (Exception e) {
      e.printStackTrace();
    }
    return retValue;
  }

  public static final List<Object> getRandomRecord(final List<MColumnDescriptor> typeList) {
    return typeList.stream().sequential().map(e -> getRandomValue(e.getColumnType()))
        .collect(Collectors.toList());
  }

  /**
   * driven by properties..
   **/
  private static final int BATCH_SIZE = Integer.getInteger("ampool.batch.size", 1000);
  private static final boolean USE_RANDOM_KEY = Boolean.getBoolean("ampool.random.key");
  private static final String PREFIX = System.getProperty("ampool.random.key", "a-");

  public static List<String> putRecords(final MTable table, final Integer recordCount,
      List<String> keys) {
    return putRecords(table, recordCount, BATCH_SIZE, keys);
  }

  public static List<String> deleteRecords(final MTable table, List<String> keys,
      boolean partialDelete) {
    if (!partialDelete) {
      keys.forEach(key -> {
        table.delete(new Delete(key));
      });
    } else {
      List<MColumnDescriptor> allColumnDescriptors =
          table.getTableDescriptor().getAllColumnDescriptors();
      keys.forEach(key -> {
        Delete delete = new Delete(key);
        delete.addColumn(
            allColumnDescriptors.get(RandomDataGenerator.getRandomInt(allColumnDescriptors.size()))
                .getColumnNameAsString());
        table.delete(delete);
      });

    }
    return keys;
  }

  public static void appendRecords(final FTable table, final Integer recordCount) {
    appendRecords(table, recordCount, BATCH_SIZE);
  }

  /**
   * Put random records to the table using the provided keys, if any, with the specified batch-size.
   * 
   * @param table the table
   * @param recordCount the number of records to put
   * @param batchSize the batch size to be used for put
   * @param keys the list of keys to be put, generate sequential keys if not provided
   */
  public static List<String> putRecords(final MTable table, final Integer recordCount,
      final long batchSize, List<String> keys) {
    /** in case keys are null, create random keys.. **/
    if (keys == null) {
      keys = IntStream.range(0, recordCount)
          .mapToObj(e -> USE_RANDOM_KEY ? UUID.randomUUID().toString() : PREFIX + e)
          .collect(Collectors.toList());
    }

    List<Put> putList =
        IntStream.range(0, BATCH_SIZE).mapToObj(e -> new Put("dummy")).collect(Collectors.toList());
    Put put;
    List<MColumnDescriptor> typeList = table.getTableDescriptor().getAllColumnDescriptors();
    int batchCounter = 0;
    for (final String key : keys) {
      put = putList.get(batchCounter);
      put.clear();
      put.setRowKey(key);
      for (MColumnDescriptor cd : typeList) {
        put.addColumn(cd.getColumnNameAsString(), getRandomValue(cd.getColumnType()));
      }
      batchCounter++;
      if (batchCounter == batchSize) {
        table.put(putList.subList(0, batchCounter));
        batchCounter = 0;
      }
    }
    if (batchCounter > 0) {
      table.put(putList.subList(0, batchCounter));
    }
    return keys;
  }

  /**
   * Append random records to the table using the provided keys, if any, with the specified
   * batch-size.
   * 
   * @param table the table
   * @param recordCount the number of records to put
   * @param batchSize the batch size to be used for put
   */
  public static void appendRecords(final FTable table, final int recordCount, final int batchSize) {
    Record[] records =
        IntStream.range(0, (int) batchSize).mapToObj(e -> new Record()).toArray(Record[]::new);
    Record rec;
    List<MColumnDescriptor> typeList = table.getTableDescriptor().getAllColumnDescriptors();
    int batchCounter = 0;
    for (int i = 0; i < recordCount; ++i) {
      rec = records[batchCounter];
      rec.clear();
      for (MColumnDescriptor cd : typeList) {
        rec.add(cd.getColumnNameAsString(), getRandomValue(cd.getColumnType()));
      }
      batchCounter++;
      if (batchCounter == batchSize) {
        table.append(records);
        batchCounter = 0;
      }
    }
    if (batchCounter > 0) {
      table.append(Arrays.copyOfRange(records, 0, batchCounter));
    }
  }
}
