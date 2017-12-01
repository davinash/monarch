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

package io.ampool.tierstore.stores.junit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MTableColumnType;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.ListType;
import io.ampool.monarch.types.MapType;
import io.ampool.monarch.types.StructType;
import io.ampool.monarch.types.UnionType;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.FileFormats;
import io.ampool.tierstore.config.CommonConfig;
import io.ampool.tierstore.config.orc.ORCWriterConfig;
import io.ampool.tierstore.config.parquet.ParquetWriterConfig;
import io.ampool.tierstore.internal.ConverterUtils;
import io.ampool.utils.TimestampUtil;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StoreTestUtils {
  public static TableDescriptor getTableDesciptorWithAllTypes(String tableName,
      final boolean includeTSColumn) {
    final TableDescriptor fTableDescriptor = new FTableDescriptor();
    fTableDescriptor.setTableName(tableName);
    fTableDescriptor.addColumn("BIN_COL", BasicTypes.BINARY);
    fTableDescriptor.addColumn("SHORT_COL", BasicTypes.SHORT);
    fTableDescriptor.addColumn("VARCHAR_COL", BasicTypes.VARCHAR);
    fTableDescriptor.addColumn("DOUBLE_COL", BasicTypes.DOUBLE);
    fTableDescriptor.addColumn("DATE_COL", BasicTypes.DATE);
    fTableDescriptor.addColumn("BIGDEC_COL", BasicTypes.BIG_DECIMAL);
    fTableDescriptor.addColumn("BOOL_COL", BasicTypes.BOOLEAN);
    fTableDescriptor.addColumn("BYTE_COL", BasicTypes.BYTE);
    fTableDescriptor.addColumn("CHAR_COL", BasicTypes.CHAR);
    fTableDescriptor.addColumn("CHARS_COL", BasicTypes.CHARS);
    fTableDescriptor.addColumn("FLOAT_COL", BasicTypes.FLOAT);
    fTableDescriptor.addColumn("INT_COL", BasicTypes.INT);
    fTableDescriptor.addColumn("LONG_COL", BasicTypes.LONG);
    fTableDescriptor.addColumn("STRING_COL", BasicTypes.STRING);
    fTableDescriptor.addColumn("TS_COL", BasicTypes.TIMESTAMP);
    if (includeTSColumn) {
      fTableDescriptor.addColumn(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, BasicTypes.LONG);
    }
    return fTableDescriptor;
  }

  public static String[] COLUMN_NAMES = {"BIN_COL", "SHORT_COL", "VARCHAR_COL", "DOUBLE_COL",
      "DATE_COL", "BIGDEC_COL", "BOOL_COL", "BYTE_COL", "CHAR_COL", "CHARS_COL", "FLOAT_COL",
      "INT_COL", "LONG_COL", "STRING_COL", "TS_COL", FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME};

  public static Object[] COLUMN_VALUES =
      {new byte[] {1}, (short) 1, "VARCHAR", 1.1, new Date(2016, 1, 1), new BigDecimal(10), true,
          (byte) 1, 'c', "s", 1.2f, 100, 1000l, "STR", new java.sql.Timestamp(1), 10000l};

  public static StoreRecord[] getDummyStoreRecords(final int noOfRecords) {
    StoreRecord[] storeRecords = new StoreRecord[noOfRecords];
    for (int i = 0; i < noOfRecords; i++) {
      storeRecords[i] = getStoreRecord();
    }
    return storeRecords;
  }

  public void validateStoreRecord(StoreRecord storeRecord) {
    if (storeRecord != null) {
      assertEquals(COLUMN_VALUES.length, storeRecord.getValues().length);
      for (int i = 0; i < storeRecord.getValues().length; i++) {
        final Object value = storeRecord.getValues()[i];
        if (COLUMN_VALUES[i] instanceof byte[]) {
          assertTrue(value instanceof byte[]);
          assertTrue(Bytes.equals((byte[]) COLUMN_VALUES[i], (byte[]) value));
        } else {
          assertTrue(COLUMN_VALUES[i].equals(value));
        }
      }
    }
  }

  public static void validateConvertedValues(Object[] convertedValues) {
    assertEquals(COLUMN_VALUES.length, convertedValues.length);
    for (int i = 0; i < convertedValues.length - 1; i++) { // skip the last timestamp record
      final Object value = convertedValues[i];
      if (COLUMN_VALUES[i] instanceof byte[]) {
        assertTrue(value instanceof byte[]);
        assertTrue(Bytes.equals((byte[]) COLUMN_VALUES[i], (byte[]) value));
      } else {
        assertTrue(COLUMN_VALUES[i].equals(value));
      }
    }
  }

  public static StoreRecord getStoreRecord() {
    final StoreRecord storeRecord = new StoreRecord(COLUMN_NAMES.length);
    for (int i = 0; i < COLUMN_NAMES.length; i++) {
      if (COLUMN_NAMES[i].equals(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) {
        storeRecord.addValue(TimestampUtil.getCurrentTime());
      } else {
        storeRecord.addValue(COLUMN_VALUES[i]);
      }
    }
    return storeRecord;
  }

  public static Record getRecord() {
    final Record storeRecord = new Record();
    for (int i = 0; i < COLUMN_NAMES.length; i++) {
      if (COLUMN_NAMES[i].equals(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) {
        storeRecord.add(COLUMN_NAMES[i], TimestampUtil.getCurrentTime());
      } else {
        storeRecord.add(COLUMN_NAMES[i], COLUMN_VALUES[i]);
      }
    }
    return storeRecord;
  }

  public static ConverterDescriptor getConverterDescriptor(final String tableName) {
    return ConverterUtils.getConverterDescriptor(getTableDesciptorWithAllTypes(tableName, false),
        FileFormats.ORC);
  }

  public static ConverterDescriptor getConverterDescriptor(final String tableName,
      boolean includeTSColumn, FileFormats fileFormat) {
    return ConverterUtils.getConverterDescriptor(
        getTableDesciptorWithAllTypes(tableName, includeTSColumn), fileFormat);
  }

  // For test purpose only
  public static Properties getWriterProps(FileFormats fileFormat) {
    Properties writerProps = new Properties();
    if (fileFormat == FileFormats.ORC) {
      writerProps.put(ORCWriterConfig.COMPRESSION_KIND, "NONE");
      writerProps.put(ORCWriterConfig.STRIPE_SIZE, "1000");
      writerProps.put(ORCWriterConfig.BUFFER_SIZE, "1000");
      writerProps.put(ORCWriterConfig.NEW_INDEX_STRIDE, "1000");
    } else {
      writerProps.put(ParquetWriterConfig.COMPRESSION_CODEC_NAME, "UNCOMPRESSED");
      writerProps.put(ParquetWriterConfig.BLOCK_SIZE, String.valueOf(256 * 1024 * 1024));
      writerProps.put(ParquetWriterConfig.PAGE_SIZE, String.valueOf(64 * 1024));
    }
    return (writerProps);
  }

  // For test purpose only
  public static Properties getCommonConfigProps(String tableName, int partitionId,
      File orcBaseDir) {
    Properties orcCommonProps = new Properties();
    orcCommonProps.setProperty(CommonConfig.TABLE_NAME, tableName);
    orcCommonProps.setProperty(CommonConfig.PARTITION_ID, String.valueOf(partitionId));
    orcCommonProps.setProperty(CommonConfig.BASE_URI, orcBaseDir.getPath());
    orcCommonProps.put(CommonConfig.TIME_BASED_PARTITIONING, "true");
    // 1 seconds
    orcCommonProps.put(CommonConfig.PARTITIIONING_INTERVAL_MS, "1000000");
    return (orcCommonProps);
  }

  private static DataType[] COMPLEXTYPES_COLUMN_TYPES = {
      new StructType(new String[] {"c1", "c2"},
          new DataType[] {BasicTypes.INT, new MapType(BasicTypes.STRING, BasicTypes.FLOAT)}),
      new StructType(new String[] {"c1", "c2"},
          new DataType[] {BasicTypes.INT, new MapType(BasicTypes.STRING, BasicTypes.FLOAT)}),
      new StructType(new String[] {"c1", "c2", "c3"},
          new DataType[] {BasicTypes.INT, BasicTypes.BIG_DECIMAL, BasicTypes.FLOAT}),
      new StructType(new String[] {"c1", "c2", "c3"},
          new DataType[] {BasicTypes.INT, BasicTypes.STRING, BasicTypes.FLOAT}),
      new StructType(new String[] {"c1", "c2", "c3"},
          new DataType[] {BasicTypes.INT, BasicTypes.STRING, new ListType(BasicTypes.FLOAT)}),
      new StructType(new String[] {"c1", "c2"},
          new DataType[] {BasicTypes.STRING,
              new ListType(new MapType(BasicTypes.STRING, BasicTypes.DOUBLE))}),
      new ListType(BasicTypes.INT), new ListType(BasicTypes.INT), new ListType(BasicTypes.BOOLEAN),
      new ListType(BasicTypes.FLOAT), new ListType(BasicTypes.DOUBLE),
      new ListType(BasicTypes.BINARY), new ListType(BasicTypes.STRING),
      new ListType(new ListType(BasicTypes.INT)),
      new ListType(new ListType(new ListType(BasicTypes.INT))),
      new ListType(new StructType(new String[] {"c1", "c2"},
          new DataType[] {BasicTypes.INT, BasicTypes.FLOAT})),
      new MapType(BasicTypes.INT, BasicTypes.STRING),
      new MapType(BasicTypes.INT, BasicTypes.STRING),
      new MapType(BasicTypes.INT, new MapType(BasicTypes.INT, BasicTypes.STRING)),
      new MapType(BasicTypes.INT, new ListType(BasicTypes.STRING)),
      new MapType(BasicTypes.STRING, new ListType(BasicTypes.FLOAT)),
      new UnionType(
          new DataType[] {BasicTypes.INT, new MapType(BasicTypes.STRING, BasicTypes.FLOAT)}),
      new UnionType(
          new DataType[] {BasicTypes.INT, new MapType(BasicTypes.STRING, BasicTypes.FLOAT)}),
      new UnionType(new DataType[] {BasicTypes.INT, BasicTypes.STRING, BasicTypes.FLOAT}),
      new UnionType(new DataType[] {BasicTypes.INT, BasicTypes.FLOAT, BasicTypes.BOOLEAN}),
      new UnionType(
          new DataType[] {BasicTypes.INT, BasicTypes.STRING, new ListType(BasicTypes.FLOAT)})};


  private static Object[] COMPLEXTYPES_COLUMN_VALUES =
      {null, new Object[] {11, new HashMap<String, Float>(2) {
        {
          put("String_2", 222.222f);
          put("String_1", 111.111f);
        }
      }}, new Object[] {11, new BigDecimal("12.345678"), 111.111f},
          new Object[] {11, "String_1", 111.111f},
          new Object[] {11, "String_1", Arrays.asList(111.111f, 222.222f)},
          new Object[] {"String_1", Arrays.asList(new HashMap<String, Double>(2) {
            {
              put("String_11", 111.111);
              put("String_12", 111.222);
            }
          }, new HashMap<String, Double>(2) {
            {
              put("String_21", 222.111);
              put("String_22", 222.222);
            }
          })}, null, Arrays.asList(1, 2, null, 4, 5), Arrays.asList(true, false, true, true),
          Arrays.asList(10.52f, 1.2f, 3.4f), Arrays.asList(10.52, 7.89, 3.4034),
          Arrays.asList(new byte[] {0, 1, 2}, new byte[0], null, new byte[] {127, 126, 125}),
          Arrays.asList("One", "Two", null, "TenThousandFiveHundredAndSomething", "", "K"),
          Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4, 5)),
          Arrays.asList(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4, 5)),
              Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4, 5))),
          Arrays.asList(new Object[] {11, 11.111f}, new Object[] {22, 22.222f}), null,
          getMapFromArray(1, "One", 2, "Two"),
          getMapFromArray(1, getMapFromArray(1, "One", 2, "Two"), 2,
              getMapFromArray(1, "One", 2, "Two")),
          getMapFromArray(1, Arrays.asList("One", "Two"), 2, Arrays.asList("Three", "Four")),
          getMapFromArray("Key_1", Arrays.asList(1.1f, 2.2f), "Key_2", Arrays.asList(3.3f, 4.4f)),
          null, new Object[] {(byte) 1, new HashMap<String, Float>(2) {
            {
              put("String_2", 222.222f);
              put("String_1", 111.111f);
            }
          }}, new Object[] {(byte) 2, 111.111f}, new Object[] {(byte) 2, false},
          new Object[] {(byte) 2, Arrays.asList(111.111f, 222.222f)}};

  private static Map<Object, Object> getMapFromArray(final Object... array) {
    Map<Object, Object> map = new HashMap<>(array.length / 2);
    putInMap(map, array);
    return map;
  }

  private static void putInMap(final Map<Object, Object> map, final Object[] array) {
    if (array.length % 2 != 0) {
      throw new IllegalArgumentException("Array of incorrect size.");
    }
    for (int i = 0; i < array.length; i += 2) {
      map.put(array[i], array[i + 1]);
    }
  }

  private static TableDescriptor getTableDesciptorWithComplexTypes(String tableName) {
    final TableDescriptor fTableDescriptor = new FTableDescriptor();
    final String colNamePrefix = "FTCOL";
    fTableDescriptor.setTableName(tableName);
    for (int i = 0; i < COMPLEXTYPES_COLUMN_TYPES.length; i++) {
      fTableDescriptor.addColumn(colNamePrefix + i,
          new MTableColumnType(COMPLEXTYPES_COLUMN_TYPES[i]));
    }
    fTableDescriptor.addColumn(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, BasicTypes.LONG);
    return fTableDescriptor;
  }

  public static ConverterDescriptor getConverterDescriptorComplexTypes(final String tableName,
      FileFormats fileFormats) {
    if (fileFormats == FileFormats.ORC)
      return ConverterUtils.getConverterDescriptor(getTableDesciptorWithComplexTypes(tableName),
          FileFormats.ORC);
    return ConverterUtils.getConverterDescriptor(getTableDesciptorWithComplexTypes(tableName),
        FileFormats.PARQUET);
  }

  public static void main(String[] args) {
    System.out.println("COMPLEXTYPES_COLUMN_TYPES::length" + COMPLEXTYPES_COLUMN_TYPES.length);
    System.out.println("COMPLEXTYPES_COLUMN_VALUES::length" + COMPLEXTYPES_COLUMN_VALUES.length);
    final FTableDescriptor test = (FTableDescriptor) getTableDesciptorWithComplexTypes("test");
    System.out.println("ColumnDescriptors:size" + test.getAllColumnDescriptors().size());
    Arrays.stream(COMPLEXTYPES_COLUMN_VALUES).forEach(VAL -> System.out.println(VAL));
    System.out.println(ConverterUtils.getConverterDescriptor(test, FileFormats.ORC));
    System.out.println(getStoreRecordWithComplexTypes());
  }

  public static StoreRecord getStoreRecordWithComplexTypes() {
    final StoreRecord storeRecord = new StoreRecord(COMPLEXTYPES_COLUMN_TYPES.length + 1);
    for (int i = 0; i < COMPLEXTYPES_COLUMN_TYPES.length; i++) {
      // Object valObject =
      // COMPLEXTYPES_COLUMN_TYPES[i].deserialize(COMPLEXTYPES_COLUMN_TYPES[i].serialize(COMPLEXTYPES_COLUMN_VALUES[i]));
      storeRecord.addValue(COMPLEXTYPES_COLUMN_VALUES[i]);
    }
    storeRecord.addValue(TimestampUtil.getCurrentTime());
    return storeRecord;
  }


}
