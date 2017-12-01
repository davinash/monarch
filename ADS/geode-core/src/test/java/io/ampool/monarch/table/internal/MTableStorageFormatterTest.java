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
package io.ampool.monarch.table.internal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MultiVersionValueWrapper;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.results.FormatAwareRow;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.interfaces.DataType;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

@Category(UnitTest.class)
@Ignore("Failing on Jenkins but passes locallly GEN-1854")
public class MTableStorageFormatterTest {

  static Map<DataType, Object> dataTypeToValueMap = new LinkedHashMap<>();
  static Map<DataType, Object> updatedDataTypeToValueMap = new LinkedHashMap<>();

  @BeforeClass
  public static void createData() {
    dataTypeToValueMap.put(BasicTypes.BYTE, new Byte("1"));
    dataTypeToValueMap.put(BasicTypes.BOOLEAN, new Boolean(false));
    dataTypeToValueMap.put(BasicTypes.CHAR, new Character('a'));
    dataTypeToValueMap.put(BasicTypes.DATE, new Date(2017, 1, 1));
    dataTypeToValueMap.put(BasicTypes.DOUBLE, new Double(10.5));
    dataTypeToValueMap.put(BasicTypes.FLOAT, new Float(100.2));
    dataTypeToValueMap.put(BasicTypes.INT, new Integer(150));
    dataTypeToValueMap.put(BasicTypes.LONG, new Long(1000));
    dataTypeToValueMap.put(BasicTypes.SHORT, new Short("5"));
    dataTypeToValueMap.put(BasicTypes.TIMESTAMP, new Timestamp(2017, 1, 1, 1, 1, 1, 1));

    // var types
    dataTypeToValueMap.put(BasicTypes.CHARS, new String("CHARS"));
    dataTypeToValueMap.put(BasicTypes.STRING, new String("STRING"));
    dataTypeToValueMap.put(BasicTypes.VARCHAR, new String("VARCHAR"));
    dataTypeToValueMap.put(BasicTypes.BINARY,
        new byte[] {1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1});

    updatedDataTypeToValueMap.put(BasicTypes.BYTE, new Byte("2"));
    updatedDataTypeToValueMap.put(BasicTypes.BOOLEAN, new Boolean(true));
    updatedDataTypeToValueMap.put(BasicTypes.CHAR, new Character('b'));
    updatedDataTypeToValueMap.put(BasicTypes.DATE, new Date(2018, 1, 1));
    updatedDataTypeToValueMap.put(BasicTypes.DOUBLE, new Double(20.5));
    updatedDataTypeToValueMap.put(BasicTypes.FLOAT, new Float(200.2));
    updatedDataTypeToValueMap.put(BasicTypes.INT, new Integer(300));
    updatedDataTypeToValueMap.put(BasicTypes.LONG, new Long(2000));
    updatedDataTypeToValueMap.put(BasicTypes.SHORT, new Short("10"));
    updatedDataTypeToValueMap.put(BasicTypes.TIMESTAMP, new Timestamp(2018, 1, 1, 1, 1, 1, 1));

    // var types
    updatedDataTypeToValueMap.put(BasicTypes.CHARS, new String("NEWCHARS"));
    updatedDataTypeToValueMap.put(BasicTypes.STRING, new String("NEWSTRING"));
    updatedDataTypeToValueMap.put(BasicTypes.VARCHAR, new String("NEWVARCHAR"));
    updatedDataTypeToValueMap.put(BasicTypes.BINARY,
        new byte[] {0, 1, 1, 0, 1, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0});
  }

  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performPutOperationNewRowSingleCol() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    descriptor.addColumn("COL1", BasicTypes.INT);

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    Put put = new Put("testKey");
    put.setTimeStamp(1);

    put.addColumn("COL1", 1);
    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    final byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // row header
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        1, // col presence indicator
        0, 0, 0, 1 // value 1
    };
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array         : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array: " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);
  }

  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performPutOperationNewRowMultipleCol() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    descriptor.addColumn("COL1", BasicTypes.INT);
    descriptor.addColumn("COL2", BasicTypes.LONG);
    descriptor.addColumn("COL3", BasicTypes.DOUBLE);

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    Put put = new Put("testKey");
    put.setTimeStamp(1);

    put.addColumn("COL1", 1);
    put.addColumn("COL2", new Long(1500));
    put.addColumn("COL3", new Double(10.5));

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    final byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // row header
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        7, // col presence indicator
        0, 0, 0, 1, // value 1 INT
        0, 0, 0, 0, 0, 0, 5, -36, // value 1500 LONG
        64, 37, 0, 0, 0, 0, 0, 0 // value 10.5 DOUBLE
    };
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array         : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array: " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);
  }


  @Test
  public void performPutOperationNewRowMultipleColNullValue() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    descriptor.addColumn("COL1", BasicTypes.LONG);
    descriptor.addColumn("COL2", BasicTypes.STRING);
    descriptor.addColumn("COL3", BasicTypes.INT);

    if (descriptor.getSchema() == null) {
      Schema.Builder sb = new Schema.Builder();
      for (final MColumnDescriptor cd : descriptor.getColumnDescriptors()) {
        sb.column(cd.getColumnNameAsString(), cd.getColumnType());
      }
      descriptor.setSchema(sb.build());
    }

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    Put put = new Put("testKey");
    put.setTimeStamp(1);

    put.addColumn("COL1", null);
    put.addColumn("COL2", "TESTSTRING");
    put.addColumn("COL3", new Integer(10));

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    final byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // row header
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        6, // col presence indicator
        0, 0, 0, 10, // value 1 INT
        84, 69, 83, 84, 83, 84, 82, 73, 78, 71, // value TESTSTRING String
        0, 0, 0, 17 // offset for string col
    };
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array         : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array: " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);

    Row formatAwareResult =
        new FormatAwareRow(put.getRowKey(), finalByteArr, descriptor, new ArrayList<>());

    System.out
        .println("MTableStorageFormatterTest.performPutOperationNewRowMultipleColNullValue :: 228"
            + "res " + formatAwareResult);

    MTableKey key = new MTableKey();
    key.setColumnPositions(Arrays.asList(0, 1, 2));
    Object getByteArr = mtableFormatter.performGetOperation(key, descriptor, finalByteArr, null);

    Row formatAwareResult1 = new FormatAwareRow(put.getRowKey(), getByteArr, descriptor,
        Arrays.asList(Bytes.toBytes("COL1"), Bytes.toBytes("COL2"), Bytes.toBytes("COL3")));
    System.out.println(
        "MTableStorageFormatterTest.performPutOperationNewRowMultipleColNullValue :: 238" + "");
  }

  @Test
  public void performPutOperationNewRowMultipleColNullEmptyValue() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    descriptor.addColumn("COL1", BasicTypes.BINARY);
    descriptor.addColumn("COL2", BasicTypes.BINARY);
    descriptor.addColumn("COL3", BasicTypes.BINARY);

    if (descriptor.getSchema() == null) {
      Schema.Builder sb = new Schema.Builder();
      for (final MColumnDescriptor cd : descriptor.getColumnDescriptors()) {
        sb.column(cd.getColumnNameAsString(), cd.getColumnType());
      }
      descriptor.setSchema(sb.build());
    }

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    Put put = new Put("testKey");
    put.setTimeStamp(1);

    put.addColumn("COL1", null);
    put.addColumn("COL2", Bytes.toBytes(""));
    put.addColumn("COL3", null);

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    final byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // row header
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        2, // col presence indicator
        -65, // value
        0, 0, 0, 13 // offset for string col
    };
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array         : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array: " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);
  }

  @Test
  public void performPutOperationNULLValueOnExistingValue() throws Exception {
    // TODO
    MTableDescriptor descriptor = new MTableDescriptor();
    descriptor.addColumn("COL1", BasicTypes.LONG);
    descriptor.addColumn("COL2", BasicTypes.STRING);
    descriptor.addColumn("COL3", BasicTypes.INT);
    descriptor.addColumn("COL4", BasicTypes.CHAR);
    descriptor.addColumn("COL5", BasicTypes.DOUBLE);

    if (descriptor.getSchema() == null) {
      Schema.Builder sb = new Schema.Builder();
      for (final MColumnDescriptor cd : descriptor.getColumnDescriptors()) {
        sb.column(cd.getColumnNameAsString(), cd.getColumnType());
      }
      descriptor.setSchema(sb.build());
    }

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    Put put = new Put("testKey");
    put.setTimeStamp(1);

    put.addColumn("COL1", 11l);
    put.addColumn("COL2", "ABC");
    put.addColumn("COL3", 1);
    put.addColumn("COL4", 'M');
    put.addColumn("COL5", 11.111D);

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // row header
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        31, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 1, 0, 77, 64, 38, 56, -44, -3, -13, -74, 70, 65, 66,
        67, 0, 0, 0, 35};
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array         : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array: " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);

    put = new Put("testKey");
    put.setTimeStamp(1);

    put.addColumn("COL1", 1111l);
    put.addColumn("COL5", 22.222D);

    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), finalValue);

    assertTrue(finalValue instanceof byte[]);

    finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray1 = {1, 1, 1, 0, // row header
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        31, 0, 0, 0, 0, 0, 0, 4, 87, 0, 0, 0, 1, 0, 77, 64, 54, 56, -44, -3, -13, -74, 70, 65, 66,
        67, 0, 0, 0, 35};
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array         : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array: " + Arrays.toString(expectedByteArray1));

    assertArrayEquals(expectedByteArray1, finalByteArr);
  }

  @Test
  public void performCheckAndPutOperationNULLValueOnExistingValue() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    descriptor.addColumn("COL1", BasicTypes.BINARY);
    descriptor.addColumn("COL2", BasicTypes.BINARY);
    descriptor.addColumn("COL3", BasicTypes.BINARY);
    descriptor.addColumn("COL4", BasicTypes.BINARY);
    descriptor.addColumn("COL5", BasicTypes.BINARY);
    // descriptor.setMaxVersions(5);
    if (descriptor.getSchema() == null) {
      Schema.Builder sb = new Schema.Builder();
      for (final MColumnDescriptor cd : descriptor.getColumnDescriptors()) {
        sb.column(cd.getColumnNameAsString(), cd.getColumnType());
      }
      descriptor.setSchema(sb.build());
    }

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    Put put = new Put("testKey");
    put.setTimeStamp(1);

    put.addColumn("COL1", Bytes.toBytes(11l));
    put.addColumn("COL2", Bytes.toBytes("ABC"));
    put.addColumn("COL3", Bytes.toBytes(1));
    put.addColumn("COL5", Bytes.toBytes(11.111D));

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    // assertTrue(finalValue instanceof byte[]);

    // byte[] finalByteArr = (byte[]) finalValue;
    // final byte[] expectedByteArray = {1, 1, 1, 0,
    // 0, 0, 0, 0, 0, 0, 0, 1,
    // 23,
    // 0, 0, 0, 0, 0, 0, 0, 11,
    // 65, 66, 67,
    // 0, 0, 0, 1,
    // 64, 38, 56, -44, -3, -13, -74, 70,
    // 0, 0, 0, 28,
    // 0, 0, 0, 24,
    // 0, 0, 0, 21,
    // 0, 0, 0, 13
    // };
    // System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
    // + "byte array : " + Arrays.toString(finalByteArr));
    // System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
    // + "expected byte array: " + Arrays.toString(expectedByteArray));
    //
    // assertArrayEquals(expectedByteArray, finalByteArr);

    put = new Put("testKey");
    put.setTimeStamp(1);

    put.addColumn("COL4", Bytes.toBytes('M'));

    mValue = MValue.fromPut(descriptor, put, MOperation.CHECK_AND_PUT);
    mValue.setCondition(3, null);

    finalValue = mtableFormatter.performCheckAndPutOperation(descriptor, mValue, mValue.getOpInfo(),
        finalValue);

    MTableKey getKey = new MTableKey(Bytes.toBytes("testKey"));
    getKey.addColumnPositions(new Integer[] {3});

    byte[] bytes = (byte[]) ((MTableStorageFormatter) mtableFormatter).performGetOperation(getKey,
        descriptor, finalValue, 0);

    System.out.println();

    mValue = MValue.fromPut(descriptor, put, MOperation.CHECK_AND_PUT);
    mValue.setCondition(3, null);

    MOpInfo.MCondition condition = mValue.getOpInfo().getCondition();

    boolean isCheckPassed = ((MTableStorageFormatter) mtableFormatter).checkValue(descriptor,
        finalValue, (byte[]) condition.getColumnValue(), condition.getColumnId());

    assertFalse(isCheckPassed);

    // assertTrue(finalValue instanceof byte[]);
    //
    // final byte[] finalByteArr = (byte[]) finalValue;
    // final byte[] expectedByteArray1 = {1, 1, 1, 0,
    // 0, 0, 0, 0, 0, 0, 0, 1,
    // 31,
    // 0, 0, 0, 0, 0, 0, 0, 11,
    // 65, 66, 67,
    // 0, 0, 0, 1,
    // 0, 0, 0, 77,
    // 64, 38, 56, -44, -3, -13, -74, 70,
    // 0, 0, 0, 32,
    // 0, 0, 0, 28,
    // 0, 0, 0, 24,
    // 0, 0, 0, 21,
    // 0, 0, 0, 13
    // };
    // System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
    // + "byte array : " + Arrays.toString(finalByteArr));
    // System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
    // + "expected byte array: " + Arrays.toString(expectedByteArray1));
    //
    // assertArrayEquals(expectedByteArray1, finalByteArr);

  }


  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performPutOperationNewRowAllFixTypes() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    Iterator<Entry<DataType, Object>> entryIterator = dataTypeToValueMap.entrySet().iterator();
    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      if (type.isFixedLength()) {
        Object value = next.getValue();
        descriptor.addColumn("col_" + type, (BasicTypes) type);
        put.addColumn("col_" + type, value);
      }
    }

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    final byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // Row Header
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -1, 3, // col presence indicator
        1, // byte value
        0, // boolean
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8 // timestamp
    };
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array         : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array: " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);
  }

  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performPutOperationNewRowAllVariableTypes() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    Iterator<Entry<DataType, Object>> entryIterator = dataTypeToValueMap.entrySet().iterator();
    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      if (!type.isFixedLength()) {
        Object value = next.getValue();
        descriptor.addColumn("col_" + type, (BasicTypes) type);
        put.addColumn("col_" + type, value);

        // System.out.println("type: " + type + " value: " +
        // Arrays.toString(type.serialize(value)));
      }
    }

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    final byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        15, // col presence indicator
        67, 72, 65, 82, 83, // var col 1 value
        83, 84, 82, 73, 78, 71, // var col 2 value
        86, 65, 82, 67, 72, 65, 82, // var col 3 value
        1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // var col 4 value
        0, 0, 0, 31, // var col 4 value offset
        0, 0, 0, 24, // var col 3 value offset
        0, 0, 0, 18, // var col 2 value offset
        0, 0, 0, 13 // var col 1 value offset
    };
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);
  }

  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performPutOperationNewRowAllTypes() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    Iterator<Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      put.addColumn("col_" + type, value);

      // System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
    }

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    final byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -1, 63, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
        // variable length cols
        1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
        67, 72, 65, 82, 83, // chars value
        83, 84, 82, 73, 78, 71, // string value
        86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 86, // varchar value offset
        0, 0, 0, 80, // string value offset
        0, 0, 0, 75, // chars value offset
        0, 0, 0, 60 // binary value offset
    };
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);
  }

  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performDeleteOperation() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    ArrayList<Integer> colsToBeDeleted = new ArrayList<>();
    int index = 0;
    Iterator<Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      if (type == BasicTypes.INT || type == BasicTypes.BINARY) {
        colsToBeDeleted.add(index);
      }
      index++;
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      put.addColumn("col_" + type, value);

      // System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
    }

    descriptor.finalizeDescriptor();

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -1, 63, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
        // variable length cols
        1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
        67, 72, 65, 82, 83, // chars value
        83, 84, 82, 73, 78, 71, // string value
        86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 86, // varchar value offset
        0, 0, 0, 80, // string value offset
        0, 0, 0, 75, // chars value offset
        0, 0, 0, 60 // binary value offset
    };
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);

    MTableKey key = new MTableKey(Bytes.toBytes("testKey"));
    key.setColumnPositions(colsToBeDeleted);
    key.setIsDeleteOp();

    MOpInfo mOpInfo = MOpInfo.fromKey(key);

    finalValue = mtableFormatter.performDeleteOperation(descriptor, mOpInfo, finalByteArr);

    assertTrue(finalValue instanceof byte[]);

    finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray1 = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -2, 62, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
        // variable length cols
        67, 72, 65, 82, 83, // chars value
        83, 84, 82, 73, 78, 71, // string value
        86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 67, // varchar value offset
        0, 0, 0, 61, // string value offset
        0, 0, 0, 56, // chars value offset
    };
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray1));

    assertArrayEquals(expectedByteArray1, finalByteArr);


  }

  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performGetOp() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    Iterator<Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      put.addColumn("col_" + type, value);

      // System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
    }

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -1, 63, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
        // variable length cols
        1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
        67, 72, 65, 82, 83, // chars value
        83, 84, 82, 73, 78, 71, // string value
        86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 86, // varchar value offset
        0, 0, 0, 80, // string value offset
        0, 0, 0, 75, // chars value offset
        0, 0, 0, 60 // binary value offset
    };
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);

    finalByteArr = (byte[]) ((MTableStorageFormatter) mtableFormatter)
        .performGetOperation(new MTableKey(Bytes.toBytes("testKey")), descriptor, finalByteArr, 0);
    // restructureByteArray(finalByteArr, descriptor);

    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);

    // List<byte[]> columns = descriptor.getColumnsByName().keySet().stream().map(key -> {
    // return key.getByteArray();
    // }).collect(Collectors.toList());
    //
    // VersionedRow
    // row =
    // new VersionedRow(Bytes.toBytes("testKey"), finalByteArr, descriptor, columns);
    // // verify row key
    // assertArrayEquals(Bytes.toBytes("testKey"), row.getRowId());
    //
    // //verify timestamp
    // assertEquals(new Long(1), row.getRowTimeStamp());
    //
    // // verify all column values
    // List<MCell> cells = row.getCells();
    // Iterator<MCell> iterator = cells.iterator();
    // iterator.forEachRemaining(cell -> {
    // Object value = mapSortedByTypeName.get(cell.getColumnType());
    // if (cell.getColumnType() == BasicTypes.BINARY) {
    // assertArrayEquals((byte[]) value, (byte[]) cell.getColumnValue());
    // } else if (cell.getColumnType() == BasicTypes.DATE) {
    // assertEquals(((Date) value).getTime(), ((Date) cell.getColumnValue()).getTime());
    // } else if (cell.getColumnType() == BasicTypes.TIMESTAMP) {
    // assertEquals(((Timestamp) value).getTime(), ((Timestamp) cell.getColumnValue()).getTime());
    // } else {
    // assertEquals(value, cell.getColumnValue());
    // }
    // assertArrayEquals(Bytes.toBytes("col_" + cell.getColumnType()), cell.getColumnName());
    // });
  }

  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performGetOpSelectedColumns() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    Iterator<Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      put.addColumn("col_" + type, value);

      System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
    }

    descriptor.finalizeDescriptor();

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -1, 63, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
        // variable length cols
        1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
        67, 72, 65, 82, 83, // chars value
        83, 84, 82, 73, 78, 71, // string value
        86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 86, // varchar value offset
        0, 0, 0, 80, // string value offset
        0, 0, 0, 75, // chars value offset
        0, 0, 0, 60 // binary value offset
    };
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);

    MTableKey getKey = new MTableKey(Bytes.toBytes("testKey"));
    getKey.addColumnPositions(new Integer[] {0, 1, 2, 3, 4});

    finalByteArr = (byte[]) ((MTableStorageFormatter) mtableFormatter).performGetOperation(getKey,
        descriptor, finalByteArr, 0);
    // restructureByteArray(finalByteArr, descriptor);

    final byte[] expectedByteArray1 = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        31, 0, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        // variable length cols
        1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
        67, 72, 65, 82, 83, // chars value
        0, 0, 0, 33, // chars value offset
        0, 0, 0, 18 // binary value offset
    };

    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray1));

    assertArrayEquals(expectedByteArray1, finalByteArr);

    // List<byte[]> columns = descriptor.getColumnsByName().keySet().stream().map(key -> {
    // return key.getByteArray();
    // }).collect(Collectors.toList());
    //
    // VersionedRow
    // row =
    // new VersionedRow(Bytes.toBytes("testKey"), finalByteArr, descriptor, columns);
    // // verify row key
    // assertArrayEquals(Bytes.toBytes("testKey"), row.getRowId());
    //
    // //verify timestamp
    // assertEquals(new Long(1), row.getRowTimeStamp());
    //
    // // verify all column values
    // List<MCell> cells = row.getCells();
    // Iterator<MCell> iterator = cells.iterator();
    // iterator.forEachRemaining(cell -> {
    // Object value = mapSortedByTypeName.get(cell.getColumnType());
    // if (cell.getColumnType() == BasicTypes.BINARY) {
    // assertArrayEquals((byte[]) value, (byte[]) cell.getColumnValue());
    // } else if (cell.getColumnType() == BasicTypes.DATE) {
    // assertEquals(((Date) value).getTime(), ((Date) cell.getColumnValue()).getTime());
    // } else if (cell.getColumnType() == BasicTypes.TIMESTAMP) {
    // assertEquals(((Timestamp) value).getTime(), ((Timestamp) cell.getColumnValue()).getTime());
    // } else {
    // assertEquals(value, cell.getColumnValue());
    // }
    // assertArrayEquals(Bytes.toBytes("col_" + cell.getColumnType()), cell.getColumnName());
    // });
  }

  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performCheckAndPutOperationAllTypes() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    Iterator<Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      put.addColumn("col_" + type, value);

      // System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
    }

    descriptor.finalizeDescriptor();

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -1, 63, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
        // variable length cols
        1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
        67, 72, 65, 82, 83, // chars value
        83, 84, 82, 73, 78, 71, // string value
        86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 86, // varchar value offset
        0, 0, 0, 80, // string value offset
        0, 0, 0, 75, // chars value offset
        0, 0, 0, 60 // binary value offset
    };
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);

    // do now checkAndPut

    put = new Put("testKey");
    put.setTimeStamp(1);

    Object checkValue = null;
    int position = -1;

    entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      position = descriptor.getColumnDescriptorsMap()
          .get(new MColumnDescriptor(Bytes.toBytes("col_" + type)));

      checkValue = value;

      Object newValue = updatedDataTypeToValueMap.get(type);
      put.addColumn("col_" + type, newValue);
      break;
    }
    mValue = MValue.fromPut(descriptor, put, MOperation.CHECK_AND_PUT);
    mValue.setCondition(position, checkValue);

    finalValue = mtableFormatter.performCheckAndPutOperation(descriptor, mValue, mValue.getOpInfo(),
        expectedByteArray);

    assertTrue(finalValue instanceof byte[]);

    finalByteArr = (byte[]) finalValue;

    final byte[] expectedByteArray1 = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -1, 63, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
        // variable length cols
        0, 1, 1, 0, 1, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, // binary value
        67, 72, 65, 82, 83, // chars value
        83, 84, 82, 73, 78, 71, // string value
        86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 86, // varchar value offset
        0, 0, 0, 80, // string value offset
        0, 0, 0, 75, // chars value offset
        0, 0, 0, 60 // binary value offset
    };

    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray1));

    assertArrayEquals(expectedByteArray1, finalByteArr);

  }


  /**
   * GEN-1983: Test is to put variable length columns and check if checkAndPut works well when check
   * is with variable length column value.
   */
  @Test
  public void performCheckAndPutWithCheckOnVariableLengthValue() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    Schema schema = new Schema.Builder().column("COL1", BasicTypes.STRING.toString())
        .column("COL11", BasicTypes.STRING.toString())
        .column("COL111", BasicTypes.STRING.toString())
        .column("COL1111", BasicTypes.STRING.toString())
        .column("COL11111", BasicTypes.STRING.toString()).column("COL2", BasicTypes.INT.toString())
        .column("COL3", BasicTypes.INT.toString()).column("COL4", BasicTypes.INT.toString())
        .build();

    descriptor.setSchema(schema);
    descriptor.finalizeDescriptor();

    put.addColumn("COL1", "JOHN1");
    put.addColumn("COL11", "JOHN11");
    put.addColumn("COL111", "JOHN111");
    put.addColumn("COL1111", "JOHN1111");
    put.addColumn("COL11111", "JOHN11111");
    put.addColumn("COL2", 1);
    put.addColumn("COL3", 30);
    put.addColumn("COL4", 10000);

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    // do now checkAndPut
    // CheckAndPut Succeeds
    Put checkAndPut = new Put(Bytes.toBytes("testKey"));
    checkAndPut.addColumn("COL3", 40);
    checkAndPut.addColumn("COL4", 35000);


    mValue = MValue.fromPut(descriptor, put, MOperation.CHECK_AND_PUT);
    mValue.setCondition(0, Bytes.toBytes("JOHN1"));

    try {
      finalValue = mtableFormatter.performCheckAndPutOperation(descriptor, mValue,
          mValue.getOpInfo(), finalValue);

      // 2nd test
      mValue.setCondition(1, Bytes.toBytes("JOHN11"));

      finalValue = mtableFormatter.performCheckAndPutOperation(descriptor, mValue,
          mValue.getOpInfo(), finalValue);

      // 3nd test
      mValue.setCondition(2, Bytes.toBytes("JOHN111"));

      finalValue = mtableFormatter.performCheckAndPutOperation(descriptor, mValue,
          mValue.getOpInfo(), finalValue);

      // 4nd test
      mValue.setCondition(3, Bytes.toBytes("JOHN1111"));

      finalValue = mtableFormatter.performCheckAndPutOperation(descriptor, mValue,
          mValue.getOpInfo(), finalValue);

      // 5nd test
      mValue.setCondition(4, Bytes.toBytes("JOHN11111"));

      finalValue = mtableFormatter.performCheckAndPutOperation(descriptor, mValue,
          mValue.getOpInfo(), finalValue);

    } catch (Throwable t) {
      t.printStackTrace();
      fail("No exception is expected");
    }
  }

  /**
   * Version Put test
   */
  @Test
  public void multiVersionPut() throws Exception {

    final byte[][] expectedByteArray = {{1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -1, 63, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
        // variable length cols
        1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
        67, 72, 65, 82, 83, // chars value
        83, 84, 82, 73, 78, 71, // string value
        86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 86, // varchar value offset
        0, 0, 0, 80, // string value offset
        0, 0, 0, 75, // chars value offset
        0, 0, 0, 60 // binary value offset
        },
        {1, 1, 1, 0, // RowHeader
            0, 0, 0, 0, 0, 0, 0, 2, // timestamp
            -1, 63, // col presence indicator
            0, // boolean
            1, // byte
            0, 97, // char
            0, 0, 55, -30, 13, -86, 72, 64, // date
            64, 37, 0, 0, 0, 0, 0, 0, // double
            66, -56, 102, 102, // float
            0, 0, 0, -106, // int
            0, 0, 0, 0, 0, 0, 3, -24, // long
            0, 5, // short
            0, 0, 55, -30, 13, -30, 37, 8, // timestamp
            // variable length cols
            1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
            67, 72, 65, 82, 83, // chars value
            83, 84, 82, 73, 78, 71, // string value
            86, 65, 82, 67, 72, 65, 82, // varchar value
            0, 0, 0, 86, // varchar value offset
            0, 0, 0, 80, // string value offset
            0, 0, 0, 75, // chars value offset
            0, 0, 0, 60 // binary value offset
        }, {1, 1, 1, 0, // RowHeader
            0, 0, 0, 0, 0, 0, 0, 3, // timestamp
            -1, 63, // col presence indicator
            0, // boolean
            1, // byte
            0, 97, // char
            0, 0, 55, -30, 13, -86, 72, 64, // date
            64, 37, 0, 0, 0, 0, 0, 0, // double
            66, -56, 102, 102, // float
            0, 0, 0, -106, // int
            0, 0, 0, 0, 0, 0, 3, -24, // long
            0, 5, // short
            0, 0, 55, -30, 13, -30, 37, 8, // timestamp
            // variable length cols
            1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
            67, 72, 65, 82, 83, // chars value
            83, 84, 82, 73, 78, 71, // string value
            86, 65, 82, 67, 72, 65, 82, // varchar value
            0, 0, 0, 86, // varchar value offset
            0, 0, 0, 80, // string value offset
            0, 0, 0, 75, // chars value offset
            0, 0, 0, 60 // binary value offset
        }, {1, 1, 1, 0, // RowHeader
            0, 0, 0, 0, 0, 0, 0, 4, // timestamp
            -1, 63, // col presence indicator
            0, // boolean
            1, // byte
            0, 97, // char
            0, 0, 55, -30, 13, -86, 72, 64, // date
            64, 37, 0, 0, 0, 0, 0, 0, // double
            66, -56, 102, 102, // float
            0, 0, 0, -106, // int
            0, 0, 0, 0, 0, 0, 3, -24, // long
            0, 5, // short
            0, 0, 55, -30, 13, -30, 37, 8, // timestamp
            // variable length cols
            1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
            67, 72, 65, 82, 83, // chars value
            83, 84, 82, 73, 78, 71, // string value
            86, 65, 82, 67, 72, 65, 82, // varchar value
            0, 0, 0, 86, // varchar value offset
            0, 0, 0, 80, // string value offset
            0, 0, 0, 75, // chars value offset
            0, 0, 0, 60 // binary value offset
        }, {1, 1, 1, 0, // RowHeader
            0, 0, 0, 0, 0, 0, 0, 5, // timestamp
            -1, 63, // col presence indicator
            0, // boolean
            1, // byte
            0, 97, // char
            0, 0, 55, -30, 13, -86, 72, 64, // date
            64, 37, 0, 0, 0, 0, 0, 0, // double
            66, -56, 102, 102, // float
            0, 0, 0, -106, // int
            0, 0, 0, 0, 0, 0, 3, -24, // long
            0, 5, // short
            0, 0, 55, -30, 13, -30, 37, 8, // timestamp
            // variable length cols
            1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
            67, 72, 65, 82, 83, // chars value
            83, 84, 82, 73, 78, 71, // string value
            86, 65, 82, 67, 72, 65, 82, // varchar value
            0, 0, 0, 86, // varchar value offset
            0, 0, 0, 80, // string value offset
            0, 0, 0, 75, // chars value offset
            0, 0, 0, 60 // binary value offset
        }, {1, 1, 1, 0, // RowHeader
            0, 0, 0, 0, 0, 0, 0, 6, // timestamp
            -1, 63, // col presence indicator
            0, // boolean
            1, // byte
            0, 97, // char
            0, 0, 55, -30, 13, -86, 72, 64, // date
            64, 37, 0, 0, 0, 0, 0, 0, // double
            66, -56, 102, 102, // float
            0, 0, 0, -106, // int
            0, 0, 0, 0, 0, 0, 3, -24, // long
            0, 5, // short
            0, 0, 55, -30, 13, -30, 37, 8, // timestamp
            // variable length cols
            1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
            67, 72, 65, 82, 83, // chars value
            83, 84, 82, 73, 78, 71, // string value
            86, 65, 82, 67, 72, 65, 82, // varchar value
            0, 0, 0, 86, // varchar value offset
            0, 0, 0, 80, // string value offset
            0, 0, 0, 75, // chars value offset
            0, 0, 0, 60 // binary value offset
        }};

    MTableDescriptor descriptor = new MTableDescriptor();
    descriptor.setMaxVersions(5);
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    Iterator<Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      put.addColumn("col_" + type, value);

      // System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
    }
    descriptor.finalizeDescriptor();

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof MultiVersionValue);

    byte[][] finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(1, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      if (i == 0) {
        // this should have byte array
        System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
            + "byte array          : " + Arrays.toString(finalByteArr[i]));
        System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
            + "expected byte array : " + Arrays.toString(expectedByteArray[i]));

        assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
      } else {
        assertNull(finalByteArr[i]);
      }
    }

    // now put next version
    // only change timestamp and put the same value
    put.setTimeStamp(2);
    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), finalValue);

    assertTrue(finalValue instanceof MultiVersionValue);

    finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(2, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      if (i == 0 || i == 1) {
        System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
            + "byte array          : " + Arrays.toString(finalByteArr[i]));
        System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
            + "expected byte array : " + Arrays.toString(expectedByteArray[i]));

        assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
      } else {
        assertNull(finalByteArr[i]);
      }
    }

    // 3rd version put directly 5th one
    put.setTimeStamp(5);
    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), finalValue);

    assertTrue(finalValue instanceof MultiVersionValue);

    finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(3, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      if (i == 0 || i == 1 || i == 2) {
        if (i == 2) {
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "byte array          : " + Arrays.toString(finalByteArr[i]));
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "expected byte array : " + Arrays.toString(expectedByteArray[4]));

          assertArrayEquals(expectedByteArray[4], finalByteArr[i]);
        } else {
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "byte array          : " + Arrays.toString(finalByteArr[i]));
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "expected byte array : " + Arrays.toString(expectedByteArray[i]));

          assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
        }
      } else {
        assertNull(finalByteArr[i]);
      }
    }

    // 3rd version put directly 5th one
    put.setTimeStamp(3);
    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), finalValue);

    assertTrue(finalValue instanceof MultiVersionValue);

    finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(4, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      if (i == 0 || i == 1 || i == 2 || i == 3) {
        if (i == 3) {
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "byte array          : " + Arrays.toString(finalByteArr[i]));
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "expected byte array : " + Arrays.toString(expectedByteArray[4]));

          assertArrayEquals(expectedByteArray[4], finalByteArr[i]);
        } else {
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "byte array          : " + Arrays.toString(finalByteArr[i]));
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "expected byte array : " + Arrays.toString(expectedByteArray[i]));

          assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
        }
      } else {
        assertNull(finalByteArr[i]);
      }
    }

    // 3rd version put directly 5th one
    put.setTimeStamp(4);
    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), finalValue);

    assertTrue(finalValue instanceof MultiVersionValue);

    finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(5, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
          + "byte array          : " + Arrays.toString(finalByteArr[i]));
      System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
          + "expected byte array : " + Arrays.toString(expectedByteArray[i]));

      assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
      assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
    }

    // basically update the version
    put.setTimeStamp(3);
    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), finalValue);

    assertTrue(finalValue instanceof MultiVersionValue);

    finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(5, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
          + "byte array          : " + Arrays.toString(finalByteArr[i]));
      System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
          + "expected byte array : " + Arrays.toString(expectedByteArray[i]));

      assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
      assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
    }

    // puttting version with highest value should remove smallest version out
    put.setTimeStamp(6);
    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), finalValue);

    assertTrue(finalValue instanceof MultiVersionValue);

    finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(5, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
          + "byte array          : " + Arrays.toString(finalByteArr[i]));
      System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
          + "expected byte array : " + Arrays.toString(expectedByteArray[i + 1]));

      assertArrayEquals(expectedByteArray[i + 1], finalByteArr[i]);
      assertArrayEquals(expectedByteArray[i + 1], finalByteArr[i]);
    }
  }


  /**
   * Version Get test
   */
  @Test
  public void multiVersionGet() throws Exception {

    final byte[][] expectedByteArray = {{1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -1, 63, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
        // variable length cols
        1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
        67, 72, 65, 82, 83, // chars value
        83, 84, 82, 73, 78, 71, // string value
        86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 86, // varchar value offset
        0, 0, 0, 80, // string value offset
        0, 0, 0, 75, // chars value offset
        0, 0, 0, 60 // binary value offset
        },
        {1, 1, 1, 0, // RowHeader
            0, 0, 0, 0, 0, 0, 0, 2, // timestamp
            -1, 63, // col presence indicator
            0, // boolean
            1, // byte
            0, 97, // char
            0, 0, 55, -30, 13, -86, 72, 64, // date
            64, 37, 0, 0, 0, 0, 0, 0, // double
            66, -56, 102, 102, // float
            0, 0, 0, -106, // int
            0, 0, 0, 0, 0, 0, 3, -24, // long
            0, 5, // short
            0, 0, 55, -30, 13, -30, 37, 8, // timestamp
            // variable length cols
            1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
            67, 72, 65, 82, 83, // chars value
            83, 84, 82, 73, 78, 71, // string value
            86, 65, 82, 67, 72, 65, 82, // varchar value
            0, 0, 0, 86, // varchar value offset
            0, 0, 0, 80, // string value offset
            0, 0, 0, 75, // chars value offset
            0, 0, 0, 60 // binary value offset
        }, {1, 1, 1, 0, // RowHeader
            0, 0, 0, 0, 0, 0, 0, 3, // timestamp
            -1, 63, // col presence indicator
            0, // boolean
            1, // byte
            0, 97, // char
            0, 0, 55, -30, 13, -86, 72, 64, // date
            64, 37, 0, 0, 0, 0, 0, 0, // double
            66, -56, 102, 102, // float
            0, 0, 0, -106, // int
            0, 0, 0, 0, 0, 0, 3, -24, // long
            0, 5, // short
            0, 0, 55, -30, 13, -30, 37, 8, // timestamp
            // variable length cols
            1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
            67, 72, 65, 82, 83, // chars value
            83, 84, 82, 73, 78, 71, // string value
            86, 65, 82, 67, 72, 65, 82, // varchar value
            0, 0, 0, 86, // varchar value offset
            0, 0, 0, 80, // string value offset
            0, 0, 0, 75, // chars value offset
            0, 0, 0, 60 // binary value offset
        }, {1, 1, 1, 0, // RowHeader
            0, 0, 0, 0, 0, 0, 0, 4, // timestamp
            -1, 63, // col presence indicator
            0, // boolean
            1, // byte
            0, 97, // char
            0, 0, 55, -30, 13, -86, 72, 64, // date
            64, 37, 0, 0, 0, 0, 0, 0, // double
            66, -56, 102, 102, // float
            0, 0, 0, -106, // int
            0, 0, 0, 0, 0, 0, 3, -24, // long
            0, 5, // short
            0, 0, 55, -30, 13, -30, 37, 8, // timestamp
            // variable length cols
            1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
            67, 72, 65, 82, 83, // chars value
            83, 84, 82, 73, 78, 71, // string value
            86, 65, 82, 67, 72, 65, 82, // varchar value
            0, 0, 0, 86, // varchar value offset
            0, 0, 0, 80, // string value offset
            0, 0, 0, 75, // chars value offset
            0, 0, 0, 60 // binary value offset
        }, {1, 1, 1, 0, // RowHeader
            0, 0, 0, 0, 0, 0, 0, 5, // timestamp
            -1, 63, // col presence indicator
            0, // boolean
            1, // byte
            0, 97, // char
            0, 0, 55, -30, 13, -86, 72, 64, // date
            64, 37, 0, 0, 0, 0, 0, 0, // double
            66, -56, 102, 102, // float
            0, 0, 0, -106, // int
            0, 0, 0, 0, 0, 0, 3, -24, // long
            0, 5, // short
            0, 0, 55, -30, 13, -30, 37, 8, // timestamp
            // variable length cols
            1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
            67, 72, 65, 82, 83, // chars value
            83, 84, 82, 73, 78, 71, // string value
            86, 65, 82, 67, 72, 65, 82, // varchar value
            0, 0, 0, 86, // varchar value offset
            0, 0, 0, 80, // string value offset
            0, 0, 0, 75, // chars value offset
            0, 0, 0, 60 // binary value offset
        }, {1, 1, 1, 0, // RowHeader
            0, 0, 0, 0, 0, 0, 0, 6, // timestamp
            -1, 63, // col presence indicator
            0, // boolean
            1, // byte
            0, 97, // char
            0, 0, 55, -30, 13, -86, 72, 64, // date
            64, 37, 0, 0, 0, 0, 0, 0, // double
            66, -56, 102, 102, // float
            0, 0, 0, -106, // int
            0, 0, 0, 0, 0, 0, 3, -24, // long
            0, 5, // short
            0, 0, 55, -30, 13, -30, 37, 8, // timestamp
            // variable length cols
            1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
            67, 72, 65, 82, 83, // chars value
            83, 84, 82, 73, 78, 71, // string value
            86, 65, 82, 67, 72, 65, 82, // varchar value
            0, 0, 0, 86, // varchar value offset
            0, 0, 0, 80, // string value offset
            0, 0, 0, 75, // chars value offset
            0, 0, 0, 60 // binary value offset
        }};

    MTableDescriptor descriptor = new MTableDescriptor();
    descriptor.setMaxVersions(5);

    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    Iterator<Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      put.addColumn("col_" + type, value);

      // System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
    }
    descriptor.finalizeDescriptor();

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof MultiVersionValue);

    byte[][] finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(1, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      if (i == 0) {
        // this should have byte array
        System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
            + "byte array          : " + Arrays.toString(finalByteArr[i]));
        System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
            + "expected byte array : " + Arrays.toString(expectedByteArray[i]));

        assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
      } else {
        assertNull(finalByteArr[i]);
      }
    }

    // now put next version
    // only change timestamp and put the same value
    put.setTimeStamp(2);
    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), finalValue);

    assertTrue(finalValue instanceof MultiVersionValue);

    finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(2, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      if (i == 0 || i == 1) {
        System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
            + "byte array          : " + Arrays.toString(finalByteArr[i]));
        System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
            + "expected byte array : " + Arrays.toString(expectedByteArray[i]));

        assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
      } else {
        assertNull(finalByteArr[i]);
      }
    }

    // 3rd version put directly 5th one
    put.setTimeStamp(5);
    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), finalValue);

    assertTrue(finalValue instanceof MultiVersionValue);

    finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(3, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      if (i == 0 || i == 1 || i == 2) {
        if (i == 2) {
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "byte array          : " + Arrays.toString(finalByteArr[i]));
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "expected byte array : " + Arrays.toString(expectedByteArray[4]));

          assertArrayEquals(expectedByteArray[4], finalByteArr[i]);
        } else {
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "byte array          : " + Arrays.toString(finalByteArr[i]));
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "expected byte array : " + Arrays.toString(expectedByteArray[i]));

          assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
        }
      } else {
        assertNull(finalByteArr[i]);
      }
    }

    // 3rd version put directly 5th one
    put.setTimeStamp(3);
    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), finalValue);

    assertTrue(finalValue instanceof MultiVersionValue);

    finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(4, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      if (i == 0 || i == 1 || i == 2 || i == 3) {
        if (i == 3) {
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "byte array          : " + Arrays.toString(finalByteArr[i]));
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "expected byte array : " + Arrays.toString(expectedByteArray[4]));

          assertArrayEquals(expectedByteArray[4], finalByteArr[i]);
        } else {
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "byte array          : " + Arrays.toString(finalByteArr[i]));
          System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
              + "expected byte array : " + Arrays.toString(expectedByteArray[i]));

          assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
        }
      } else {
        assertNull(finalByteArr[i]);
      }
    }

    // 3rd version put directly 5th one
    put.setTimeStamp(4);
    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), finalValue);

    assertTrue(finalValue instanceof MultiVersionValue);

    finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(5, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
          + "byte array          : " + Arrays.toString(finalByteArr[i]));
      System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
          + "expected byte array : " + Arrays.toString(expectedByteArray[i]));

      assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
      assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
    }

    // basically update the version
    put.setTimeStamp(3);
    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), finalValue);

    assertTrue(finalValue instanceof MultiVersionValue);

    finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(5, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
          + "byte array          : " + Arrays.toString(finalByteArr[i]));
      System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
          + "expected byte array : " + Arrays.toString(expectedByteArray[i]));

      assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
      assertArrayEquals(expectedByteArray[i], finalByteArr[i]);
    }

    // puttting version with highest value should remove smallest version out
    put.setTimeStamp(6);
    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), finalValue);

    assertTrue(finalValue instanceof MultiVersionValue);

    finalByteArr = ((MultiVersionValue) finalValue).getVersions();

    // number of versions
    assertEquals(5, finalByteArr.length);

    for (int i = 0; i < finalByteArr.length; i++) {
      System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
          + "byte array          : " + Arrays.toString(finalByteArr[i]));
      System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
          + "expected byte array : " + Arrays.toString(expectedByteArray[i + 1]));

      assertArrayEquals(expectedByteArray[i + 1], finalByteArr[i]);
      assertArrayEquals(expectedByteArray[i + 1], finalByteArr[i]);
    }

    // do single version put

    // negative callback argument means newest value first
    // zero call back args means all version
    // positive callback args means oldest value first

    // as first value is removed after putting 6th version
    int expectedIndex = 1;

    MTableKey testKey = new MTableKey(Bytes.toBytes("testKey"));
    int numberOfVersionsToRead = 1;
    testKey.setMaxVersions(numberOfVersionsToRead);
    Object getValue = mtableFormatter.performGetOperation(testKey, descriptor, finalValue,
        numberOfVersionsToRead);

    assertTrue(getValue instanceof MultiVersionValueWrapper);

    byte[][] multiversionByteArr = ((MultiVersionValueWrapper) getValue).getVal();

    assertEquals(Math.abs(numberOfVersionsToRead), multiversionByteArr.length);

    for (int i = 0; i < Math.abs(numberOfVersionsToRead); i++) {
      assertEquals(MTableStorageFormatter.readTimeStamp(expectedByteArray[expectedIndex + i]),
          MTableStorageFormatter.readTimeStamp(multiversionByteArr[i]));
      assertArrayEquals(expectedByteArray[expectedIndex + i], multiversionByteArr[i]);
    }

    numberOfVersionsToRead = 3;
    testKey.setMaxVersions(numberOfVersionsToRead);
    getValue = mtableFormatter.performGetOperation(testKey, descriptor, finalValue,
        numberOfVersionsToRead);

    assertTrue(getValue instanceof MultiVersionValueWrapper);

    multiversionByteArr = ((MultiVersionValueWrapper) getValue).getVal();

    assertEquals(Math.abs(numberOfVersionsToRead), multiversionByteArr.length);

    for (int i = 0; i < Math.abs(numberOfVersionsToRead); i++) {
      assertEquals(MTableStorageFormatter.readTimeStamp(expectedByteArray[expectedIndex + i]),
          MTableStorageFormatter.readTimeStamp(multiversionByteArr[i]));
      assertArrayEquals(expectedByteArray[expectedIndex + i], multiversionByteArr[i]);
    }

    // max versions
    numberOfVersionsToRead = descriptor.getMaxVersions();
    testKey.setMaxVersions(numberOfVersionsToRead);
    getValue = mtableFormatter.performGetOperation(testKey, descriptor, finalValue,
        numberOfVersionsToRead);

    assertTrue(getValue instanceof MultiVersionValueWrapper);

    multiversionByteArr = ((MultiVersionValueWrapper) getValue).getVal();

    assertEquals(Math.abs(numberOfVersionsToRead), multiversionByteArr.length);

    for (int i = 0; i < Math.abs(numberOfVersionsToRead); i++) {
      assertEquals(MTableStorageFormatter.readTimeStamp(expectedByteArray[expectedIndex + i]),
          MTableStorageFormatter.readTimeStamp(multiversionByteArr[i]));
      assertArrayEquals(expectedByteArray[expectedIndex + i], multiversionByteArr[i]);
    }

    // more than max versions
    numberOfVersionsToRead = descriptor.getMaxVersions();
    testKey.setMaxVersions(numberOfVersionsToRead);
    getValue = mtableFormatter.performGetOperation(testKey, descriptor, finalValue,
        numberOfVersionsToRead + 1);

    assertTrue(getValue instanceof MultiVersionValueWrapper);

    multiversionByteArr = ((MultiVersionValueWrapper) getValue).getVal();

    assertEquals(Math.abs(numberOfVersionsToRead), multiversionByteArr.length);

    for (int i = 0; i < Math.abs(numberOfVersionsToRead); i++) {
      assertEquals(MTableStorageFormatter.readTimeStamp(expectedByteArray[expectedIndex + i]),
          MTableStorageFormatter.readTimeStamp(multiversionByteArr[i]));
      assertArrayEquals(expectedByteArray[expectedIndex + i], multiversionByteArr[i]);
    }

    // ------------------------ REVERSE WAY ------------------------------------------------------

    numberOfVersionsToRead = -1;
    testKey.setMaxVersions(numberOfVersionsToRead);
    expectedIndex = expectedByteArray.length - 1;
    getValue = mtableFormatter.performGetOperation(testKey, descriptor, finalValue,
        numberOfVersionsToRead);

    assertTrue(getValue instanceof MultiVersionValueWrapper);

    multiversionByteArr = ((MultiVersionValueWrapper) getValue).getVal();

    assertEquals(Math.abs(numberOfVersionsToRead), multiversionByteArr.length);

    for (int i = 0; i < Math.abs(numberOfVersionsToRead); i++) {
      assertEquals(MTableStorageFormatter.readTimeStamp(expectedByteArray[expectedIndex - i]),
          MTableStorageFormatter.readTimeStamp(multiversionByteArr[i]));
      assertArrayEquals(expectedByteArray[expectedIndex - i], multiversionByteArr[i]);
    }

    numberOfVersionsToRead = -3;
    testKey.setMaxVersions(numberOfVersionsToRead);
    expectedIndex = expectedByteArray.length - 1;
    getValue = mtableFormatter.performGetOperation(testKey, descriptor, finalValue,
        numberOfVersionsToRead);

    assertTrue(getValue instanceof MultiVersionValueWrapper);

    multiversionByteArr = ((MultiVersionValueWrapper) getValue).getVal();

    assertEquals(Math.abs(numberOfVersionsToRead), multiversionByteArr.length);

    for (int i = 0; i < Math.abs(numberOfVersionsToRead); i++) {
      assertEquals(MTableStorageFormatter.readTimeStamp(expectedByteArray[expectedIndex - i]),
          MTableStorageFormatter.readTimeStamp(multiversionByteArr[i]));
      assertArrayEquals(expectedByteArray[expectedIndex - i], multiversionByteArr[i]);
    }

    // max versions
    numberOfVersionsToRead = -1 * descriptor.getMaxVersions();
    testKey.setMaxVersions(numberOfVersionsToRead);
    expectedIndex = expectedByteArray.length - 1;
    getValue = mtableFormatter.performGetOperation(testKey, descriptor, finalValue,
        numberOfVersionsToRead);

    assertTrue(getValue instanceof MultiVersionValueWrapper);

    multiversionByteArr = ((MultiVersionValueWrapper) getValue).getVal();

    assertEquals(Math.abs(numberOfVersionsToRead), multiversionByteArr.length);

    for (int i = 0; i < Math.abs(numberOfVersionsToRead); i++) {
      assertEquals(MTableStorageFormatter.readTimeStamp(expectedByteArray[expectedIndex - i]),
          MTableStorageFormatter.readTimeStamp(multiversionByteArr[i]));
      assertArrayEquals(expectedByteArray[expectedIndex - i], multiversionByteArr[i]);
    }

    // more than max versions
    numberOfVersionsToRead = -1 * descriptor.getMaxVersions();
    testKey.setMaxVersions(numberOfVersionsToRead);
    expectedIndex = expectedByteArray.length - 1;
    getValue = mtableFormatter.performGetOperation(testKey, descriptor, finalValue,
        numberOfVersionsToRead - 1);

    assertTrue(getValue instanceof MultiVersionValueWrapper);

    multiversionByteArr = ((MultiVersionValueWrapper) getValue).getVal();

    assertEquals(Math.abs(numberOfVersionsToRead), multiversionByteArr.length);

    for (int i = 0; i < Math.abs(numberOfVersionsToRead); i++) {
      assertEquals(MTableStorageFormatter.readTimeStamp(expectedByteArray[expectedIndex - i]),
          MTableStorageFormatter.readTimeStamp(multiversionByteArr[i]));
      assertArrayEquals(expectedByteArray[expectedIndex - i], multiversionByteArr[i]);
    }


  }


  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performPutOperationNewRowAllTypesPartialPut() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    Iterator<Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      if (!(type == BasicTypes.STRING)) {
        System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
        put.addColumn("col_" + type, value);
      }


    }

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    final byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -1, 55, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
        // variable length cols
        1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
        67, 72, 65, 82, 83, // chars value
        86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 80, // binary value offset
        0, 0, 0, 75, // chars value offset
        0, 0, 0, 60 // varchar value offset
    };
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);
  }

  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performPutOperationNewRowAllTypesFullVersionUpdate() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    Iterator<Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      // System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
      put.addColumn("col_" + type, value);
    }

    descriptor.finalizeDescriptor();

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -1, 63, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
        // variable length cols
        1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
        67, 72, 65, 82, 83, // chars value
        83, 84, 82, 73, 78, 71, // string value
        86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 86, // varchar value offset
        0, 0, 0, 80, // string value offset
        0, 0, 0, 75, // chars value offset
        0, 0, 0, 60 // binary value offset
    };

    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);

    // now update the complete version

    System.out.println("---------------------UPDATION-----------------------------");

    put = new Put("testKey");
    put.setTimeStamp(2);

    // this to keep the order and a mix of variable and fixed length columns
    mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(updatedDataTypeToValueMap);

    entryIterator = mapSortedByTypeName.entrySet().iterator();

    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
      put.addColumn("col_" + type, value);
    }

    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue = mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(),
        expectedByteArray);

    assertTrue(finalValue instanceof byte[]);

    finalByteArr = (byte[]) finalValue;

    final byte[] expectedByteArray2 = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 2, // timestamp
        -1, 63, // col presence indicator
        1, // boolean
        2, // byte
        0, 98, // char
        0, 0, 55, -23, 101, 91, 116, 64, // date
        64, 52, -128, 0, 0, 0, 0, 0, // double
        67, 72, 51, 51, // float
        0, 0, 1, 44, // int
        0, 0, 0, 0, 0, 0, 7, -48, // long
        0, 10, // short
        0, 0, 55, -23, 101, -109, 81, 8, // timestamp
        // variable length cols
        0, 1, 1, 0, 1, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, // binary value
        78, 69, 87, 67, 72, 65, 82, 83, // chars value
        78, 69, 87, 83, 84, 82, 73, 78, 71, // string value
        78, 69, 87, 86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 92, // varchar value offset
        0, 0, 0, 83, // string value offset
        0, 0, 0, 75, // chars value offset
        0, 0, 0, 60 // binary value offset
    };

    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray2));

    assertArrayEquals(expectedByteArray2, finalByteArr);
  }

  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performPutOperationNewRowAllTypesOnlyFixedColUpdate() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    Iterator<Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      // System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
      put.addColumn("col_" + type, value);
    }

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -1, 63, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
        // variable length cols
        1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
        67, 72, 65, 82, 83, // chars value
        83, 84, 82, 73, 78, 71, // string value
        86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 86, // varchar value offset
        0, 0, 0, 80, // string value offset
        0, 0, 0, 75, // chars offset
        0, 0, 0, 60, // binary value offset
    };

    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);

    // now update the complete version

    System.out.println("---------------------UPDATION-----------------------------");

    put = new Put("testKey");
    put.setTimeStamp(2);

    // this to keep the order and a mix of variable and fixed length columns
    mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(updatedDataTypeToValueMap);

    entryIterator = mapSortedByTypeName.entrySet().iterator();

    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      if (type.isFixedLength()) {
        System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
        put.addColumn("col_" + type, value);
      }
    }

    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue = mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(),
        expectedByteArray);

    assertTrue(finalValue instanceof byte[]);

    finalByteArr = (byte[]) finalValue;

    final byte[] expectedByteArray2 = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 2, // timestamp
        -1, 63, // col presence indicator
        1, // boolean
        2, // byte
        0, 98, // char
        0, 0, 55, -23, 101, 91, 116, 64, // date
        64, 52, -128, 0, 0, 0, 0, 0, // double
        67, 72, 51, 51, // float
        0, 0, 1, 44, // int
        0, 0, 0, 0, 0, 0, 7, -48, // long
        0, 10, // short
        0, 0, 55, -23, 101, -109, 81, 8, // timestamp
        // variable length cols
        1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, // binary value
        67, 72, 65, 82, 83, // chars value
        83, 84, 82, 73, 78, 71, // string value
        86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 86, // varchar value offset
        0, 0, 0, 80, // string value offset
        0, 0, 0, 75, // chars offset
        0, 0, 0, 60, // binary value offset
    };

    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray2));

    assertArrayEquals(expectedByteArray2, finalByteArr);
  }

  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performPutOperationNewRowAllTypesOnlyVariableColUpdate() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    Iterator<Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      if (type.isFixedLength()) {
        System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
        put.addColumn("col_" + type, value);
      }
    }
    descriptor.finalizeDescriptor();

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -18, 23, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
    };

    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);

    // now update the complete version

    System.out.println("---------------------UPDATION-----------------------------");

    put = new Put("testKey");
    put.setTimeStamp(2);

    // this to keep the order and a mix of variable and fixed length columns
    mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(updatedDataTypeToValueMap);

    entryIterator = mapSortedByTypeName.entrySet().iterator();

    while (entryIterator.hasNext()) {

      Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      if (!type.isFixedLength()) {
        System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
        put.addColumn("col_" + type, value);
      }
    }

    mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    finalValue = mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(),
        expectedByteArray);

    assertTrue(finalValue instanceof byte[]);

    finalByteArr = (byte[]) finalValue;

    final byte[] expectedByteArray2 = {1, 1, 1, 0, // RowHeader
        0, 0, 0, 0, 0, 0, 0, 2, // timestamp
        -1, 63, // col presence indicator
        0, // boolean
        1, // byte
        0, 97, // char
        0, 0, 55, -30, 13, -86, 72, 64, // date
        64, 37, 0, 0, 0, 0, 0, 0, // double
        66, -56, 102, 102, // float
        0, 0, 0, -106, // int
        0, 0, 0, 0, 0, 0, 3, -24, // long
        0, 5, // short
        0, 0, 55, -30, 13, -30, 37, 8, // timestamp
        // variable length cols
        0, 1, 1, 0, 1, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, // binary value
        78, 69, 87, 67, 72, 65, 82, 83, // chars value
        78, 69, 87, 83, 84, 82, 73, 78, 71, // string value
        78, 69, 87, 86, 65, 82, 67, 72, 65, 82, // varchar value
        0, 0, 0, 92, // binary value offset
        0, 0, 0, 83, // chars value offset
        0, 0, 0, 75, // string value offset
        0, 0, 0, 60 // varchar value offset
    };

    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array          : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array : " + Arrays.toString(expectedByteArray2));

    assertArrayEquals(expectedByteArray2, finalByteArr);
  }

  /**
   * Check if byte format when inserting first row
   */
  @Test
  public void performPutOperationNewRow27Long() throws Exception {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    for (int i = 0; i < 27; i++) {
      descriptor.addColumn("col_" + i, BasicTypes.LONG);
      put.addColumn("col_" + i, new Long(i));
    }

    byte[] magicNoEncodingIdReservedBytes = descriptor.getStorageFormatterIdentifiers();

    StorageFormatter mtableFormatter =
        StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
            magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(descriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(descriptor, mValue, mValue.getOpInfo(), null);

    assertTrue(finalValue instanceof byte[]);

    final byte[] finalByteArr = (byte[]) finalValue;
    final byte[] expectedByteArray = {1, 1, 1, 0, // Row Header
        0, 0, 0, 0, 0, 0, 0, 1, // timestamp
        -1, -1, -1, 7, // col presence indicator
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
        3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0,
        0, 7, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0,
        0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 13, 0, 0, 0, 0, 0, 0, 0, 14, 0,
        0, 0, 0, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0,
        18, 0, 0, 0, 0, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 21, 0, 0, 0, 0,
        0, 0, 0, 22, 0, 0, 0, 0, 0, 0, 0, 23, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 25, 0,
        0, 0, 0, 0, 0, 0, 26};
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "byte array         : " + Arrays.toString(finalByteArr));
    System.out.println("MTableStorageFormatterTest.performPutOperationNewRow :: "
        + "expected byte array: " + Arrays.toString(expectedByteArray));

    assertArrayEquals(expectedByteArray, finalByteArr);
  }


  @Test
  public void allCurrentColumnsExists() throws Exception {
    MTableStorageFormatter mtableFormatter =
        (MTableStorageFormatter) StorageFormatters.getInstance((byte) 1, (byte) 1, (byte) 0);

    IBitMap cols = new BitMap(10);
    List<Integer> columns = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        cols.set(i);
      }
    }

    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        columns.add(i);
      }
    }

    assertTrue(mtableFormatter.allNewColumnsAlreadyExists(cols, columns));

    columns.clear();
    for (int i = 0; i < 10; i++) {
      if (i % 2 != 0) {
        columns.add(i);
      }
    }

    assertFalse(mtableFormatter.allNewColumnsAlreadyExists(cols, columns));

    for (int i = 0; i < 10; i++) {
      cols.set(i);
    }

    columns.clear();
    for (int i = 0; i < 10; i++) {
      if (i % 2 != 0) {
        columns.add(i);
      }
    }

    assertTrue(mtableFormatter.allNewColumnsAlreadyExists(cols, columns));

    cols.clear();
    columns.clear();
    for (int i = 0; i < 10; i++) {
      if (i % 2 != 0) {
        columns.add(i);
      }
    }

    assertFalse(mtableFormatter.allNewColumnsAlreadyExists(cols, columns));
  }


  @Test
  public void bitSetTests() {
    BitSet set = new BitSet(10);
    set.set(0, 5);
    set.set(7);
    System.out.println("MTableStorageFormatterTest.bitSetTests :: " + "len: " + set.length()
        + " card " + set.cardinality());

    set.set(0, 10);
    for (int i = 0; i < 10; i++) {
      System.out.println("MTableStorageFormatterTest.bitSetTests :: " + "Bit is set at ? " + i
          + " : " + set.get(i));
    }
    System.out.println("-------------------------------------------------------------------------");
    BitSet newSet = new BitSet(2);
    newSet.set(2);
    newSet.set(7);

    for (int i = 0; i < 10; i++) {
      System.out.println("MTableStorageFormatterTest.bitSetTests :: " + "Bit is set at ? " + i
          + " : " + newSet.get(i));
    }

    set.andNot(newSet);

    System.out.println("-------------------------------------------------------------------------");

    for (int i = 0; i < 10; i++) {
      System.out.println("MTableStorageFormatterTest.bitSetTests :: " + "Bit is set at ? " + i
          + " : " + set.get(i));
    }

    BitSet bits = new BitSet(3);
    bits.set(0);
    bits.set(1);
    bits.set(2);

    System.out.println("MTableStorageFormatterTest.bitSetTests :: 2395" + "bits " + bits);


  }

}
