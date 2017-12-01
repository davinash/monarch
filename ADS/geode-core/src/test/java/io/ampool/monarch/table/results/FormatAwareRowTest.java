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
package io.ampool.monarch.table.results;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MultiVersionValueWrapper;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.internal.MOperation;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.table.internal.MTableStorageFormatter;
import io.ampool.monarch.table.internal.MValue;
import io.ampool.monarch.table.internal.MultiVersionValue;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.table.internal.StorageFormatter;
import io.ampool.monarch.table.internal.StorageFormatters;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.TypeHelper;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Category(UnitTest.class)
@Ignore("Failing on Jenkins but passes locallly GEN-1854")
public class FormatAwareRowTest {

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

  @Test
  public void testGetColumnIndices() {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    boolean prevAdded = false;
    List<byte[]> columnNames = new ArrayList<>();

    Iterator<Map.Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Map.Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      if (!prevAdded) {
        columnNames.add(Bytes.toBytes("col_" + type));
        prevAdded = true;
      } else {
        prevAdded = false;
      }
      put.addColumn("col_" + type, value);
      System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
    }

    FormatAwareRow formatAwareResult = new FormatAwareRow();

    List<Integer> listOfColumnIndices =
        formatAwareResult.getListOfColumnIndices(descriptor, columnNames);
    System.out.println("FormatAwareResultTest.testGetColumnIndices :: " + "indices "
        + TypeHelper.deepToString(listOfColumnIndices));
    Integer[] arr = new Integer[listOfColumnIndices.size()];
    listOfColumnIndices.toArray(arr);
    assertArrayEquals(new Integer[] {0, 2, 4, 6, 8, 10, 12}, arr);

    listOfColumnIndices = formatAwareResult.getListOfColumnIndices(descriptor, new ArrayList<>());
    System.out.println("FormatAwareResultTest.testGetColumnIndices :: " + "indices "
        + TypeHelper.deepToString(listOfColumnIndices));
    arr = new Integer[listOfColumnIndices.size()];
    listOfColumnIndices.toArray(arr);
    assertArrayEquals(new Integer[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, arr);

    listOfColumnIndices = formatAwareResult.getListOfColumnIndices(descriptor, Arrays.asList());
    System.out.println("FormatAwareResultTest.testGetColumnIndices :: " + "indices "
        + TypeHelper.deepToString(listOfColumnIndices));
    arr = new Integer[listOfColumnIndices.size()];
    listOfColumnIndices.toArray(arr);
    assertArrayEquals(new Integer[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, arr);
  }

  @Test
  public void testResult() {
    MTableDescriptor descriptor = new MTableDescriptor();
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    boolean prevAdded = false;
    List<byte[]> columnNames = new ArrayList<>();

    Iterator<Map.Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Map.Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      if (!prevAdded) {
        columnNames.add(Bytes.toBytes("col_" + type));
        prevAdded = true;
      } else {
        prevAdded = false;
      }
      put.addColumn("col_" + type, value);
      System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
    }

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

    Row formatAwareResult =
        new FormatAwareRow(put.getRowKey(), finalByteArr, descriptor, new ArrayList<>());
    assertFalse(formatAwareResult.isEmpty());
    assertArrayEquals(put.getRowKey(), formatAwareResult.getRowId());
    assertEquals(new Long(1), formatAwareResult.getRowTimeStamp());
    assertEquals(dataTypeToValueMap.size(), formatAwareResult.size());
    List<Cell> cells = formatAwareResult.getCells();

    assertEquals(dataTypeToValueMap.size(), cells.size());
    ArrayList<DataType> keys = new ArrayList<>(mapSortedByTypeName.keySet());
    ArrayList<Object> values = new ArrayList<>(mapSortedByTypeName.values());

    for (int i = 0; i < cells.size(); i++) {
      Cell mCell = cells.get(i);
      assertEquals(keys.get(i), mCell.getColumnType());
      if (mCell.getColumnType() == BasicTypes.BINARY) {
        assertArrayEquals((byte[]) values.get(i), (byte[]) mCell.getColumnValue());
      } else if (mCell.getColumnType() == BasicTypes.DATE) {
        assertEquals(((Date) values.get(i)).getTime(), ((Date) mCell.getColumnValue()).getTime());
      } else if (mCell.getColumnType() == BasicTypes.TIMESTAMP) {
        assertEquals(((Timestamp) values.get(i)).getTime(),
            ((Timestamp) mCell.getColumnValue()).getTime());
      } else {
        assertEquals(values.get(i), mCell.getColumnValue());
      }
    }
  }

  /**
   * Version Get test - TODO to improve
   */
  @Test
  public void testMultiVersionResult() throws Exception {

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

    int max_versions = 5;
    MTableDescriptor descriptor = new MTableDescriptor();
    descriptor.setMaxVersions(max_versions);
    Put put = new Put("testKey");
    put.setTimeStamp(1);

    // this to keep the order and a mix of variable and fixed length columns
    TreeMap<DataType, Object> mapSortedByTypeName = new TreeMap<DataType, Object>((o1, o2) -> {
      return ((BasicTypes) o1).name().compareTo(((BasicTypes) o2).name());
    });
    mapSortedByTypeName.putAll(dataTypeToValueMap);

    Iterator<Map.Entry<DataType, Object>> entryIterator = mapSortedByTypeName.entrySet().iterator();
    while (entryIterator.hasNext()) {

      Map.Entry<DataType, Object> next = entryIterator.next();
      DataType type = next.getKey();
      Object value = next.getValue();
      descriptor.addColumn("col_" + type, (BasicTypes) type);
      put.addColumn("col_" + type, value);

      // System.out.println("type: " + type + " value: " + Arrays.toString(type.serialize(value)));
    }

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

    // negative callback argument means newest value first
    // zero call back args means all version
    // positive callback args means oldest value first

    // as first value is removed after putting 6th version
    int expectedIndex = 1;

    int numberOfVersionsToRead = 1;
    MTableKey testKey = new MTableKey(Bytes.toBytes("testKey"));
    testKey.setMaxVersions(numberOfVersionsToRead);
    Object getValue = mtableFormatter.performGetOperation(testKey, descriptor, finalValue,
        numberOfVersionsToRead);

    assertTrue(getValue instanceof MultiVersionValueWrapper);

    byte[][] multiversionByteArr = ((MultiVersionValueWrapper) getValue).getVal();

    assertEquals(Math.abs(numberOfVersionsToRead), multiversionByteArr.length);

    Row formatAwareResult =
        new FormatAwareRow(put.getRowKey(), multiversionByteArr, descriptor, new ArrayList<>());
    assertFalse(formatAwareResult.isEmpty());
    assertArrayEquals(put.getRowKey(), formatAwareResult.getRowId());

    Map<Long, SingleVersionRow> versionRows = formatAwareResult.getAllVersions();

    assertEquals(numberOfVersionsToRead, versionRows.size());

    for (int i = 0; i < Math.abs(numberOfVersionsToRead); i++) {
      assertEquals(MTableStorageFormatter.readTimeStamp(expectedByteArray[expectedIndex + i]),
          MTableStorageFormatter.readTimeStamp(multiversionByteArr[i]));
      assertArrayEquals(expectedByteArray[expectedIndex + i], multiversionByteArr[i]);

      // check for each version
      descriptor.setMaxVersions(1);
      Row singleVersion = new FormatAwareRow(put.getRowKey(), expectedByteArray[expectedIndex + i],
          descriptor, new ArrayList<>());
      descriptor.setMaxVersions(max_versions);

      SingleVersionRow currentVersion = versionRows.get(singleVersion.getRowTimeStamp());
      assertEquals(0, currentVersion.compareTo(singleVersion.getLatestRow()));

    }

    // create result from this object

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

}
