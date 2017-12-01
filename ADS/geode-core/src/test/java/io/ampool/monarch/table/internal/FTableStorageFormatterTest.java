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

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.interfaces.DataType;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

@Category(UnitTest.class)
public class FTableStorageFormatterTest {

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
  public void testOffsetForEachColumns() {

    FTableDescriptor tableDescriptor = new FTableDescriptor();
    Schema.Builder sb = new Schema.Builder();
    int NUM_OF_COLUMNS = 5;
    String COLUMN_NAME_PREFIX = "COL";
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      // sets a partitioningColumn
      if (colmnIndex == 0) {
        tableDescriptor.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      sb.column(COLUMN_NAME_PREFIX + colmnIndex);
      // tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX +
      // colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setSchema(sb.build());
    tableDescriptor.finalizeDescriptor();

    byte[] value = new byte[] {0, 0, 9, 57, 25, 37, -102, -24, // INSERTION TS
        67, 79, 76, 48, 67, 79, 76, 49, 67, 79, 76, 50, 67, 79, 76, 51, 67, 79, 76, 52, 0, 0, 0, 24,
        0, 0, 0, 20, 0, 0, 0, 16, 0, 0, 0, 12, 0, 0, 0, 8};

    FTableStorageFormatter formatter = new FTableStorageFormatter((byte) 1, (byte) 1, (byte) 1);
    Map<Integer, Pair<Integer, Integer>> offsetsForEachColumn =
        formatter.getOffsetsForEachColumn(tableDescriptor, value, new ArrayList<>());
    System.out.println("FTableStorageFormatterTest.testOffsetForEachColumns :: 2783 " + "offsets "
        + offsetsForEachColumn);
  }

}
