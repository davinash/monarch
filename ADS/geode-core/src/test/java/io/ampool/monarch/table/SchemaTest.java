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
package io.ampool.monarch.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.IOException;

import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.MapType;
import io.ampool.monarch.types.StructType;
import io.ampool.monarch.types.interfaces.DataType;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class SchemaTest {
  /**
   * A simple test to create schema with column type as binary. Assert that the schema is as
   * expected after serialization and deserialization.
   *
   * @throws IOException if an error occurs during write or read from DataOutput/DataInput
   * @throws ClassNotFoundException if the deserialization target class is not found
   */
  @Test
  public void testSimpleSchema() throws IOException, ClassNotFoundException {
    final String[] columns = new String[] {"c1", "c2", "c3"};
    final String expected = "struct<c1:BINARY,c2:BINARY,c3:BINARY>";

    Schema schema = new Schema(columns);

    assertEquals("Incorrect schema string.", expected, schema.toString());
    assertEquals("Incorrect number of columns.", 3, schema.getNumberOfColumns());
    assertSchema(columns, schema);

    /* write the schema to DataOutput and then assert on the read schema from DataInput */
    ByteArrayDataOutputStream out = new ByteArrayDataOutputStream();
    schema.toData(out);
    final byte[] buffer = out.buffer();

    DataInput in = new ByteArrayDataInputStream(buffer);
    Schema schema1 = new Schema();
    schema1.fromData(in);
    assertEquals("Incorrect number of columns.", 3, schema1.getNumberOfColumns());
    assertSchema(columns, schema1);
  }

  private void assertSchema(String[] columns, Schema schema) {
    for (int i = 0; i < columns.length; i++) {
      assertEquals("Incorrect column name for: " + columns[i], columns[i],
          schema.getColumnDescriptorByName(columns[i]).getColumnNameAsString());
      assertEquals("Incorrect column type for: " + columns[i], BasicTypes.BINARY,
          schema.getColumnDescriptorByName(columns[i]).getColumnType());
      assertEquals("Incorrect column index for: " + columns[i], i,
          schema.getColumnDescriptorByName(columns[i]).getIndex());
      assertTrue("Column should be present: " + columns[i], schema.containsColumn(columns[i]));
      assertTrue("Column should be present: " + columns[i],
          schema.containsColumn(columns[i].getBytes()));
    }
  }

  /**
   * Test that schema from string is populated as expected.
   */
  @Test
  public void testSchemaFromString() {
    final String schemaStr =
        "struct<c1:INT,c2:map<INT,STRING>,c3:FLOAT,c4:struct<a:DOUBLE,b:LONG>>";
    final String[] expectedNames = new String[] {"c1", "c2", "c3", "c4"};
    final DataType[] expectedTypes =
        new DataType[] {BasicTypes.INT, new MapType(BasicTypes.INT, BasicTypes.STRING),
            BasicTypes.FLOAT, new StructType(new String[] {"a", "b"},
                new DataType[] {BasicTypes.DOUBLE, BasicTypes.LONG})};

    final Schema schema = Schema.fromString(schemaStr);
    assertEquals("Incorrect schema string.", schemaStr, schema.toString());
    assertEquals("Incorrect number of columns.", expectedNames.length, schema.getNumberOfColumns());
    for (int i = 0; i < expectedNames.length; i++) {
      assertTrue("Column should be present: " + expectedNames[i],
          schema.containsColumn(expectedNames[i]));
      assertEquals("Incorrect index for column: " + expectedNames[i], i,
          schema.getColumnDescriptorByName(expectedNames[i]).getIndex());
      assertEquals("Incorrect type for column (by-name): " + expectedNames[i], expectedTypes[i],
          schema.getColumnDescriptorByName(expectedNames[i]).getColumnType());
      assertEquals("Incorrect type for column (by-index): " + expectedNames[i], expectedTypes[i],
          schema.getColumnDescriptorByIndex(i).getColumnType());
    }
  }
}
