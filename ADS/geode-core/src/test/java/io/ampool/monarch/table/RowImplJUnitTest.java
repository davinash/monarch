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

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.RowImpl;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testng.Assert;

import java.util.List;

@Category(MonarchTest.class)
public class RowImplJUnitTest {

  int numColumns = 5;
  String columnName = "name";
  String columnValue = "value";

  @Test
  public void testMResultImpl() {

    byte[] rowId = Bytes.toBytes("rowid");
    long timestamp = System.currentTimeMillis();
    int singleColLength = Bytes.toBytes(columnValue + 1).length;
    int byteLength = Bytes.SIZEOF_LONG /* Timestamp */
        + numColumns * Bytes.SIZEOF_INT /* column length */
        + singleColLength * numColumns /* Value length */;

    byte[] rowValue = new byte[byteLength];

    int curLenPos = Bytes.SIZEOF_LONG;
    int curDataPos = Bytes.SIZEOF_LONG /* Timestamp */
        + numColumns * Bytes.SIZEOF_INT /* column length */;

    Bytes.putLong(rowValue, 0, timestamp);

    MTableDescriptor tableDescriptor = new MTableDescriptor();

    for (int i = 0; i < numColumns; i++) {
      byte[] column = Bytes.toBytes(this.columnValue + i);

      Bytes.putInt(rowValue, curLenPos, column.length);
      curLenPos += Bytes.SIZEOF_INT;

      Bytes.putBytes(rowValue, curDataPos, column, 0, column.length);
      curDataPos += column.length;

      tableDescriptor.addColumn(Bytes.toBytes(columnName + i));
    }

    RowImpl result = new RowImpl(rowId, rowValue, tableDescriptor);

    ByteArrayKey inputRowValue = new ByteArrayKey(rowValue);
    ByteArrayKey returnRowValue = new ByteArrayKey(result.getRowValue());
    Assert.assertEquals(returnRowValue, inputRowValue);

    List<Cell> cells = result.getCells();
    int i = 0;
    for (Cell cell : cells) {
      ByteArrayKey actualColumnValue = new ByteArrayKey((byte[]) cell.getColumnValue());
      ByteArrayKey expectedColValue = new ByteArrayKey(Bytes.toBytes(this.columnValue + i++));
      Assert.assertEquals(actualColumnValue, expectedColValue);
    }

    result = new RowImpl(rowId, timestamp, cells);
    returnRowValue = new ByteArrayKey(result.getRowValue());
    Assert.assertEquals(returnRowValue, inputRowValue);
  }
}
