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
package io.ampool.store;

import java.io.Serializable;
import java.util.Collection;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.DeSerializedRow;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.Row;

/**
 * Storerecord which contains ftable data record and unique sequence number
 */
public class StoreRecord extends DeSerializedRow implements Serializable {
  private int currentIndex = 0;

  public StoreRecord(int numOfCols) {
    super(numOfCols);
  }

  public StoreRecord(final Row row) {
    super(row.getCells().stream().map(Cell::getColumnValue).toArray());
  }

  public StoreRecord reset(final Object[] values) {
    this.values = values;
    return this;
  }

  public void addValue(Object value) {
    setValue(currentIndex++, value);
  }

  public Object[] getValues() {
    return values;
  }

  public long getTimeStamp() {
    // it is based on assumption that last object value is always insertion timestamp
    if (values == null || values.length == 0) {
      return -1;
    }
    return (long) values[values.length - 1];
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < values.length; i++) {
      sb.append(" ").append(values[i]);
    }
    return sb.toString();
  }

  /**
   * Strictly byte format should match | COL 1 Length | COL 1 Value | COL 2 Length | COL 2 Value |
   * COL 3 Length | COL 3 Value |
   *
   * @return array of bytes
   */
  public byte[] getBytes() {
    // create byte array format strictly in the form

    byte[][] valueBytes = new byte[values.length][];
    int totalLength = Bytes.SIZEOF_INT * values.length;
    for (int i = 0; i < values.length; i++) {
      // for each field cast to java type
      valueBytes[i] = (byte[]) values[i];
      totalLength += valueBytes[i].length;
    }
    byte[] bytes = new byte[totalLength];
    // copy each value to create array
    int writePos = 0;
    for (int i = 0; i < valueBytes.length; i++) {
      Bytes.putInt(bytes, writePos, valueBytes[i].length);
      writePos += Bytes.SIZEOF_INT;
      Bytes.putBytes(bytes, writePos, valueBytes[i], 0, valueBytes[i].length);
      writePos += valueBytes[i].length;
    }
    return bytes;
  }

  public byte[] serialize(final Collection<MColumnDescriptor> cds) {
    byte[][] valueBytes = new byte[values.length][];
    int totalLength = Bytes.SIZEOF_INT * values.length;
    int index = 0;
    for (final MColumnDescriptor cd : cds) {
      // for each field cast to java type
      valueBytes[index] = cd.getColumnType().serialize(values[index]);
      totalLength += valueBytes[index].length;
      index++;
    }
    byte[] bytes = new byte[totalLength];
    // copy each value to create array
    int writePos = 0;
    for (int i = 0; i < valueBytes.length; i++) {
      Bytes.putInt(bytes, writePos, valueBytes[i].length);
      writePos += Bytes.SIZEOF_INT;
      Bytes.putBytes(bytes, writePos, valueBytes[i], 0, valueBytes[i].length);
      writePos += valueBytes[i].length;
    }
    return bytes;
  }

  public void updateValue(int colIdx, Object value) {
    values[colIdx] = value;
  }
}
