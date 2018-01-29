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

package io.ampool.monarch.table.ftable;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.internal.ByteArrayKey;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Used to perform append operations for a single row in FTable. To perform a append, instantiate a
 * Record object with the column values to add and for each column to be added, execute
 * {@link Record#add}.
 *
 * @since 0.2.0.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class Record implements Serializable {

  private static final long serialVersionUID = 276079151702058918L;
  private Map<ByteArrayKey, Object> columnName2ValueMap;

  private long serializedSize = 0;

  public Record() {
    columnName2ValueMap = new HashMap<>();
    columnName2ValueMap
        .put(new ByteArrayKey(Bytes.toBytes(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)), 0L);
  }

  /**
   * Add the column value for this row.
   * <p>
   * 
   * @param columnName Name of column.
   * @param columnValue value of the column.
   * @throws IllegalArgumentException
   */
  public void add(byte[] columnName, byte[] columnValue) {
    if (columnName == null || columnName.length == 0) {
      throw new IllegalArgumentException("Name of the column can not be null or empty");
    }
    columnName2ValueMap.put(new ByteArrayKey(columnName), columnValue);
  }

  /**
   * Add the column value for this row.
   * <p>
   *
   * @param columnName Name of column.
   * @param columnValue value of the column.
   * @throws IllegalArgumentException
   */
  public void add(byte[] columnName, Object columnValue) {
    if (columnName == null || columnName.length == 0) {
      throw new IllegalArgumentException("Name of the column can not be null or empty");
    }
    columnName2ValueMap.put(new ByteArrayKey(columnName), columnValue);
  }

  /**
   * Add the column value for this row.
   * <p>
   * 
   * @param columnName Name of column.
   * @param columnValue value of the column.
   * @throws IllegalArgumentException
   */
  public void add(String columnName, byte[] columnValue) {
    if (columnName == null || columnName.length() == 0) {
      throw new IllegalArgumentException("Name of the column can not be null or empty");
    }
    columnName2ValueMap.put(new ByteArrayKey(Bytes.toBytes(columnName)), columnValue);
  }

  /**
   * Add the column value for this row.
   * <p>
   * 
   * @param columnName Name of column.
   * @param columnValue value of the column.
   * @throws IllegalArgumentException
   */
  public void add(String columnName, Object columnValue) {
    if (columnName == null || columnName.length() == 0) {
      throw new IllegalArgumentException("Name of the column can not be null or empty");
    }
    columnName2ValueMap.put(new ByteArrayKey(Bytes.toBytes(columnName)), columnValue);
  }

  /**
   * Reset the column list in MPut.
   */
  public void clear() {
    this.columnName2ValueMap.clear();
  }

  public Map<ByteArrayKey, Object> getValueMap() {
    return columnName2ValueMap;
  }

  @Override
  public String toString() {
    return "Record{" + "columnName2ValueMap=" + columnName2ValueMap + ", serializedSize="
        + serializedSize + '}';
  }
  // // only for debugging purpose.
  //
  // /**
  // * <em>INTERNAL</em>
  // * @param length serialized size of the row
  // */
  // @InterfaceAudience.Private
  // public void setSerializedSize(long length) {
  // this.serializedSize = length;
  // }
  //
  // /**
  // * <em>INTERNAL</em>
  // */
  // @InterfaceAudience.Private
  // public long getSerializedSize() {
  // return this.serializedSize;
  // }

}
