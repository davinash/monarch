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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.MOperation;
import io.ampool.monarch.table.internal.MValue;

/**
 * Used to perform Put operations for a single row. To perform a Put, instantiate a MPut object with
 * the row to insert and for each column to be inserted, execute {@link #addColumn addColumn}. Use
 * {@link #setTimeStamp} to set the timestamp specific to this Put.
 *
 * @since 0.2.0.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class Put implements Serializable {

  private static final long serialVersionUID = 6842957238097937199L;
  private byte[] rowKey;
  private Map<ByteArrayKey, Object> columnName2ValueMap;

  private long timeStamp;
  private long serializedSize = 0;


  public Put(String rowKey) {
    if (rowKey == null || rowKey.isEmpty()) {
      throw new IllegalStateException("Row Key for Column cannot be null or empty");
    }
    columnName2ValueMap = new HashMap<>();
    timeStamp = 0;
    this.rowKey = Bytes.toBytes(rowKey);
  }

  /**
   * <em>INTERNAL</em>
   */
  @InterfaceAudience.Private
  public Map<ByteArrayKey, Object> getColumnValueMap() {
    return columnName2ValueMap;
  }

  /**
   * Create a Put operation for the specified row.
   * 
   * @param rowKey row key.
   * @throws IllegalStateException
   */
  public Put(byte[] rowKey) {
    if (rowKey == null) {
      throw new IllegalStateException("Row Key cannot be null");
    }
    if (rowKey.length == 0) {
      throw new IllegalStateException("Row Key cannot be 0 size");
    }
    this.rowKey = rowKey;
    columnName2ValueMap = new HashMap<>();
    timeStamp = 0;
  }

  /**
   * Method for retrieving the rowKey
   * 
   * @return rowKey
   */
  public byte[] getRowKey() {
    return rowKey;
  }

  /**
   * Add the column from the specific row.
   * <p>
   * 
   * @param columnName Name of column.
   * @param columnValue value of the column.
   * @throws IllegalArgumentException
   */
  public void addColumn(byte[] columnName, byte[] columnValue) {
    if (columnName == null || columnName.length == 0) {
      throw new IllegalArgumentException("Name of the column can not be null or empty");
    }
    columnName2ValueMap.put(new ByteArrayKey(columnName), columnValue);
  }

  /**
   * Add the column from the specific row.
   * <p>
   * 
   * @param columnName Name of column.
   * @param columnValue value of the column.
   * @throws IllegalArgumentException
   */
  public void addColumn(String columnName, byte[] columnValue) {
    if (columnName == null || columnName.length() == 0) {
      throw new IllegalArgumentException("Name of the column can not be null or empty");
    }
    columnName2ValueMap.put(new ByteArrayKey(Bytes.toBytes(columnName)), columnValue);
  }

  /**
   * Add the column from the specific row.
   * <p>
   * 
   * @param columnName Name of column.
   * @param columnValue value of the column.
   * @throws IllegalArgumentException
   */
  public void addColumn(String columnName, Object columnValue) {
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

  /**
   * Set row key to a new value in MPut.
   * 
   * @param rowKey new row key.
   * @throws IllegalStateException
   */
  public void setRowKey(byte[] rowKey) {
    if (rowKey == null || rowKey.length == 0) {
      throw new IllegalStateException("Row Key for Column cannot be null");
    }
    this.rowKey = rowKey;
  }

  /**
   * Set row key to a new value in MPut.
   * 
   * @param rowKey new row key.
   * @throws IllegalStateException
   */
  public void setRowKey(String rowKey) {
    if (rowKey == null || rowKey.length() == 0) {
      throw new IllegalStateException("Row Key for Column cannot be null");
    }
    this.rowKey = Bytes.toBytes(rowKey);
  }

  /**
   * Returns the timestamp in MPut
   * 
   * @return timestamp
   */
  public long getTimeStamp() {
    return timeStamp;
  }

  /**
   * Set the timestamp for MPut
   * 
   * @param timeStamp timestamp
   * @throws IllegalArgumentException
   */
  public void setTimeStamp(long timeStamp) {
    if (timeStamp < 0) {
      throw new IllegalArgumentException("Timestamp cannot be negative");
    }
    this.timeStamp = timeStamp;
  }

  // only for debugging purpose.

  /**
   * <em>INTERNAL</em>
   * 
   * @param length serialized size of the row
   */
  @InterfaceAudience.Private
  public void setSerializedSize(long length) {
    this.serializedSize = length;
  }

  /**
   * <em>INTERNAL</em>
   */
  @InterfaceAudience.Private
  public long getSerializedSize() {
    return this.serializedSize;
  }


  /**
   * Helper method mainly used from Cache Loaders This API is for use with Cache Loaders.
   * 
   * @param tableDescriptor Table Descriptor
   * @return Serialized reprsentation of the Put Object.
   */
  public Object serialize(MTableDescriptor tableDescriptor) {
    return MValue.fromPut(tableDescriptor, this, MOperation.PUT);
  }
}
