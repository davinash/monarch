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
import java.util.ArrayList;
import java.util.List;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

/**
 * Used to perform Delete operations on a single row.
 * <p>
 * To delete an entire row, instantiate a Delete object with the row to delete.
 * <p>
 * <code> MDelete delete = new MDelete(Bytes.toBytes("rowKey1")); </code>
 * </p>
 * <p>
 * <code>table.delete(delete);</code>
 * </p>
 *
 * To delete specific columns of a row, execute {@link #addColumn(byte[]) deleteColumns} for each
 * column to delete.
 * <p>
 * <code> MDelete delete = new MDelete(Bytes.toBytes("rowKey2"));</code>
 * </p>
 * <p>
 * <code>delete.addColumn(Bytes.toBytes("COLUMN-1")); </code>
 * </p>
 * <p>
 * <code>delete.addColumn(Bytes.toBytes("COLUMN-3")); </code>
 * </p>
 * <p>
 * <code>table.delete(delete);</code>
 * </p>
 *
 * delete specific versions of specific columns is not supported
 * <p>
 * Specifying timestamps, will delete all versions with a timestamp less than or equal to that
 * passed. If no timestamp is specified, an entry is added with a timestamp of 'now' where 'now' is
 * the servers's System.currentTimeMillis(). Specifying a timestamp to the deleteColumn method will
 * delete versions only with a timestamp equal to that specified.
 * 
 * @since 0.2.0.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class Delete implements Serializable {

  private static final long serialVersionUID = 7023051631875038906L;

  private final byte[] rowKey;
  private List<byte[]> columnNameList;

  private long timeStamp;

  /**
   * Create a Delete operation for the specified row.
   * <p>
   * If no further operations are done, this will delete everything associated with the specified
   * row
   * 
   * @param rowKey row key
   */
  public Delete(byte[] rowKey) {
    this.rowKey = rowKey;
    this.columnNameList = new ArrayList<>();
    this.timeStamp = 0;
  }

  /**
   * Helper constructor for string key
   * 
   * @param rowKey row key
   */
  public Delete(String rowKey) {
    this(Bytes.toBytes(rowKey));
  }

  /**
   * Method for retrieving the delete's row
   * 
   * @return rowKey
   */
  public byte[] getRowKey() {
    return rowKey;
  }

  /**
   * Method for retrieving the list of column names associated with this row
   * 
   * @return columnNameList
   */
  public List<byte[]> getColumnNameList() {
    return columnNameList;
  }

  /**
   * Delete the latest version of the specified column.
   * 
   * @param columnName column name to delete
   * @throws IllegalArgumentException if the columnName is null or empty.
   */
  public void addColumn(byte[] columnName) {
    if (columnName == null || columnName.length == 0) {
      throw new IllegalArgumentException("Name of the column can not be null or empty");
    }
    this.columnNameList.add(columnName);
  }

  /**
   * Delete the latest version of the specified column.
   * 
   * @param columnName column name to delete
   * @throws IllegalArgumentException if the columnName is null or empty.
   */
  public void addColumn(String columnName) {
    if (columnName == null || columnName.length() == 0) {
      throw new IllegalArgumentException("Name of the column can not be null or empty");
    }
    this.addColumn(Bytes.toBytes(columnName));
  }

  /**
   * Set the timestamp for which the records to be deleted. Specifying timestamps, will delete all
   * versions with a timestamp less than or equal to
   * 
   * @param timestamp
   * @throws IllegalArgumentException if the timestamp is negative
   */
  public void setTimestamp(long timestamp) {
    if (timeStamp < 0) {
      throw new IllegalArgumentException("Timestamp cannot be negative");
    }
    this.timeStamp = timestamp;
  }

  /**
   * Method for retrieving the timestamp
   * 
   * @return timestamp
   */
  public long getTimestamp() {
    return timeStamp;
  }

}
