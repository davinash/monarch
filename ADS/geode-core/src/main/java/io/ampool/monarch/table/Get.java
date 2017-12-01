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
 * Used to perform Get operations on a single row.
 * <p>
 * To get everything for a row, instantiate a Get object with the row to get. To further narrow the
 * scope of what to Get, use the methods below.
 * <p>
 * To get specific columns, execute {@link #addColumn(byte[]) addColumn} for each column to
 * retrieve.
 * <p>
 * To only retrieve row with a specific timestamp, execute {@link #setTimeStamp(long) setTimestamp}.
 * 
 * @since 0.2.0.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class Get implements Serializable {

  private static final long serialVersionUID = 9124525642223435017L;

  /**
   * Default value for Versions to get
   */
  private static final int MAX_VERSIONS = 1;

  private byte[] rowKey;
  private long timeStamp;
  private List<byte[]> columnNameList;
  private int maxVersions = Integer.MIN_VALUE;
  private boolean isOldestValueFirst = false;

  /**
   * Create a Get operation for the specified row.
   * <p>
   * If no further operations are done, this will get the latest version of all columns in the
   * specified row.
   * 
   * @param rowKey row key
   * @throws IllegalStateException if rowKey is null or empty
   *
   */
  public Get(byte[] rowKey) {
    if (rowKey == null || rowKey.length == 0) {
      throw new IllegalStateException("Row Key null or empty");
    }
    this.rowKey = rowKey;
    this.columnNameList = new ArrayList<>();
    this.timeStamp = 0;
  }

  /**
   * Create a Get operation for the specified string rowkey.
   * <p>
   * If no further operations are done, this will get the latest version of all columns in the
   * specified row.
   *
   * @param rowKey row key
   * @throws IllegalStateException if rowKey is null or empty
   */
  public Get(String rowKey) {
    if (rowKey == null || rowKey.length() == 0) {
      throw new IllegalStateException("Row Key null or empty");
    }
    this.rowKey = Bytes.toBytes(rowKey);
    this.columnNameList = new ArrayList<>();
    this.timeStamp = 0;
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
   * Set row key to a new value in MGet.
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
   * Set row key to a new value in MGet.
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
   * Method for retrieving the list of column names associated with this row
   * 
   * @return columnNameList
   */
  public List<byte[]> getColumnNameList() {
    return columnNameList;
  }

  /**
   * Get the column from the specific row.
   * <p>
   * 
   * @param columnName Name of column.
   */
  public void addColumn(byte[] columnName) {
    if (columnName == null || columnName.length == 0) {
      throw new IllegalArgumentException("Column Name cannot be null or empty");
    }
    this.columnNameList.add(columnName);
  }

  /**
   * Get the column from the specific row.
   * <p>
   * 
   * @param columnName String Name of column.
   */
  public void addColumn(String columnName) {
    if (columnName == null || columnName.length() == 0) {
      throw new IllegalArgumentException("Column Name cannot be null or empty");
    }
    this.columnNameList.add(Bytes.toBytes(columnName));
  }

  /**
   * Method for retrieving the timestamp
   * 
   * @return timestamp
   */
  public long getTimeStamp() {
    return timeStamp;
  }

  /**
   * Method for setting timestamp.
   *
   * @param timeStamp timestamp
   * @throws IllegalArgumentException if timestamp is negative
   */
  public void setTimeStamp(long timeStamp) {
    if (timeStamp < 0) {
      throw new IllegalArgumentException("Timestamp cannot be negative");
    }
    this.timeStamp = timeStamp;
  }

  /**
   * Get up to the specified number of versions. Value as ZERO will get all versions.
   *
   * Default order is newer to older.
   *
   * @param maxVersions - Maximum number of versions to fetch
   *
   */
  public void setMaxVersionsToFetch(int maxVersions) throws IllegalArgumentException {
    setMaxVersionsToFetch(maxVersions, false);
  }

  /**
   * Get up to the specified number of versions. This value as ZERO will get all versions.
   *
   * Default order is newer to older.
   *
   * @param maxVersions - Maximum number of versions to fetch
   * @param isOldestValueFirst - True means older to newer version. False means newer to older
   *        version.
   *
   */
  public void setMaxVersionsToFetch(int maxVersions, boolean isOldestValueFirst)
      throws IllegalArgumentException {
    if (maxVersions < 0) {
      throw new IllegalArgumentException("Setting negative max version is allowed");
    }
    this.isOldestValueFirst = isOldestValueFirst;
    this.maxVersions = maxVersions;
  }

  /**
   * Returns max version per result.
   *
   * Value ZERO means getting all versions.
   *
   * @return max version per result.
   */
  public int getMaxVersionsToFetch() {
    if (this.maxVersions == Integer.MIN_VALUE)
      return MAX_VERSIONS;
    else
      return this.maxVersions;
  }

  /**
   * Describes order of results in get
   *
   * True : Older version to Newer version False : Newer version to Older version
   *
   * @return max version per result.
   */
  public boolean isOlderValueFirst() {
    return this.isOldestValueFirst;
  }
}
