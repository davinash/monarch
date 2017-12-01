/*
 * ========================================================================= Copyright (c) 2015
 * Ampool, Inc. All Rights Reserved. This product is protected by U.S. and international copyright
 * and intellectual property laws.
 * =========================================================================
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
