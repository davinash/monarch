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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Row;

/**
 * Default implementation of MResult interface.
 *
 * @since 0.2.0.0
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class RowImpl implements Row, Serializable, Comparable<Row> {

  private static final long serialVersionUID = 7694737985621138578L;
  private byte[] rowId;
  private Long rowTimeStamp;
  /**
   * RowValue is the basic holder for complete MResult excluding rowkey
   */
  private byte[] rowValue;
  /**
   * List of cells in current Result. It gets constructed from rowValue field
   */
  private List<Cell> cells;

  /**
   * Case when there is no value against key i.e. Key only result
   *
   * @param rowId
   *
   */
  public RowImpl(byte[] rowId) {
    this(rowId, null);
  }

  public RowImpl(byte[] rowId, Long rowTimeStamp) {
    this.rowId = rowId;
    this.rowTimeStamp = rowTimeStamp;
    this.rowValue = null;
    this.cells = null;
  }

  /**
   * Used by MTable API
   */

  /**
   * Constructor to generate MResultImpl.
   * 
   * @param rowId is the key
   * @param rowValue is the byte[] it has following format | 8 bytes of Timestamp | byte[] of value
   *        |
   * @param tableDescriptor
   */
  public RowImpl(byte[] rowId, byte[] rowValue, MTableDescriptor tableDescriptor) {
    this(rowId, rowValue, tableDescriptor, null);
  }

  /**
   * Constructor to generate MResultImpl.
   * 
   * @param rowId is the key
   * @param rowValue is the byte[] it has following format | 8 bytes of Timestamp | byte[] of value
   *        |
   * @param columnNameList list of columns
   * @param tableDescriptor
   */
  public RowImpl(byte[] rowId, byte[] rowValue, MTableDescriptor tableDescriptor,
      List<byte[]> columnNameList) {
    this.rowId = rowId;
    this.rowValue = rowValue;
    if (rowValue == null) {
      this.rowTimeStamp = null;
      this.cells = null;
      return;
    }
    this.rowTimeStamp = Bytes.toLong(rowValue);
    this.cells = MResultParser.getCells(tableDescriptor, rowValue, columnNameList, false);
  }

  public RowImpl(byte[] rowId, byte[] rowValue, MTableDescriptor tableDescriptor,
      List<byte[]> columnNameList, boolean withVersions) {
    this.rowId = rowId;
    this.rowValue = rowValue;
    if (rowValue == null) {
      this.rowTimeStamp = null;
      this.cells = null;
      return;
    }
    this.rowTimeStamp = Bytes.toLong(rowValue);
    this.cells = MResultParser.getCells(tableDescriptor, rowValue, columnNameList, true);
  }


  /**
   * Constructor for initializing MResult from list of cells
   *
   * Make sure you pass same number of cells while transforming
   * 
   * @param rowId
   * @param rowTimeStamp
   * @param cells
   */
  public RowImpl(byte[] rowId, long rowTimeStamp, List<Cell> cells) {
    this.rowId = rowId;
    this.rowTimeStamp = rowTimeStamp;
    this.rowValue = null;
    this.cells = cells;
  }

  private byte[] reFormatCells(long rowTimeStamp, List<Cell> cells) {
    this.cells = new ArrayList<>(cells.size());

    int length = Bytes.SIZEOF_LONG; // 8 bytes for timestamp
    for (Cell cell : cells) {
      length += Bytes.SIZEOF_INT; // 4 bytes for storing length
      if (cell.getValueLength() > 0) {
        length += cell.getValueLength(); // bytes for storing actual column value
      }
    }

    byte[] tempBuffer = new byte[length];

    // Writing timestamp
    Bytes.putLong(tempBuffer, 0, rowTimeStamp);

    int curDataPos = Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT * cells.size();
    int curLenPos = Bytes.SIZEOF_LONG;
    // Constructing rowValue from cells
    for (Cell cell : cells) {
      // Writing cell length
      // Note: This will include cells with lenght -1 indicating
      // client was expecting those cells but they have being deleted by some server operation like
      // filters.
      int valueLength = cell.getValueLength();
      Bytes.putInt(tempBuffer, curLenPos, valueLength);
      curLenPos += Bytes.SIZEOF_INT;

      // Writing Data
      if (valueLength > 0) {
        Bytes.putBytes(tempBuffer, curDataPos, ((CellRef) cell).getValueArrayCopy(), 0,
            valueLength);
        ((CellRef) cell).init(tempBuffer, curDataPos, valueLength);
        curDataPos += valueLength;
        this.cells.add(cell);
      }
    }
    return tempBuffer;
  }

  /**
   * Initialize the result.. Added mainly to reuse existing MResult objects rather than re-creating
   * the objects each time.
   *
   * @param rowId the row-id
   * @param rowValue row value as byte array
   */
  public void init(byte[] rowId, byte[] rowValue) {
    this.rowId = rowId;
    this.rowValue = rowValue;
    if (rowValue != null) {
      this.rowTimeStamp = Bytes.toLong(rowValue);
    }
  }

  @Override
  public List<Cell> getCells() {
    return this.cells == null ? new ArrayList<>() : this.cells;
  }

  @Override
  public int size() {
    return this.cells == null ? 0 : this.cells.size();
  }

  @Override
  public boolean isEmpty() {
    return this.cells == null || this.cells.size() == 0;
  }

  @Override
  public Long getRowTimeStamp() {
    return rowTimeStamp;
  }

  @Override
  public byte[] getRowId() {
    return this.rowId;
  }

  public byte[] getRowValue() {
    if (this.rowValue == null && this.cells != null) {
      this.rowValue = reFormatCells(rowTimeStamp, cells);
    }
    return this.rowValue;
  }

  @Override
  public int compareTo(Row item) {
    if (this == item) {
      return 0;
    }

    byte[] rowId = item.getRowId();
    if (rowId == null) {
      if (this.rowId == null) {
        return 0;
      } else {
        return -1;
      }
    }
    return Bytes.compareTo(this.rowId, rowId);
  }

  @Override
  public Map<Long, SingleVersionRow> getAllVersions() {
    final TreeMap<Long, SingleVersionRow> versionMap = new TreeMap<Long, SingleVersionRow>();
    versionMap.put(getRowTimeStamp(), getLatestRow());
    return versionMap;
  }

  @Override
  public SingleVersionRow getVersion(final Long timestamp) {
    if (timestamp == this.rowTimeStamp) {
      return getLatestRow();
    }
    return null;
  }

  @Override
  public SingleVersionRow getLatestRow() {
    if (this.rowValue == null || this.getCells() == null || this.rowTimeStamp == null)
      return null;
    return new SingleVersionRowImpl(this.rowId, this.rowTimeStamp, this.cells);
  }

  /**
   * Get the raw value respective to this row.
   *
   * @return the raw value
   */
  @Override
  public Object getRawValue() {
    return this.rowValue;
  }
}
