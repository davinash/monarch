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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.types.BasicTypes;

/**
 * Implementation of MResult interface. This implementation handles multiple versions
 * <p>
 * Use this implementation for byte array having multiple versions
 * <p>
 * <p>
 * <p>
 * --------------------------------------------------------------------------------------------------------------------------------------------
 * | Number of Versions | Version Length | Timestamp |
 * LengthOfColumn1|lengthOfColumn2|lengthOfColumn3|ColumnValue1|ColumnValue2|ColumnValue3 |
 * --------------------------------------------------------------------------------------------------------------------------------------------
 *
 * @since 1.0.0.3
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class VersionedRow implements Row, Serializable, Comparable<Row> {

  private static final long serialVersionUID = 4941996811907249170L;

  /**
   * This will hold the data
   */
  private final byte[] multiVersionedRow;

  /**
   * Table descriptor to create result
   */
  private final TableDescriptor tableDescriptor;

  /**
   * Columns to be shown in result
   */
  private final List<byte[]> columnsInResult;

  /**
   * Map holding timestamp vs row value
   */
  private Map<Long, SingleVersionRow> rowMap;
  private final byte[] rowKey;
  private final Map<ByteArrayKey, MColumnDescriptor> columnNameToColumnTypeMap;

  private boolean isFTable = false;

  /**
   * Use this constructor when tabledescriptor and columns to get are known
   */
  public VersionedRow(final byte[] rowKey, byte[] versionedRow, MTableDescriptor tableDescriptor,
      List<byte[]> columnNameList) {
    this.rowKey = rowKey;
    this.multiVersionedRow = versionedRow;
    this.tableDescriptor = tableDescriptor;
    this.columnNameToColumnTypeMap = tableDescriptor.getColumnsByName();
    if (columnNameList == null || columnNameList.size() == 0) {
      this.columnsInResult = new LinkedList<>();
      // get all columns
      tableDescriptor.getAllColumnDescriptors().forEach(
          mColumnDescriptor -> this.columnsInResult.add(mColumnDescriptor.getColumnName()));

    } else {
      this.columnsInResult = columnNameList;
    }

    this.rowMap = new LinkedHashMap<Long, SingleVersionRow>();
    initialize(false);
  }

  /**
   * Use this constructor when tabledescriptor and columns to get are known
   */
  public VersionedRow(final byte[] rowKey, byte[] versionedRow, TableDescriptor tableDescriptor,
      List<byte[]> columnNameList) {
    this.rowKey = rowKey;
    this.multiVersionedRow = versionedRow;
    this.tableDescriptor = tableDescriptor;
    this.columnNameToColumnTypeMap = tableDescriptor.getColumnsByName();
    if (columnNameList == null || columnNameList.size() == 0) {
      this.columnsInResult = new LinkedList<>();
      // get all columns
      tableDescriptor.getColumnDescriptors().forEach(
          mColumnDescriptor -> this.columnsInResult.add(mColumnDescriptor.getColumnName()));
    } else {
      this.columnsInResult = columnNameList;
    }

    this.rowMap = new LinkedHashMap<Long, SingleVersionRow>();
    initialize(false);
  }

  /**
   * Use this constructor when tabledescriptor and columns to get are known and for ftable
   */
  public VersionedRow(final byte[] rowKey, byte[] versionedRow, FTableDescriptor tableDescriptor,
      List<byte[]> columnNameList) {
    this.rowKey = rowKey;
    // for ftable row byte array format is different
    /**
     * ------------------------------------------------------------------------------------------- |
     * Header (4bytes) | LengthOfColumn1 | Column 1 value | LengthOfColumn2 | Column 2 value |..
     * -------------------------------------------------------------------------------------------
     *
     */

    // add
    this.multiVersionedRow = versionedRow;
    this.tableDescriptor = tableDescriptor;
    this.columnNameToColumnTypeMap = tableDescriptor.getColumnsByName();
    if (columnNameList == null || columnNameList.size() == 0) {
      this.columnsInResult = new LinkedList<>();
      // get all columns
      tableDescriptor.getAllColumnDescriptors().forEach(
          mColumnDescriptor -> this.columnsInResult.add(mColumnDescriptor.getColumnName()));
    } else {
      this.columnsInResult = columnNameList;
    }

    this.rowMap = new LinkedHashMap<Long, SingleVersionRow>();
    initialize(true);
  }


  /**
   * Key only row
   *
   * @param rowKey
   */
  public VersionedRow(final byte[] rowKey) {
    this.rowKey = rowKey;
    this.multiVersionedRow = null;
    this.tableDescriptor = null;
    this.columnNameToColumnTypeMap = null;
    this.columnsInResult = null;
    this.rowMap = new LinkedHashMap<Long, SingleVersionRow>();
  }

  /**
   * Use this constructor when you have cells readily available
   */
  public VersionedRow(final byte[] rowKey, final Long rowTimeStamp, final List<Cell> newCells,
      TableDescriptor tableDescriptor) {
    this.rowKey = rowKey;
    this.multiVersionedRow = null;
    this.tableDescriptor = tableDescriptor;
    this.columnNameToColumnTypeMap = null;
    this.columnsInResult = null;
    this.rowMap = new LinkedHashMap<Long, SingleVersionRow>();

    SingleVersionRow versionedRow = null;
    if (this.tableDescriptor instanceof MTableDescriptor) {
      versionedRow = new SingleVersionRowImpl(rowKey, rowTimeStamp, newCells);
    } else if (this.tableDescriptor instanceof FTableDescriptor) {
      versionedRow = new SingleVersionRowImpl(rowKey, rowTimeStamp, newCells);
    }
    rowMap.put(versionedRow.getTimestamp(), versionedRow);
  }

  /**
   * Use this constructor to create dummy row with rowId and rowTimeStamp This will create
   * 'numberOfCells' with null as cell value
   */
  public VersionedRow(final byte[] rowId, final Long rowTimeStamp, final int numberOfCells) {
    this.rowKey = rowId;
    this.multiVersionedRow = null;
    this.tableDescriptor = null;
    this.columnNameToColumnTypeMap = null;
    this.columnsInResult = null;
    this.rowMap = new LinkedHashMap<Long, SingleVersionRow>();

    List<Cell> newCells = new ArrayList<>();
    for (int i = 0; i < numberOfCells; i++) {
      CellRef dummy = new CellRef(Bytes.toBytes("DUMMY"), BasicTypes.BINARY);
      dummy.init(null, -1, -1);
      newCells.add(dummy);
    }

    SingleVersionRow versionedRow =
        new SingleVersionRowImpl(rowKey, rowTimeStamp, newCells, false, false, null);
    rowMap.put(versionedRow.getTimestamp(), versionedRow);
  }

  // This method will extract
  // rows, no of cells, versions etc
  private void initialize(final boolean isFTable) {
    if (isFTable)
      extractFTableRows();
    else
      extractRows();
  }

  private void extractRows() {
    if (this.multiVersionedRow == null) {
      return;
    }
    // now value will have
    // |no of versions | v1 length | v1 timestamp |v1 col1 length|v1 col2 length|..|v1 coln
    // length|col1 data|...|coln data|v2 length | v2 timestamp |v2 col1 length|v2 col2 length|..|v2
    // coln length|col1 data|...|coln data|...

    int versions = Bytes.toInt(multiVersionedRow);
    int dataReadPosition = Bytes.SIZEOF_INT;
    for (int i = 0; i < versions; i++) {
      int currentVersionLength = Bytes.toInt(this.multiVersionedRow, dataReadPosition);
      dataReadPosition += Bytes.SIZEOF_INT;
      byte[] currentValue = new byte[currentVersionLength];
      System.arraycopy(this.multiVersionedRow, dataReadPosition, currentValue, 0,
          currentVersionLength);
      dataReadPosition += currentVersionLength;
      if (columnsInResult.size() == 0 && tableDescriptor != null) {
        // no column selection, get all columns
        tableDescriptor.getAllColumnDescriptors()
            .forEach(mCol -> columnsInResult.add(mCol.getColumnName()));
      }
      SingleVersionRow versionedRow;
      if (this.tableDescriptor instanceof MTableDescriptor) {
        versionedRow = new SingleVersionRowImpl(rowKey, currentValue, columnsInResult,
            columnNameToColumnTypeMap);
      } else {
        versionedRow = new SingleVersionRowImpl(rowKey, currentValue, columnsInResult,
            columnNameToColumnTypeMap);
      }
      rowMap.put(versionedRow.getTimestamp(), versionedRow);
    }
  }

  private void extractFTableRows() {
    // after parsing set new byte array in multiversioned row or make it null
    if (this.multiVersionedRow == null) {
      return;
    }
    // now value will have
    /**
     * ------------------------------------------------------------------------------------------- |
     * Header (4bytes) | LengthOfColumn1 | Column 1 value | LengthOfColumn2 | Column 2 value |..
     * -------------------------------------------------------------------------------------------
     *
     * convert to
     * ------------------------------------------------------------------------------------------------------
     * | Timestamp |
     * LengthOfColumn1|lengthOfColumn2|lengthOfColumn3|ColumnValue1|ColumnValue2|ColumnValue3 |
     * ------------------------------------------------------------------------------------------------------
     *
     */

    int versions = 1;
    byte[] dummyTimestamp = Bytes.toBytes(-1l);
    int dataReadPosition = 0;

    int currentVersionLength = this.multiVersionedRow.length + Bytes.SIZEOF_LONG;
    byte[] currentValue = new byte[currentVersionLength];

    int dest_offset = 0;
    Bytes.putBytes(currentValue, dest_offset, dummyTimestamp, 0, dummyTimestamp.length);
    dest_offset += dummyTimestamp.length;
    final int numberOfCols = this.columnsInResult.size();
    int valuePosition = dest_offset + (Bytes.SIZEOF_INT * numberOfCols);
    for (int i = 0; i < numberOfCols; i++) {
      final int colValueLength = Bytes.toInt(this.multiVersionedRow, dataReadPosition);
      Bytes.putBytes(currentValue, dest_offset, this.multiVersionedRow, dataReadPosition,
          Bytes.SIZEOF_INT);
      dest_offset += Bytes.SIZEOF_INT;
      dataReadPosition += Bytes.SIZEOF_INT;
      Bytes.putBytes(currentValue, valuePosition, this.multiVersionedRow, dataReadPosition,
          colValueLength);
      dataReadPosition += colValueLength;
      valuePosition += colValueLength;
    }
    SingleVersionRow versionedRow =
        new SingleVersionRowImpl(rowKey, currentValue, columnsInResult, columnNameToColumnTypeMap);
    rowMap.put(versionedRow.getTimestamp(), versionedRow);
  }

  @Override
  public List<Cell> getCells() {
    final SingleVersionRow latestRow = getLatestRow();
    if (latestRow != null)
      return latestRow.getCells();
    // TODO: With null should we return null or empty list
    // To match current impl sending empty list
    return new ArrayList<Cell>();
  }

  @Override
  public int size() {
    final List<Cell> cells = getCells();
    if (cells != null)
      return cells.size();
    return 0;
  }

  @Override
  public boolean isEmpty() {
    if (size() == 0)
      return true;
    return false;
  }

  @Override
  public Long getRowTimeStamp() {
    final SingleVersionRow latestRow = getLatestRow();
    if (latestRow != null) {
      return latestRow.getTimestamp();
    }
    return null;
  }

  @Override
  public byte[] getRowId() {
    return rowKey;
  }

  /**
   * TODO : Current implmentation compares key and latest row
   */
  @Override
  public int compareTo(final Row item) {
    byte[] rowId = item.getRowId();
    if (rowId != null && this.rowKey != null) {
      final int compare = Bytes.compareTo(this.rowKey, rowId);
      if (compare != 0) {
        return compare;
      }
    } else {
      // Not matching object
      return -1;
    }
    // now compare latest row
    if (item instanceof VersionedRow) {
      final SingleVersionRow currentRow = getLatestRow();
      final SingleVersionRow row = item.getLatestRow();
      if (currentRow != null && row != null) {
        return currentRow.compareTo(row);
      }
    }
    return -1;
  }

  @Override
  public Map<Long, SingleVersionRow> getAllVersions() {
    if (rowMap != null)
      return rowMap;
    return null;
  }

  @Override
  public SingleVersionRow getVersion(final Long timestamp) {
    if (rowMap != null)
      return rowMap.get(timestamp);
    return null;
  }

  @Override
  public SingleVersionRow getLatestRow() {
    if (rowMap != null && !rowMap.isEmpty()) {
      return rowMap.entrySet().iterator().next().getValue();
    }
    return null;
  }

  /**
   * Get the raw value respective to this row.
   *
   * @return the raw value
   */
  @Override
  public Object getRawValue() {
    return this.multiVersionedRow;
  }

  /**
   * Returns byte array format
   *
   * @return byte array format
   */
  public byte[] getRawByteArray() {
    int noOfVersions = rowMap.size();
    int finalLength = Bytes.SIZEOF_INT + Bytes.SIZEOF_INT * noOfVersions;
    Iterator<Entry<Long, SingleVersionRow>> iterator = rowMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<Long, SingleVersionRow> entry = iterator.next();
      finalLength += ((SingleVersionRowImpl) entry.getValue()).getRowValue().length;
    }
    byte[] retArray = new byte[finalLength];
    System.arraycopy(Bytes.toBytes(noOfVersions), 0, retArray, 0,
        Bytes.toBytes(noOfVersions).length);
    int dataReadPosition = Bytes.SIZEOF_INT;
    iterator = rowMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<Long, SingleVersionRow> entry = iterator.next();
      final byte[] value = ((SingleVersionRowImpl) entry.getValue()).getRowValue();
      System.arraycopy(Bytes.toBytes(value.length), 0, retArray, dataReadPosition,
          Bytes.toBytes(value.length).length);
      dataReadPosition += Bytes.SIZEOF_INT;
      System.arraycopy(value, 0, retArray, dataReadPosition, value.length);
      dataReadPosition += value.length;
    }
    return retArray;
  }


  /**
   * Strictly internal method. Set filtered rows.
   *
   * @param rowMap
   */
  @InterfaceAudience.Private
  public void setUpdatedVersions(Map<Long, SingleVersionRow> rowMap) {
    if (rowMap == null) {
      this.rowMap = Collections.emptyMap();
    } else {
      this.rowMap = rowMap;
    }
  }
}
