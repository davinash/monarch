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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.types.BasicTypes;

/**
 * <p>
 * <p>
 * ------------------------------------------------------------------------------------------------------
 * | Timestamp |
 * LengthOfColumn1|lengthOfColumn2|lengthOfColumn3|ColumnValue1|ColumnValue2|ColumnValue3 |
 * ------------------------------------------------------------------------------------------------------
 *
 * Single version row implementation
 */
public class SingleVersionRowImpl implements SingleVersionRow {

  private static final long serialVersionUID = -4918021069828398085L;
  private final byte[] rowValue;
  private final List<byte[]> columns;

  private final Map<ByteArrayKey, MColumnDescriptor> nameByColumnDescriptor;
  private List<Cell> cells;
  private final byte[] rowKey;
  private final Long timestamp;

  public SingleVersionRowImpl(final byte[] rowKey, final byte[] rowValue,
      final List<byte[]> columns,
      final Map<ByteArrayKey, MColumnDescriptor> nameByColumnDescriptor) {
    this.rowKey = rowKey;
    this.rowValue = rowValue;
    this.columns = columns;
    this.nameByColumnDescriptor = nameByColumnDescriptor;
    this.cells = new LinkedList<Cell>();
    extractCells();
    this.timestamp = Bytes.toLong(this.rowValue);
    processCells(this.cells, true);
  }

  public SingleVersionRowImpl(final byte[] rowKey, final Long timestamp, List<Cell> cells) {
    this.rowKey = rowKey;
    this.rowValue = null;
    this.columns = null;
    this.nameByColumnDescriptor = null;
    this.timestamp = timestamp;
    this.cells = cells;
    processCells(cells, false);
  }

  public SingleVersionRowImpl(final byte[] rowKey, final long timestamp, List<Cell> cells,
      boolean removeDummyCells, final boolean hasPrimaryColumn, final byte[] primaryColumnName) {
    this.rowKey = rowKey;
    this.rowValue = null;
    this.columns = null;
    this.nameByColumnDescriptor = null;
    this.timestamp = timestamp;
    this.cells = cells;
    processCells(cells, removeDummyCells);
  }

  private void processCells(final List<Cell> cells, final boolean removeDummyCells) {
    if (removeDummyCells) {
      final List<Cell> newCells = new LinkedList<Cell>();
      for (Cell cell : cells) {
        // remove -1 length special cells
        if (cell.getValueLength() >= 0) {
          newCells.add(cell);
        } else if (Bytes.equals(Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME), cell.getColumnName())) {
          newCells.add(cell);
        }
      }
      this.cells = newCells;
    }
  }

  public SingleVersionRowImpl() {
    this.rowKey = null;
    this.rowValue = null;
    this.columns = new ArrayList<>();
    this.nameByColumnDescriptor = new HashMap<>();
    this.cells = new LinkedList<Cell>();
    this.timestamp = 0L;
  }

  private void extractCells() {
    int dataReadPosition = Bytes.SIZEOF_LONG;
    int[] lengthArr = new int[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      final int len = Bytes.toInt(this.rowValue, dataReadPosition);
      lengthArr[i] = len;
      dataReadPosition += Bytes.SIZEOF_INT;
    }

    for (int i = 0; i < columns.size(); i++) {
      Cell cell;
      if (lengthArr[i] < 0) {
        cell = new CellRef(Bytes.toBytes("DUMMY"), BasicTypes.BINARY);
        ((CellRef) cell).init(null, -1, -1);
        // cell.
      } else if (Bytes.equals(columns.get(i), Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME))) {
        cell = new CellRef(Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME), BasicTypes.BINARY);
        ((CellRef) cell).init(this.rowKey, 0, this.rowKey.length);
      } else {
        byte[] cellValue = new byte[lengthArr[i]];
        System.arraycopy(this.rowValue, dataReadPosition, cellValue, 0, lengthArr[i]);
        dataReadPosition += lengthArr[i];
        final byte[] col = columns.get(i);
        cell = new CellRef(columns.get(i),
            nameByColumnDescriptor.get(new ByteArrayKey(columns.get(i))).getColumnType(), cellValue,
            0, cellValue.length);
        // TODO : If we omit empty cells many test fail so find way for filters
        // if(cellValue.length > 0)
      }
      this.cells.add(cell);
    }
  }

  public Object constructRawByteValue(TableDescriptor tableDescriptor) {

    // make dummy put operation call so as to get the byte array
    Put dummyPut = new Put(new byte[] {1});
    dummyPut.setTimeStamp(this.timestamp);
    for (int i = 0; i < this.cells.size(); i++) {
      Cell cell = this.cells.get(i);
      if (cell.getColumnValue() != null) {
        dummyPut.addColumn(Bytes.toString(cell.getColumnName()), cell.getColumnValue());
      }
    }

    MTableDescriptor mTableDescriptor =
        new MTableDescriptor(((MTableDescriptor) tableDescriptor).getTableType());
    mTableDescriptor.setSchema(tableDescriptor.getSchema());

    StorageFormatter mtableFormatter = MTableUtils.getStorageFormatter(tableDescriptor);

    MValue mValue = MValue.fromPut(mTableDescriptor, dummyPut, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(mTableDescriptor, mValue, mValue.getOpInfo(), null);

    return finalValue;
  }

  @Deprecated
  public byte[] getRowValue() {
    if (this.rowValue != null) {
      return this.rowValue;
    } else if (rowValue == null && this.cells.size() > 0) {
      // directly cells are added
      int finalLength = Bytes.SIZEOF_LONG;
      int valueLength = 0;
      for (int i = 0; i < this.cells.size(); i++) {
        final Cell cell = this.cells.get(i);
        if (cell.getColumnValue() == null || cell.getValueLength() <= 0) {
          finalLength += Bytes.SIZEOF_INT;
        } else {
          valueLength += cell.getValueLength();
          finalLength += Bytes.SIZEOF_INT + cell.getValueLength();
        }
      }

      byte[] tempArray = new byte[finalLength];
      byte[] valueArray = new byte[valueLength];
      byte[] ZERO = Bytes.toBytes(0);
      byte[] SPECIAL_LENGTH = Bytes.toBytes(-1);
      System.arraycopy(Bytes.toBytes(this.timestamp), 0, tempArray, 0,
          Bytes.toBytes(this.timestamp).length);
      int dataReadPosition = Bytes.SIZEOF_LONG;
      int valuePos = 0;
      for (int i = 0; i < this.cells.size(); i++) {
        final Cell cell = this.cells.get(i);
        if (cell.getColumnValue() == null || cell.getValueLength() <= 0) {
          if (cell.getValueLength() < 0) {
            System.arraycopy(SPECIAL_LENGTH, 0, tempArray, dataReadPosition, SPECIAL_LENGTH.length);
            dataReadPosition += SPECIAL_LENGTH.length;
          } else {
            System.arraycopy(ZERO, 0, tempArray, dataReadPosition, ZERO.length);
            dataReadPosition += ZERO.length;
          }
        } else {
          System.arraycopy(Bytes.toBytes(cell.getValueArray().length), 0, tempArray,
              dataReadPosition, Bytes.toBytes(cell.getValueArray().length).length);
          dataReadPosition += Bytes.toBytes(cell.getValueArray().length).length;
          System.arraycopy(cell.getValueArray(), 0, valueArray, valuePos,
              cell.getValueArray().length);
          valuePos += cell.getValueArray().length;
        }
      }
      System.arraycopy(valueArray, 0, tempArray, dataReadPosition, valueArray.length);
      return tempArray;
    } else {
      int finalLength = Bytes.SIZEOF_LONG;

      byte[] tempArray = new byte[finalLength];
      System.arraycopy(Bytes.toBytes(this.timestamp), 0, tempArray, 0,
          Bytes.toBytes(this.timestamp).length);
      return tempArray;
    }
  }

  @Override
  public List<Cell> getCells() {
    return cells;
  }

  @Override
  public int size() {
    return cells.size();
  }

  @Override
  public boolean isEmpty() {
    return cells.isEmpty();
  }

  @Override
  public byte[] getRowId() {
    return rowKey;
  }

  @Override
  public Long getTimestamp() {
    return this.timestamp;
  }

  @Override
  public int compareTo(final SingleVersionRow item) {
    // TODO to implement
    return 0;
  }
}
