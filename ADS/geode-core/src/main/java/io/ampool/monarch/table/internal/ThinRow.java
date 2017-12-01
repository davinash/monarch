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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.TableDescriptor;

public class ThinRow implements Row, Serializable {
  private static final long serialVersionUID = -5404691196089003710L;
  private byte[] key;
  private byte[] value;
  private boolean isInitialized = false;
  private final ThinRowShared sharedResult;
  private List<Cell> cells = null;
  private RowFormat rowFormat;

  public interface RowOperation {
    int init(List<Cell> cells, final byte[] key, final byte[] buf, ThinRowShared trs);

    long getRowTimestamp(final byte[] buf);

    int getOffset();
  }

  /**
   * The possible row formats..
   */
  public enum RowFormat implements RowOperation {
    M_FULL_ROW() {
      private int offset = Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG;

      @Override
      public int init(List<Cell> cells, final byte[] key, byte[] buf, ThinRowShared trs) {
        Encoding.getEncoding(buf[RowHeader.OFFSET_ENCODING]).initFullRow(key, buf, trs);
        return 0;
      }

      @Override
      public long getRowTimestamp(byte[] buf) {
        return Bytes.toLong(buf, Bytes.SIZEOF_INT, Bytes.SIZEOF_LONG);
      }

      @Override
      public int getOffset() {
        return offset;
      }
    },
    M_PARTIAL_ROW() {
      private int offset = Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG;

      @Override
      public int init(List<Cell> cells, byte[] key, byte[] buf, ThinRowShared trs) {
        Encoding.getEncoding(buf[RowHeader.OFFSET_ENCODING]).initPartialRow(key, buf, trs);
        return 0;
      }

      @Override
      public long getRowTimestamp(byte[] buf) {
        return Bytes.toLong(buf, Bytes.SIZEOF_INT, Bytes.SIZEOF_LONG);
      }

      @Override
      public int getOffset() {
        return offset;
      }
    },
    ////////////////////////////////////////////////////////////////////////////
    F_FULL_ROW() {
      @Override
      public int init(List<Cell> cells, byte[] key, byte[] buf, ThinRowShared trs) {
        return trs.getDescriptor().getEncoding().initFullRow(key, buf, trs);
      }

      @Override
      public long getRowTimestamp(byte[] buf) {
        return -1;
      }

      @Override
      public int getOffset() {
        return 0;
      }
    },
    F_PARTIAL_ROW() {
      @Override
      public int init(List<Cell> cells, byte[] key, byte[] buf, ThinRowShared trs) {
        return trs.getDescriptor().getEncoding().initPartialRow(key, buf, trs);
      }

      @Override
      public long getRowTimestamp(byte[] buf) {
        return -1;
      }

      @Override
      public int getOffset() {
        return 0;
      }
    },
    MTABLE_WITHOUT_NULL_CELLS() {
      private int offset = Bytes.SIZEOF_INT;

      /**
       * For MTable: Initialize the cells from the specified byte-array. This assumes that the
       * byte-array structure is like: header,c1-len,c2-len,c3-len,....,c1-val,c2-val,c3-val,....
       *
       * @param cells the cells to be initialized with respective values (bytes)
       * @param buf the complete row value
       * @param trs
       * @return the number of cells where the data is populated
       */
      @Override
      public int init(List<Cell> cells, final byte[] key, byte[] buf, ThinRowShared trs) {

        StorageFormatter storageFormatter = MTableUtils.getStorageFormatter(trs.getDescriptor());

        Map<Integer, Pair<Integer, Integer>> offsetsForEachColumn =
            storageFormatter.getOffsetsForEachColumn(trs.getDescriptor(), buf, trs.getColumnIds());
        Integer columnIndex;
        int index = 0;
        for (final Cell cell : cells) {
          MColumnDescriptor columnDescriptor = trs.getDescriptor().getSchema()
              .getColumnDescriptorByName(Bytes.toString(cell.getColumnName()));
          columnIndex = columnDescriptor.getIndex();
          if (offsetsForEachColumn.containsKey(columnIndex)) {
            // value exists so put corresponding offsets
            Pair<Integer, Integer> offsets = offsetsForEachColumn.get(columnIndex);
            ((CellRef) cell).init(buf, offsets.getFirst(),
                offsets.getSecond() - offsets.getFirst());
            index++;
          }
        }
        // // remove null cells
        List<Cell> newCells = new ArrayList<>();
        for (Cell cell : cells) {
          if (cell.getColumnValue() != null) {
            newCells.add(cell);
          }
        }
        cells.clear();
        cells.addAll(newCells);
        return index;
      }

      @Override
      public long getRowTimestamp(final byte[] buf) {
        return Bytes.toLong(buf, offset);
      }

      @Override
      public int getOffset() {
        return this.offset;
      }
    },
  }

  public ThinRow(final byte[] key, final Object value, final ThinRowShared sharedResult,
      final RowFormat rowFormat, boolean recreateCells) {
    this.key = key;
    this.value = (byte[]) value;
    this.sharedResult = sharedResult;
    this.cells = sharedResult.getCells();
    if (recreateCells) {
      this.cells = new ArrayList<>();
      sharedResult.getCells().forEach(cell -> {
        this.cells.add(new CellRef(cell.getColumnName(), cell.getColumnType()));
      });
    }
    this.rowFormat = rowFormat;
  }

  public ThinRow(final byte[] key, final Object value, final ThinRowShared sharedResult,
      final RowFormat rowFormat) {
    this(key, value, sharedResult, rowFormat, false);
  }

  /**
   * Get the encoding format of this row.
   *
   * @return the row format
   */
  public RowFormat getRowFormat() {
    return this.rowFormat;
  }

  /**
   * Reset the key and value of the row so that it gets reused.
   *
   * @param key the key
   * @param value the value
   */
  public void reset(final byte[] key, final byte[] value) {
    this.key = key;
    this.value = value;
    this.isInitialized = false;
  }

  /**
   * Create a list of the Cell's in this result.
   *
   * @return List of Cells; Cell value can be null if its value is deleted.
   */
  @Override
  public List<Cell> getCells() {
    return isInitialized ? this.cells : initCells(key);
  }

  private List<Cell> initCells(final byte[] key) {
    isInitialized = true;

    int count = rowFormat.init(this.cells, key, this.value, sharedResult);
    return this.cells;
  }
  // public byte[] toBytes() {
  // int lenLen = (Bytes.SIZEOF_INT * this.cells.size());
  // int dataLen = 0;
  // for (Cell cell : this.cells) {
  // if (cell.getValueLength() > 0) {
  // dataLen += cell.getValueLength();
  // }
  // }
  // int hdrLen = Bytes.SIZEOF_INT + Bytes.SIZEOF_INT + OFFSET;
  // int dataPos = hdrLen + lenLen;
  // int totalLen = dataPos + dataLen;
  // byte[] newValue = new byte[totalLen];
  // System.arraycopy(Bytes.toBytes(1), 0, newValue, 0, Bytes.SIZEOF_INT);
  // System.arraycopy(Bytes.toBytes(totalLen), 0, newValue, Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
  // System.arraycopy(Bytes.toBytes(this.getRowTimeStamp()), 0, newValue, OFFSET, Bytes
  // .SIZEOF_LONG);
  // int wLenPos = hdrLen;
  // for (Cell cell : this.cells) {
  // byte[] lb = Bytes.toBytes(cell.getValueLength());
  // System.arraycopy(lb, 0, newValue, wLenPos, lb.length);
  // wLenPos += lb.length;
  // if (cell.getValueLength() > 0) {
  // System.arraycopy(cell.getValueArray(), cell.getValueOffset(), newValue, dataPos, cell
  // .getValueLength());
  // dataPos += cell.getValueLength();
  // }
  // }
  // return newValue;
  // }

  /**
   * Get the size of the underlying MCell []
   *
   * @return size of MCell
   */
  @Override
  public int size() {
    if (!isInitialized) {
      initCells(key);
    }
    return this.cells.size();
  }

  /**
   * Check if the underlying MCell [] is empty or not
   *
   * @return true if empty
   */
  @Override
  public boolean isEmpty() {
    if (!isInitialized) {
      initCells(key);
    }
    return this.cells.isEmpty();
  }

  /**
   * Gets the timestamp associated with the row value.
   *
   * @return timestamp for the row
   */
  @Override
  public Long getRowTimeStamp() {
    return this.rowFormat.getRowTimestamp(value);
  }

  /**
   * Gets the row Id for this instance of result
   *
   * @return rowid of this result
   */
  @Override
  public byte[] getRowId() {
    return key;
  }

  @Override
  public int compareTo(Row item) {
    return 0;
  }

  /**
   * Get all versions.
   *
   * @return Map of timestamp (version) to {@link SingleVersionRow}
   */
  @Override
  public Map<Long, SingleVersionRow> getAllVersions() {
    Map<Long, SingleVersionRow> map = new HashMap<>(1);
    map.put(this.getRowTimeStamp(),
        new SingleVersionRowImpl(key, this.getRowTimeStamp(), this.getCells()));
    return map;
  }

  /**
   * Get particular version. If not found returns null.
   *
   * @param timestamp - Version identifier
   * @return {@link SingleVersionRow}
   */
  @Override
  public SingleVersionRow getVersion(Long timestamp) {
    return this.getRowTimeStamp().equals(timestamp)
        ? new SingleVersionRowImpl(key, this.getRowTimeStamp(), this.getCells()) : null;
  }

  /**
   * Gives latest version.
   *
   * @return Latest version
   */
  @Override
  public SingleVersionRow getLatestRow() {
    return new SingleVersionRowImpl(key, this.getRowTimeStamp(), this.getCells());
  }

  /**
   * Get the raw value respective to this row.
   *
   * @return the raw value
   */
  @Override
  public Object getRawValue() {
    return this.value;
  }

  /**
   * Create the instance of this class using specified options.
   *
   * @param td the table descriptor
   * @param rf the row format
   * @return an instance of ThinRow
   */
  public static ThinRow create(final TableDescriptor td, final RowFormat rf) {
    final List<Cell> cells = td.getColumnDescriptors().stream()
        .map(cd -> new CellRef(cd.getColumnName(), cd.getColumnType()))
        .collect(Collectors.toList());
    return new ThinRow(null, null, new ThinRowShared(cells, td, Collections.emptyList()), rf);
  }
}
