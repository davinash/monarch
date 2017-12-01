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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDescriptor;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MResultParser {
  private static final int DATA_POSITION_OFFSET = Bytes.SIZEOF_LONG;
  private static final int LENGTH_SIZE = Bytes.SIZEOF_INT;
  private static final int INVALID_OFFSET = -1;
  private static final int INVALID_LENGTH = -1;

  private static Cell initCell(MColumnDescriptor columnDescriptor, byte[] buffer, int curLenPos,
      int curDataPos) {
    Cell cell = null;
    /* Read the length of the column */
    int columnValueLength = Bytes.toInt(buffer, curLenPos);

    // Column with length -1 indicates that the cell has been deleted by server operation like
    // filter etc.
    // so we should not create cell for it.
    if (columnValueLength == -1) {
      cell = null;
    } else {
      cell = new CellRef(columnDescriptor.getColumnName(), columnDescriptor.getColumnType(), buffer,
          curDataPos, columnValueLength);
    }
    return cell;
  }

  public static List<Cell> getCells(MTableDescriptor tDescriptor, byte[] buffer,
      List<byte[]> columnNameList, boolean withVersions) {

    List<Cell> result = null;
    int curLenPos = DATA_POSITION_OFFSET;
    if (withVersions) {
      curLenPos += 8;
    }
    Set<ByteArrayKey> columnNameKeys;

    // Handling for case when result does not contain any cell
    if (buffer.length > curLenPos) {
      if (columnNameList == null || columnNameList.isEmpty()) {
        columnNameKeys = tDescriptor.getColumnsByName().keySet();
      } else {
        columnNameKeys = new LinkedHashSet<>(columnNameList.size());
        for (byte[] columnName : columnNameList) {
          columnNameKeys.add(new ByteArrayKey(columnName));
        }
      }

      int curDataPos = LENGTH_SIZE * columnNameKeys.size() + DATA_POSITION_OFFSET;
      if (withVersions) {
        curDataPos += 8;
      }
      result = new ArrayList<>(columnNameKeys.size());

      for (ByteArrayKey columnNameKey : columnNameKeys) {
        MColumnDescriptor columnDescriptor = tDescriptor.getColumnsByName().get(columnNameKey);
        Cell cell = initCell(columnDescriptor, buffer, curLenPos, curDataPos);
        curLenPos += LENGTH_SIZE;
        if (cell != null) {
          result.add(cell);
          if (cell.getColumnValue() != null) {
            curDataPos += cell.getValueLength();
          }
        }
      }
    }
    return result;
  }

  public static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * Return only requested cells, specified column-index, and for skipped cells return empty cells
   * (with byte-array of 0 length). This method assumes that it gets only columns that are to be
   * retrieved.
   * <p>
   * 
   * @param td the table descriptor
   * @param buf the byte buffer corresponding to the row
   * @param readColIds the list of column ids to be returned with actual data
   * @return the list of cells with valid data for specified columns and empty cell for others
   */
  public static List<Cell> getCells(final MTableDescriptor td, final byte[] buf,
      final int[] readColIds) {
    Map<MColumnDescriptor, Integer> tdMap = td.getColumnDescriptorsMap();
    List<Cell> result = new ArrayList<>(tdMap.size());

    int curDataPos = Bytes.SIZEOF_INT * tdMap.size() + Bytes.SIZEOF_LONG;
    int curLenPos = Bytes.SIZEOF_LONG;
    int len;
    /** return the specified columns or all if none are requested **/
    if (readColIds == null || readColIds.length == 0) {
      for (Map.Entry<MColumnDescriptor, Integer> e : tdMap.entrySet()) {
        len = Bytes.toInt(buf, curLenPos);
        MColumnDescriptor columnDescriptor = e.getKey();
        result.add(new CellRef(columnDescriptor.getColumnName(), columnDescriptor.getColumnType(),
            buf, curDataPos, len));
        curDataPos += len;
        curLenPos += Bytes.SIZEOF_INT;
      }
    } else {
      Arrays.sort(readColIds);
      int index = 0;
      byte[] bytes;
      curDataPos = Bytes.SIZEOF_INT * readColIds.length + Bytes.SIZEOF_LONG;
      Cell cell = null;
      for (Map.Entry<MColumnDescriptor, Integer> e : tdMap.entrySet()) {
        if (index < readColIds.length && e.getValue() == readColIds[index]) {
          len = Bytes.toInt(buf, curLenPos);
          index++;
          curDataPos += len;
          curLenPos += Bytes.SIZEOF_INT;
          cell = initCell(e.getKey(), buf, curDataPos, curDataPos + len);
        } else {
          cell = initCell(e.getKey(), buf, -1, -1);
        }
        result.add(cell);
      }
    }
    return result;
  }

  /**
   * Similar to {@link MResultParser#getCells(MTableDescriptor, byte[], List, boolean)} but rather
   * than returning new list of cells this method just initializes existing cell references. Also,
   * it reuses the byte-array, for all cells, by using the respective offset and length.
   * <p>
   * 
   * @param cells the cells to be initialized
   * @param buf the byte-array of all columns
   * @param readColIds the column-ids to read
   */
  public static void initCells(final List<Cell> cells, final byte[] buf, final int[] readColIds) {
    int curDataPos = Bytes.SIZEOF_INT * cells.size() + Bytes.SIZEOF_LONG;
    int curLenPos = Bytes.SIZEOF_LONG;
    int len;
    /** return the specified columns or all if none are requested **/
    if (readColIds == null || readColIds.length == 0) {
      for (final Cell cell : cells) {
        len = Bytes.toInt(buf, curLenPos);
        ((CellRef) cell).init(buf, curDataPos, len);
        curDataPos += len;
        curLenPos += Bytes.SIZEOF_INT;
      }
    } else {
      int index = 0;
      int rowIndex = 0;
      curDataPos = Bytes.SIZEOF_INT * readColIds.length + Bytes.SIZEOF_LONG;
      for (final Cell cell : cells) {
        if (index < readColIds.length && rowIndex == readColIds[index]) {
          len = Bytes.toInt(buf, curLenPos);
          ((CellRef) cell).init(buf, curDataPos, len);
          index++;
          curDataPos += len;
          curLenPos += Bytes.SIZEOF_INT;
        } else {
          ((CellRef) cell).init(buf, curDataPos, 0);
        }
        rowIndex++;
      }
    }
  }

  /**
   * Similar to above method except that the unwanted columns (cells) are not present itself. The
   * above method expects that all cells and unwanted cells are initialized to empty byte-array i.e.
   * null value.
   *
   * @param cells the cells to be initialized
   * @param buf the byte-array of all columns
   * @param readColIds the list of column column-ids to read
   * @see MResultParser#initCells(List, byte[], int[])
   */
  public static int initCells(final List<Cell> cells, final byte[] buf,
      final List<Integer> readColIds) {
    int curDataPos = Bytes.SIZEOF_INT * cells.size() + Bytes.SIZEOF_LONG;
    int curLenPos = Bytes.SIZEOF_LONG;
    int len;
    int index = 0;
    /** return the specified columns or all if none are requested **/
    if (readColIds == null || readColIds.size() == 0) {
      for (final Cell cell : cells) {
        len = Bytes.toInt(buf, curLenPos);
        if (len != -1) {
          ((CellRef) cell).init(buf, curDataPos, len);
          curDataPos += len;
          curLenPos += Bytes.SIZEOF_INT;
          index++;
        }
      }
    } else {
      curDataPos = Bytes.SIZEOF_INT * readColIds.size() + Bytes.SIZEOF_LONG;
      for (final Cell cell : cells) {
        // if (index < readColIds.size() && rowIndex == readColIds.get(index)) {
        len = Bytes.toInt(buf, curLenPos);
        if (len != -1) {
          ((CellRef) cell).init(buf, curDataPos, len);
          index++;
          curDataPos += len;
          curLenPos += Bytes.SIZEOF_INT;
        }
        // }
      }
    }
    return index;
  }

  public static int initCells(List<Cell> cells, byte[] buf, int offset, List<Integer> readColIds) {
    int curDataPos = offset + Bytes.SIZEOF_INT * cells.size() + Bytes.SIZEOF_LONG;
    int curLenPos = offset + Bytes.SIZEOF_LONG;
    int len;
    int index = 0;
    /** return the specified columns or all if none are requested **/
    if (readColIds == null || readColIds.size() == 0) {
      for (final Cell cell : cells) {
        len = Bytes.toInt(buf, curLenPos);
        if (len != -1) {
          ((CellRef) cell).init(buf, curDataPos, len);
          curDataPos += len;
          curLenPos += Bytes.SIZEOF_INT;
          index++;
        } else {
          ((CellRef) cell).init(buf, -1, -1);
        }
      }
    } else {
      curDataPos = offset + Bytes.SIZEOF_INT * readColIds.size() + Bytes.SIZEOF_LONG;
      for (final Cell cell : cells) {
        // if (index < readColIds.size() && rowIndex == readColIds.get(index)) {
        len = Bytes.toInt(buf, curLenPos);
        if (len != -1) {
          ((CellRef) cell).init(buf, curDataPos, len);
          index++;
          curDataPos += len;
          curLenPos += Bytes.SIZEOF_INT;
        } else {
          ((CellRef) cell).init(buf, -1, -1);
        }
        // }
      }
    }
    return index;
  }
}
