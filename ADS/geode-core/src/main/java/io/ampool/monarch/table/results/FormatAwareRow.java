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

package io.ampool.monarch.table.results;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.ampool.classification.InterfaceAudience;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MultiVersionValueWrapper;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.BitMap;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.CellRef;
import io.ampool.monarch.table.internal.Encoding;
import io.ampool.monarch.table.internal.IBitMap;
import io.ampool.monarch.table.internal.InternalRow;
import io.ampool.monarch.table.internal.MOperation;
import io.ampool.monarch.table.internal.MTableStorageFormatter;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.MValue;
import io.ampool.monarch.table.internal.MultiVersionValue;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.table.internal.SingleVersionRowImpl;
import io.ampool.monarch.table.internal.StorageFormatter;
import io.ampool.monarch.table.internal.ThinRowShared;
import io.ampool.monarch.table.internal.VersionedRow;
import io.ampool.monarch.types.BasicTypes;

public class FormatAwareRow implements Row, InternalRow, Serializable {

  private static final long serialVersionUID = -1117787027437152271L;
  private VersionedRow ftableRow = null;
  private boolean isFTable = false;
  private SingleVersionRow singleVersionRow;
  private Long timestamp;
  private List<Cell> cells;
  private TableDescriptor tableDescriptor;
  private Object rowValue;
  private byte[] rowKey;

  /*
   * Map holding timestamp vs row value
   */
  private Map<Long, SingleVersionRow> multiVersions = new LinkedHashMap<>();


  public FormatAwareRow() {}

  public FormatAwareRow(byte[] rowId, Long rowTimeStamp, TableDescriptor tableDescriptor,
      List<Cell> newCells) {
    this.isFTable = tableDescriptor instanceof FTableDescriptor;

    if (!this.isFTable) {
      this.rowKey = rowId;
      this.timestamp = rowTimeStamp;
      this.cells = newCells;
      this.tableDescriptor = tableDescriptor;

      this.singleVersionRow = new SingleVersionRowImpl(this.rowKey, this.timestamp, this.cells);
      multiVersions.put(this.timestamp, singleVersionRow);

      // as it is required to return in serialized form
      this.rowValue = constructRawByteValue();
    } else {
      this.ftableRow = new VersionedRow(rowId, rowTimeStamp, newCells, tableDescriptor);

    }
  }

  public FormatAwareRow(final byte[] rowKey, Object bytes, TableDescriptor tableDescriptor,
      List<byte[]> columnNameList) {
    dummyConstructor(rowKey, bytes, tableDescriptor, columnNameList, false);
  }

  public FormatAwareRow(final byte[] rowKey, Object bytes, TableDescriptor tableDescriptor,
      List<byte[]> columnNameList, boolean skipEmptyCells) {
    dummyConstructor(rowKey, bytes, tableDescriptor, columnNameList, skipEmptyCells);
  }

  @Override
  public void reset(final Object key, final Object value, final Encoding encoding,
      final List<byte[]> columnNameList) {
    dummyConstructor((byte[]) key, value, this.tableDescriptor, columnNameList, false);
  }

  @Override
  public void reset(Object key, Object value, Encoding encoding, int offset, int length) {
    // .. stub..
  }

  private void dummyConstructor(final byte[] rowKey, Object bytes, TableDescriptor tableDescriptor,
      List<byte[]> columnNameList, boolean skipEmptyCells) {
    this.isFTable = tableDescriptor instanceof FTableDescriptor;
    this.rowKey = rowKey;
    this.rowValue = bytes;
    this.tableDescriptor = tableDescriptor;
    this.cells = new LinkedList<Cell>();

    if (this.rowValue != null) {
      if (!this.isFTable) {
        // for now only for mtable

        // second condition is for the purpose when scan returns with single version byte array
        if (((MTableDescriptor) tableDescriptor).getMaxVersions() == 1
            || (((MTableDescriptor) tableDescriptor).getMaxVersions() > 1
                && this.rowValue instanceof byte[])) {
          // single version result
          if (this.rowValue instanceof byte[] && ((byte[]) this.rowValue).length > 0) {
            extractCellsFromRow((byte[]) this.rowValue, tableDescriptor, columnNameList, this.cells,
                skipEmptyCells);
            // TODO this implementation can be improved to refer this cells only
            this.singleVersionRow =
                new SingleVersionRowImpl(this.rowKey, this.timestamp, this.cells);
            multiVersions.put(this.timestamp, singleVersionRow);
          }
        } else {
          // extract multiple versions
          if (this.rowValue instanceof MultiVersionValue) {
            // this is the case when directly read from map during scan
            this.rowValue = ((MultiVersionValue) this.rowValue).getVersions();
          }

          if (this.rowValue instanceof MultiVersionValueWrapper) {
            // this is the case when directly read from map during scan
            this.rowValue = ((MultiVersionValueWrapper) this.rowValue).getVal();
          }

          if (this.rowValue instanceof byte[][]) {
            byte[][] versions = (byte[][]) this.rowValue;
            for (int i = 0; i < versions.length; i++) {
              if (versions[i] == null) {
                continue;
              }
              if (i == versions.length - 1) {
                // this is latest row
                extractCellsFromRow(versions[i], tableDescriptor, columnNameList, this.cells,
                    skipEmptyCells);
                // TODO this implementation can be improved to refer this cells only
                this.singleVersionRow =
                    new SingleVersionRowImpl(this.rowKey, this.timestamp, this.cells);
                multiVersions.put(this.timestamp, this.singleVersionRow);
              } else {
                LinkedList<Cell> newCells = new LinkedList<Cell>();
                extractCellsFromRow(versions[i], tableDescriptor, columnNameList, newCells,
                    skipEmptyCells);
                // TODO this implementation can be improved to refer this cells only
                SingleVersionRow newSingleVersionRow =
                    new SingleVersionRowImpl(this.rowKey, this.timestamp, newCells);
                multiVersions.put(this.timestamp, newSingleVersionRow);
              }
            }
          }
        }
      } else {
        this.ftableRow = new VersionedRow(rowKey, (byte[]) bytes,
            (FTableDescriptor) tableDescriptor, columnNameList);
      }
    }
  }

  public FormatAwareRow(byte[] bytes) {
    this.ftableRow = null;
    this.isFTable = false;
    this.singleVersionRow = null;
    this.timestamp = null;
    this.cells = Collections.EMPTY_LIST;
    this.tableDescriptor = null;
    this.rowValue = null;
    rowKey = bytes;
  }

  private Object constructRawByteValue() {
    if (this.multiVersions.size() == 1) {
      // make dummy put operation call so as to get the byte array
      Put dummyPut = new Put(new byte[] {1});
      dummyPut.setTimeStamp(this.getRowTimeStamp());
      for (int i = 0; i < this.cells.size(); i++) {
        Cell cell = this.cells.get(i);
        if (cell.getColumnValue() != null) {
          dummyPut.addColumn(Bytes.toString(cell.getColumnName()), cell.getColumnValue());
        }
      }

      MTableDescriptor mTableDescriptor = (MTableDescriptor) this.tableDescriptor;

      StorageFormatter mtableFormatter = MTableUtils.getStorageFormatter(this.tableDescriptor);

      MValue mValue = MValue.fromPut(mTableDescriptor, dummyPut, MOperation.PUT);

      Object finalValue =
          mtableFormatter.performPutOperation(mTableDescriptor, mValue, mValue.getOpInfo(), null);

      return finalValue;
    } else {
      byte[][] twoDBytes = new byte[this.multiVersions.size()][];
      // make dummy put operation call so as to get the byte array
      SingleVersionRow[] values =
          this.multiVersions.values().toArray(new SingleVersionRow[this.multiVersions.size()]);
      for (int i = 0; i < values.length; i++) {
        twoDBytes[i] =
            (byte[]) ((SingleVersionRowImpl) values[i]).constructRawByteValue(this.tableDescriptor);
      }
      return new MultiVersionValueWrapper(twoDBytes);
    }
  }

  private void extractCellsFromRow(byte[] rowValue, TableDescriptor tableDescriptor,
      List<byte[]> columnNameList, List<Cell> cells, boolean skipEmptyCells) {
    if (rowValue == null || rowValue.length == 0) {
      return;
    }

    boolean isSelectedColumns = columnNameList != null && columnNameList.size() > 0;

    Map<ByteArrayKey, MColumnDescriptor> columnsByName = tableDescriptor.getSchema().getColumnMap();

    StorageFormatter storageFormatter = MTableUtils.getStorageFormatter(tableDescriptor);
    if (storageFormatter instanceof MTableStorageFormatter) {
      this.timestamp = ((MTableStorageFormatter) storageFormatter).readTimeStamp(rowValue);
    }
    Map<Integer, Pair<Integer, Integer>> offsetsForEachColumn =
        getOffsetsForEachColumn(storageFormatter, (MTableDescriptor) tableDescriptor, rowValue,
            getListOfColumnIndices(tableDescriptor, columnNameList));
    // create list of cells

    if (isSelectedColumns) {
      // the order in which is columns are requsted is required

      for (int i = 0; i < columnNameList.size(); i++) {
        byte[] colName = columnNameList.get(i);

        MColumnDescriptor columnDescriptor = columnsByName.get(new ByteArrayKey(colName));
        int pos = columnDescriptor.getIndex();
        if (offsetsForEachColumn.containsKey(pos)) {
          Pair<Integer, Integer> v = offsetsForEachColumn.get(pos);
          Cell cell = null;
          if (Bytes.equals(columnDescriptor.getColumnName(),
              Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME))) {
            cell = new CellRef(Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME), BasicTypes.BINARY);
            ((CellRef) cell).init(this.rowKey, 0, this.rowKey.length);
          } else {
            cell = new CellRef(columnDescriptor.getColumnName(), columnDescriptor.getColumnType(),
                rowValue, v.getFirst(), v.getSecond() - v.getFirst());
          }
          cells.add(cell);
        } else {
          // as we provide cell with value null create those cells
          if (!skipEmptyCells) {
            Cell cell = null;
            if (Bytes.equals(columnDescriptor.getColumnName(),
                Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME))) {
              cell = new CellRef(Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME), BasicTypes.BINARY);
              ((CellRef) cell).init(this.rowKey, 0, this.rowKey.length);
            } else {
              cell =
                  new CellRef(columnDescriptor.getColumnName(), columnDescriptor.getColumnType());
            }
            cells.add(cell);
          }
        }
      }
    } else {
      int numOfColumns = ((MTableDescriptor) tableDescriptor).getNumOfColumns();
      for (int i = 0; i < numOfColumns; i++) {
        int pos = i;
        MColumnDescriptor columnDescriptor =
            ((MTableDescriptor) tableDescriptor).getColumnDescriptorByIndex(pos);
        if (offsetsForEachColumn.containsKey(pos)) {
          Pair<Integer, Integer> v = offsetsForEachColumn.get(pos);
          Cell cell = null;
          if (Bytes.equals(columnDescriptor.getColumnName(),
              Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME))) {
            cell = new CellRef(Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME), BasicTypes.BINARY);
            ((CellRef) cell).init(this.rowKey, 0, this.rowKey.length);
          } else {
            cell = new CellRef(columnDescriptor.getColumnName(), columnDescriptor.getColumnType(),
                rowValue, v.getFirst(), v.getSecond() - v.getFirst());
          }
          cells.add(cell);
        } else {
          // as we provide cell with value null create those cells
          // TODO Should we create these cells are not. Get requires but scan with filters doesn't
          if (!skipEmptyCells) {
            Cell cell = null;
            if (Bytes.equals(columnDescriptor.getColumnName(),
                Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME))) {
              cell = new CellRef(Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME), BasicTypes.BINARY);
              ((CellRef) cell).init(this.rowKey, 0, this.rowKey.length);
            } else {
              cell =
                  new CellRef(columnDescriptor.getColumnName(), columnDescriptor.getColumnType());
            }
            cells.add(cell);
          }
        }
      }
    }


  }

  private Map<Integer, Pair<Integer, Integer>> getOffsetsForEachColumn(
      StorageFormatter storageFormatter, MTableDescriptor tableDescriptor, byte[] oldValueBytes,
      List<Integer> selectiveCols) {
    int BITMAP_START_POS = MTableStorageFormatter.BITMAP_START_POS;

    int dataStartPos = BITMAP_START_POS + tableDescriptor.getBitMapLength();
    IBitMap oldBitMap = MTableStorageFormatter.readBitMap(tableDescriptor, oldValueBytes);
    // if (selectiveCols.size() > 0) {
    // // TODO Load bit set according to column indices as although size is 5 bit set is created
    // // of length is of 2 for 0,2,4,6,8
    // BitSet tempBitSet = new BitSet();
    // selectiveCols.forEach(col -> {
    // tempBitSet.set(col);
    // });
    // int bitMapLength = tempBitSet.toByteArray().length;
    // oldBitMap = MTableStorageFormatter.readBitMap(bitMapLength, oldValueBytes);
    // }

    List<Integer> fixedLengthColumns = tableDescriptor.getFixedLengthColumns();
    List<Integer> varLengthColumns = tableDescriptor.getVaribleLengthColumns();

    Map<Integer, Pair<Integer, Integer>> posToOffsetMap = new HashMap<>();

    int dataPosition = dataStartPos;

    for (int i = 0; i < fixedLengthColumns.size(); i++) {
      final Integer colIndex = fixedLengthColumns.get(i);
      int length =
          tableDescriptor.getColumnDescriptorByIndex(colIndex).getColumnType().lengthOfByteArray();
      if (oldBitMap.get(colIndex)) {
        if ((selectiveCols.size() == 0 || selectiveCols.contains(colIndex))) {
          Pair offsets = new Pair(dataPosition, dataPosition + length);
          posToOffsetMap.put(colIndex, offsets);
        }
        dataPosition += length;
      }
    }

    int offsetPosition = oldValueBytes.length - Bytes.SIZEOF_INT;

    IBitMap varColsBitMap = new BitMap(tableDescriptor.getNumOfColumns());
    varLengthColumns.forEach(col -> varColsBitMap.set(col));

    varColsBitMap.and(oldBitMap);
    int numOfVarColsInCurrentValue = varColsBitMap.cardinality();
    int totalOffSetOverhead = numOfVarColsInCurrentValue * Bytes.SIZEOF_INT;
    int offsetStartingPos = (oldValueBytes.length - totalOffSetOverhead);

    for (int i = 0; i < varLengthColumns.size(); i++) {
      final Integer colIndex = varLengthColumns.get(i);
      if (oldBitMap.get(colIndex)) {
        if ((selectiveCols.size() == 0 || selectiveCols.contains(colIndex))) {
          Pair<Integer, Integer> offsets = null;

          if ((oldValueBytes.length - totalOffSetOverhead) == (offsetPosition)) {
            // this is last set bit
            offsets = new Pair(Bytes.toInt(oldValueBytes, offsetPosition), offsetStartingPos);
          } else {
            offsets = new Pair(Bytes.toInt(oldValueBytes, offsetPosition),
                Bytes.toInt(oldValueBytes, offsetPosition - Bytes.SIZEOF_INT));

          }
          posToOffsetMap.put(colIndex, offsets);
        }
        offsetPosition -= Bytes.SIZEOF_INT;
      }
    }

    return posToOffsetMap;
  }

  public List<Integer> getListOfColumnIndices(TableDescriptor tableDescriptor,
      List<byte[]> columnNameList) {

    Map<MColumnDescriptor, Integer> columnDescriptorsMap =
        tableDescriptor.getColumnDescriptorsMap();

    if (columnNameList == null || columnNameList.size() == 0) {
      return new LinkedList<>(columnDescriptorsMap.values());
    }

    MColumnDescriptor DUMMY_CD = new MColumnDescriptor();
    final List<Integer> colPositions = new LinkedList<>();
    columnNameList.forEach(name -> {
      DUMMY_CD.setColumnName(name);
      colPositions.add(columnDescriptorsMap.get(DUMMY_CD));
    });
    return colPositions;
  }

  @Override
  public List<Cell> getCells() {
    if (!isFTable) {
      if (this.singleVersionRow != null) {
        return this.singleVersionRow.getCells();
      }
    } else {
      if (this.ftableRow != null) {
        return this.ftableRow.getCells();
      }
    }
    return Collections.emptyList();

  }

  @Override
  public void writeSelectedColumns(DataOutput out, List<Integer> columns) throws IOException {
    //// ....
  }

  @Override
  public int getOffset() {
    return 0;
  }

  @Override
  public int getLength() {
    return 0;
  }

  @Override
  public int size() {
    if (!isFTable) {
      if (this.singleVersionRow != null) {
        return this.singleVersionRow.size();
      }
    } else {
      if (this.ftableRow != null) {
        return this.ftableRow.size();
      }
    }
    return 0;
  }

  @Override
  public boolean isEmpty() {
    if (!isFTable) {
      if (this.singleVersionRow != null) {
        return this.singleVersionRow.isEmpty();
      }
    } else {
      if (this.ftableRow != null) {
        return this.ftableRow.isEmpty();
      }
    }
    return true;
  }

  @Override
  public Long getRowTimeStamp() {
    if (!isFTable) {
      return getLatestRow().getTimestamp();
    } else {
      if (this.ftableRow != null) {
        return this.ftableRow.getRowTimeStamp();
      }
    }
    return null;
  }

  @Override
  public byte[] getRowId() {
    if (!isFTable) {
      return this.rowKey;
    } else {
      if (this.ftableRow != null) {
        return this.ftableRow.getRowId();
      }
    }
    return null;
  }

  @Override
  public int compareTo(Row item) {
    if (!isFTable) {
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
      if (item instanceof Row) {
        final SingleVersionRow currentRow = getLatestRow();
        final SingleVersionRow row = item.getLatestRow();
        if (currentRow != null && row != null) {
          return currentRow.compareTo(row);
        }
      }
      return -1;
    } else {
      return this.ftableRow.compareTo(item);
    }
  }

  @Override
  public Map<Long, SingleVersionRow> getAllVersions() {
    if (!isFTable) {
      return multiVersions;
    } else {
      return this.ftableRow.getAllVersions();
    }
  }

  @Override
  public SingleVersionRow getVersion(Long timestamp) {
    if (!isFTable) {
      return multiVersions.get(timestamp);
    } else {
      return this.ftableRow.getVersion(timestamp);
    }
  }

  @Override
  public SingleVersionRow getLatestRow() {
    if (!isFTable) {
      Iterator<SingleVersionRow> iterator = multiVersions.values().iterator();
      while (iterator.hasNext()) {
        return iterator.next();
      }
      return null;
    } else {
      return this.ftableRow.getLatestRow();
    }
  }

  /**
   * Get the raw value respective to this row.
   *
   * @return the raw value
   */
  @Override
  public Object getRawValue() {
    return getRawByteArray();
  }

  @Override
  public ThinRowShared getRowShared() {
    return null;
  }

  public TableDescriptor getTableDescriptor() {
    return this.tableDescriptor;
  }

  public Object getRawByteArray() {
    // TODO IMPL
    if (!isFTable) {
      if (this.rowValue instanceof byte[]) {
        return (byte[]) this.rowValue;
      } else if (this.rowValue instanceof byte[][]) {
        byte[][] rowValue = (byte[][]) this.rowValue;
        return new MultiVersionValueWrapper(rowValue);
      } else if (this.rowValue instanceof MultiVersionValue) {
        byte[][] rowValue = ((MultiVersionValue) this.rowValue).getVersions();
        return new MultiVersionValueWrapper(rowValue);
      } else if (this.rowValue instanceof MultiVersionValueWrapper) {
        return this.rowValue;
      }
      return new byte[0];
    } else {
      return this.ftableRow.getRawByteArray();
    }
  }

  /**
   * Strictly internal method. Set filtered rows.
   */
  @InterfaceAudience.Private
  public void setUpdatedVersions(Map<Long, SingleVersionRow> rowMap) {
    if (rowMap == null) {
      this.multiVersions = Collections.emptyMap();
      this.singleVersionRow = null;
    } else {
      this.multiVersions = rowMap;
      this.singleVersionRow = getLatestRow();
      this.cells = getCells();
      this.timestamp = this.singleVersionRow.getTimestamp();
      this.rowValue = constructRawByteValue();
    }
  }
}
