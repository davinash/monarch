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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.store.StoreRecord;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

/**
 * This class will have operation performed according to encoding scheme and based on how byte
 * buffer is stored.
 * <p>
 */
public class FTableStorageFormatter implements StorageFormatter {
  private static final Logger logger = LogService.getLogger();
  private static final boolean USE_DEFAULT_VALUES = true;
  private final byte magicNumber;
  private final byte encoding;
  private final byte reserved;

  public FTableStorageFormatter(byte magicNumber, byte encoding, byte reserved) {
    this.magicNumber = magicNumber;
    this.encoding = encoding;
    this.reserved = reserved;
  }

  @Override
  public Object performPutOperation(TableDescriptor tableDescriptor, MValue mValue, MOpInfo mOpInfo,
      Object oldValue) {

    return null;
  }

  @Override
  public Object performCheckAndPutOperation(TableDescriptor tableDescriptor, MValue mValue,
      MOpInfo opInfo, Object oldValueBytes) {
    throw new UnsupportedOperationException("Operation not supported for FTable");
  }

  @Override
  public Object performCheckAndDeleteOperation(TableDescriptor gTableDescriptor, MOpInfo opInfo,
      Object oldValueBytes) {
    throw new UnsupportedOperationException("Operation not supported for FTable");
  }

  @Override
  public Object performDeleteOperation(TableDescriptor tableDescriptor, MOpInfo opInfo,
      Object oldValueBytes) {
    throw new UnsupportedOperationException("Operation not supported for FTable");
  }

  @Override
  public Object performGetOperation(MTableKey mTableKey, TableDescriptor tableDescriptor,
      Object rawByteValue, Object aCallbackArgument) {

    throw new UnsupportedOperationException("Operation not supported for FTable");
  }

  @Override
  public Map<Integer, Pair<Integer, Integer>> getOffsetsForEachColumn(TableDescriptor td,
      byte[] oldValueBytes, List<Integer> selectiveCols) {

    // TODO Impl for encoding bit 1, older ftable version format

    Map<Integer, Pair<Integer, Integer>> offsetsForEachColumn = new LinkedHashMap<>();
    List<Integer> fixedLengthColumns = ((AbstractTableDescriptor) td).getFixedLengthColumns();
    List<Integer> variableLengthColumns = ((AbstractTableDescriptor) td).getVaribleLengthColumns();
    int dataPos = 0;
    for (int i = 0; i < fixedLengthColumns.size(); i++) {
      int fixIndex = fixedLengthColumns.get(i);
      MColumnDescriptor columnDescriptorByIndex =
          ((AbstractTableDescriptor) td).getColumnDescriptorByIndex(fixIndex);
      int bytesSize = columnDescriptorByIndex.getColumnType().lengthOfByteArray();
      if ((selectiveCols.size() == 0 || selectiveCols.contains(fixIndex))) {
        offsetsForEachColumn.put(columnDescriptorByIndex.getIndex(),
            new Pair<>(dataPos, dataPos + bytesSize));
      }
      dataPos += bytesSize;
    }

    int totalOffSetOverhead = variableLengthColumns.size() * Bytes.SIZEOF_INT;
    int offsetPosition = oldValueBytes.length - Bytes.SIZEOF_INT;
    int offsetStartingPos = (oldValueBytes.length - totalOffSetOverhead);

    for (int i = 0; i < variableLengthColumns.size(); i++) {
      final Integer colIndex = variableLengthColumns.get(i);
      if ((selectiveCols.size() == 0 || selectiveCols.contains(colIndex))) {
        Pair<Integer, Integer> offsets = null;
        if ((oldValueBytes.length - totalOffSetOverhead) == (offsetPosition)) {
          // this is last set bit
          offsets = new Pair(Bytes.toInt(oldValueBytes, offsetPosition), offsetStartingPos);
        } else {
          offsets = new Pair(Bytes.toInt(oldValueBytes, offsetPosition),
              Bytes.toInt(oldValueBytes, offsetPosition - Bytes.SIZEOF_INT));

        }
        offsetsForEachColumn.put(colIndex, offsets);
      }
      offsetPosition -= Bytes.SIZEOF_INT;
    }
    return offsetsForEachColumn;
  }

  /**
   * This will record in following byteArray format
   *
   * |Fixed_length_Col_1|...|Fixed_length_Col_N|Variable_length_Col_1|...|Variable_length_Col_N|Offset_Variable_length_Col_N|...|Offset_Variable_length_Col_1|
   */
  public byte[] getRecordInBytes(FTableDescriptor td, Record record) {
    if (td.getNumOfColumns() == 0) {
      return null;
    }

    if (this.encoding == (byte) 2) {

      Map<ByteArrayKey, Object> colValueMap = record.getValueMap();

      List<Integer> fixedLengthColumns = td.getFixedLengthColumns();
      List<Integer> variableLengthColumns = td.getVaribleLengthColumns();

      ByteArrayDataOutput out = ByteStreams.newDataOutput();
      int dataPos = 0;
      for (int i = 0; i < fixedLengthColumns.size(); i++) {
        int fixIndex = fixedLengthColumns.get(i);
        MColumnDescriptor columnDescriptorByIndex = td.getColumnDescriptorByIndex(fixIndex);
        ByteArrayKey byteArrayKey = new ByteArrayKey(columnDescriptorByIndex.getColumnName());
        byte[] columnValue = null;
        if (colValueMap.containsKey(byteArrayKey)) {
          // write this value otherwise write default values
          Object inColumnValue = colValueMap.get(byteArrayKey);
          if (inColumnValue instanceof byte[]/* && !BasicTypes.BINARY.equals(type) */) {
            columnValue = (byte[]) inColumnValue;
          } else {
            columnValue = columnDescriptorByIndex.getColumnType().serialize(inColumnValue);
          }
        } else {
          if (USE_DEFAULT_VALUES) {
            if (columnDescriptorByIndex.getColumnType().isFixedLength()) {
              columnValue = columnDescriptorByIndex.getColumnType()
                  .serialize(columnDescriptorByIndex.getColumnType().defaultValue());
            }
          }
        }
        dataPos += columnValue.length;
        out.write(columnValue);
      }

      ArrayList<Integer> offsetList = new ArrayList<>(variableLengthColumns.size());
      for (int i = 0; i < variableLengthColumns.size(); i++) {
        int fixIndex = variableLengthColumns.get(i);
        MColumnDescriptor columnDescriptorByIndex = td.getColumnDescriptorByIndex(fixIndex);
        ByteArrayKey byteArrayKey = new ByteArrayKey(columnDescriptorByIndex.getColumnName());
        byte[] columnValue = null;
        if (colValueMap.containsKey(byteArrayKey)) {
          // write this value otherwise write default values
          Object inColumnValue = colValueMap.get(byteArrayKey);
          if (inColumnValue instanceof byte[]/* && !BasicTypes.BINARY.equals(type) */) {
            columnValue = (byte[]) inColumnValue;
          } else {
            columnValue = columnDescriptorByIndex.getColumnType().serialize(inColumnValue);
          }
          offsetList.add(dataPos);
          dataPos += columnValue.length;
          out.write(columnValue);
        } else {
          // write only length
          offsetList.add(dataPos);
        }
      }

      // write offset in reverse way
      for (int i = offsetList.size() - 1; i >= 0; i--) {
        out.write(Bytes.toBytes(offsetList.get(i)));
      }
      byte[] valueBytes = out.toByteArray();
      // System.out.println("FTableStorageFormatter.getRecordInBytes :: 163 " + "final record byte
      // array "+ Arrays
      // .toString(valueBytes));
      return valueBytes;
    } else if (this.encoding == (byte) 1) {
      // older way implementation
      Map<MColumnDescriptor, Integer> columnToIdMap = td.getColumnDescriptorsMap();
      byte[][] data = new byte[columnToIdMap.size()][];
      int totalLength = 0;
      int columnLengthSize = 4;
      for (Map.Entry<MColumnDescriptor, Integer> entry : columnToIdMap.entrySet()) {
        final ByteArrayKey byteArrayKey = new ByteArrayKey(entry.getKey().getColumnName());
        if (record.getValueMap().containsKey(byteArrayKey)) {
          final DataType type = entry.getKey().getColumnType();
          final Object inColumnValue = record.getValueMap().get(byteArrayKey);
          byte[] columnValue;
          if (inColumnValue instanceof byte[] && !BasicTypes.BINARY.equals(type)) {
            columnValue = (byte[]) inColumnValue;
          } else {
            columnValue = type.serialize(inColumnValue);
          }
          data[entry.getValue()] = columnValue;
          totalLength += columnValue.length + columnLengthSize;
        } else {
          totalLength += columnLengthSize;
        }
      }

      byte[] valueBytes = new byte[totalLength];
      int offset = 0;
      for (final byte[] bytes : data) {
        if (bytes == null) {
          // add column length
          Bytes.putBytes(valueBytes, offset, Bytes.toBytes(0), 0, Bytes.SIZEOF_INT);
          offset += Bytes.SIZEOF_INT;
          continue;
        }
        // add column length
        Bytes.putBytes(valueBytes, offset, Bytes.toBytes(bytes.length), 0, Bytes.SIZEOF_INT);
        offset += Bytes.SIZEOF_INT;
        // add column length
        Bytes.putBytes(valueBytes, offset, bytes, 0, bytes.length);
        offset += bytes.length;
      }

      return valueBytes;
    }
    return null;
  }

  public byte[] convertFromStoreRecord(FTableDescriptor fTableDescriptor, StoreRecord storeRecord) {
    Record rec = new Record();
    for (int i = 0; i < storeRecord.getValues().length; i++) {
      Object value = storeRecord.getValues()[i];
      MColumnDescriptor columnDescriptorByIndex = fTableDescriptor.getColumnDescriptorByIndex(i);
      rec.add(columnDescriptorByIndex.getColumnNameAsString(), value);
    }
    return (byte[]) fTableDescriptor.getEncoding().serializeValue(fTableDescriptor, rec);
  }
}
