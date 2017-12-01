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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MultiVersionValueWrapper;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.exceptions.MCheckOperationFailException;
import io.ampool.monarch.table.exceptions.RowKeyDoesNotExistException;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.MPredicateHelper;
import org.apache.geode.internal.cache.VMCachedDeserializable;

/**
 * This class will have operation performed according to encoding scheme and based on how byte
 * buffer is stored.
 * <p>
 */
public class MTableStorageFormatter implements StorageFormatter {

  private static final List<Integer> EMPTY_LIST = new ArrayList<Integer>();
  private byte DEFAULT_SCHEMA_VERSION = 1;

  public static int BITMAP_START_POS = RowHeader.getHeaderLength() + Bytes.SIZEOF_LONG;

  private final byte magicNumber;
  private final byte encoding;
  private final byte reserved;

  private static Map<String, MTableStorageFormatter> storageFormatterMap = new HashMap<>();
  private final RowHeader rowHeader;

  public MTableStorageFormatter(byte magicNumber, byte encoding, byte reserved) {
    this.magicNumber = magicNumber;
    this.encoding = encoding;
    this.reserved = reserved;
    this.rowHeader =
        new RowHeader(this.magicNumber, this.encoding, DEFAULT_SCHEMA_VERSION, this.reserved);
  }

  /**
   * Store value in following byte array format
   * <p>
   * ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   * |RowHeader|timestamp|-bitmap-fixed-cols-|Fixed-length-col-values|variable-value-offset-1|variable-value-offset-2|...|variable-value-offset-n|variable-data-1|...|variable-data-n|
   * |----4----|----8----|-NO of Cols (bits)-|---variable
   * length-----|----------4------------|-----------4-----------|...|-----------4-----------|-----var-------|...|-----var-------|
   * ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   **/
  public Object performPutOperation(final TableDescriptor gTableDescriptor, final MValue mValue,
      final MOpInfo mOpInfo, final Object oldValue) {
    MTableDescriptor tableDescriptor = (MTableDescriptor) gTableDescriptor;

    Object operatedValueArr = null;

    // if multi-versioned
    if (tableDescriptor.getMaxVersions() > 1) {
      // TODO Can be done in better way...
      // load tree map if existing value other wise
      if (oldValue != null && oldValue instanceof MultiVersionValue) {
        // load tree map
        byte[][] oldValueByteArr = ((MultiVersionValue) oldValue).getVersions();
        TreeMap<Long, byte[]> versionedMap = loadMapOfVersions(oldValueByteArr);
        // if entry exists then update after merging
        // if entry do not exists then find next smallest entry and merge with it.
        // if reach to max limit then remove last entry and place this one
        long timestamp = mOpInfo.getTimestamp();
        if (timestamp == 0) {
          timestamp = System.nanoTime();
        }
        // choose next lower entry and merge with it
        // if(versionedMap.containsKey(timestamp)){
        // // exact version update...
        // }
        byte[] valueToMergeWith = null;
        if (versionedMap.containsKey(timestamp)) {
          valueToMergeWith = versionedMap.get(timestamp);
        } else {
          Map.Entry<Long, byte[]> entryToMergeWith = versionedMap.lowerEntry(timestamp);
          if (entryToMergeWith != null) {
            valueToMergeWith = entryToMergeWith.getValue();
          }
        }
        if (valueToMergeWith == null) {
          // no lowest entry found
          if (versionedMap.size() == tableDescriptor.getMaxVersions()) {
            // we can safely ignore this put as it wont have any effect and return original two d
            // array
            return oldValue;
          } else {
            byte[] bytes = putOperation(mValue, mOpInfo, oldValue, tableDescriptor);
            versionedMap.put(timestamp, bytes);
          }
        } else {
          // need to create copy otherwise it will change old value only
          byte[] copyOfOldVersion = new byte[valueToMergeWith.length];
          System.arraycopy(valueToMergeWith, 0, copyOfOldVersion, 0, valueToMergeWith.length);
          byte[] bytes = putOperation(mValue, mOpInfo, copyOfOldVersion, tableDescriptor);
          if (versionedMap.size() == tableDescriptor.getMaxVersions()
              && !versionedMap.containsKey(timestamp)) {
            // all versions are full means remove lowest version entry
            Long smallestKey = versionedMap.firstKey();
            versionedMap.remove(smallestKey);
          }
          versionedMap.put(timestamp, bytes);
        }
        byte[][] bytes1 = versionedMap.values().toArray(new byte[versionedMap.size()][]);
        // get value size
        // TODO Can be improved
        int valueSize = 0;
        for (int i = 0; i < bytes1.length; i++) {
          if (bytes1[i] != null) {
            valueSize += bytes1[i].length;
          }
        }
        // ((MultiVersionValue) oldValue).setVersions(bytes1, valueSize);
        MultiVersionValue multiVersionValue = new MultiVersionValue(
            tableDescriptor.getRowHeader().getHeaderBytes(), tableDescriptor.getMaxVersions());
        multiVersionValue.setVersions(bytes1, valueSize);
        return multiVersionValue;
        // return oldValue;
      } else {
        MultiVersionValue multiVersionValue = new MultiVersionValue(
            tableDescriptor.getRowHeader().getHeaderBytes(), tableDescriptor.getMaxVersions());
        byte[][] multiVersionValues = new byte[1][];
        multiVersionValues[0] = putOperation(mValue, mOpInfo, oldValue, tableDescriptor);
        multiVersionValue.setVersions(multiVersionValues, multiVersionValues[0].length);
        return multiVersionValue;
      }
    } else {
      operatedValueArr = putOperation(mValue, mOpInfo, oldValue, tableDescriptor);
    }
    return operatedValueArr;
  }

  private TreeMap<Long, byte[]> loadMapOfVersions(Object oldValue) {
    byte[][] oldValueBytes = (byte[][]) oldValue;
    TreeMap<Long, byte[]> versionMap = new TreeMap<>();
    for (int i = 0; i < oldValueBytes.length; i++) {
      byte[] valueBytes = oldValueBytes[i];
      if (valueBytes != null) {
        long ts = readTimeStamp(valueBytes);
        versionMap.put(ts, oldValueBytes[i]);
      }
    }
    return versionMap;
  }

  private byte[] putOperation(MValue mValue, MOpInfo mOpInfo, Object oldValue,
      MTableDescriptor tableDescriptor) {
    byte[] operatedValueArr;// actual put operation
    /**
     * 1. Plain put no old byte array or null simple create byte array and return newly formed byte
     * array 2. Partial put and partial come 2nd time - merging two put in single version 3. Multi
     * versioned put - not thinking right so
     *
     */
    // this should be always at least for now otherwise can be checked from magic no and combination
    byte[] oldValueBytes = null;
    List<Integer> columnList = mOpInfo.getColumnList();

    if (oldValue instanceof byte[]) {
      oldValueBytes = (byte[]) oldValue;
    }

    final byte[] rowHeaderBytes = tableDescriptor.getRowHeaderBytes();

    long timestamp = mOpInfo.getTimestamp();
    if (timestamp == 0) {
      // set current time stamp
      timestamp = System.nanoTime();
    }
    final IBitMap newBitMap = getBitMap(tableDescriptor, columnList);

    int overheadForOffsetCalc = rowHeaderBytes.length + Bytes.SIZEOF_LONG;

    byte[][] mValueData = mValue.getData();
    // dont set bit for null values

    if (oldValue == null || oldValueBytes == null
        || (oldValueBytes == null && (columnList.size() == tableDescriptor.getNumOfColumns()))) {

      for (int i = 0; i < mValueData.length; i++) {
        if ((mValueData[i] == null || mValueData[i].length == 0) && columnList.contains(i)) {
          newBitMap.set(i, false);
        }
      }

      // new insert create raw byte array
      byte[] newBitMapBytes = newBitMap.toByteArray();
      overheadForOffsetCalc = overheadForOffsetCalc + newBitMapBytes.length;

      byte[] valueArray = getSingleByteBuffer(newBitMap, overheadForOffsetCalc, tableDescriptor,
          mValue.getLength(), mValueData, columnList);

      operatedValueArr = new byte[overheadForOffsetCalc + valueArray.length];

      int pos = 0;

      System.arraycopy(rowHeaderBytes, 0, operatedValueArr, pos, rowHeaderBytes.length);
      pos += rowHeaderBytes.length;

      System.arraycopy(Bytes.toBytes(timestamp), 0, operatedValueArr, pos, Bytes.SIZEOF_LONG);
      pos += Bytes.SIZEOF_LONG;

      System.arraycopy(newBitMapBytes, 0, operatedValueArr, pos, newBitMapBytes.length);
      pos += newBitMapBytes.length;

      System.arraycopy(valueArray, 0, operatedValueArr, pos, valueArray.length);
      pos += valueArray.length;
    } else {
      // update case
      // ignoring schema version updates
      // considering only single version
      // set new values and timestamp

      List<Integer> fixedLengthColumns = new ArrayList<>(tableDescriptor.getFixedLengthColumns());
      List<Integer> varLengthColumns = new ArrayList<>(tableDescriptor.getVaribleLengthColumns());

      varLengthColumns.retainAll(columnList);
      fixedLengthColumns.retainAll(columnList);

      // read bit map
      IBitMap oldBitMap = readBitMap(tableDescriptor, oldValueBytes);
      newBitMap.or(oldBitMap);
      for (int i = 0; i < mValueData.length; i++) {
        if ((mValueData[i] == null || mValueData[i].length == 0) && columnList.contains(i)) {
          newBitMap.set(i, false);
        }
      }

      byte[] newBitMapBytes = newBitMap.toByteArray();

      overheadForOffsetCalc = overheadForOffsetCalc + newBitMapBytes.length;

      // check if we can replace values inline
      if (varLengthColumns.size() == 0 && allNewColumnsAlreadyExists(oldBitMap, columnList)) {
        byte[][] data = mValueData;
        int pos = 0;

        System.arraycopy(rowHeaderBytes, 0, oldValueBytes, pos, rowHeaderBytes.length);
        pos += rowHeaderBytes.length;

        System.arraycopy(Bytes.toBytes(timestamp), 0, oldValueBytes, pos, Bytes.SIZEOF_LONG);
        pos += Bytes.SIZEOF_LONG;

        System.arraycopy(newBitMapBytes, 0, oldValueBytes, pos, newBitMapBytes.length);
        pos += newBitMapBytes.length;

        // this mean replace values at position
        Collections.sort(columnList);

        List<Integer> fixedLengthColumnsToItr = tableDescriptor.getFixedLengthColumns();

        for (int i = 0; i < fixedLengthColumnsToItr.size(); i++) {
          final Integer dataIndex = fixedLengthColumnsToItr.get(i);
          if (columnList.contains(dataIndex)) {
            byte[] actualData = data[dataIndex];
            System.arraycopy(actualData, 0, oldValueBytes, pos, actualData.length);
            pos += actualData.length;
          } else {
            pos += tableDescriptor.getColumnDescriptorByIndex(dataIndex).getColumnType()
                .lengthOfByteArray();
          }
        }
        return oldValueBytes;
      } else {
        // needs re-allocation of byte array
        // modify data from mvalue to create new byte array
        final byte[][] data = mValueData;

        Map<Integer, Pair<Integer, Integer>> offsetForEachFields =
            loadDataOffsetForEachFields(oldValueBytes, tableDescriptor, EMPTY_LIST);
        int finalDataLength = 0;
        List<Integer> updatedColumnList = new ArrayList<>();
        // iterate through data, if null check if value already exists and add that value other wise
        // keep it as null
        for (int i = 0; i < data.length; i++) {
          if (data[i] == null) {
            // check if value already present in the old byte array if so read that
            if (oldBitMap.get(i)) {
              // read this value
              Pair<Integer, Integer> offsetPair = offsetForEachFields.get(i);
              if (offsetPair.getFirst() != -1 && offsetPair.getSecond() != 1) {
                data[i] = new byte[offsetPair.getSecond() - offsetPair.getFirst()];
                System.arraycopy(oldValueBytes, offsetPair.getFirst(), data[i], 0,
                    offsetPair.getSecond() - offsetPair.getFirst());
                finalDataLength += data[i].length;
                updatedColumnList.add(i);
              }
            }
          } else {
            finalDataLength += data[i].length;
            updatedColumnList.add(i);
          }
        }

        byte[] valueArray = getSingleByteBuffer(newBitMap, overheadForOffsetCalc, tableDescriptor,
            finalDataLength, data, updatedColumnList);

        operatedValueArr = new byte[overheadForOffsetCalc + valueArray.length];

        int pos = 0;

        System.arraycopy(rowHeaderBytes, 0, operatedValueArr, pos, rowHeaderBytes.length);
        pos += rowHeaderBytes.length;

        System.arraycopy(Bytes.toBytes(timestamp), 0, operatedValueArr, pos, Bytes.SIZEOF_LONG);
        pos += Bytes.SIZEOF_LONG;

        System.arraycopy(newBitMapBytes, 0, operatedValueArr, pos, newBitMapBytes.length);
        pos += newBitMapBytes.length;

        System.arraycopy(valueArray, 0, operatedValueArr, pos, valueArray.length);
        pos += valueArray.length;
      }
    }

    return operatedValueArr;
  }

  public static IBitMap readBitMap(final MTableDescriptor tableDescriptor,
      final byte[] oldValueBytes) {
    int bitMapLength = tableDescriptor.getBitMapLength();
    return readBitMap(bitMapLength, oldValueBytes);
  }

  public static IBitMap readBitMap(final int bitMapLength, final byte[] oldValueBytes) {
    if (oldValueBytes.length > BITMAP_START_POS + bitMapLength) {
      byte[] bitMapBytes = new byte[bitMapLength];
      System.arraycopy(oldValueBytes, BITMAP_START_POS, bitMapBytes, 0, bitMapLength);
      return new BitMap(bitMapBytes);
    }
    return new BitMap(bitMapLength);
  }

  private Map<Integer, Pair<Integer, Integer>> loadDataOffsetForEachFields(
      final byte[] oldValueBytes, MTableDescriptor tableDescriptor, List<Integer> selectiveCols) {

    int dataStartPos = BITMAP_START_POS + tableDescriptor.getBitMapLength();
    IBitMap oldBitMap = readBitMap(tableDescriptor, oldValueBytes);
    // if(selectiveCols.size() > 0){
    // int bitMapLength = (int) Math.ceil(((double) selectiveCols.size()) / Byte.SIZE);
    // oldBitMap = readBitMap(bitMapLength,oldValueBytes);
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

  // test this
  public boolean allNewColumnsAlreadyExists(final IBitMap colPresenceIndicator,
      final List<Integer> columnList) {
    boolean allValuesExists = true;
    for (int i = 0; i < columnList.size(); i++) {
      allValuesExists = allValuesExists && colPresenceIndicator.get(columnList.get(i));
    }
    return allValuesExists;
  }

  @Override
  public Object performCheckAndPutOperation(final TableDescriptor gTableDescriptor,
      final MValue mValue, final MOpInfo opInfo, final Object oldValueBytes) {
    // here first check for condition matching then perform put operation
    MTableDescriptor tableDescriptor = (MTableDescriptor) gTableDescriptor;
    final MOpInfo.MCondition condition = opInfo.getCondition();
    int checkPosition = condition.getColumnId();
    byte[] columnValue = (byte[]) condition.getColumnValue();

    boolean isCheckPassed = checkValue(tableDescriptor, oldValueBytes, columnValue, checkPosition);

    if (!isCheckPassed) {
      // throw exception
      throw new MCheckOperationFailException("put check failed");
    }

    return performPutOperation(tableDescriptor, mValue, mValue.getOpInfo(), oldValueBytes);
  }

  public boolean checkValue(final MTableDescriptor tableDescriptor, final Object oldValue,
      final byte[] checkValue, final int checkPosition) {
    byte[] oldValueBytes = null;
    if (oldValue instanceof byte[]) {
      oldValueBytes = (byte[]) oldValue;
    } else if (tableDescriptor.getMaxVersions() > 1 && oldValue instanceof MultiVersionValue) {
      oldValueBytes = ((MultiVersionValue) oldValue).getLatestVersion();
    }
    IBitMap bitSet = readBitMap(tableDescriptor, oldValueBytes);

    if (bitSet.get(checkPosition)) {
      // value exist in byte array
      if (checkValue == null) {
        // if old value don't exists and value to be checked is also null then consider this as a
        // false
        return false;
      } else {
        // read the value and compare

        final boolean isFixedLengthColumn = tableDescriptor.isFixedLengthColumn(checkPosition);

        byte[] existingValue = null;

        List<Integer> columns = tableDescriptor.getVaribleLengthColumns();
        int dataStartPos = oldValueBytes.length - Bytes.SIZEOF_INT;

        if (isFixedLengthColumn) {
          columns = tableDescriptor.getFixedLengthColumns();
          dataStartPos = BITMAP_START_POS + tableDescriptor.getBitMapLength();
        }

        IBitMap varColsBitMap = new BitMap(tableDescriptor.getNumOfColumns());
        columns.forEach(col -> varColsBitMap.set(col));

        varColsBitMap.and(bitSet);
        int numOfVarColsInCurrentValue = varColsBitMap.cardinality();

        int totalOffSetOverhead = numOfVarColsInCurrentValue * Bytes.SIZEOF_INT;
        int offsetStartingPos = (oldValueBytes.length - totalOffSetOverhead);

        for (int i = 0; i < columns.size(); i++) {
          int colIndex = columns.get(i);
          if (bitSet.get(colIndex)) {
            // col is present increment data size
            if (isFixedLengthColumn) {
              int lengthOfByteArray = tableDescriptor.getColumnDescriptorByIndex(colIndex)
                  .getColumnType().lengthOfByteArray();
              if (colIndex != checkPosition) {
                dataStartPos += lengthOfByteArray;
              } else {
                // read the value to compare
                existingValue = new byte[lengthOfByteArray];
                System.arraycopy(oldValueBytes, dataStartPos, existingValue, 0, lengthOfByteArray);
                if (Bytes.compareTo(existingValue, checkValue) == 0) {
                  // check is passed now replace the value
                  return true;
                }
                break;
              }
            } else {
              if (colIndex != checkPosition) {
                dataStartPos -= Bytes.SIZEOF_INT;
              } else {
                int startOffSet = Bytes.toInt(oldValueBytes, dataStartPos);
                int endOffSet = Bytes.toInt(oldValueBytes, dataStartPos - Bytes.SIZEOF_INT);
                if ((offsetStartingPos) == (dataStartPos)) {
                  // this is last set bit
                  endOffSet = offsetStartingPos;
                }
                int lengthOfByteArray = endOffSet - startOffSet;
                existingValue = new byte[lengthOfByteArray];
                System.arraycopy(oldValueBytes, startOffSet, existingValue, 0, lengthOfByteArray);
                break;
              }
            }
          }
        }
        if (existingValue == null) {
          // value not found, this should not happen
          return false;
        } else {
          if (Bytes.compareTo(existingValue, checkValue) == 0) {
            return true;
          }
        }
      }
    } else {
      if (checkValue == null) {
        // if old value don't exists and value to be checked is also null then consider this as a
        // true
        return true;
      }
    }
    return false;
  }

  @Override
  public Object performCheckAndDeleteOperation(final TableDescriptor gTableDescriptor,
      final MOpInfo opInfo, final Object oldValueBytes) {
    MTableDescriptor tableDescriptor = (MTableDescriptor) gTableDescriptor;
    final MOpInfo.MCondition condition = opInfo.getCondition();
    int checkPosition = condition.getColumnId();
    byte[] columnValue = (byte[]) condition.getColumnValue();

    boolean isCheckPassed = checkValue(tableDescriptor, oldValueBytes, columnValue, checkPosition);

    if (!isCheckPassed) {
      // throw exception
      throw new MCheckOperationFailException("delete check failed");
    }

    return performDeleteOperation(tableDescriptor, opInfo, oldValueBytes);
  }

  @Override
  public Object performDeleteOperation(TableDescriptor tableDescriptor, MOpInfo opInfo,
      Object oldValue) {
    if (oldValue == null) {
      throw new RowKeyDoesNotExistException("Row Id does not exists");
    }

    // deleting columns from old value and returning it
    // if multiversion then delete applied to all the versions having TS below given TS
    MTableDescriptor mTableDescriptor = (MTableDescriptor) tableDescriptor;

    // if multi-versioned
    if (mTableDescriptor.getMaxVersions() > 1) {
      // multiversion delete
      if (oldValue instanceof MultiVersionValue) {
        MultiVersionValue multiVersionValue = (MultiVersionValue) oldValue;
        long timestamp = opInfo.getTimestamp();
        if (timestamp == 0) {
          timestamp = multiVersionValue.getLatestVersionTimeStamp();
        }
        byte[][] versions = multiVersionValue.getVersions();

        List<Integer> columnList = opInfo.getColumnList();
        int valueSize = 0;
        List<byte[]> newVersions = new LinkedList<>();

        boolean selectiveDelete = false;
        if (columnList != null && columnList.size() > 0) {
          selectiveDelete = true;
        }

        for (int i = 0; i < versions.length; i++) {
          byte[] version = versions[i];
          if (version == null) {
            continue;
          }
          long versionTS = readTimeStamp(version);
          if (versionTS <= timestamp) {
            if (selectiveDelete) {
              byte[] valueAfterDelete = doSelectiveDelete(mTableDescriptor, version, columnList);
              versions[i] = valueAfterDelete;
              valueSize += valueAfterDelete.length;
            }
          } else {
            if (!selectiveDelete) {
              valueSize += version.length;
              newVersions.add(version);
            }
          }
        }
        if (!selectiveDelete) {
          multiVersionValue.setVersions(newVersions.toArray(new byte[1][]), valueSize);
        } else {
          multiVersionValue.setVersions(versions, valueSize);
        }
        return multiVersionValue;
      }
    } else {
      // single versions delete
      if (oldValue instanceof byte[]) {
        byte[] oldValueBytes = (byte[]) oldValue;
        if (oldValueBytes.length > 0) {
          // get list of columns to be deleted
          List<Integer> columnList = opInfo.getColumnList();
          if (columnList != null && columnList.size() > 0) {
            return doSelectiveDelete(mTableDescriptor, oldValueBytes, columnList);
          }
        }
        return oldValue;
      }
    }
    return null;
  }

  public boolean matchesTimestamp(CompareOp lessOrEqual, byte[] oldValueBytes, long timestamp) {
    long readTimeStamp = readTimeStamp(oldValueBytes);

    Predicate predicateForLong = MPredicateHelper.getPredicateForLong(lessOrEqual, timestamp);
    return predicateForLong.test(readTimeStamp);
  }

  private byte[] doSelectiveDelete(MTableDescriptor tableDescriptor, byte[] oldValueBytes,
      List<Integer> columnPositions) {

    if (columnPositions != null && columnPositions.size() > 0) {
      // do selective delete
      List<Integer> fixedLengthColumns =
          new ArrayList<Integer>(tableDescriptor.getFixedLengthColumns());
      List<Integer> varLengthColumns =
          new ArrayList<Integer>(tableDescriptor.getVaribleLengthColumns());
      fixedLengthColumns.removeAll(columnPositions);
      varLengthColumns.removeAll(columnPositions);

      IBitMap delBitMap = getBitMap(tableDescriptor, columnPositions);
      IBitMap newBitMap = readBitMap(tableDescriptor, oldValueBytes);

      newBitMap.andNot(delBitMap);

      // newBitMap.xor();

      ArrayList<Integer> columnsRemainingAfterDelete = new ArrayList<>(fixedLengthColumns);
      columnsRemainingAfterDelete.addAll(varLengthColumns);

      Map<Integer, Pair<Integer, Integer>> offsetsMap =
          loadDataOffsetForEachFields(oldValueBytes, tableDescriptor, columnsRemainingAfterDelete);

      ByteArrayDataOutput out = ByteStreams.newDataOutput(oldValueBytes.length);

      int curPos = 0;
      LinkedList<Integer> offsets = new LinkedList<>();
      out.write(oldValueBytes, 0, BITMAP_START_POS);
      out.write(newBitMap.toByteArray());
      curPos += BITMAP_START_POS + newBitMap.toByteArray().length;
      // write all fixed length values
      for (int i = 0; i < fixedLengthColumns.size(); i++) {
        Integer colIndex = fixedLengthColumns.get(i);
        if (offsetsMap.containsKey(colIndex)) {
          int len = offsetsMap.get(colIndex).getSecond() - offsetsMap.get(colIndex).getFirst();
          out.write(oldValueBytes, offsetsMap.get(colIndex).getFirst(), len);
          curPos += len;
        }
      }

      for (int i = 0; i < varLengthColumns.size(); i++) {
        Integer colIndex = varLengthColumns.get(i);
        if (offsetsMap.containsKey(colIndex)) {
          offsets.add(curPos);
          int len = offsetsMap.get(colIndex).getSecond() - offsetsMap.get(colIndex).getFirst();
          out.write(oldValueBytes, offsetsMap.get(colIndex).getFirst(), len);
          curPos += len;
        }
      }

      Iterator<Integer> integerIterator = offsets.descendingIterator();
      while (integerIterator.hasNext()) {
        out.writeInt(integerIterator.next());
      }

      byte[] finalByteArray = out.toByteArray();
      return finalByteArray;
    }
    return oldValueBytes;

  }

  /**
   * @param aCallbackArgument - Integer value - Number of version to read. Positive value means
   *        older value first Negative value means newer value first
   */
  @Override
  public Object performGetOperation(final MTableKey mTableKey,
      final TableDescriptor gTableDescriptor, Object rawByteValue, final Object aCallbackArgument) {
    // Thread.dumpStack();
    MTableDescriptor tableDescriptor = (MTableDescriptor) gTableDescriptor;

    if (tableDescriptor.getMaxVersions() == 1) {
      if (rawByteValue instanceof byte[]) {
        byte[] selectiveGet =
            (byte[]) doSelectiveGet(tableDescriptor, rawByteValue, mTableKey.getColumnPositions());
        if (mTableKey.getTimeStamp() != 0) {
          long readTimeStamp = readTimeStamp(selectiveGet);
          if (readTimeStamp != mTableKey.getTimeStamp()) {
            // non matching ts return null
            return null;
          }
        }
        return selectiveGet;
      }
    } else {
      if (rawByteValue instanceof VMCachedDeserializable) {
        // This is used when get request is forwarded from one server to other
        // other server returns the value as VMCachedDeserializable
        // Underlying value should be MultiVersionValue
        rawByteValue = ((VMCachedDeserializable) rawByteValue).getDeserializedForReading();
      }

      if (rawByteValue instanceof MultiVersionValueWrapper) {
        MultiVersionValue multiVersionValue = new MultiVersionValue(
            tableDescriptor.getRowHeader().getHeaderBytes(), tableDescriptor.getMaxVersions());
        byte[][] twoDBytes = ((MultiVersionValueWrapper) rawByteValue).getVal();
        int finalLength = 0;
        for (int i = 0; i < twoDBytes.length; i++) {
          finalLength += twoDBytes[i].length;
        }
        multiVersionValue.setVersions(twoDBytes, finalLength);
        rawByteValue = multiVersionValue;
      }

      if (rawByteValue instanceof MultiVersionValue) {
        // get from multiversion
        MultiVersionValue multiVersionValue = (MultiVersionValue) rawByteValue;
        int maxVersionsToRead = 1;
        if (mTableKey.getMaxVersions() != 0) {
          maxVersionsToRead = mTableKey.getMaxVersions();
        }
        long timestamp = mTableKey.getTimeStamp();
        if (timestamp != 0) {
          // TODO What should we return multiversion return format or single version for now using
          // multiversion format

          byte[] versionByTimeStamp = multiVersionValue.getVersionByTimeStamp(timestamp);
          byte[] versionedValue = (byte[]) doSelectiveGet(tableDescriptor, versionByTimeStamp,
              mTableKey.getColumnPositions());
          if (versionedValue != null) {
            byte[][] versions = new byte[1][];
            versions[0] = versionedValue;
            return new MultiVersionValueWrapper(versions);
          } else {
            return new MultiVersionValueWrapper(new byte[0][]);
          }
        } else {
          // get n versions
          byte[][] versionsByOrder = multiVersionValue.getVersionsByOrder(maxVersionsToRead);
          // need to do selective get
          for (int i = 0; i < versionsByOrder.length; i++) {
            versionsByOrder[i] = (byte[]) doSelectiveGet(tableDescriptor, versionsByOrder[i],
                mTableKey.getColumnPositions());
          }
          return new MultiVersionValueWrapper(versionsByOrder);
        }
      }
    }
    return null;
  }

  @Override
  public Map<Integer, Pair<Integer, Integer>> getOffsetsForEachColumn(
      TableDescriptor tableDescriptor, byte[] oldValueBytes, List<Integer> selectiveCols) {
    return loadDataOffsetForEachFields(oldValueBytes, (MTableDescriptor) tableDescriptor,
        selectiveCols);
  }

  private Object doSelectiveGet(MTableDescriptor tableDescriptor, Object rawByteValue,
      List<Integer> columnPositions) {

    byte[] oldValueBytes = (byte[]) rawByteValue;
    if (columnPositions != null && columnPositions.size() > 0) {
      // do selective get
      List<Integer> fixedLengthColumns =
          new ArrayList<Integer>(tableDescriptor.getFixedLengthColumns());
      List<Integer> varLengthColumns =
          new ArrayList<Integer>(tableDescriptor.getVaribleLengthColumns());
      fixedLengthColumns.retainAll(columnPositions);
      varLengthColumns.retainAll(columnPositions);

      IBitMap newBitMap = getBitMap(tableDescriptor, columnPositions);
      IBitMap oldBitMap = readBitMap(tableDescriptor, oldValueBytes);
      newBitMap.and(oldBitMap);

      Map<Integer, Pair<Integer, Integer>> offsetsMap =
          loadDataOffsetForEachFields(oldValueBytes, tableDescriptor, columnPositions);

      ByteArrayDataOutput out = ByteStreams.newDataOutput(oldValueBytes.length);

      int curPos = 0;
      LinkedList<Integer> offsets = new LinkedList<>();
      out.write(oldValueBytes, 0, BITMAP_START_POS);
      out.write(newBitMap.toByteArray());
      curPos += BITMAP_START_POS + newBitMap.toByteArray().length;
      // write all fixed length values
      for (int i = 0; i < fixedLengthColumns.size(); i++) {
        Integer colIndex = fixedLengthColumns.get(i);
        if (offsetsMap.containsKey(colIndex)) {
          int len = offsetsMap.get(colIndex).getSecond() - offsetsMap.get(colIndex).getFirst();
          out.write(oldValueBytes, offsetsMap.get(colIndex).getFirst(), len);
          curPos += len;
        }
      }

      for (int i = 0; i < varLengthColumns.size(); i++) {
        Integer colIndex = varLengthColumns.get(i);
        if (offsetsMap.containsKey(colIndex)) {
          offsets.add(curPos);
          int len = offsetsMap.get(colIndex).getSecond() - offsetsMap.get(colIndex).getFirst();
          out.write(oldValueBytes, offsetsMap.get(colIndex).getFirst(), len);
          curPos += len;
        }
      }

      Iterator<Integer> integerIterator = offsets.descendingIterator();
      while (integerIterator.hasNext()) {
        out.writeInt(integerIterator.next());
      }

      byte[] finalByteArray = out.toByteArray();
      return finalByteArray;
    }
    return oldValueBytes;
  }

  /**
   * For now it will convert to the versionedRow formatted byte array
   */
  public byte[] restructureByteArray(byte[] rawByteValue, MTableDescriptor tableDescriptor) {
    Map<Integer, Pair<Integer, Integer>> offsetsMap =
        loadDataOffsetForEachFields(rawByteValue, tableDescriptor, EMPTY_LIST);

    int numOfColumns = tableDescriptor.getNumOfColumns();

    int lengthOverHead = numOfColumns * Bytes.SIZEOF_INT;

    int dataSize = dataSize(rawByteValue, tableDescriptor);

    byte[] bytes = new byte[Bytes.SIZEOF_INT + Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG + lengthOverHead
        + dataSize];

    int writePos = 0;
    // write number of versions
    System.arraycopy(Bytes.toBytes(1), 0, bytes, writePos, Bytes.SIZEOF_INT);
    writePos += Bytes.SIZEOF_INT;

    // write length of version
    System.arraycopy(Bytes.toBytes(Bytes.SIZEOF_LONG + lengthOverHead + dataSize), 0, bytes,
        writePos, Bytes.SIZEOF_INT);
    writePos += Bytes.SIZEOF_INT;

    // write timestamp
    System.arraycopy(rawByteValue, RowHeader.getHeaderLength(), bytes, writePos, Bytes.SIZEOF_LONG);
    writePos += Bytes.SIZEOF_LONG;

    int dataPos = writePos + lengthOverHead;
    // now write all values
    int length = 0;
    for (int i = 0; i < numOfColumns; i++) {
      length = 0;
      if (offsetsMap.containsKey(i) /* Here add condition doing selective get */) {
        // value present write the value and length
        Pair<Integer, Integer> offsets = offsetsMap.get(i);
        length = offsets.getSecond() - offsets.getFirst();
        System.arraycopy(rawByteValue, offsets.getFirst(), bytes, dataPos, length);
        dataPos += length;
      }
      System.arraycopy(Bytes.toBytes(length), 0, bytes, writePos, Bytes.SIZEOF_INT);
      writePos += Bytes.SIZEOF_INT;
    }
    return bytes;
  }

  private int dataSize(byte[] rawByteValue, MTableDescriptor tableDescriptor) {
    IBitMap oldBitMap = readBitMap(tableDescriptor, rawByteValue);
    List<Integer> varibleLengthColumns = tableDescriptor.getVaribleLengthColumns();
    int offsetOverhead = 0;

    for (int i = 0; i < varibleLengthColumns.size(); i++) {
      if (oldBitMap.get(varibleLengthColumns.get(i))) {
        offsetOverhead += Bytes.SIZEOF_INT;
      }
    }

    return rawByteValue.length - BITMAP_START_POS - offsetOverhead
        - tableDescriptor.getBitMapLength();
  }


  private IBitMap getBitMap(final TableDescriptor mTableDescriptor,
      final List<Integer> columnList) {
    final int numOfColumns = mTableDescriptor.getNumOfColumns();
    IBitMap bitSet = new BitMap(numOfColumns);
    // BitSet bitSet = new BitSet(numOfColumns);
    if (columnList.size() != 0) {
      for (Integer colIndex : columnList) {
        bitSet.set(colIndex);
      }
    }
    return bitSet;
  }

  public static byte[] getSingleByteBuffer(IBitMap bitSet, final int overheadForOffsetCalc,
      final TableDescriptor tableDescriptor, final int dataSize, final byte[][] data,
      final List<Integer> columns) {

    List<Integer> fixedLengthColumns =
        new ArrayList<Integer>(((MTableDescriptor) tableDescriptor).getFixedLengthColumns());
    List<Integer> varLengthColumns =
        new ArrayList<Integer>(((MTableDescriptor) tableDescriptor).getVaribleLengthColumns());
    byte[] valueArray = null;
    int varLenColOverhead = 0;

    varLengthColumns.retainAll(columns);
    fixedLengthColumns.retainAll(columns);
    for (int i = 0; i < fixedLengthColumns.size(); i++) {
      Integer index = fixedLengthColumns.get(i);
      if (!bitSet.get(index)) {
        fixedLengthColumns.remove(index);
      }
    }

    for (int i = 0; i < varLengthColumns.size(); i++) {
      Integer index = varLengthColumns.get(i);
      if (!bitSet.get(index)) {
        varLengthColumns.remove(index);
      }
    }
    varLenColOverhead = varLengthColumns.size() * Bytes.SIZEOF_INT;
    valueArray = new byte[dataSize + varLenColOverhead];

    int destPos = 0;

    for (int i = 0; i < fixedLengthColumns.size(); i++) {
      final Integer fixIndex = fixedLengthColumns.get(i);
      byte[] dataPart = data[fixIndex];
      if (dataPart != null && dataPart.length > 0) {
        System.arraycopy(dataPart, 0, valueArray, destPos, dataPart.length);
        destPos += dataPart.length;
      }
    }
    int offSetPos = valueArray.length;
    // destPos += varLenColOverhead;

    for (int i = 0; i < varLengthColumns.size(); i++) {
      final Integer fixIndex = varLengthColumns.get(i);
      byte[] dataPart = data[fixIndex];
      if (dataPart != null && dataPart.length > 0) {
        System.arraycopy(Bytes.toBytes(destPos + overheadForOffsetCalc), 0, valueArray,
            (offSetPos - Bytes.SIZEOF_INT), Bytes.SIZEOF_INT);
        offSetPos -= Bytes.SIZEOF_INT;
        System.arraycopy(dataPart, 0, valueArray, destPos, dataPart.length);
        destPos += dataPart.length;
      }
    }
    return valueArray;
  }

  public static long readTimeStamp(byte[] value) {
    return Bytes.toLong(value, RowHeader.getHeaderLength());
  }
}
