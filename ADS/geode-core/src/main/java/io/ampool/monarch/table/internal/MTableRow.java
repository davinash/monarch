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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.interfaces.DataType;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

/**
 * Class represents internal byte array of MultipleColumnValues Byte Arrays is described as below
 * Assume that you have table with 3 columns.
 * ------------------------------------------------------------------------------------------------------
 * | Timestamp |
 * LengthOfColumn1|lengthOfColumn2|lengthOfColumn3|ColumnValue1|ColumnValue2|ColumnValue3 |
 * ------------------------------------------------------------------------------------------------------
 * TimeStamp is optional for this class for intermediate states. But Client will always send
 * TimeStamp Information , If set it will long i.e. 8bytes other length information is stored in 4
 * bytes.
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MTableRow {
  /**
   * total number of columns
   **/
  private final int numOfColumns;
  /**
   * internal buffer which stores the columnValue information
   */
  private byte[] buffer;
  /**
   * buffer length, this is used to track when to re-alloc the buffer
   */
  private int bufferLen;

  /**
   * used to track from where to write length and data information
   */
  private int dataPos;
  private int lengthPos;

  private static final int ROW_TUPLE_VERSION_COUNTER_LENGTH = Bytes.SIZEOF_INT;
  private static final int ROW_TUPLE_EACH_VERSION_LENGTH = Bytes.SIZEOF_INT;
  public static final int ROW_TUPLE_OVERHEAD =
      ROW_TUPLE_VERSION_COUNTER_LENGTH + ROW_TUPLE_EACH_VERSION_LENGTH;

  static Logger logger = LogService.getLogger();

  private int ensureCapacity(int minCapacity, int bufferPos) {
    if (bufferPos + minCapacity < this.bufferLen) {
      return bufferPos;
    } else {
      int newLen = bufferPos + minCapacity;
      this.buffer = Arrays.copyOf(this.buffer, newLen);
      this.bufferLen = newLen;
      return bufferPos;
    }
  }

  public MTableRow(int numOfColumns, boolean withTimeStamp, long timeStamp) {
    this.numOfColumns = numOfColumns;
    int initialLength = 0;

    if (withTimeStamp) {
      this.lengthPos = 8;
      initialLength = (Bytes.SIZEOF_INT * this.numOfColumns) + this.lengthPos;
      this.buffer = new byte[initialLength];
      /* Write timeStamp first */
      Bytes.putLong(this.buffer, 0, timeStamp);
    } else {
      this.lengthPos = 0;
      initialLength = (Bytes.SIZEOF_INT * this.numOfColumns) + this.lengthPos;
      this.buffer = new byte[initialLength];
    }
    this.dataPos = initialLength;
  }

  public MTableRow(byte[] value, boolean withTimeStamp, int numOfColumns) {
    this.numOfColumns = numOfColumns;
    this.buffer = value;
    if (withTimeStamp) {
      this.lengthPos = 8;
      this.dataPos = (Bytes.SIZEOF_INT * this.numOfColumns) + this.lengthPos;
    } else {
      this.lengthPos = 0;
      this.dataPos = (Bytes.SIZEOF_INT * this.numOfColumns) + this.lengthPos;
    }
    this.buffer = value;
  }

  private MTableRow(byte[] value, boolean withTimeStamp, int numOfColumns, boolean withVersions) {
    this.numOfColumns = numOfColumns;
    this.buffer = value;
    if (withTimeStamp) {
      if (withVersions) {
        this.lengthPos = 8 + 8;
        this.dataPos = (Bytes.SIZEOF_INT * this.numOfColumns) + this.lengthPos;
      } else {
        this.lengthPos = 8;
        this.dataPos = (Bytes.SIZEOF_INT * this.numOfColumns) + this.lengthPos;
      }
    } else {
      throw new IllegalStateException("Should not happen");
    }
    this.buffer = value;
  }

  /**
   * Creates a MTableRow object from the actual row byte array from the map This byte array contains
   * following components.
   * |--------------------------------------------------------------------------------------------------------------
   * | Number of Versions | Length( 1st Version) | Data(1st Version) | Length(2nd Version) |
   * Data(2nd Version) ... |
   * |--------------------------------------------------------------------------------------------------------------
   * 
   * @param value row value
   * @param numOfColumns
   * @return returns {@link MTableRow} instance created from input value
   */
  public static MTableRow createFromRawValue(byte[] value, int numOfColumns) {
    return new MTableRow(value, true, numOfColumns, true);
  }

  /**
   * Write the column value this function is update both the segments of the byte buffer i.e. Length
   * Segment and Data Segment.
   *
   * @param columnValue
   */
  public void writeColumn(byte[] columnValue) {
    if (columnValue == null) {
      /* Write the length in Length Segment for this column */
      Bytes.putInt(this.buffer, this.lengthPos, 0);
      this.lengthPos += Bytes.SIZEOF_INT;

    } else {
      /* Write the length in Length Segment for this column */
      Bytes.putInt(this.buffer, this.lengthPos, columnValue.length);
      this.lengthPos += Bytes.SIZEOF_INT;

      int pos = this.ensureCapacity(columnValue.length, this.dataPos);
      System.arraycopy(columnValue, 0, buffer, pos, columnValue.length);
      this.dataPos = pos + columnValue.length;
    }
  }


  /**
   * Write the column value this function is update both the segments of the byte buffer i.e. Length
   * Segment and Data Segment.
   *
   * @param inColumnValue
   */
  public void writeColumn(MColumnDescriptor columnDescriptors, Object inColumnValue) {
    if (inColumnValue == null) {
      /* Write the length in Length Segment for this column */
      Bytes.putInt(this.buffer, this.lengthPos, 0);
      this.lengthPos += Bytes.SIZEOF_INT;
    } else {
      DataType type = columnDescriptors.getColumnType();

      byte[] columnValue;
      /** use binary serialization for empty byte-arrays else use as is.. **/
      if (inColumnValue instanceof byte[] && !BasicTypes.BINARY.equals(type)) {
        columnValue = (byte[]) inColumnValue;
      } else {
        columnValue = type.serialize(inColumnValue);
      }

      /* Write the length in Length Segment for this column */
      Bytes.putInt(this.buffer, this.lengthPos, columnValue.length);
      this.lengthPos += Bytes.SIZEOF_INT;

      int pos = this.ensureCapacity(columnValue.length, this.dataPos);
      System.arraycopy(columnValue, 0, buffer, pos, columnValue.length);
      this.dataPos = pos + columnValue.length;
    }
  }

  /**
   * Return the column value at the requested position
   **/
  public byte[] getColumnAt(int position) {
    if (position > this.numOfColumns) {
      throw new RuntimeException("Invalid position requested");
    }

    int length = 0;
    int dataPosition = this.dataPos;

    if (position == 0) {
      /**
       * CASE : when the position requested is 0, then just read the first length from
       * this.lengthPosition.
       */
      // Reading Length
      length = Bytes.toInt(this.buffer, this.lengthPos);
    } else {
      /**
       * CASE : Column Position is greater than 0 Iterate over the value till the position is
       * reached.
       */
      int lenghPosition = this.lengthPos;
      for (int i = 0; i < position; i++) {
        // Reading Length
        length = Bytes.toInt(this.buffer, lenghPosition);
        dataPosition += length;
        lenghPosition += 4;
      }
      // Reading Length
      length = Bytes.toInt(this.buffer, lenghPosition);
    }

    byte[] t = null;
    try {
      t = Arrays.copyOfRange(this.buffer, dataPosition, dataPosition + length);
    } catch (Error e) {
      logger.error("BUFFER" + Arrays.toString(this.buffer));
      logger.error("MTableRow.getColumnAt.dataPosition => " + dataPosition + "  length " + length
          + " position -> " + position);
    } catch (Exception ex) {
      logger.error("BUFFER" + Arrays.toString(this.buffer));
      logger.error("MTableRow.getColumnAt.dataPosition => " + dataPosition + "  length " + length
          + " position -> " + position);
    }

    return t;
  }

  /**
   * Helper function for south layer to get the value of the object.
   * 
   * @param tableDescriptor
   * @return returns MColumnDescriptor to column value map
   */
  public Map<MColumnDescriptor, Object> getColumnValue(MTableDescriptor tableDescriptor) {
    if (tableDescriptor == null) {
      return null;
    }
    Map<MColumnDescriptor, Object> result = new LinkedHashMap<>();

    tableDescriptor.getAllColumnDescriptors().forEach((C) -> {
      DataType type = C.getColumnType();
      int position = tableDescriptor.getColumnDescriptorsMap().get(C);
      byte[] buffer = getColumnAt(position);
      if (buffer.length == 0) {
        result.put(C, null);
      } else {
        Object value = type.deserialize(buffer);
        result.put(C, value);
      }
    });

    return result;
  }


  /**
   * Helper function for south layer to get the value of the object.
   * 
   * @param tableDescriptor
   * @return returns columnname to columnvalue map
   */
  public Map<String, byte[]> getColumnNameValue(MTableDescriptor tableDescriptor) {
    if (tableDescriptor == null) {
      return null;
    }
    Map<String, byte[]> result = new LinkedHashMap<>();
    tableDescriptor.getAllColumnDescriptors().forEach((C) -> {
      int position = tableDescriptor.getColumnDescriptorsMap().get(C);
      result.put(C.getColumnNameAsString(), getColumnAt(position));
    });
    return result;
  }


  public byte[] getByteArray() {
    return this.buffer;
  }
}
