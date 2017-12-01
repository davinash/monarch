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

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.interfaces.DataType;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * The wrapper class to hold the actual value (byte-array of columns) and additional information
 * that need to be passed per value.
 *
 */
public final class MValue implements DataSerializableFixedID {

  private byte encoding = 0;
  private byte[][] data;
  protected int length = 0;
  /**
   * operation information
   **/
  private MOpInfo opInfo = new MOpInfo();

  public MValue() {
    //// For DataSerializer..
  }

  public MValue(final byte encoding, final byte operation, final int numberOfColumns) {
    this.encoding = encoding;
    this.opInfo.setOp(MOperation.valueOf(operation));
    this.data = new byte[numberOfColumns][];
  }

  /**
   * Create the value object from {@link Put}
   *
   * @param td the table descriptor
   * @param put an existing put object
   * @param op the operation
   * @return the required information for put wrapped in this
   */
  public static MValue fromPut(final MTableDescriptor td, final Put put, final MOperation op) {
    MValue val = new MValue((byte) 0, op.ordinalByte(), td.getNumOfColumns());
    val.setTimestamp(put.getTimeStamp());
    Map<MColumnDescriptor, Integer> columnToIdMap = td.getColumnDescriptorsMap();

    MColumnDescriptor cd;
    int columnId;
    DataType type;
    byte[] columnValue;
    Object inColumnValue;
    ByteArrayKey bak = new ByteArrayKey();
    for (Map.Entry<MColumnDescriptor, Integer> entry : columnToIdMap.entrySet()) {
      cd = entry.getKey();
      columnId = entry.getValue();
      type = cd.getColumnType();
      bak.setData(cd.getColumnName());
      if (put.getColumnValueMap().containsKey(bak)) {
        inColumnValue = put.getColumnValueMap().get(bak);
        /** use binary serialization for empty byte-arrays else use as is.. **/
        if (inColumnValue instanceof byte[] && !BasicTypes.BINARY.equals(type)) {
          columnValue = (byte[]) inColumnValue;
        } else {
          columnValue = type.serialize(inColumnValue);
        }
        if (columnValue != null)
          val.setColumn(columnId, columnValue);
      }
    }
    return val;
  }

  /**
   * Create the value object from {@link io.ampool.monarch.table.ftable.Record}
   *
   * @param td the table descriptor
   * @param record an existing put object
   * @param op the operation
   * @return the required information for put wrapped in this
   */
  public static MValue fromRecord(final FTableDescriptor td, final Record record,
      final MOperation op) {
    MValue val = new MValue((byte) 0, op.ordinalByte(), td.getNumOfColumns());
    Map<MColumnDescriptor, Integer> columnToIdMap = td.getColumnDescriptorsMap();

    MColumnDescriptor cd;
    int columnId;
    DataType type;
    byte[] columnValue;
    Object inColumnValue;
    ByteArrayKey bak = new ByteArrayKey();
    for (Map.Entry<MColumnDescriptor, Integer> entry : columnToIdMap.entrySet()) {
      cd = entry.getKey();
      columnId = entry.getValue();
      type = cd.getColumnType();
      bak.setData(cd.getColumnName());
      final Map<ByteArrayKey, Object> valueMap = record.getValueMap();
      if (valueMap.containsKey(bak)) {
        inColumnValue = valueMap.get(bak);
        /** use binary serialization for empty byte-arrays else use as is.. **/
        if (inColumnValue instanceof byte[] && !BasicTypes.BINARY.equals(type)) {
          columnValue = (byte[]) inColumnValue;
        } else {
          columnValue = type.serialize(inColumnValue);
        }
        val.setColumn(columnId, columnValue);
      }
    }
    return val;
  }

  /**
   * Get the value represented as a single byte-array beginning with timestamp and subsequently
   * followed by length of all columns followed by column values.
   *
   * @return the row represented as single byte-array
   * @deprecated
   */
  public byte[] getValue() {
    byte[] bytes = new byte[length + Long.BYTES + (data.length * Integer.BYTES)];
    int lenPos = Long.BYTES;
    int dataPos = lenPos + (data.length * Integer.BYTES);
    Bytes.putLong(bytes, 0, opInfo.getTimestamp());
    for (byte[] bs : this.data) {
      if (bs != null) {
        Bytes.putInt(bytes, lenPos, bs.length);
        Bytes.putBytes(bytes, dataPos, bs, 0, bs.length);
        dataPos += bs.length;
      } else {
        Bytes.putInt(bytes, lenPos, 0);
      }
      lenPos += Integer.BYTES;
    }
    return bytes;
  }

  /** delegated to MOpInfo **/
  public MOpInfo getOpInfo() {
    return this.opInfo;
  }

  public void setTimestamp(final long timestamp) {
    this.opInfo.setTimestamp(timestamp);
  }

  public void setCondition(final int columnId, final Object columnValue) {
    this.opInfo.setCondition(columnId, columnValue);
  }

  public void setColumn(final int columnIdx, final byte[] value) {
    this.opInfo.addColumn(columnIdx);
    this.length += value.length;
    this.data[columnIdx] = value;
  }

  public int getLength() {
    return this.length;
  }

  @Override
  public int getDSFID() {
    return AMPL_MVALUE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeByte(encoding, out);
    DataSerializer.writeInteger(length, out);
    DataSerializer.writeObject(opInfo, out);
    DataSerializer.writeArrayOfByteArrays(data, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.encoding = DataSerializer.readByte(in);
    this.length = DataSerializer.readInteger(in);
    this.opInfo = DataSerializer.readObject(in);
    this.data = DataSerializer.readArrayOfByteArrays(in);
  }


  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  public byte[][] getData() {
    return this.data;
  }

  /**
   * Returns length of value
   *
   * @return Length of value
   */
  public int getValueLength() {
    return this.length;
  }
}
