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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataSerializable;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;

/**
 * This class represents the key which will be used to store the data in underlying region.
 * MTableKey is wrapper over the byte[] with hashCode and equals implemented. This key is also used
 * to carry more information from client in case of Partial list of columns and checkAndPut and its
 * variant operations.
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MTableKey implements IMKey, VersionedDataSerializable {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = 2427319789257652397L;
  private byte[] data;
  private long timeStamp;

  public static final int KEY_OPERATION_MASKS = 0xFF;
  public static final int KEY_OPERATION_GET = 2;
  public static final int KEY_OPERATION_PUT = 1;
  public static final int KEY_OPERATION_DELETE = 4;
  public static final int KEY_OPERATION_CHECK_AND_PUT = 8;
  public static final int KEY_OPERATION_CHECK_AND_DELETE = 9;

  public int operationMode = 0;

  private int maxVersions = 1;
  /**
   * This is used to selective column gets
   */
  private List<Integer> columnPositions;

  /*
   * This is used for checkAndPut/Get/Delete variant operations.
   */
  private byte[] columnValue = null;

  public int getNumberofColumns() {
    return numberofColumns;
  }

  public void setNumberofColumns(int numberofColumns) {
    this.numberofColumns = numberofColumns;
  }

  /**
   * To be used by checkAndDelete
   */
  private int numberofColumns = 0;

  public MTableKey(byte[] rowKey) {
    this.data = rowKey;
    this.columnPositions = new ArrayList<>();
  }

  public MTableKey(final ArrayList<Integer> columnPositions, final int operation) {
    this.data = null;
    this.operationMode |= operation;
    this.columnPositions = columnPositions;
  }

  public MTableKey() {}

  @Override
  public String toString() {
    return "MTableKey={key=" + BasicTypes.STRING.deserialize(data) + ", hashCode=" + hashCode()
        + "}";
  }

  public byte[] getBytes() {
    return data;
  }

  public int hashCode() {
    return Arrays.hashCode(data);
  }

  public boolean equals(Object other) {
    return other instanceof IMKey && Arrays.equals(data, ((IMKey) other).getBytes());
  }

  public void resetColumnPositions() {
    this.columnPositions.clear();
  }

  /**
   * Sets if the operation is PUT when the key is set
   */
  public void setIsPutOp() {
    this.operationMode |= KEY_OPERATION_PUT;
  }

  /**
   * Sets if the operation is GET when the key is set
   */
  public void setIsGetOp() {
    this.operationMode |= KEY_OPERATION_GET;
  }

  /**
   * Sets if the operation is DELETE when the key is set
   */
  public void setIsDeleteOp() {
    this.operationMode |= KEY_OPERATION_DELETE;
  }

  /**
   * Set the key mode to Check and Put
   */
  public void setIsCheckAndPutOp() {
    this.operationMode |= KEY_OPERATION_CHECK_AND_PUT;
  }

  /**
   * Set the key mode to Check and Delete
   */
  public void setIsCheckAndDeleteOp() {
    this.operationMode |= KEY_OPERATION_CHECK_AND_DELETE;
  }


  public boolean isDeleteOp() {
    if ((operationMode & KEY_OPERATION_MASKS) == KEY_OPERATION_DELETE) {
      return true;
    }
    return false;
  }

  public boolean isPutOp() {
    if ((operationMode & KEY_OPERATION_MASKS) == KEY_OPERATION_PUT) {
      return true;
    }
    return false;
  }

  public boolean isGetOp() {
    if ((operationMode & KEY_OPERATION_MASKS) == KEY_OPERATION_GET) {
      return true;
    }
    return false;
  }

  public boolean isCheckAndDeleteOp() {
    if ((operationMode & KEY_OPERATION_MASKS) == KEY_OPERATION_CHECK_AND_DELETE) {
      return true;
    }
    return false;
  }

  public boolean isCheckAndPutOp() {
    if ((operationMode & KEY_OPERATION_MASKS) == KEY_OPERATION_CHECK_AND_PUT) {
      return true;
    }
    return false;
  }


  @Override
  public void toData(DataOutput dataOutput) throws IOException {
    DataSerializer.writeByteArray(this.data, dataOutput);
    DataSerializer.writeArrayList((ArrayList<Integer>) this.columnPositions, dataOutput);
    DataSerializer.writeLong(this.timeStamp, dataOutput);
    DataSerializer.writeInteger(this.operationMode, dataOutput);
    DataSerializer.writeByteArray(this.columnValue, dataOutput);
    DataSerializer.writeInteger(this.numberofColumns, dataOutput);
    DataSerializer.writeInteger(this.maxVersions, dataOutput);

  }


  @Override
  public void fromData(DataInput dataInput) throws IOException, ClassNotFoundException {
    this.data = DataSerializer.readByteArray(dataInput);
    this.columnPositions = DataSerializer.readArrayList(dataInput);
    this.timeStamp = DataSerializer.readLong(dataInput);
    this.operationMode = DataSerializer.readInteger(dataInput);
    this.columnValue = DataSerializer.readByteArray(dataInput);
    this.numberofColumns = DataSerializer.readInteger(dataInput);
    this.maxVersions = DataSerializer.readInteger(dataInput);
  }


  @Override
  public int compareTo(IMKey o) {
    return Bytes.compareTo(this.getBytes(), o.getBytes());
  }

  public List<Integer> getColumnPositions() {
    return this.columnPositions;
  }

  public void setColumnPositions(List<Integer> columnPositions) {
    this.columnPositions = columnPositions;
  }

  public void addColumnPosition(int pos) {
    this.columnPositions.add(pos);
  }

  public void addColumnPositions(final Integer[] columnPositions) {
    Collections.addAll(this.columnPositions, columnPositions);
  }

  public void setKey(final byte[] rowKey) {
    this.data = rowKey;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public long getTimeStamp() {
    return this.timeStamp;
  }

  public byte[] getColumnValue() {
    return columnValue;
  }

  public void setColumnValue(byte[] columnValue) {
    this.columnValue = columnValue;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  public int getMaxVersions() {
    return maxVersions;
  }

  public void setMaxVersions(final int maxVersions) {
    this.maxVersions = maxVersions;
  }
}
