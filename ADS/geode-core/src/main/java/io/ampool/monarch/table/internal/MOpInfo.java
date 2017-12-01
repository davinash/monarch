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
import java.util.List;

import io.ampool.monarch.types.TypeHelper;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;

/**
 * A wrapper class holding the required data for any operation like put/check-and-put. It wraps the
 * following: - the current timestamp - the list of columns being modified during partial put or
 * delete - the {@link MOperation} being executed - the condition to be checked before the operation
 * (like checkAndPut or checkAndDelete)
 * <p>
 */
public class MOpInfo implements DataSerializableFixedID {
  /**
   * A simple class indicating the check condition.. It consists of a pair with column-index and
   * respective value to be compared against.
   */
  public static final class MCondition implements DataSerializableFixedID {
    private int columnId;
    private Object columnValue;

    public MCondition() {
      //// For DataSerializer..
    }

    /**
     * Constructor..
     *
     * @param columnId the column index
     * @param columnValue the column value
     */
    public MCondition(final int columnId, final Object columnValue) {
      this.columnId = columnId;
      this.columnValue = columnValue;
    }

    /**
     * Get the column index.
     *
     * @return the column index
     */
    public int getColumnId() {
      return columnId;
    }

    /**
     * Get the column value.
     *
     * @return the column value
     */
    public Object getColumnValue() {
      return columnValue;
    }

    @Override
    public String toString() {
      return "<columnId=" + columnId + ", columnValue=" + TypeHelper.deepToString(columnValue)
          + '>';
    }

    @Override
    public int getDSFID() {
      return AMPL_MCONDITION;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeInteger(columnId, out);
      DataSerializer.writeObject(columnValue, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.columnId = DataSerializer.readInteger(in);
      this.columnValue = DataSerializer.readObject(in);
    }

    @Override
    public Version[] getSerializationVersions() {
      return null;
    }
  }

  /** private members.. **/
  private MOperation op = MOperation.NONE;
  private long timestamp = 0L;
  private MCondition condition = null;
  private List<Integer> columnList = new ArrayList<>(10);

  public MOpInfo() {
    /// For DataSerializer
  }

  /** setters to be used only within package.. **/
  protected void setOp(final MOperation op) {
    this.op = op;
  }

  public void setTimestamp(final long timestamp) {
    this.timestamp = timestamp;
  }

  protected void setCondition(final int columnId, final Object columnValue) {
    this.condition = new MCondition(columnId, columnValue);
  }

  protected void addColumn(final int columnId) {
    this.columnList.add(columnId);
  }

  /**
   * Provide the list of columns being modified.
   *
   * @return the list of column ids
   */
  public List<Integer> getColumnList() {
    return columnList;
  }

  /**
   * Provide the respective condition including the column-id and the value to be compared
   * (byte-array).
   *
   * @return a pair containing column-id and the value to be compared
   */
  public MCondition getCondition() {
    return condition;
  }

  /**
   * Provide the timestamp.
   *
   * @return the timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Provide the operation type.
   *
   * @return the operation type
   */
  public MOperation getOp() {
    return op;
  }

  /**
   * Create the object from the key (i.e. {@link MTableKey}. It wraps the required information from
   * key and passes on.
   *
   * @param key the key with required details
   * @return the operation information
   */
  public static MOpInfo fromKey(final MTableKey key) {
    MOpInfo opInfo = new MOpInfo();
    List<Integer> list = key.getColumnPositions();
    opInfo.timestamp = key.getTimeStamp();

    opInfo.op = key.isPutOp() ? MOperation.PUT
        : key.isCheckAndPutOp() ? MOperation.CHECK_AND_PUT
            : key.isDeleteOp() ? MOperation.DELETE
                : key.isCheckAndDeleteOp() ? MOperation.CHECK_AND_DELETE : MOperation.NONE;

    /** in case of check-and-<op> the first column was always of the condition.. normalize it **/
    if (opInfo.op == MOperation.CHECK_AND_DELETE || opInfo.op == MOperation.CHECK_AND_PUT) {
      if (list == null || list.size() == 0) {
        throw new IllegalArgumentException("The column-index must be provided for: " + opInfo.op);
      }
      opInfo.columnList = new ArrayList<>(list.subList(1, list.size()));
      opInfo.condition = new MCondition(list.get(0), key.getColumnValue());
    } else {
      opInfo.columnList = list == null ? new ArrayList<>() : list;
    }

    return opInfo;
  }

  /**
   * Create the object from value. It gathers the information, available in value, and creates this
   * object and passes it on.
   *
   * @param value the value object with required details
   * @return the operation information
   */
  public static MOpInfo fromValue(final MValue value) {
    return value.getOpInfo();
  }

  @Override
  public String toString() {
    return "{op=" + op + ", timestamp=" + timestamp + ", condition=" + condition + ", columnList="
        + columnList.toString() + '}';
  }


  @Override
  public int getDSFID() {
    return AMPL_MOPINFO;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeByte(op.ordinalByte(), out);
    DataSerializer.writeLong(timestamp, out);
    DataSerializer.writeObject(condition, out);
    DataSerializer.writeArrayList((ArrayList) columnList, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.op = MOperation.valueOf(DataSerializer.readByte(in));
    this.timestamp = DataSerializer.readLong(in);
    this.condition = DataSerializer.readObject(in);
    this.columnList = DataSerializer.readArrayList(in);
  }



  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
