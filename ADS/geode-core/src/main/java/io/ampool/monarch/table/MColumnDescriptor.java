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
package io.ampool.monarch.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.exceptions.IllegalColumnNameException;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.DataTypeFactory;
import io.ampool.monarch.types.interfaces.DataType;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataSerializable;

/**
 * An MColumnDescriptor contains information about a column It is used as input when creating a
 * table or adding a column.
 *
 * @since 0.2.0.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class MColumnDescriptor implements VersionedDataSerializable {

  private static final long serialVersionUID = -856983533278200205L;
  /**
   * Type of the column, default type will be binary which is byte[]
   */
  private DataType columnType;
  /**
   * Name of the column
   */
  private byte[] columnName;

  /**
   * Index of the column in the schema.
   */
  private int index;

  /**
   * Maximum length of the column Name. SQL currently have maximum length equal to 128.
   */
  private final static int MAX_COLUMN_LENGTH = 128;

  public MColumnDescriptor() {}

  public MColumnDescriptor(final String columnName, final DataType columnType, final int index)
      throws IllegalColumnNameException {
    if (columnName == null || columnName.isEmpty() || columnName.length() > MAX_COLUMN_LENGTH) {
      throw new IllegalColumnNameException("Invalid ColumnName: `" + columnName
          + "`: Column-name should have length between 1 and " + MAX_COLUMN_LENGTH);
    }
    this.columnName = Bytes.toBytes(columnName);
    this.columnType = columnType;
    this.index = index;
  }

  /**
   * Get the index of this column in the table schema/descriptor.
   *
   * @return the position of this column in the schema
   */
  public int getIndex() {
    return this.index;
  }

  public void setIndex(final int index) {
    this.index = index;
  }

  /**
   * Construct a MColumnDescriptor object using ColumnName and column type ID string; also does
   * verification.
   * 
   * @param columnName Name of the column
   * @param columnTypeId Type of the column See{@link DataType}
   * @throws IllegalColumnNameException throws in following conditions 1. ColumnName is null 2.
   *         ColumnName is Empty. 3. ColumnName length exceeding 128.
   * @throws IllegalArgumentException throws in following conditions 1. Column type string cannot be
   *         parsed to a valid DataType.
   */
  public MColumnDescriptor(final String columnName, final String columnTypeId)
      throws IllegalColumnNameException {
    if (columnName != null && !columnName.isEmpty()) {
      this.columnName = Bytes.toBytes(columnName);
      this.columnType = DataTypeFactory.getTypeFromString(columnTypeId);
    } else {
      throw new IllegalColumnNameException("Empty or Null columnName is not allowed");
    }
    if (columnName.length() > MAX_COLUMN_LENGTH) {
      throw new IllegalColumnNameException(
          "ColumnName length is greater than Maximum Allowed Length");
    }
  }

  /**
   * Construct a MColumnDescriptor object using ColumnName and the specified MTableColumnType, also
   * does verification.
   * 
   * @param columnName Name of the column
   * @param columnType Type of the column See{@link MTableColumnType}
   * @throws IllegalColumnNameException throws in following conditions 1. ColumnName is null 2.
   *         ColumnName is Empty. 3. ColumnName length exceeding 128.
   */
  public MColumnDescriptor(final String columnName, final MTableColumnType columnType)
      throws IllegalColumnNameException {
    if (columnName == null || columnName.isEmpty()) {
      throw new IllegalColumnNameException("Empty or Null columnName is not allowed");
    } else if (columnName.length() > MAX_COLUMN_LENGTH) {
      throw new IllegalColumnNameException(
          "ColumnName length is greater than Maximum Allowed Length");
    }
    this.columnName = Bytes.toBytes(columnName);
    if (columnType != null) {
      this.columnType = columnType.getObjectType();
    } else {
      this.columnType = BasicTypes.BINARY;
    }
  }

  /**
   * Construct a MColumnDescriptor object using ColumnName and the specified MTableColumnType, also
   * does verification.
   * 
   * @param columnName Name of the column
   * @param columnType Type of the column See{@link MTableColumnType}
   * @throws IllegalColumnNameException throws in following conditions 1. ColumnName is null 2.
   *         ColumnName is Empty. 3. ColumnName length exceeding 128.
   */
  public MColumnDescriptor(final byte[] columnName, final MTableColumnType columnType)
      throws IllegalColumnNameException {
    if (columnName == null || columnName.length <= 0) {
      throw new IllegalColumnNameException("Empty or Null columnName is not allowed");
    } else {
      this.columnName = columnName;
      if (columnType != null) {
        this.columnType = columnType.getObjectType();
      } else {
        this.columnType = BasicTypes.BINARY;
      }
    }
  }

  /**
   * Construct a MColumnDescriptor with default Type to be BasicType.BINARY.
   * 
   * @param columnName
   */
  public MColumnDescriptor(byte[] columnName) {
    this(columnName, BasicTypes.BINARY);
  }

  /**
   * Construct a MColumnDescriptor with default Type to be BasicType.BINARY.
   * 
   * @param columnName
   */
  public MColumnDescriptor(byte[] columnName, BasicTypes type) {
    if (columnName == null || columnName.length <= 0) {
      throw new IllegalColumnNameException("Empty or Null columnName is not allowed");
    } else {
      this.columnName = columnName;
    }
    if (type != null) {
      this.columnType = type;
    } else {
      this.columnType = BasicTypes.BINARY;
    }
  }

  /**
   * Get the column Name
   *
   * @return column Name
   */
  public byte[] getColumnName() {
    return this.columnName;
  }

  public String getColumnNameAsString() {
    return Bytes.toString(this.columnName);
  }

  /**
   * Set the column Name
   *
   * @param columnName columnName
   */
  public void setColumnName(byte[] columnName) {
    this.columnName = columnName;
  }

  public DataType getColumnType() {
    return this.columnType;
  }

  public void setColumnType(final DataType columnType) {
    this.columnType = columnType;
  }


  public int hashCode() {
    return Arrays.hashCode(this.columnName);
  }

  public boolean equals(Object other) {
    return other instanceof MColumnDescriptor
        && Arrays.equals(this.columnName, ((MColumnDescriptor) other).columnName);
  }

  /**
   * Writes the state of this object as primitive data to the given <code>DataOutput</code>.
   * <p>
   * Since 5.7 it is possible for any method call to the specified <code>DataOutput</code> to throw
   * {@link org.apache.geode.GemFireRethrowable}. It should <em>not</em> be caught by user code. If
   * it is it <em>must</em> be rethrown.
   *
   * @param out the data output stream
   * @throws IOException A problem occurs while writing to <code>out</code>
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(columnName, out);
    DataSerializer.writeObject(columnType, out);
    DataSerializer.writePrimitiveInt(index, out);
  }

  /**
   * Reads the state of this object as primitive data from the given <code>DataInput</code>.
   *
   * @param in the data input stream
   * @throws IOException A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException A class could not be loaded while reading from <code>in</code>
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.columnName = DataSerializer.readByteArray(in);
    this.columnType = DataSerializer.readObject(in);
    this.index = DataSerializer.readPrimitiveInt(in);
  }

  @Override
  public String toString() {
    return "MColumnDescriptor{" + "columnType=" + columnType + ", columnName="
        + Bytes.toString(columnName) + ", columnIndex=" + this.index + '}';
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
