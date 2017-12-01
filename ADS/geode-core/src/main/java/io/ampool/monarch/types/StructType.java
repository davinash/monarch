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

package io.ampool.monarch.types;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Predicate;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.types.interfaces.TypePredicate;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.interfaces.TypePredicateOp;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataSerializable;

/**
 * Implementation for Struct object type.. It takes two arguments list of columns and list of
 * respective types. The value for this object type is an array of values in the same order of
 * column names/types. The serialization and deserialization consumes and produces the data in the
 * same order of types that was provided when creating the type.
 * <p>
 *
 * Since version: 0.2.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class StructType implements DataType, TypePredicate, VersionedDataSerializable {
  public static final String NAME = "struct";
  private static final long serialVersionUID = 5358743143115677211L;
  private String[] columnNames;
  private DataType[] columnTypes;

  public StructType() {
    ////
  }

  /**
   * Construct the Struct object-type from the provided inputs.
   *
   * @param columnNames an array of column names
   * @param columnTypes an array of the respective column types
   */
  public StructType(final String[] columnNames, final DataType[] columnTypes) {
    if (columnNames.length == 0) {
      throw new IllegalArgumentException("MStructObjectType: Must have at least one column.");
    }
    if (columnNames.length != columnTypes.length) {
      throw new IllegalArgumentException(
          "MStructObjectType: Column name and type should be of same length.");
    }
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
  }

  /**
   * Provide the required predicate for execution. It takes following things into consideration: -
   * the predicate operation {@link CompareOp} - the initial value, all the values shall be compared
   * against this value
   *
   * @param op the predicate operation
   * @param initialValue the (initial) value to be tested against using this predicate
   * @return a predicate that tests the two arguments for the specified operation
   */
  @Override
  public Predicate getPredicate(final TypePredicateOp op, final Object initialValue) {
    return null;
  }

  /**
   * Construct and get the object with correct type from bytes.
   *
   * @param bytes an array of bytes representing an object
   * @param offset offset in the byte-array
   * @param length number of bytes, from offset, to read from the byte-array
   * @return the actual Java object with the respective type
   */
  @Override
  public Object deserialize(final byte[] bytes, final Integer offset, final Integer length) {
    if (length <= 0) {
      return null;
    }
    int size = TypeHelper.readInt(bytes, offset);
    Object[] array = new Object[size];
    int off = offset + TypeHelper.IntegerLength;
    int len;
    for (int i = 0; i < size; i++) {
      len = TypeHelper.readInt(bytes, off);
      off += TypeHelper.IntegerLength;
      array[i] = columnTypes[i].deserialize(bytes, off, len);
      off += len;
    }
    return array;
  }


  /**
   * Convert the Java object into bytes correctly for the specified type.
   *
   * @param object the actual Java object
   * @return the bytes representing object
   */
  @Override
  public byte[] serialize(Object object) {
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    if (object != null && object instanceof Object[]) {
      Object[] array = (Object[]) object;
      assert columnNames.length == array.length;
      TypeHelper.writeInt(bos, array.length);
      for (int i = 0; i < columnNames.length; i++) {
        TypeHelper.writeBytes(bos, columnTypes[i].serialize(array[i]));
      }
    }
    return bos.toByteArray();
  }

  /**
   * The category of this object type.
   *
   * @return Category.struct
   */
  @Override
  public Category getCategory() {
    return Category.Struct;
  }

  /**
   * The string representation of the type.
   *
   * @return the string representation of this object
   */
  public String toString() {
    StringBuilder sb = new StringBuilder(32);
    sb.append(NAME).append(COMPLEX_TYPE_BEGIN_CHAR);
    for (int i = 0; i < columnNames.length; i++) {
      sb.append(columnNames[i]).append(COMPLEX_TYPE_NAME_TYPE_SEPARATOR)
          .append(columnTypes[i].toString()).append(COMPLEX_TYPE_SEPARATOR);
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.append(COMPLEX_TYPE_END_CHAR);
    return sb.toString();
  }

  /**
   * Return whether the specified object-type is equal-to this object-type.
   *
   * @param other the other object to be compared
   * @return true if the other object is same as this; false otherwise
   */
  public boolean equals(final Object other) {
    return this == other || (other instanceof StructType
        && Arrays.deepEquals(columnNames, ((StructType) other).columnNames)
        && Arrays.deepEquals(columnTypes, ((StructType) other).columnTypes));
  }

  /**
   * Provide the sub-types of this type..
   *
   * @return an array of sub-types of the struct
   */
  public DataType[] getColumnTypes() {
    return this.columnTypes;
  }

  public String[] getColumnNames() {
    return this.columnNames;
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
    DataSerializer.writeStringArray(this.columnNames, out);
    DataSerializer.writeObjectArray(this.columnTypes, out);
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
    this.columnNames = DataSerializer.readStringArray(in);
    this.columnTypes = (DataType[]) DataSerializer.readObjectArray(in);
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
