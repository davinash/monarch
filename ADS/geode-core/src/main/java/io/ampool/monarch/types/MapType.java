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
import java.util.LinkedHashMap;
import java.util.Map;
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
 * Implementation for Map object type..
 * <p>
 * Since version: 0.2.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MapType implements DataType, TypePredicate, VersionedDataSerializable {
  public static final String NAME = "map";
  private static final long serialVersionUID = 6741918366100613714L;
  private DataType typeOfKey;
  private DataType typeOfValue;

  public MapType() {
    ////
  }

  public MapType(final DataType typeOfKey, final DataType typeOfValue) {
    this.typeOfKey = typeOfKey;
    this.typeOfValue = typeOfValue;
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
    /** return null for zero length.. **/
    if (length <= 0) {
      return null;
    }
    int size = TypeHelper.readInt(bytes, offset);
    Map<Object, Object> map = new LinkedHashMap<>(size);
    int off = offset + TypeHelper.IntegerLength;
    int len;
    for (int i = 0; i < size; i++) {
      /** read key **/
      len = TypeHelper.readInt(bytes, off);
      off += TypeHelper.IntegerLength;
      Object k = typeOfKey.deserialize(bytes, off, len);
      off += len;

      /** read value **/
      len = TypeHelper.readInt(bytes, off);
      off += TypeHelper.IntegerLength;
      Object v = typeOfValue.deserialize(bytes, off, len);

      map.put(k, v);
      off += len;
    }
    return map;
  }

  /**
   * Convert the Java object into bytes correctly for the specified type.
   *
   * @param object the actual Java object
   * @return the bytes representing object
   */
  @SuppressWarnings("unchecked")
  @Override
  public byte[] serialize(final Object object) {
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    if (object != null && object instanceof Map) {
      Map<Object, Object> map = (Map<Object, Object>) object;
      TypeHelper.writeInt(bos, map.size());
      map.forEach((k, v) -> {
        TypeHelper.writeBytes(bos, typeOfKey.serialize(k));
        TypeHelper.writeBytes(bos, typeOfValue.serialize(v));
      });
    }
    return bos.toByteArray();
  }

  @Override
  public Category getCategory() {
    return Category.Map;
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
  public Predicate getPredicate(TypePredicateOp op, Object initialValue) {
    return null;
  }

  public String toString() {
    return NAME + COMPLEX_TYPE_BEGIN_CHAR + typeOfKey.toString() + COMPLEX_TYPE_SEPARATOR
        + typeOfValue.toString() + COMPLEX_TYPE_END_CHAR;
  }

  public boolean equals(final Object other) {
    return this == other
        || (other instanceof MapType && typeOfKey.equals(((MapType) other).typeOfKey)
            && typeOfValue.equals(((MapType) other).typeOfValue));
  }

  public DataType getTypeOfKey() {
    return this.typeOfKey;
  }

  public DataType getTypeOfValue() {
    return this.typeOfValue;
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
    DataSerializer.writeObject(this.typeOfKey, out);
    DataSerializer.writeObject(this.typeOfValue, out);
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
    this.typeOfKey = DataSerializer.readObject(in);
    this.typeOfValue = DataSerializer.readObject(in);
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
