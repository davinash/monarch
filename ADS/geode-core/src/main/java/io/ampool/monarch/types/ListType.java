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
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
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
 * Implementation for List (Array) object type.
 *
 * Since version: 0.2.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ListType implements DataType, TypePredicate, VersionedDataSerializable {
  public static final String NAME = "array";
  private static final long serialVersionUID = 1706806567399347815L;
  private DataType typeOfElement;

  public ListType() {
    ////
  }

  public ListType(final DataType type) {
    this.typeOfElement = type;
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
    List<Object> list = new ArrayList<>(size);
    int off = offset + TypeHelper.IntegerLength;
    int len;
    for (int i = 0; i < size; i++) {
      len = TypeHelper.readInt(bytes, off);
      off += TypeHelper.IntegerLength;
      list.add(typeOfElement.deserialize(bytes, off, len));
      off += len;
    }
    return list;
  }

  /**
   * Convert the List of objects into bytes correctly using the specified type.
   *
   * @param object the actual Java object
   * @return the bytes representing object
   */
  @Override
  public byte[] serialize(final Object object) {
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    if (object != null) {
      if (object instanceof List) {
        List list = (List) object;
        TypeHelper.writeInt(bos, list.size());
        for (Object e : list) {
          TypeHelper.writeBytes(bos, typeOfElement.serialize(e));
        }
      } else if (object instanceof Object[] || object.getClass().isArray()) {
        final int size = Array.getLength(object);
        TypeHelper.writeInt(bos, size);
        for (int i = 0; i < size; i++) {
          TypeHelper.writeBytes(bos, typeOfElement.serialize(Array.get(object, i)));
        }
      }
    }
    return bos.toByteArray();
  }

  /**
   * Provide the required predicate for execution. It takes following things into consideration: -
   * the argument type {@link BasicTypes} - the predicate operation {@link CompareOp} - the initial
   * value, all the values shall be compared against this value
   *
   * @param op the predicate operation
   * @param initialValue the (initial) value to be tested against using this predicate
   * @return a predicate that tests the two arguments for the specified operation
   */
  @Override
  public Predicate getPredicate(TypePredicateOp op, Object initialValue) {
    return null;
  }

  @Override
  public Category getCategory() {
    return Category.List;
  }

  /**
   * Provide the type of the elements stored in list (array).
   *
   * @return the type of elements of the list
   */
  public DataType getTypeOfElement() {
    return typeOfElement;
  }

  public String toString() {
    return NAME + COMPLEX_TYPE_BEGIN_CHAR + typeOfElement.toString() + COMPLEX_TYPE_END_CHAR;
  }

  public boolean equals(final Object other) {
    return this == other
        || (other instanceof ListType && typeOfElement.equals(((ListType) other).typeOfElement));
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
    DataSerializer.writeObject(this.typeOfElement, out);
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
    this.typeOfElement = DataSerializer.readObject(in);
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
