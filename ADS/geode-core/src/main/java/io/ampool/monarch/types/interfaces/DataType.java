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

package io.ampool.monarch.types.interfaces;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.types.CompareOp;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * An interface dealing with serialization/de-serialization of the data stored in Monarch. One can
 * implement the specific serialization/de-serialization logic to interpret the data stored in
 * Monarch regions so that the necessary predicates (filters/functions) can be executed. This can
 * help in filtering out the unwanted rows/records that match/does-not-match a specific condition.
 * Along with ser-de one should implement a method that provides a {@link Predicate} to test the
 * required condition {@link CompareOp} with the specified arguments.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface DataType extends Serializable {

  Object DEFAULT_VALUE = 0;

  /**
   * The type definition begin character when defining a complex type.
   */
  char COMPLEX_TYPE_BEGIN_CHAR = '<';
  /**
   * The type definition end character when defining a complex type.
   */
  char COMPLEX_TYPE_END_CHAR = '>';
  /**
   * The separator used to separate sub-types when defining a complex type.
   */
  char COMPLEX_TYPE_SEPARATOR = ',';
  /**
   * The separator used to separate column-name and column-type when defining a complex type. This
   * is for the types with named columns (like Struct or Union) with respective types.
   */
  char COMPLEX_TYPE_NAME_TYPE_SEPARATOR = ':';
  /**
   * The character indicates beginning of arguments being passed to the type. Anything specified
   * between begin and end characters is provided to the type.
   */
  char TYPE_ARGS_BEGIN_CHAR = '(';
  /**
   * This character indicates end of arguments being passed to the type. It should be the last
   * character.
   */
  char TYPE_ARGS_END_CHAR = ')';

  /**
   * Construct and get the object with correct type from bytes. It just calls the other method with
   * offset and length.
   * 
   * @see DataType#deserialize(byte[], Integer, Integer)
   *
   * @param bytes the bytes representing an object
   * @return the actual Java object with the respective type
   */
  default Object deserialize(final byte[] bytes) {
    return deserialize(bytes, 0, bytes.length);
  }

  /**
   * Construct and get the object with correct type from bytes of specified length from the provided
   * offset.
   *
   * @param bytes an array of bytes representing an object
   * @param offset offset in the byte-array
   * @param length number of bytes, from offset, to read from the byte-array
   * @return the actual Java object with the respective type
   */
  Object deserialize(final byte[] bytes, final Integer offset, final Integer length);

  /**
   * Convert the Java object into bytes correctly for the specified type.
   *
   * @param object the actual Java object
   * @return the bytes representing object
   */
  byte[] serialize(final Object object);

  enum Category {
    Basic, List, Map, Struct, Union
  }

  /**
   * Returns the Category See {@link Category}
   * 
   * @return category.
   */
  Category getCategory();

  /**
   * Whether this is column is fixed length column or not.
   * 
   * @return Boolean suggesting if it is the fixed length data type .
   */
  default boolean isFixedLength() {
    return false;
  }

  /**
   * If this type is fixed length type then what is length of byte array it serializes too Otherwise
   * return -1 for variable length
   * 
   * @return Max possible length of serialized byte array for this data type.
   */
  default int lengthOfByteArray() {
    return -1;
  }

  /**
   * If fixed length type then return default value
   *
   * @return Default value.
   */
  default Object defaultValue() {
    return null;
  }

}
