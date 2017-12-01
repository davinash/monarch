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

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.types.interfaces.TypePredicate;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.interfaces.TypePredicateOp;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * The container class to hold the required attributes for predicate test.
 *
 * Since version: 0.2.0
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class MPredicateHolder implements Serializable {

  private static final long serialVersionUID = -3690903064402957047L;
  public int columnIdx;
  public Object arg2;
  public DataType argType;
  public TypePredicateOp operation;

  /**
   * The container to hold the required attributes for predicate test.
   *
   * @param columnIdx the column-index to be used for test
   * @param argType the type of the column to be used for test
   * @param operation the predicate operation to be tested (like arithmetic operations or equals)
   * @param arg2 the constant value to be tested against
   */
  public MPredicateHolder(final int columnIdx, final DataType argType,
      final TypePredicateOp operation, final Object arg2) {
    Objects.requireNonNull(arg2, "Argument value cannot be null.");
    this.columnIdx = columnIdx;
    this.argType = argType;
    this.operation = operation;
    this.arg2 = arg2;
  }

  /**
   * Provide the required predicate for execution. It takes following things into consideration: -
   * the argument type {@link BasicTypes} - the predicate operation {@link CompareOp} - the initial
   * value, all the values shall be compared against this value
   *
   * @return a predicate that tests the two arguments for the specified operation
   */
  public Predicate getPredicate() {
    if (!(argType instanceof TypePredicate)) {
      throw new IllegalArgumentException("Must implement IMObjectPredicate: " + argType.getClass());
    }
    return ((TypePredicate) argType).getPredicate(operation, arg2);
  }

  /**
   * Provide the {@link BasicTypes} used for executing specified predicate operations.
   *
   * @return the object type of the arguments used to test with this predicate
   */
  public DataType getArgObjectType() {
    return argType;
  }

  /**
   * String representation of the predicate.
   *
   * @return the string representing predicate, displays all its attributes
   */
  public String toString() {
    return String.format("argType= %s, columnIdx= %d, operation= %s, arg2= %s", argType.toString(),
        columnIdx, operation.toString(), String.valueOf(arg2));
  }
}
