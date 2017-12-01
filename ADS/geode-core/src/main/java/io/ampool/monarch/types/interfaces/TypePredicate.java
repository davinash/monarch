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

import java.util.function.Predicate;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.BasicTypes;

/**
 * An interface dealing with predicate operations for the respective data types.
 * <p>
 * Since version: 0.2.0
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface TypePredicate {
  /**
   * Provide the required predicate for execution. It takes following things into consideration: -
   * the argument type {@link BasicTypes} - the predicate operation {@link CompareOp} - the initial
   * value, all the values shall be compared against this value
   *
   * @param op the predicate operation
   * @param initialValue the (initial) value to be tested against using this predicate
   * @return a predicate that tests the two arguments for the specified operation
   */
  Predicate getPredicate(TypePredicateOp op, Object initialValue);
}
