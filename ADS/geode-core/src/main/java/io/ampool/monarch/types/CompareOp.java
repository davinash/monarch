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
import io.ampool.monarch.types.interfaces.TypePredicateOp;

/**
 * Predicates to be used to evaluate expressions.
 *
 * Since version: 0.2.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum CompareOp implements TypePredicateOp {
  /**
   * Predicate corresponding to '<' operation.
   */
  LESS,

  /**
   * Predicate corresponding to '<=' operation.
   */
  LESS_OR_EQUAL,

  /**
   * Predicate corresponding to '>' operation.
   */
  GREATER,

  /**
   * Predicate corresponding to '>=' operation.
   */
  GREATER_OR_EQUAL,

  /**
   * Predicate corresponding to '==' operation.
   */
  EQUAL,

  /**
   * Predicate corresponding to '!=' operation.
   */
  NOT_EQUAL,

  /**
   * Predicate for Regex operation. This will only work with String.
   */
  REGEX
}
