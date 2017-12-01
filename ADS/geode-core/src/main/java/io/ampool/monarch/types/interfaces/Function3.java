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

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Function;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

/**
 * A functional interface with three arguments. A typical for deserialization method that takes
 * three arguments, like byte-array, offset in the array, and length of array, and returns the
 * object after deserialization..
 * <p>
 * <p>
 * Since version: 0.2.0
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
@FunctionalInterface
public interface Function3<T1, T2, T3, R> extends Serializable {
  /**
   * Applies this function to the given arguments. It takes three arguments processes and then
   * returns the result.
   *
   * @param arg1 the first argument
   * @param arg2 the second argument
   * @param arg3 the third argument
   * @return the function result
   */
  R apply(T1 arg1, T2 arg2, T3 arg3);

  /**
   * Returns a composed function that first applies this function to its input, and then applies the
   * {@code after} function to the result. If evaluation of either function throws an exception, it
   * is relayed to the caller of the composed function.
   *
   * @param <V> the type of output of the {@code after} function, and of the composed function
   * @param after the function to apply after this function is applied
   * @return a composed function that first applies this function and then applies the {@code after}
   *         function
   * @throws NullPointerException if after is null
   */
  default <V> Function3<T1, T2, T3, V> andThen(Function<? super R, ? extends V> after) {
    Objects.requireNonNull(after);
    return (T1 arg1, T2 arg2, T3 arg3) -> after.apply(apply(arg1, arg2, arg3));
  }
}
