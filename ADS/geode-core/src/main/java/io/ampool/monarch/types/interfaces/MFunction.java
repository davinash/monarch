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
 * A functional interface with single arguments. A typical for pre-serialization and
 * post-deserialization methods that are used for pre/post processing the data before or after the
 * respective operation. These are useful for converting the objects from one type to another but
 * using ser-de of other types. An example usage for this is Hive decimal-type or Spark Array/Map
 * types where the respective Java object (i.e. BigDecimal -> HiveDecimal) needs to be converted to
 * respective type after de-serialization or the scala Array/Map need to converted to Java counter
 * parts before serializing these.
 * <p>
 * <p>
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
@FunctionalInterface
public interface MFunction<T, R> extends Serializable {

  /**
   * Applies this function to the given argument.
   *
   * @param t the function argument
   * @return the function result
   */
  R apply(T t);

  /**
   * Returns a composed function that first applies the {@code before} function to its input, and
   * then applies this function to the result. If evaluation of either function throws an exception,
   * it is relayed to the caller of the composed function.
   *
   * @param <V> the type of input to the {@code before} function, and to the composed function
   * @param before the function to apply before this function is applied
   * @return a composed function that first applies the {@code before} function and then applies
   *         this function
   * @throws NullPointerException if before is null
   *
   * @see #andThen(Function)
   */
  default <V> MFunction<V, R> compose(Function<? super V, ? extends T> before) {
    Objects.requireNonNull(before);
    return (V v) -> apply(before.apply(v));
  }

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
   *
   * @see #compose(Function)
   */
  default <V> MFunction<T, V> andThen(Function<? super R, ? extends V> after) {
    Objects.requireNonNull(after);
    return (T t) -> after.apply(apply(t));
  }

  /**
   * Returns a function that always returns its input argument.
   *
   * @param <T> the type of the input and output objects to the function
   * @return a function that always returns its input argument
   */
  static <T> MFunction<T, T> identity() {
    return t -> t;
  }
}
