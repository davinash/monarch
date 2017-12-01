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
package io.ampool.monarch.table.filter;

import java.util.function.Predicate;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.exceptions.MFilterException;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.interfaces.TypePredicate;

/**
 * This is a generic filter to be used to filter by comparison. It takes an operator (equal,
 * greater, not equal, etc) and a byte [] comparator.
 *
 * Filters involving comparision can extend this class.
 * <p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CompareFilter extends FilterBase {

  private static final long serialVersionUID = 5344952498792162490L;
  protected Object initialValue;
  protected CompareOp compareOp;

  /**
   * Constructor.
   * 
   * @param compareOp the compare op for row matching
   * @param initialValue value to compare with
   */
  public CompareFilter(final CompareOp compareOp, final Object initialValue) {
    this.initialValue = initialValue;
    this.compareOp = compareOp;
  }

  /**
   * @return Returns the defined Compare Operator See {@link CompareOp}
   */
  public CompareOp getOperator() {
    return compareOp;
  }

  /**
   * Return the Initial value used for comparasion.
   * 
   * @return initial value used for comparasion.
   */
  public Object getValue() {
    return initialValue;
  }

  /**
   * Compare Function.
   * 
   * @param compareOp Compare Operator function See {@link CompareOp}
   * @param value Value to compare with {@link #initialValue}
   * @param type Type of the column value See{@link DataType}
   * @param columnName Name of the column whose value to be compared
   * @return true if values matches otherwise false.
   */

  public boolean doCompare(final CompareOp compareOp, final Object value, DataType type,
      byte[] columnName) {
    if (!(type instanceof TypePredicate)) {
      throw new MFilterException("Column type for column '" + Bytes.toString(columnName)
          + "' must implement TypePredicate to support push down predicates");
    }
    Predicate predicate = ((TypePredicate) type).getPredicate(compareOp, initialValue);
    return predicate.test(value);
  }

  @Override
  public String toString() {
    return String.format("%s (%s, %s)", this.getClass().getSimpleName(), this.compareOp.name(),
        String.valueOf(this.initialValue));
  }
}
