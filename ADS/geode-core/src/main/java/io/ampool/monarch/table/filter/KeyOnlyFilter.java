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

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.results.FormatAwareRow;
import io.ampool.monarch.table.results.KeyOnlyRow;

import java.util.ArrayList;
import java.util.Collections;

/**
 * A filter that will only return the key(the value will be empty in result).
 * <p>
 * This filter can be used to grab all of the keys without having to also grab the values.
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class KeyOnlyFilter extends FilterBase {

  private static final long serialVersionUID = -8556470618872155033L;
  boolean lenAsVal;

  public KeyOnlyFilter() {
    this(false);
  }

  public KeyOnlyFilter(boolean lenAsVal) {
    this.lenAsVal = lenAsVal;
  }

  @Override
  public Row transformResult(Row result) {
    return createKeyOnlyResult(result);
  }

  @Override
  public boolean hasFilterRow() {
    return true;
  }

  @Override
  public boolean hasFilterCell() {
    return false;
  }

  private Row createKeyOnlyResult(Row result) {
    if (result instanceof KeyOnlyRow) {
      return new KeyOnlyRow(result.getRowId(), result.getRowTimeStamp(), result.getCells().size(),
          ((KeyOnlyRow) result).getTableDescriptor());
    }
    if (result instanceof FormatAwareRow) {
      return new FormatAwareRow(result.getRowId(), result.getRowTimeStamp(),
          ((FormatAwareRow) result).getTableDescriptor(), new ArrayList<Cell>());
    }
    return null;
  }

}
