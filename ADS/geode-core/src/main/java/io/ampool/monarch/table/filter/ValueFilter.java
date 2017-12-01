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
import io.ampool.monarch.types.CompareOp;

/**
 * This filter is used to filter based on column value. It takes an operator (equal, greater, not
 * equal, etc) and a byte [] comparator for the cell value. To test the value of a single qualifier
 * when scanning multiple qualifiers, use {@link SingleColumnValueFilter}.
 * <P>
 * </P>
 * Example: using a filter to match on any matching column value:
 * <P>
 * </P>
 * MScan scan = new MScan();<br>
 * MFilter filter = new ValueFilter(CompareOp.EQUAL, VALUE);<br>
 * scan.setFilter(filter);<br>
 * MResultScanner scanner = table.getScanner(scan);<br>
 * <P>
 * </P>
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueFilter extends CompareFilter {

  private static final long serialVersionUID = 550709438364222288L;

  /**
   * Constructor specifying compare operator and value of type same as columntype
   *
   * @param compareOp
   * @param value
   */
  public ValueFilter(CompareOp compareOp, Object value) {
    super(compareOp, value);
  }

  @Override
  public boolean hasFilterRow() {
    return false;
  }

  @Override
  public boolean hasFilterCell() {
    return true;
  }

  @Override
  public ReturnCode filterCell(Cell cell, byte[] rowKey) {
    if (!doCompare(this.compareOp, cell.getColumnValue(), cell.getColumnType(),
        cell.getColumnName())) {
      return ReturnCode.SKIP;
    }
    return ReturnCode.INCLUDE;
  }
}
