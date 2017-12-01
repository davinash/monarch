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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.exceptions.MFilterException;

/**
 * Implementation of {@link Filter} that represents an ordered List of Filters which will be
 * evaluated with a specified boolean operator {@link Operator#MUST_PASS_ALL} (<code>AND</code>) or
 * {@link Operator#MUST_PASS_ONE} (<code>OR</code>). Since you can use Filter Lists as children of
 * Filter Lists, you can create a hierarchy of filters to be evaluated.
 * <p>
 * <br>
 * {@link Operator#MUST_PASS_ALL} evaluates lazily: evaluation stops as soon as one filter does not
 * include the KeyValue.
 * <p>
 * <br>
 * {@link Operator#MUST_PASS_ONE} evaluates non-lazily: all filters are always evaluated.
 * <p>
 * <br>
 * Defaults to {@link Operator#MUST_PASS_ALL}.
 * <p>
 * <p>
 * <P>
 * </P>
 * Example: using a filter list to match a {@link SingleColumnValueFilter} and {@link KeyOnlyFilter}
 * condition:
 * <P>
 * </P>
 * MScan scan = new MScan();<br>
 * MFilterList filterList = new MFilterList(MFilterList.Operator.MUST_PASS_ALL);<br>
 * MFilter singleColumnValueFilter = new SingleColumnValueFilter("mycol1", CompareOp.LESS,
 * VALUE);<br>
 * filterList.addFilter(singleColumnValueFilter);<br>
 * MFilter keyOnlyFilter = new KeyOnlyFilter();<br>
 * filterList.addFilter(keyOnlyFilter);<br>
 * scan.setFilter(filterList);<br>
 * MResultScanner scanner = table.getScanner(scan);<br>
 * <P>
 * </P>
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class FilterList extends FilterBase {

  private static final long serialVersionUID = -5743757073188274838L;

  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public static enum Operator {
    /**
     * !AND
     */
    MUST_PASS_ALL,
    /**
     * !OR
     */
    MUST_PASS_ONE
  }

  private static final int MAX_LOG_FILTERS = 5;
  private Operator operator = Operator.MUST_PASS_ALL;
  private List<Filter> filters = new ArrayList<Filter>();

  /**
   * Reference Cell used by {@link #transformCell(Cell, byte[])} for validation purpose.
   */
  private Cell referenceCell = null;

  /**
   * When filtering a given Cell in {@link #filterCell(Cell, byte[])}, this stores the transformed
   * Cell to be returned by {@link #transformCell(Cell, byte[])}.
   * <p>
   * Individual filters transformation are applied only when the filter includes the Cell.
   * Transformations are composed in the order specified by {@link #filters}.
   */
  private Cell transformedCell = null;

  public FilterList() {
    this(Operator.MUST_PASS_ALL);
  }

  public FilterList(final Filter[] filters) {
    this(new ArrayList<Filter>(Arrays.asList(filters)));
  }

  public FilterList(List<Filter> filters) {
    this(Operator.MUST_PASS_ALL, filters);
  }

  public FilterList(Operator operator) {
    this(operator, new ArrayList<Filter>());
  }

  public FilterList(Operator operator, final Filter... filters) {
    this(operator, new ArrayList<Filter>(Arrays.asList(filters)));
  }

  public FilterList(Operator operator, List<Filter> filters) {
    this.operator = operator;
    if (filters instanceof ArrayList) {
      this.filters = filters;
    } else {
      this.filters = new ArrayList<Filter>(filters);
    }
  }

  /**
   * Get the operator.
   *
   * @return operator
   */
  public Operator getOperator() {
    return operator;
  }

  /**
   * Sets the operator.
   *
   */
  public void setOperator(Operator operator) {
    this.operator = operator;
  }

  /**
   * Get the filters.
   *
   * @return filters
   */
  public List<Filter> getFilters() {
    return filters;
  }

  /**
   * Add a filter.
   *
   * @param filter another filter
   */
  public FilterList addFilter(Filter filter) {
    if (this.isReversed() != filter.isReversed()) {
      throw new MFilterException(
          "Filters in the list must have the same reversed flag, this.reversed="
              + this.isReversed());
    }
    this.filters.add(filter);
    return this;
  }

  @Override
  public void reset() {
    int numFilters = filters.size();
    for (int i = 0; i < numFilters; i++) {
      filters.get(i).reset();
    }
  }

  @Override
  public boolean hasFilterRow() {
    int numFilters = filters.size();
    for (int i = 0; i < numFilters; i++) {
      if (filters.get(i).hasFilterRow()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean hasFilterCell() {
    int numFilters = filters.size();
    for (int i = 0; i < numFilters; i++) {
      if (filters.get(i).hasFilterCell()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean filterAllRemaining() {
    int listize = filters.size();
    for (int i = 0; i < listize; i++) {
      if (filters.get(i).filterAllRemaining()) {
        if (operator == Operator.MUST_PASS_ALL) {
          return true;
        }
      } else {
        if (operator == Operator.MUST_PASS_ONE) {
          return false;
        }
      }
    }
    return operator == Operator.MUST_PASS_ONE;
  }

  @Override
  public boolean filterRowKey(Row result) {
    boolean flag = (this.operator == Operator.MUST_PASS_ONE) ? true : false;
    int numFilters = filters.size();
    for (int i = 0; i < numFilters; i++) {
      Filter filter = filters.get(i);
      if (this.operator == Operator.MUST_PASS_ALL) {
        if (filter.filterAllRemaining() || filter.filterRowKey(result)) {
          flag = true;
        }
      } else if (this.operator == Operator.MUST_PASS_ONE) {
        if (!filter.filterAllRemaining() && !filter.filterRowKey(result)) {
          flag = false;
        }
      }
    }
    return flag;
  }

  @Override
  public ReturnCode filterCell(Cell cell, byte[] rowKey) {
    this.referenceCell = cell;

    // Accumulates successive transformation of every filter that includes the Cell:
    Cell transformed = cell;

    ReturnCode rc = operator == Operator.MUST_PASS_ONE ? ReturnCode.SKIP : ReturnCode.INCLUDE;
    int listize = filters.size();
    for (int i = 0; i < listize; i++) {
      Filter filter = filters.get(i);
      if (operator == Operator.MUST_PASS_ALL) {
        if (filter.filterAllRemaining()) {
          return ReturnCode.NEXT_ROW;
        }
        ReturnCode code = filter.filterCell(cell, rowKey);
        switch (code) {
          case INCLUDE:
          case INCLUDE_AND_NEXT_COL:
            rc = ReturnCode.INCLUDE;
            transformed = filter.transformCell(transformed, rowKey);
            continue;
          case SEEK_NEXT_USING_HINT:
            // TODO Implement later
            return code;
          default:
            return code;
        }
      } else if (operator == Operator.MUST_PASS_ONE) {
        if (filter.filterAllRemaining()) {
          continue;
        }
        switch (filter.filterCell(cell, rowKey)) {
          case INCLUDE:
          case INCLUDE_AND_NEXT_COL:
            rc = ReturnCode.INCLUDE;
            transformed = filter.transformCell(transformed, rowKey);
            break;
          case NEXT_ROW:
            break;
          case SKIP:
            break;
          case NEXT_COL:
            break;
          case SEEK_NEXT_USING_HINT:
            break;
          default:
            throw new IllegalStateException("Received code is not valid.");
        }
      }
    }
    // Save the transformed Cell for transform():
    this.transformedCell = transformed;
    return rc;
  }

  @Override
  public Cell transformCell(Cell cell, byte[] rowKey) {
    // same cell will be passed for tranformation so returning transformedCell
    return transformedCell;
  }

  @Override
  public Row transformResult(Row result) {
    int numFilters = filters.size();
    Row transformed = result;
    for (int i = 0; i < numFilters; i++) {
      transformed = filters.get(i).transformResult(transformed);
    }
    return transformed;
  }

  @Override
  public List<Cell> filterRowCells(List<Cell> kvs) {
    int numFilters = filters.size();
    List<Cell> transformed = kvs;
    for (int i = 0; i < numFilters; i++) {
      transformed = filters.get(i).filterRowCells(transformed);
    }
    return transformed;
  }

  @Override
  public boolean filterRow() {
    int numResults = filters.size();
    for (int i = 0; i < numResults; i++) {
      Filter filter = filters.get(i);
      if (operator == Operator.MUST_PASS_ALL) {
        if (filter.filterRow()) {
          return true;
        }
      } else if (operator == Operator.MUST_PASS_ONE) {
        if (!filter.filterRow()) {
          return false;
        }
      }
    }
    return operator == Operator.MUST_PASS_ONE;
  }

  @Override
  public void setReversed(boolean reversed) {
    int numFilters = filters.size();
    for (int i = 0; i < numFilters; i++) {
      filters.get(i).setReversed(reversed);
    }
    this.reversed = reversed;
  }

  @Override
  public String toString() {
    return "FilterList={Operator:" + this.getOperator() + ",filters:" + getFilters() + "}";
  }
}
