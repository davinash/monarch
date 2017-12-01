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
package io.ampool.monarch.table.filter.internal;

import io.ampool.classification.InterfaceAudience;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.internal.CellRef;
import io.ampool.monarch.table.internal.VersionedRow;
import io.ampool.monarch.table.results.FormatAwareRow;

import java.util.ArrayList;
import java.util.List;

/**
 * <em>INTERNAL</em>
 *
 * Common place to execute Filters during scan.
 */
@InterfaceAudience.Private
public class MFilterExecutor {

  public enum FilterRunStatus {
    FILTER_ALL_REMAINING, FILTER_CURRENT_ROW, ROW_TRANSFORMED
  }

  public static class FilterExecutionResult {
    public final FilterRunStatus status;
    public final Row row;

    public FilterExecutionResult(FilterRunStatus status, Row row) {
      this.status = status;
      this.row = row;
    }
  }

  public static FilterExecutionResult executeFilter(Filter filter, Row row,
      TableDescriptor tableDescriptor) {
    FilterRunStatus status = FilterRunStatus.ROW_TRANSFORMED;
    // Resetting filter
    filter.reset();
    // Checking if we should end scan
    if (filter.filterAllRemaining()) {
      status = FilterRunStatus.FILTER_ALL_REMAINING;
    }

    boolean hasRowFilter = filter.hasFilterRow();
    boolean hasCellFilter = filter.hasFilterCell();

    if (hasRowFilter) {
      // Filter out this row?
      if (filter.filterRowKey(row)) {
        status = FilterRunStatus.FILTER_CURRENT_ROW;
      }
    }

    if (hasCellFilter) {
      // Filter based on cell values and tranform cell values
      List<Cell> cells = row.getCells();
      List<Cell> newCells = new ArrayList<>();

      boolean skipAllRemainingCells = false;
      int index = 0;
      for (; index < cells.size(); index++) {
        Cell cell = cells.get(index);
        Filter.ReturnCode returnCode = filter.filterCell(cell, row.getRowId());
        switch (returnCode) {
          case INCLUDE:
          case INCLUDE_AND_NEXT_COL:
            // TODO Add handling for versions
            Cell newCell = filter.transformCell(cell, row.getRowId());
            newCells.add(newCell);
            break;
          case SKIP:
          case NEXT_COL:
            // TODO Add handling for versions
            // Marking cells for exclusion
            // This is needed as client would be expecting all/selected columns as specified in scan
            // and would not that filter(s) have deleted this cell
            ((CellRef) cell).init(null, -1, -1);
            // TODO Revisit This is done to remove unwanted cells
            // newCells.add(cell);
            break;
          case NEXT_ROW:
            // Need to break outer for loop as we are done adding cells
            skipAllRemainingCells = true;
            break;
          case SEEK_NEXT_USING_HINT:
            // TODO implement later
            break;
        }
        if (skipAllRemainingCells) {
          break;
        }
      }

      // Marking all remaining cells (if any) for exclusion
      // This is needed as client would be expecting all/selected columns as specified in scan
      // and would not that filter(s) have deleted this cell
      while (index < cells.size()) {
        Cell cell = cells.get(index);
        // Marking cells for exclusion
        ((CellRef) cell).init(null, -1, -1);
        // TODO Revisit This is done to remove unwanted cells
        // newCells.add(cell);
        index++;
      }
      // Currently, typecasting tableRow to MResultImpl and calling its set method
      // Need to refactor it in a better way
      // This MResult includes -1 length marker cells
      // Based on ftable or mtable create versioned row or formatawareresult
      if (row instanceof VersionedRow) {
        row = new VersionedRow(row.getRowId(), row.getRowTimeStamp(), newCells, tableDescriptor);
      } else if (row instanceof FormatAwareRow) {
        row = new FormatAwareRow(row.getRowId(), row.getRowTimeStamp(),
            ((FormatAwareRow) row).getTableDescriptor(), newCells);
      }
    }


    if (hasRowFilter) {
      // Transforming final result after cell transformation
      row = filter.transformResult(row);
    }

    if (hasCellFilter) {
      List<Cell> cells = row.getCells();
      // Final chance to modify cells after transformation
      List<Cell> newCells = filter.filterRowCells(cells);
      if (row instanceof VersionedRow) {
        row = new VersionedRow(row.getRowId(), row.getRowTimeStamp(), newCells, tableDescriptor);
      } else if (row instanceof FormatAwareRow) {
        row = new FormatAwareRow(row.getRowId(), row.getRowTimeStamp(),
            ((FormatAwareRow) row).getTableDescriptor(), newCells);
      }

    }

    if (hasRowFilter) {
      // Skip this row if filter says so
      if (filter.filterRow()) {
        status = FilterRunStatus.FILTER_CURRENT_ROW;
      }
    }

    return new FilterExecutionResult(status, row);
  }
}
