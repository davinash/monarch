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
package io.ampool.monarch.table.region;

import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.KeyOnlyFilter;
import io.ampool.monarch.table.filter.RowFilter;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.filter.ValueFilter;
import io.ampool.monarch.table.filter.internal.MFilterExecutor;
import io.ampool.monarch.table.filter.internal.TimestampFilter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.CellRef;
import io.ampool.monarch.table.internal.Encoding;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.InternalRow;
import io.ampool.monarch.table.internal.ScanStatus;
import io.ampool.monarch.table.internal.ServerRow;
import io.ampool.monarch.table.internal.ThinRow;
import io.ampool.monarch.table.internal.ThinRowShared;
import io.ampool.monarch.table.internal.VersionedRow;
import io.ampool.monarch.table.results.FormatAwareRow;
import io.ampool.monarch.types.interfaces.Function3;
import org.apache.geode.internal.cache.RegionEntry;


public class ScanUtils {
  public static SortedMap<IMKey, RegionEntry> getRangeMap(
      final SortedMap<IMKey, RegionEntry> rowMap, final IMKey startRow, final IMKey stopRow,
      final boolean includeStartRow) {
    SortedMap<IMKey, RegionEntry> result;
    if (startRow == null && stopRow == null) {
      result = rowMap;
    } else if (startRow == null) {
      // result = rowMap.headMap(stopRow, false);
      result = rowMap instanceof NavigableMap
          ? ((NavigableMap<IMKey, RegionEntry>) rowMap).headMap(stopRow, false)
          : rowMap.headMap(stopRow);
    } else if (stopRow == null) {
      // result = rowMap.tailMap(startRow, includeStartRow);
      result = rowMap instanceof NavigableMap
          ? ((NavigableMap<IMKey, RegionEntry>) rowMap).tailMap(startRow, includeStartRow)
          : rowMap.tailMap(startRow);
    } else {
      // result = rowMap.subMap(startRow, includeStartRow, stopRow, false);
      result = subMap(rowMap, startRow, includeStartRow, stopRow, false);
    }
    return result;
  }

  private static SortedMap<IMKey, RegionEntry> subMap(final SortedMap<IMKey, RegionEntry> map,
      final IMKey from, final boolean f, final IMKey to, final boolean t) {
    if (map instanceof NavigableMap) {
      return ((NavigableMap<IMKey, RegionEntry>) map).subMap(from, f, to, t);
    } else {
      /* TODO: need to check and use include from/to key as well, if specified */
      return map.subMap(from, to);
    }
  }

  /**
   * Get the required row producer. Depending on situations the storage (cells) is reused for
   * multiple results. The possibilities are: - VersionedRow: Used in following cases -- If > 1
   * versions are queried -- The table is FTable -- The row transformation filters are applied
   * during scan - ThinRow: used in all the remaining cases
   *
   * @param scan the scan
   * @param td the table descriptor
   * @return the appropriate row producer
   */
  public static BiFunction<byte[], Object, Row> getRowProducer(final Scan scan,
      final TableDescriptor td) {
    List<Cell> cells;
    List<Integer> columns = scan.getColumns();
    List<MColumnDescriptor> cds = td.getAllColumnDescriptors();
    cells = createCells(scan, cds);
    ThinRowShared thinRowShared = new ThinRowShared(cells, td, columns);
    // return scan.getMaxVersions() > 1 || ScanCommand.isVersionedRowNeeded(scan.getFilter())

    if (!isSingleVersion(td, scan) || !isSimpleFilter(scan.getFilter())
        || hasValueFilter(scan.getFilter())) {
      return hasValueFilter(scan.getFilter())
          ? (k, v) -> new ThinRow(k, v, thinRowShared, ThinRow.RowFormat.MTABLE_WITHOUT_NULL_CELLS)
          : (k, v) -> new FormatAwareRow(k, v, td, scan.getColumnNameList());
    } else {
      final String fmt = (td instanceof FTableDescriptor ? "F_" : "M_")
          + (isSelectedColumns(td, scan) ? "PARTIAL" : "FULL") + "_ROW";
      return (k, v) -> new ThinRow(k, v, thinRowShared, ThinRow.RowFormat.valueOf(fmt));
    }
  }

  public static Function3<byte[], Object, Encoding, InternalRow> serverRowProducer(final Scan scan,
      final TableDescriptor td) {
    // List<Cell> cells = td.getAllColumnDescriptors().stream()
    // .map(cd -> new CellRef(cd.getColumnName(), cd.getColumnType()))
    // .collect(Collectors.toList());
    // final ThinRowShared trs = new ThinRowShared(cells, td, scan.getColumns());

    return !isSingleVersion(td, scan) || !isSimpleFilter(scan.getFilter())
        || hasValueFilter(scan.getFilter())
            ? (k, v, enc) -> new FormatAwareRow(k, v, td, scan.getColumnNameList())
            : (k, v, enc) -> new ServerRow(enc, k, v,
                new ThinRowShared(td.getAllColumnDescriptors().stream()
                    .map(cd -> new CellRef(cd.getColumnName(), cd.getColumnType()))
                    .collect(Collectors.toList()), td, scan.getColumns()));
  }

  public static List<Cell> createCells(final Scan scan, final List<MColumnDescriptor> cds) {
    List<Cell> cells;
    if (isKeyOnlyFilter(scan.getFilter())) {
      cells = Collections.emptyList();
    } else {
      cells = (scan.getColumns() == null || scan.getColumns().isEmpty() ? cds.stream()
          : scan.getColumns().stream().map(cds::get))
              .map(cd -> new CellRef(cd.getColumnName(), cd.getColumnType()))
              .collect(Collectors.toList());
    }
    return cells;
  }

  static boolean hasValueFilter(final Filter filter) {
    if (filter instanceof FilterList) {
      for (Filter f : ((FilterList) filter).getFilters()) {
        boolean r = hasValueFilter(f);
        if (r) {
          return true;
        }
      }
    } else {
      return filter instanceof ValueFilter;
    }
    return false;
  }

  static boolean isKeyOnlyFilter(final Filter filter) {
    if (filter instanceof FilterList) {
      for (Filter f : ((FilterList) filter).getFilters()) {
        boolean r = isKeyOnlyFilter(f);
        if (r) {
          return true;
        }
      }
    } else {
      return filter instanceof KeyOnlyFilter;
    }
    return false;
  }

  static boolean isSimpleFilter(final Filter filter) {
    if (filter == null) {
      return true;
    }
    if (filter instanceof FilterList) {
      for (Filter f : ((FilterList) filter).getFilters()) {
        boolean r = isSimpleFilter(f);
        if (!r) {
          return false;
        }
      }
      return true;
    } else {
      return filter instanceof SingleColumnValueFilter || filter instanceof RowFilter
          || filter instanceof TimestampFilter;
    }
  }

  public static boolean isSingleVersion(final TableDescriptor td, final Scan scan) {
    return td instanceof FTableDescriptor || (td instanceof MTableDescriptor
        && ((MTableDescriptor) td).getMaxVersions() == 1 && scan.getMaxVersions() == 1);
  }

  public static boolean isSelectedColumns(final TableDescriptor td, final Scan scan) {
    return scan.getColumns().size() > 0 && scan.getColumns().size() != td.getNumOfColumns();
  }

  public static ScanEntryHandler.Status executeFilter(final Row row, final Filter filter,
      TableDescriptor td) {
    MFilterExecutor.FilterExecutionResult result = MFilterExecutor.executeFilter(filter, row, td);
    ScanEntryHandler.Status status = ScanEntryHandler.Status.UNKNOWN;
    switch (result.status) {
      case FILTER_ALL_REMAINING:
        status = ScanEntryHandler.Status.STOP;
        break;
      case FILTER_CURRENT_ROW:
        status = ScanEntryHandler.Status.NEXT;
        break;
      case ROW_TRANSFORMED:
        status = ScanEntryHandler.Status.SEND;
        break;
    }
    return status;
  }

  /**
   * Execute simple and non-intrusive filters like RowFilter/SingleColumnValueFilter that only
   * decide whether or not the row matches provided filters. The execution of filters does not
   * modify the row. When multiple filters are specified along with AND or OR either all or at least
   * one filter is expected match, respectively.
   *
   * @param row the row on which to execute the filters
   * @param filter the filter to execute
   * @param td the table descriptor
   * @return true if specified filter(s) match; false otherwise
   */
  public static boolean executeSimpleFilter(final Row row, final Filter filter,
      final TableDescriptor td) {
    if (filter instanceof FilterList) {
      FilterList fl = (FilterList) filter;
      final FilterList.Operator operator = fl.getOperator();
      if (operator == FilterList.Operator.MUST_PASS_ALL) {
        return fl.getFilters().stream().allMatch(e -> executeSimpleFilter(row, e, td));
      } else {
        return fl.getFilters().stream().anyMatch(e -> executeSimpleFilter(row, e, td));
      }
    } else if (filter instanceof SingleColumnValueFilter) {
      final SingleColumnValueFilter cf = (SingleColumnValueFilter) filter;
      final MColumnDescriptor cd = td.getColumnByName(cf.getColumnNameString());
      return cf.doCompare(cf.getOperator(), row.getValue(cd.getIndex()), cd.getColumnType(),
          cd.getColumnName());
    } else if (filter instanceof RowFilter || filter instanceof TimestampFilter) {
      return !filter.filterRowKey(row);
    }
    return filter == null;
  }

  /**
   * Runs filters on the given value, Do not adds to values and sent over the connection
   *
   * @param scan the scan object
   * @param row the row
   * @return ScanStatus
   */
  public static Object[] applyFilter(Scan scan, Row row, TableDescriptor tableDescriptor) {
    Row value = null;
    ScanStatus status = ScanStatus.FOUND_MATCHING_ROW;
    if (scan.hasFilter()) {
      Filter filter = scan.getFilter();

      MFilterExecutor.FilterExecutionResult result =
          MFilterExecutor.executeFilter(filter, row, tableDescriptor);
      switch (result.status) {
        case FILTER_ALL_REMAINING:
          value = null;
          status = ScanStatus.STOP_FURTHER_ITERATION;
          break;
        case FILTER_CURRENT_ROW:
          value = null;
          status = ScanStatus.CONTINUE_TO_NEXT_ROW;
          break;
        case ROW_TRANSFORMED:
          value = result.row;
          status = ScanStatus.FOUND_MATCHING_ROW;
          break;
      }
    }
    return new Object[] {status, value};
  }

  /**
   * Running RowFilter before fetching value if present
   *
   * @param scan the scanner object
   * @param keyBytes byte-array representing the key
   * @return true if the row-key filter passes; false otherwise
   */
  public static boolean filterRowKey(Scan scan, final byte[] keyBytes) {
    boolean filterRowKey = false;
    Filter filter = scan.getFilter();
    if (filter instanceof RowFilter) {
      filterRowKey = filter.filterRowKey(new VersionedRow(keyBytes));
    } else if (filter instanceof FilterList) {
      List<Filter> filters = ((FilterList) filter).getFilters();
      for (Filter mFilter : filters) {
        if (mFilter instanceof RowFilter) {
          boolean dofilter = mFilter.filterRowKey(new VersionedRow(keyBytes));
          if (dofilter) {
            filterRowKey = true;
            break;
          }
        }
      }
    }
    return filterRowKey;
  }
}
