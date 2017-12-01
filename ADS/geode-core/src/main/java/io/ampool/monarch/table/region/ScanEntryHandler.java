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

import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.internal.MFilterExecutor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.ScanStatus;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.table.results.FormatAwareRow;
import io.ampool.monarch.types.TypeHelper;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The interface to handle entries from the bucket-region map during scan.
 * <p>
 */
public interface ScanEntryHandler {
  enum Status {
    NEXT, SEND, TRANSFORMED, STOP, UNKNOWN;
  }

  Status handle(final Row row, final ScanContext sc) throws IOException, InterruptedException;

  /**
   * The simplest entry-handler that does not invoke any filters. It simply appends the entry
   * (key-value pair) to the queue that is being sent to the client.
   */
  final class NoFilterHandler implements ScanEntryHandler {
    static final ScanEntryHandler SELF = new NoFilterHandler();

    @Override
    public Status handle(final Row row, final ScanContext sc)
        throws IOException, InterruptedException {
      if (row.getRawValue() != null) {
        sc.addRow(row);
        return Status.SEND;
      }
      return Status.NEXT;
    }
  }

  /**
   * The entry handler that invokes the provided filters during processing the entry. It sends the
   * entry to clients only when all filters match; including row-filter and all column filters.
   */
  final class SimpleFilterHandler implements ScanEntryHandler {
    static final SimpleFilterHandler SELF = new SimpleFilterHandler();

    @Override
    public Status handle(final Row row, final ScanContext sc)
        throws IOException, InterruptedException {
      if (ScanUtils.filterRowKey(sc.scan, row.getRowId())) {
        return Status.NEXT;
      }

      if (row.getRawValue() != null) {
        // sc.row.reset((byte[]) key, (byte[]) value);
        long l1 = System.nanoTime();
        boolean s =
            ScanUtils.executeSimpleFilter(row, sc.scan.getFilter(), sc.getTableDescriptor());
        sc.execFilterTime += (System.nanoTime() - l1);
        if (s) {
          sc.addRow(row);
          return Status.SEND;
        }
      }
      return Status.NEXT;
    }
  }

  final class MultiVersionHandler implements ScanEntryHandler {
    static final ScanEntryHandler SELF = new MultiVersionHandler();

    @Override
    public Status handle(final Row row, final ScanContext sc)
        throws IOException, InterruptedException {
      /*
       * Since we are iterating on a view of the map that may reflect changes in the underlying map
       * we must be prepared for the case that no more elements exist now even though hasNext()
       * returned true above.
       */
      if (ScanUtils.filterRowKey(sc.scan, row.getRowId())) {

        return Status.NEXT;
      }

      if (row.getRawValue() == null
          || (row.getRawValue() instanceof byte[] && ((byte[]) row.getRawValue()).length == 0)) {
        return Status.NEXT;
      }

      Object data = row.getRawValue();

      if (sc.td instanceof MTableDescriptor) {
        // perform version selection and column selections
        sc.scanKey.setKey(row.getRowId());
        sc.scanKey.setMaxVersions(sc.scan.getMaxVersions() * sc.versionOrderIdentifier);
        // When supporting multi versioned get for scan need to change to mscan.getMaxVersions
        data = sc.storageFormatter.performGetOperation(sc.scanKey, sc.getTableDescriptor(), data,
            null);
      }

      /* column selection.. for filters */
      if (!sc.returnFullValue) {
        if (sc.td instanceof FTableDescriptor) {
          data = FTableByteUtils.extractSelectedColumns((byte[]) row.getRawValue(),
              sc.scanKey.getColumnPositions());
        }
      }

      Row row1 = new FormatAwareRow(row.getRowId(), data, sc.td, sc.scan.getColumnNameList());

      /* return the requested versions if no filters are specified */
      if (!sc.scan.hasFilter()) {
        Object value1 = ((FormatAwareRow) row1).getRawByteArray();
        sc.addEntry(row.getRowId(), value1);
        return Status.SEND;
      }

      Status status = Status.UNKNOWN;
      data = ((FormatAwareRow) row1).getRawByteArray();
      Filter filter = sc.scan.getFilter();
      if (sc.scan.isFilterOnLatestVersionOnly()) {
        long l1 = System.nanoTime();
        MFilterExecutor.FilterExecutionResult result =
            MFilterExecutor.executeFilter(filter, row1, sc.td);
        sc.execFilterTime += (System.nanoTime() - l1);
        switch (result.status) {
          case FILTER_ALL_REMAINING:
            status = Status.STOP;
            break;
          case FILTER_CURRENT_ROW:
            status = Status.NEXT;
            break;
          case ROW_TRANSFORMED:
            data = ((FormatAwareRow) result.row).getRawByteArray();
            status = Status.SEND;
            break;
        }
      } else {
        boolean areVersionsFiltered = false;
        // get each version row
        Map<Long, SingleVersionRow> allVersions = ((FormatAwareRow) row1).getAllVersions();
        Map<Long, SingleVersionRow> allVersionsAfterFilter = new LinkedHashMap<>();
        Iterator<Map.Entry<Long, SingleVersionRow>> iterator = allVersions.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<Long, SingleVersionRow> firstVersion = iterator.next();
          Row versionedRow = new FormatAwareRow(row1.getRowId(), firstVersion.getKey(), sc.td,
              firstVersion.getValue().getCells());

          long l1 = System.nanoTime();
          Object[] innerStatus = ScanUtils.applyFilter(sc.scan, versionedRow, sc.td);
          sc.execFilterTime += (System.nanoTime() - l1);

          if (innerStatus[0] == ScanStatus.FOUND_MATCHING_ROW) {
            allVersionsAfterFilter.put(firstVersion.getKey(),
                ((FormatAwareRow) innerStatus[1]).getLatestRow());
            areVersionsFiltered = true;
            status = Status.SEND;
          }
        }
        if (areVersionsFiltered) {
          // set versions after the filter
          ((FormatAwareRow) row1).setUpdatedVersions(allVersionsAfterFilter);
          // get byte value
          data = ((FormatAwareRow) row1).getRawByteArray();
        } else {
          status = Status.NEXT;
        }
      }
      if (status == Status.SEND) {
        sc.addEntry(row1.getRowId(), data);
      }
      return status;
    }
  }
}
