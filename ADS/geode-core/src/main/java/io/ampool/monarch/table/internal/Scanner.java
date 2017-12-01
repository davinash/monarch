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
package io.ampool.monarch.table.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.types.CompareOp;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

/**
 * Abstract implemention of {@link io.ampool.monarch.table.Scanner} defining methods common for
 * various implementation
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public abstract class Scanner implements io.ampool.monarch.table.Scanner {
  protected static final Logger logger = LogService.getLogger();

  @Override
  public Iterator iterator() {
    return new Iterator<Row>() {
      // The next RowMResult, possibly pre-read
      Row next = null;

      // return true if there is another item pending, false if there isn't.
      // this method is where the actual advancing takes place, but you need
      // to call next() to consume it. hasNext() will only advance if there
      // isn't a pending next().
      @Override
      public boolean hasNext() {
        if (next == null) {
          next = io.ampool.monarch.table.internal.Scanner.this.next();
          return next != null;
        }
        return true;
      }

      // get the pending next item and advance the iterator. returns null if
      // there is no next item.
      @Override
      public Row next() {
        // since hasNext() does the real advancing, we call this to determine
        // if there is a next before proceeding.
        if (!hasNext()) {
          return null;
        }

        // if we get to here, then hasNext() has given us an item to return.
        // we want to return the item and then null out the next pointer, so
        // we use a temporary variable.
        Row temp = next;
        next = null;
        return temp;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Get no of rows from table.
   * 
   * @param nbRows number of rows to return
   * @return Between zero and number of rows MResult in array. Mscan is done if returned array is of
   *         zero length
   */
  @Override
  public Row[] next(int nbRows) {
    ArrayList<Row> resultSets = new ArrayList<Row>(nbRows);
    for (int i = 0; i < nbRows; i++) {
      Row next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new Row[resultSets.size()]);
  }

  protected Set<Integer> getApplicableBucketIds(Scan scan, TableDescriptor tableDescriptor) {
    Set<Integer> bucketIdSet = null;
    boolean isOrderedScan = tableDescriptor instanceof MTableDescriptor
        && MTableType.ORDERED_VERSIONED.equals(((MTableDescriptor) tableDescriptor).getTableType());
    if (isOrderedScan) {
      Set<Integer> callerBucketIdSet = scan.getBucketIds();

      bucketIdSet =
          MTableUtils.getBucketIdSet(scan.getStartRow(), scan.getStopRow(), tableDescriptor);
      if (callerBucketIdSet.isEmpty() == false) {
        // If the caller provided specific bucket ID's find the intersection
        bucketIdSet.retainAll(callerBucketIdSet);
      }
    } else if (tableDescriptor instanceof FTableDescriptor) {
      bucketIdSet = getBucketIdsForFTable((FTableDescriptor) tableDescriptor, scan.getFilter());
    } else {
      bucketIdSet = scan.getBucketIds();
      if (scan.getStartRow() != null || scan.getStopRow() != null) {
        throw new IllegalArgumentException("Key range should not be provided for UnOrdered scan.");
      }
      if (scan.isReversed()) {
        throw new IllegalArgumentException("There is no ordering for UnOrdered scan.");
      }
      // if no buckets were specified include all buckets
      if (bucketIdSet.isEmpty()) {
        bucketIdSet = MTableUtils.getBucketIdSet(null, null, tableDescriptor);
      }
    }
    return bucketIdSet;
  }

  /**
   * Get the bucket-ids to be used by the scanner for all single-column-value filter using
   * partitioning-column with EQUALS condition only. The bucket-id is derived from the hash-code of
   * the respective column value as the respective column was used for distribution of the rows
   * during ingestion. In case of multiple condition/filters, the below optimization is used only
   * when all the condition/filters are used along with AND operator. The optimization is skipped
   * for OR conditions and in such cases all the buckets are used for scan.
   * 
   * @param td the table descriptor
   * @param filter the filters to be used during scan
   * @return the buckets to be scanned/traversed during the scan
   */
  private static Set<Integer> getBucketIdsForFTable(final FTableDescriptor td,
      final Filter filter) {
    final ByteArrayKey partitioningColumn = td.getPartitioningColumn();
    final int numBuckets = td.getTotalNumOfSplits();
    Set<Integer> bucketIds = Collections.emptySet();
    //// if no partitioning column is specified, use all buckets.
    if (partitioningColumn != null) {
      final byte[] column = partitioningColumn.getByteArray();
      int bid;
      if ((bid = getBucketIdForFilter(filter, column, numBuckets)) >= 0) {
        bucketIds = Collections.singleton(bid);
      } else if (filter instanceof FilterList) {
        FilterList filterList = (FilterList) filter;
        List<Filter> filters = filterList.getFilters();
        switch (filterList.getOperator()) {
          case MUST_PASS_ALL:
            bucketIds = filters.stream().map(f -> getBucketIdForFilter(f, column, numBuckets))
                .filter(i -> i >= 0).collect(Collectors.toSet());
            break;
          case MUST_PASS_ONE:
            if (filters.size() == 1
                && (bid = getBucketIdForFilter(filters.get(0), column, numBuckets)) >= 0) {
              bucketIds = Collections.singleton(bid);
            }
            break;
        }
      }
    }
    return bucketIds.isEmpty() ? MTableUtils.getBucketIdSet(null, null, td) : bucketIds;
  }

  /**
   * Return the bucket-id to be used by the scanner if the single-column-value-filter having
   * partitioning-column with EQUALS condition was used. The bucket-id is derived from the hash-code
   * of the value specified in the filter.
   * <p>
   * If the filter is not single-column-value or the column-name specified is not the
   * partitioning-column then -1 is returned.
   * 
   * @param filter the filter to be processed
   * @param columnName the partitioning column name
   * @param numBuckets total number of buckets/partitions for the table
   * @return the bucket-id derived from the value in the filter; -1 if the filter is not
   *         single-column-value
   */
  private static int getBucketIdForFilter(final Filter filter, final byte[] columnName,
      final int numBuckets) {
    SingleColumnValueFilter sf;
    if (filter instanceof SingleColumnValueFilter) {
      sf = (SingleColumnValueFilter) filter;
      if (sf.getOperator() == CompareOp.EQUAL && Arrays.equals(columnName, sf.getColumnName())) {
        return PartitionedRegionHelper.getHashKey(MTableUtils.getDeepHashCode(sf.getValue()),
            numBuckets);
      }
    }
    return -1;
  }

  @Override
  public Row[] nextBatch() {
    throw new UnsupportedOperationException("batch mode is not implemented in this scanner");
  }
}
