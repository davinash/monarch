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

package org.apache.geode.internal.cache;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.internal.MFilterExecutor;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.CellRef;
import io.ampool.monarch.table.internal.InternalTable;
import io.ampool.monarch.table.internal.ThinRow;
import io.ampool.monarch.table.internal.ThinRowShared;
import io.ampool.monarch.table.region.ScanUtils;
import io.ampool.monarch.table.results.FormatAwareRow;
import io.ampool.monarch.types.MPredicateHolder;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MCountFunction extends FunctionAdapter implements InternalEntity {
  private static final Logger logger = LogService.getLogger();

  /**
   * Singleton instance of CountFunction that can be reused..
   */
  public static final MCountFunction COUNT_FUNCTION = new MCountFunction();

  private static final MPredicateHolder[] EMPTY_ARR = new MPredicateHolder[0];
  private static final long serialVersionUID = 3666092637235975870L;

  /**
   * Wrapper class for arguments to this function..
   */
  public static final class Args implements Serializable {

    private boolean countEvictedEntries = true;
    private MPredicateHolder[] predicates;
    private Set<Integer> bucketIds;
    private String regionName;
    private Filter[] filter;

    public Args(final String regionName, final Set<Integer> bucketIds,
        final boolean countEvictedEntries) {
      this.regionName = regionName;
      this.bucketIds = bucketIds;
      this.predicates = EMPTY_ARR;
      this.filter = null;
      this.countEvictedEntries = countEvictedEntries;
    }

    public void setPredicates(final MPredicateHolder[] predicates) {
      this.predicates = predicates == null ? EMPTY_ARR : predicates;
    }

    public void setBucketIds(final Set<Integer> bucketIds) {
      this.bucketIds = bucketIds;
    }

    public void setFilter(Filter[] filter) {
      this.filter = filter;
    }
  }

  /**
   * Simple function to get counts per buckets (only primary) on each server for the specified
   * region.
   *
   * @param fc the function context
   */
  @Override
  public void execute(final FunctionContext fc) {
    Cache cache = CacheFactory.getAnyInstance();
    final MCountFunction.Args args = (MCountFunction.Args) fc.getArguments();
    if (args == null || args.regionName == null) {
      throw new IllegalArgumentException("Region name must be provided.");
    }

    final String regionName = args.regionName;
    final Region r = cache.getRegion(regionName);
    if (r == null) {
      logger.info("MCountFunction: Region does not exist: {}", args.regionName);
      fc.getResultSender().lastResult(0L);
      return;
    }
    if (!(r instanceof PartitionedRegion)) {
      throw new IllegalArgumentException("Supported only for PartitionedRegion.");
    }

    final PartitionedRegion region = (PartitionedRegion) r;
    Set<Integer> buckets;
    if (args.bucketIds == null || args.bucketIds.isEmpty()) {
      buckets = region.getDataStore().getAllLocalBucketIds();
    } else {
      buckets = args.bucketIds;
    }
    long totalCount = getTotalCount(regionName, region.getDataStore(), buckets, args.filter,
        args.countEvictedEntries);
    fc.getResultSender().lastResult(totalCount);
  }

  /**
   * Get total count of entries for this region -- count only entries from the specified buckets and
   * that match all the specified predicates.
   *
   * @param regionName the region/table name
   * @param region the data-store for the partitioned region
   * @param buckets the buckets to be used for counting the entries
   * @param phs the predicates that must be matched
   * @param countEvictedEntries
   * @return the total number of entries in the region for the specified criteria
   */
  @SuppressWarnings("unchecked")
  private long getTotalCount(final String regionName, final PartitionedRegionDataStore region,
      final Set<Integer> buckets, final Filter[] phs, final boolean countEvictedEntries) {
    InternalTable table =
        (InternalTable) ((MonarchCacheImpl) MCacheFactory.getAnyInstance()).getAnyTable(regionName);
    if (table instanceof FTable) {
      return getFTableTotalCount(regionName, region, buckets, phs, (FTable) table,
          countEvictedEntries);
    } else {
      // Default MTable count
      return getMTableTotalCount(regionName, region, buckets, phs, (MTable) table);
    }
  }

  private long getMTableTotalCount(final String regionName, final PartitionedRegionDataStore region,
      final Set<Integer> buckets, final Filter[] phs, MTable mTable) {
    long totalCount = 0;
    if (phs != null && phs.length == 0) {
      totalCount = buckets.stream().map(region::getLocalBucketById)
          .filter(e -> e != null && e.getBucketAdvisor().isPrimary()).mapToLong(LocalRegion::size)
          .sum();
    } else {
      FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
      if (phs != null) {
        for (int i = 0; i < phs.length; i++) {
          filterList.addFilter(phs[i]);
        }
      }
      Map iMap;
      MTableDescriptor td = mTable.getTableDescriptor();
      for (Integer bucket : buckets) {
        BucketRegion br = region.getLocalBucketById(bucket);
        if (br == null || !br.getBucketAdvisor().isPrimary()) {
          continue;
        }
        iMap = (Map) br.getRegionMap().getInternalMap();
        long count =
            iMap.values().stream().filter(e -> e != null).map(e -> ((RegionEntry) e)._getValue())
                .filter(e -> !(e instanceof Token)).filter(e -> {
                  // change reverting for now TODO :BMERGE
                  // Row tableRow = new RowImpl((null), (byte[]) e, td, null, true);
                  Row tableRow = new FormatAwareRow(Bytes.EMPTY_BYTE_ARRAY, e,
                      mTable.getTableDescriptor(), new ArrayList<>());

                  MFilterExecutor.FilterExecutionResult result =
                      MFilterExecutor.executeFilter(filterList, tableRow, td);
                  // MResultParser.initCells(cells, (byte[]) e, (int[]) null);
                  // return isRowMatchPredicates(phs, cells);
                  return result.status == MFilterExecutor.FilterRunStatus.ROW_TRANSFORMED;
                }).count();
        totalCount += count;
      }
    }
    return totalCount;
  }

  @SuppressWarnings("unchecked")
  private long getFTableTotalCount(final String regionName, final PartitionedRegionDataStore region,
      final Set<Integer> buckets, final Filter[] phs, FTable table,
      final boolean countEvictedEntries) {
    long totalCount = 0;

    /* use record count from each block, wherever possible, rather than iterating over all */
    if (phs == null || phs.length == 0) {
      Map<Object, Object> iMap;
      for (Integer bucket : buckets) {
        BucketRegion br = region.getLocalBucketById(bucket);
        if (br == null || !br.getBucketAdvisor().isPrimary()) {
          continue;
        }
        iMap = (Map<Object, Object>) br.getRegionMap().getInternalMap();
        /* count the total records from all blocks */
        totalCount += iMap.values().stream().filter(Objects::nonNull)
            .map(e -> ((RegionEntry) e)._getValue()).filter(e -> !(e instanceof Token))
            .map(e -> e instanceof VMCachedDeserializable
                ? ((VMCachedDeserializable) e).getDeserializedForReading() : e)
            .filter(e -> e instanceof BlockValue).mapToLong(e -> ((BlockValue) e).getCurrentIndex())
            .sum();
        /* count the evicted entries as well, when required */
        if (countEvictedEntries) {
          totalCount += br.getEvictions();
        }
      }
      return totalCount;
    }
    FTable ftable = (FTable) table;
    FTableDescriptor td = ftable.getTableDescriptor();
    List<MColumnDescriptor> cds = td.getAllColumnDescriptors();
    List<Cell> cells =
        cds.stream().map(cold -> new CellRef(cold.getColumnName(), cold.getColumnType()))
            .collect(Collectors.toList());
    ThinRowShared thinRowShared = new ThinRowShared(cells, td, Collections.EMPTY_LIST);
    final ThinRow row = ThinRow.create(td, ThinRow.RowFormat.F_FULL_ROW);
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    for (int i = 0; i < phs.length; i++) {
      filterList.addFilter(phs[i]);
    }
    Map iMap;

    for (Integer bucket : buckets) {
      BucketRegion br = region.getLocalBucketById(bucket);
      if (br == null || !br.getBucketAdvisor().isPrimary()) {
        continue;
      }
      long evictedcount = 0;
      if (countEvictedEntries)
        evictedcount = br.getEvictions();
      iMap = (Map) br.getRegionMap().getInternalMap();
      // TODO: Optimization to remove temp arraylist records.
      long count = iMap.values().stream().filter(e -> e != null)
          .map(e -> ((RegionEntry) e)._getValue()).filter(e -> !(e instanceof Token)).flatMap(e -> {
            ArrayList<byte[]> records = new ArrayList<byte[]>();
            Object value = e;
            if (value instanceof VMCachedDeserializable) {
              value = ((VMCachedDeserializable) value).getDeserializedForReading();
            }
            if (value instanceof BlockValue) {
              Iterator itr = ((BlockValue) value).iterator();
              while (itr.hasNext()) {
                records.add((byte[]) itr.next());
              }
            }
            return records.stream();
          }).filter(e -> {
            row.reset(null, (byte[]) e);
            return ScanUtils.executeSimpleFilter(row, filterList, td);
          }).count();
      totalCount = totalCount + count + evictedcount;
    }
    return totalCount;
  }

  @Override
  public String getId() {
    return this.getClass().getName();
  }
}
