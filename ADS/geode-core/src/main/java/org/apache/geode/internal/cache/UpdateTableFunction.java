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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.exceptions.TruncateTableException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.filter.internal.BlockKeyFilter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.ftable.exceptions.FTableNotExistsException;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.CompareOp;
import io.ampool.store.StoreHandler;
import io.ampool.tierstore.SharedTierStore;
import io.ampool.tierstore.TierStore;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

public class UpdateTableFunction implements Function, InternalEntity {
  private static final long serialVersionUID = 6096309114892773481L;
  private static final Logger logger = LogService.getLogger();
  private FilterList blockFilters;
  private BlockKey startKey = null;
  private BlockKey stopKey = null;

  @Override
  public void execute(FunctionContext context) {
    Object[] args = (Object[]) context.getArguments();
    String tableName = (String) args[0];
    Filter filter = (Filter) args[1];
    Map<byte[], Object> colValues = (Map<byte[], Object>) args[2];
    blockFilters = handleSpecialColumnFilters(filter);
    try {
      TableDescriptor td = (TableDescriptor) CacheFactory.getAnyInstance()
          .getRegion(MTableUtils.AMPL_META_REGION_NAME).get(tableName);
      updateFTable(tableName, filter, colValues, td);
      context.getResultSender().sendResult(true);
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    }
    context.getResultSender().lastResult(true);
  }

  private void updateFTable(final String tableName, final Filter filter,
      Map<byte[], Object> colValues, final TableDescriptor td) {
    /*
     * Truncate from in memory Truncate from WAL Truncate from Tiers 1 to N
     */
    Map<Integer, Object> columnIdxToValueMap = new Int2ObjectLinkedOpenHashMap<>(colValues.size());
    for (Map.Entry<byte[], Object> entry : colValues.entrySet()) {
      final int idx = td.getColumnByName(Bytes.toString(entry.getKey())).getIndex();
      columnIdxToValueMap.put(idx, entry.getValue());
    }
    try {
      updateInMemory(tableName, filter, columnIdxToValueMap, td);
      /**
       * To keep it simple, instead of truncating from the WAL flush the WAL and truncate the tier
       * stores truncating WAL has the following complexities 1. WAL stores full blocks, truncation
       * will need the blocks to be exploded and rewritten to new files. 2. More thread sync issues
       * between truncate, scan, WAL monitor and eviction threads.
       *
       * As a side effect some WAL records which are not yet ready to go to tiers (time based
       * eviction from memory(future)) will be pushed to tier.
       */
      flushWALDataToTiers(tableName, td);
      updateInTiers(tableName, filter, columnIdxToValueMap, td);
    } catch (Exception e) {
      logger.error("Exception while truncating table data", e);
      throw new TruncateTableException(e.getMessage());
    }
  }

  private void updateInMemory(final String tableName, final Filter filter,
      Map<Integer, Object> colValues, final TableDescriptor td) {
    /**
     * For all primary buckets for the region For all the blocks in a bucket If the block falls in
     * range lock the block Process all records in block and re-arrange.
     */
    if (td == null) {
      throw new FTableNotExistsException("Table not found: " + tableName);
    }
    int totalBuckets = td.getTotalNumOfSplits();
    Region region = CacheFactory.getAnyInstance().getRegion(tableName);
    for (int i = 0; i < totalBuckets; i++) {
      BucketRegion br = ((PartitionedRegion) region).getDataStore().getLocalBucketById(i);
      if (br != null && br.getBucketAdvisor().isHosting() && br.getBucketAdvisor().isPrimary()) {
        updateBucket(region, br, filter, colValues, td);
      }
    }
  }

  private void updateBucket(Region region, final BucketRegion br, final Filter filter,
      Map<Integer, Object> colValues, TableDescriptor td) {
    RowTupleConcurrentSkipListMap internalMap =
        (RowTupleConcurrentSkipListMap) br.getRegionMap().getInternalMap();
    Map realMap = internalMap.getInternalMap();

    /* TODO: Can we skip this bucket? */

    Iterator<Map.Entry<IMKey, RegionEntry>> itr = realMap.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<IMKey, RegionEntry> blockRegionEntry = itr.next();
      final BlockKey lastKey = (BlockKey) blockRegionEntry.getKey();
      if (filterRowKey(lastKey)) {
        /* block not of interest */
        continue;
      }
      synchronized (lastKey) {
        Object value = blockRegionEntry.getValue()._getValue();
        if (value == null || Token.isInvalidOrRemoved(value)) {
          continue;
        }
        if (value instanceof VMCachedDeserializable) {
          value = ((VMCachedDeserializable) value).getDeserializedForReading();
        }
        BlockValue blockValue = (BlockValue) value;
        int oldIdx = blockValue.getCurrentIndex();
        updateBlock(lastKey, blockValue, filter, colValues, td);
        if (blockValue.getCurrentIndex() == 0) {
          // TODO: remove the block, this will maintain the order of keys in the table
          region.destroy(lastKey);
        } else if (oldIdx != blockValue.getCurrentIndex()) {
          region.put(lastKey, blockValue);
        }
      }
    }
  }

  private void updateBlock(BlockKey lastKey, BlockValue blockValue, final Filter filter,
      Map<Integer, Object> colValues, TableDescriptor td) {
    blockValue.updateBlock(lastKey, filter, colValues, td);
  }

  /**
   * Running RowFilter before fetching value if present
   *
   * @param blockKey Block key
   * @return
   */
  private boolean filterRowKey(final BlockKey blockKey) {
    boolean filterRowKey = false;
    Filter filter = blockFilters;
    if (filter instanceof BlockKeyFilter) {
      filterRowKey = ((BlockKeyFilter) filter).filterBlockKey(blockKey);
    } else if (filter instanceof FilterList) {
      List<Filter> filters = ((FilterList) filter).getFilters();
      for (Filter mFilter : filters) {
        if (mFilter instanceof BlockKeyFilter) {
          boolean dofilter = ((BlockKeyFilter) mFilter).filterBlockKey(blockKey);;
          if (dofilter) {
            filterRowKey = true;
            break;
          }
        }
      }
    }
    return filterRowKey;
  }

  private void updateInTiers(final String tableName, final Filter filter,
      Map<Integer, Object> colValues, final TableDescriptor td) throws IOException {
    if (td == null) {
      throw new FTableNotExistsException("Table not found: " + tableName);
    }
    int totalBuckets = td.getTotalNumOfSplits();
    Region region = CacheFactory.getAnyInstance().getRegion(tableName);
    for (Map.Entry<String, TierStoreConfiguration> tierEntry : ((FTableDescriptor) td)
        .getTierStores().entrySet()) {
      String storeName = tierEntry.getKey();
      TierStore store = StoreHandler.getInstance().getTierStore(storeName);
      boolean isShared = store instanceof SharedTierStore;
      for (int i = 0; i < totalBuckets; i++) {
        BucketRegion br = ((PartitionedRegion) region).getDataStore().getLocalBucketById(i);
        if (br != null && br.getBucketAdvisor().isHosting()) {
          if (!isShared || br.getBucketAdvisor().isPrimary()) {
            updateTierBucket(store, tableName, i, filter, colValues, td,
                tierEntry.getValue().getTierProperties());
          }
        }
      }
    }
  }

  private void updateTierBucket(TierStore store, String tableName, int i, Filter filter,
      Map<Integer, Object> colValues, TableDescriptor td, Properties tierProperties)
      throws IOException {
    store.updateBucket(tableName, i, filter, colValues, td, tierProperties);
  }

  private void flushWALDataToTiers(String tableName, final TableDescriptor td) {
    if (td == null) {
      throw new FTableNotExistsException("Table not found: " + tableName);
    }
    int totalBuckets = td.getTotalNumOfSplits();
    for (int i = 0; i < totalBuckets; i++) {
      StoreHandler.getInstance().flushWriteAheadLog(tableName, i);
    }
  }

  public FilterList handleSpecialColumnFilters(Filter filter) {
    final FilterList blockFiltersList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    if (filter instanceof FilterList) {
      final FilterList filterList = (FilterList) filter;
      filterList.getFilters().forEach(mFilter -> {
        if (mFilter instanceof FilterList) {
          // as this contains list either of one should pass so changing operator to
          // Operator.MUST_PASS_ONE
          blockFiltersList.setOperator(FilterList.Operator.MUST_PASS_ONE);
          blockFiltersList.addFilter(transformFilterList((FilterList) mFilter));
        } else {
          final BlockKeyFilter blockKeyFilter = getBlockKeyFilter(mFilter);
          if (blockKeyFilter != null) {
            blockFiltersList.addFilter(blockKeyFilter);
          }
        }
      });
    } else {
      final BlockKeyFilter blockKeyFilter = getBlockKeyFilter(filter);
      if (blockKeyFilter != null) {
        blockFiltersList.addFilter(blockKeyFilter);
      }
    }
    return blockFiltersList;
  }


  public FilterList transformFilterList(FilterList filters) {
    FilterList newFilterList = new FilterList(filters.getOperator());
    filters.getFilters().forEach(mFilter -> {
      if (mFilter instanceof FilterList) {
        newFilterList.addFilter(transformFilterList((FilterList) mFilter));
      } else {
        final BlockKeyFilter blockKeyFilter = getBlockKeyFilter(mFilter);
        if (blockKeyFilter != null) {
          newFilterList.addFilter(blockKeyFilter);
        }
      }
    });
    return newFilterList;
  }

  BlockKeyFilter getBlockKeyFilter(Filter filter) {
    BlockKeyFilter blockKeyFilter = null;
    if (filter instanceof SingleColumnValueFilter && ((SingleColumnValueFilter) filter)
        .getColumnNameString().equals(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) {
      final SingleColumnValueFilter columnValueFilter = ((SingleColumnValueFilter) filter);
      final Object val = columnValueFilter.getValue();
      if ((val instanceof byte[])) {
        blockKeyFilter = new BlockKeyFilter(columnValueFilter.getOperator(), (byte[]) val);
      } else if ((val instanceof Long)) {
        blockKeyFilter =
            new BlockKeyFilter(columnValueFilter.getOperator(), Bytes.toBytes((Long) val));
      }
      // updateStartStopRow to get correct range map
      updateStartStopRow(columnValueFilter.getOperator(), val);
    }
    return blockKeyFilter;
  }

  private void updateStartStopRow(final CompareOp operator, final Object val) {
    long timeStamp = -1l;
    if ((val instanceof byte[])) {
      timeStamp = Bytes.toLong((byte[]) val);
    } else if ((val instanceof Long)) {
      timeStamp = (Long) val;
    } else {
      return;
    }
    switch (operator) {
      case LESS:
        // update stop key if current key is greater than old one
        if (stopKey == null) {
          stopKey = new BlockKey(timeStamp);
        } else {
          if (timeStamp > stopKey.getStartTimeStamp()) {
            stopKey.setStartTimeStamp(timeStamp);
          }
        }
        break;
      case LESS_OR_EQUAL:
        // as we want to include stopKey too so will add 1 to current value
        if (stopKey == null) {
          stopKey = new BlockKey(timeStamp + 1);
        } else {
          if (timeStamp >= stopKey.getStartTimeStamp()) {
            stopKey.setStartTimeStamp(timeStamp + 1);
          }
        }
        break;
      case GREATER:
        // update start key if current key is smaller than old one
        if (startKey == null) {
          startKey = new BlockKey(timeStamp);
        } else {
          if (timeStamp < startKey.getStartTimeStamp()) {
            startKey.setStartTimeStamp(timeStamp);
          }
        }
        break;
      case GREATER_OR_EQUAL:
        if (startKey == null) {
          startKey = new BlockKey(timeStamp - 1);
        } else {
          if (timeStamp <= startKey.getStartTimeStamp()) {
            startKey.setStartTimeStamp(timeStamp - 1);
          }
        }
        break;
      case EQUAL:
        // if start and/or stopkey not set then set with this value
        if (startKey == null) {
          startKey = new BlockKey(timeStamp);
        }
        if (stopKey == null) {
          stopKey = new BlockKey(timeStamp);
        }
        break;
    }
  }

  @Override
  public String getId() {
    return this.getClass().getName();
  }
}
