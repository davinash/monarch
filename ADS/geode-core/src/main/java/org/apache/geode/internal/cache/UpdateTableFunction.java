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
import java.util.Map;
import java.util.Properties;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.exceptions.TruncateTableException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.ftable.exceptions.FTableNotExistsException;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.orc.OrcUtils;
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

  @Override
  public void execute(FunctionContext context) {
    Object[] args = (Object[]) context.getArguments();
    String tableName = (String) args[0];
    Filter filter = (Filter) args[1];
    Map<byte[], Object> colValues = (Map<byte[], Object>) args[2];
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
    final OrcUtils.OrcOptions opts = new OrcUtils.OrcOptions(filter, td);
    for (int i = 0; i < totalBuckets; i++) {
      BucketRegion br = ((PartitionedRegion) region).getDataStore().getLocalBucketById(i);
      if (br != null && br.getBucketAdvisor().isHosting() && br.getBucketAdvisor().isPrimary()) {
        updateBucket(region, br, filter, colValues, td, opts);
      }
    }
  }

  private void updateBucket(Region region, final BucketRegion br, final Filter filter,
      Map<Integer, Object> colValues, TableDescriptor td, final OrcUtils.OrcOptions opts) {
    RowTupleConcurrentSkipListMap internalMap =
        (RowTupleConcurrentSkipListMap) br.getRegionMap().getInternalMap();
    Map realMap = internalMap.getInternalMap();

    /* TODO: Can we skip this bucket? */

    Iterator<Map.Entry<IMKey, RegionEntry>> itr = realMap.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<IMKey, RegionEntry> blockRegionEntry = itr.next();
      final BlockKey lastKey = (BlockKey) blockRegionEntry.getKey();
      synchronized (lastKey) {
        Object value = blockRegionEntry.getValue()._getValue();
        if (value == null || Token.isInvalidOrRemoved(value)) {
          continue;
        }
        if (value instanceof VMCachedDeserializable) {
          value = ((VMCachedDeserializable) value).getDeserializedForReading();
        }
        BlockValue blockValue = (BlockValue) value;
        /* based on column-statistics, skip the blocks that do not have rows matching filters */
        if (!OrcUtils.isBlockNeeded(opts, blockValue)) {
          continue;
        }
        final boolean isUpdated = blockValue.updateBlock(lastKey, filter, colValues, td);
        if (isUpdated) {
          region.put(lastKey, blockValue);
        }
      }
    }
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

  @Override
  public String getId() {
    return this.getClass().getName();
  }
}
