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

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.exceptions.RowKeyDoesNotExistException;
import io.ampool.monarch.table.exceptions.TruncateTableException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.ftable.exceptions.FTableNotExistsException;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.*;
import io.ampool.monarch.table.region.ScanUtils;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.CompareOp;
import io.ampool.orc.OrcUtils;
import io.ampool.store.StoreHandler;
import io.ampool.tierstore.SharedTierStore;
import io.ampool.tierstore.TierStore;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class TruncateTableFunction implements Function, InternalEntity {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = 786833738305344287L;

  @Override
  public void execute(FunctionContext context) {
    Object[] args = (Object[]) context.getArguments();
    String tableName = (String) args[0];
    Filter filter = (Filter) args[1];
    boolean preserveOlderVersions = (boolean) args[2];
    try {
      TableDescriptor td = MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
      if (td instanceof FTableDescriptor) {
        truncateFTable(tableName, filter);
      } else {
        truncateMTable(tableName, filter, preserveOlderVersions);
      }

      context.getResultSender().sendResult(true);
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    }
    context.getResultSender().lastResult(true);
  }

  private void truncateMTable(String tableName, Filter filter, boolean preserveOlderVersions) {
    if (preserveOlderVersions) {

      truncateFromMemory(tableName, filter);

    } else {

      MTable mTable = MCacheFactory.getAnyInstance().getMTable(tableName);
      Scan scan = new Scan();
      scan.setFilter(filter);
      scan.setMaxVersions();
      scan.setFilterOnLatestVersionOnly(false);
      Scanner mTableScanner = mTable.getScanner(scan);
      Iterator<Row> iterator = mTableScanner.iterator();

      while (iterator.hasNext()) {
        Row row = iterator.next();
        Delete delete = new Delete(row.getRowId());
        delete.setTimestamp(row.getRowTimeStamp());
        mTable.delete(delete);
      }
      mTableScanner.close();


    }
  }

  private void truncateFTable(final String tableName, final Filter filter) {
    /*
     * Truncate from in memory Truncate from WAL Truncate from Tiers 1 to N
     */
    try {
      truncateFromMemory(tableName, filter);
      // truncateFromWAL(tableName, filter);
      /**
       * To keep it simple, instead of truncating from the WAL flush the WAL and truncate the tier
       * stores truncating WAL has the following complexities 1. WAL stores full blocks, truncation
       * will need the blocks to be exploded and rewritten to new files. 2. More thread sync issues
       * between truncate, scan, WAL monitor and eviction threads.
       *
       * As a side effect some WAL records which are not yet ready to go to tiers (time based
       * eviction from memory(future)) will be pushed to tier.
       */
      flushWALDataToTiers(tableName);
      truncatefromTiers(tableName, filter);
    } catch (Exception e) {
      logger.error("Exception while truncating table data", e);
      throw new TruncateTableException(e.getMessage());
    }
  }

  private void flushWALDataToTiers(String tableName) {
    TableDescriptor td = MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
    if (td == null) {
      throw new FTableNotExistsException("Table not found: " + tableName);
    }
    int totalBuckets = td.getTotalNumOfSplits();
    for (int i = 0; i < totalBuckets; i++) {
      StoreHandler.getInstance().flushWriteAheadLog(tableName, i);
    }
  }

  private void truncatefromTiers(final String tableName, final Filter filter) throws IOException {
    TableDescriptor td = MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
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
        BucketRegion br = ((TablePartitionedRegion) region).getDataStore().getLocalBucketById(i);
        if (br != null && br.getBucketAdvisor().isHosting()) {
          if (!isShared || br.getBucketAdvisor().isPrimary()) {
            truncateTierBucket(store, tableName, i, filter, td,
                tierEntry.getValue().getTierProperties());
          }
        }
      }
    }
  }

  private void truncateTierBucket(TierStore store, String tableName, int i, Filter filter,
      TableDescriptor td, Properties tierProperties) throws IOException {
    store.truncateBucket(tableName, i, filter, td, tierProperties);
  }

  private void truncateFromMemory(final String tableName, final Filter filter) {
    /**
     * For all primary buckets for the region For all the blocks in a bucket If the block falls in
     * range lock the block Process all records in block and re-arrange.
     */
    TableDescriptor td = MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
    if (td == null) {
      throw new FTableNotExistsException("Table not found: " + tableName);
    }
    int totalBuckets = td.getTotalNumOfSplits();
    Region region = CacheFactory.getAnyInstance().getRegion(tableName);
    final OrcUtils.OrcOptions opts = new OrcUtils.OrcOptions(filter, td);
    for (int i = 0; i < totalBuckets; i++) {
      BucketRegion br = ((PartitionedRegion) region).getDataStore().getLocalBucketById(i);
      if (br != null && br.getBucketAdvisor().isHosting() && br.getBucketAdvisor().isPrimary()) {
        truncateBucket(region, br, filter, td, opts);
      }
    }
  }

  private void truncateBucket(Region region, final BucketRegion br, final Filter filter,
      TableDescriptor td, final OrcUtils.OrcOptions opts) {
    RowTupleConcurrentSkipListMap internalMap =
        (RowTupleConcurrentSkipListMap) br.getRegionMap().getInternalMap();
    Map realMap = internalMap.getInternalMap();

    /* TODO: Can we skip this bucket? */

    Iterator<Map.Entry<IMKey, RegionEntry>> itr = realMap.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<IMKey, RegionEntry> blockRegionEntry = itr.next();
      BlockKey lastKey = null;
      MKeyBase mKeyBase = null;


      if (td instanceof FTableDescriptor) {
        lastKey = (BlockKey) blockRegionEntry.getKey();
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
          int oldIdx = blockValue.getCurrentIndex();
          truncateBlock(lastKey, blockValue, filter, td);
          if (blockValue.getCurrentIndex() == 0) {
            // TODO: remove the block, this will maintain the order of keys in the table
            region.destroy(lastKey);
          } else if (oldIdx != blockValue.getCurrentIndex()) {
            region.put(lastKey, blockValue);
          }
          ((FTableBucketRegion) br).incActualCount(-1 * (oldIdx - blockValue.getCurrentIndex()));
        }
      } else {
        MTableDescriptor mtd = (MTableDescriptor) td;
        mKeyBase = (MKeyBase) blockRegionEntry.getKey();
        System.out
            .println("TruncateTableFunction.truncateBucket" + Bytes.toString(mKeyBase.getBytes()));
        PartitionedRegion pr =
            (PartitionedRegion) CacheFactory.getAnyInstance().getRegion(region.getName());
        BucketRegion primaryBucketForKey = getPrimaryBucketForKey(pr, mKeyBase);

        if (primaryBucketForKey != null) {
          Object value = primaryBucketForKey.get(mKeyBase);
          Object valueAfterFilter = performDeleteOperationWithFilter(td, value, filter);
          if (valueAfterFilter == null) {
            pr.destroy(mKeyBase);
          } else if (mtd.getMaxVersions() > 1) {
            pr.put(mKeyBase, valueAfterFilter, null);
          }
        } // no primary bucket for this key
      }

    }
  }


  /**
   * performs delete operation on the value with matching filter
   *
   * @param tableDescriptor table descriptor instance
   * @param value value on which delete needs to be performed
   * @param filter filter to be applied before deleting
   * @return value after deleting all matched version/rows
   */
  private Object performDeleteOperationWithFilter(TableDescriptor tableDescriptor, Object value,
      Filter filter) {
    if (value == null) {
      throw new RowKeyDoesNotExistException("Row Id does not exists");
    }

    MTableDescriptor mTableDescriptor = (MTableDescriptor) tableDescriptor;
    final ThinRow row = ThinRow.create(mTableDescriptor, ThinRow.RowFormat.M_FULL_ROW);

    // if multi-versioned
    if (value instanceof MultiVersionValue) {
      MultiVersionValue multiVersionValue = (MultiVersionValue) value;
      byte[][] versions = multiVersionValue.getVersions();
      int valueSize = 0;
      final List<byte[]> newVersions = new LinkedList<>();
      for (int i = 0; i < versions.length; i++) {
        byte[] version = versions[i];
        if (version == null) {
          continue;
        }
        row.reset(null, version);
        if (!ScanUtils.executeSimpleFilter(row, filter, tableDescriptor)) {
          valueSize += version.length;
          newVersions.add(version);
        }
      }
      if (newVersions.isEmpty()) {
        return null;
      }
      multiVersionValue.setVersions(newVersions.toArray(new byte[1][]), valueSize);
      return multiVersionValue;
    } else {
      // single versions delete
      row.reset(null, (byte[]) value);
      if (!ScanUtils.executeSimpleFilter(row, filter, tableDescriptor)) {
        return value;
      }
    }
    return null;
  }



  private BucketRegion getPrimaryBucketForKey(PartitionedRegion partitionedRegion, Object key) {
    int bucketId = PartitionedRegionHelper.getHashKey(partitionedRegion, null, key, null, null);
    BucketRegion localBucketById = partitionedRegion.getDataStore().getLocalBucketById(bucketId);
    if (localBucketById != null && localBucketById.getBucketAdvisor().isPrimary()) {
      return localBucketById;
    }
    return null;
  }

  private void truncateBlock(BlockKey lastKey, BlockValue blockValue, final Filter filter,
      TableDescriptor td) {
    blockValue.truncateBlock(lastKey, filter, td);
  }

  @Override
  public String getId() {
    return this.getClass().getName();
  }
}
