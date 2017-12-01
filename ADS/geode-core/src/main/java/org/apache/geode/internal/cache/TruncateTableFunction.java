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
import io.ampool.monarch.table.filter.internal.BlockKeyFilter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.ftable.exceptions.FTableNotExistsException;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.*;
import io.ampool.monarch.table.region.ScanUtils;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.CompareOp;
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
  private FilterList blockFilters;
  private BlockKey startKey = null;
  private BlockKey stopKey = null;

  @Override
  public void execute(FunctionContext context) {
    Object[] args = (Object[]) context.getArguments();
    String tableName = (String) args[0];
    Filter filter = (Filter) args[1];
    boolean preserveOlderVersions = (boolean) args[2];
    blockFilters = handleSpecialColumnFilters(filter);
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
        BucketRegion br = ((TableRegion) region).getDataStore().getLocalBucketById(i);
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
    for (int i = 0; i < totalBuckets; i++) {
      BucketRegion br = ((PartitionedRegion) region).getDataStore().getLocalBucketById(i);
      if (br != null && br.getBucketAdvisor().isHosting() && br.getBucketAdvisor().isPrimary()) {
        truncateBucket(region, br, filter, td);
      }
    }
  }

  private void truncateBucket(Region region, final BucketRegion br, final Filter filter,
      TableDescriptor td) {
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
