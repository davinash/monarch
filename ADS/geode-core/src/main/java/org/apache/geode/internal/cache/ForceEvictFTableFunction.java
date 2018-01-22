/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.cache;

import io.ampool.internal.TierHelper;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.exceptions.TruncateTableException;
import io.ampool.monarch.table.ftable.exceptions.FTableNotExistsException;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.table.region.map.RowTupleLRURegionMap;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.lru.LRUEntry;
import org.apache.geode.internal.cache.lru.NewLRUClockHand;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;

public class ForceEvictFTableFunction implements Function, InternalEntity {
  private static final long serialVersionUID = 6096309114892773981L;
  private static final Logger logger = LogService.getLogger();

  @Override
  public void execute(FunctionContext context) {
    Object[] args = (Object[]) context.getArguments();
    String tableName = (String) args[0];
    try {
      flushFTable(tableName);
      context.getResultSender().sendResult(true);
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    }
    context.getResultSender().lastResult(true);
  }

  private void flushFTable(final String tableName) {
    try {
      /**
       * The entire in memory data of the table needs to be flushed to the tiers. Hence read the
       * entire data from the server and push it to WAL. WAL will flush the data to tier.
       */
      flushFromMemory(tableName);

    } catch (Exception e) {
      logger.error("Exception while force evicting table data", e);
      throw new TruncateTableException(e.getMessage());
    }
  }

  private void flushFromMemory(final String tableName) {
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
      if (br != null && br.getBucketAdvisor().isHosting()) {
        flushFromBucket(br);
      }
    }
  }

  private void flushFromBucket(final BucketRegion br) {
    final NewLRUClockHand lruList = ((RowTupleLRURegionMap) br.entries)._getLruList();
    while (true) {
      final LRUEntry lruEntry = (LRUEntry) lruList.getLRUEntry();
      if (lruEntry == null) {
        break;
      }
      if (TierHelper.overflowToNextStorageTier(br, lruEntry) != 0) {
        br.incEvictions(1L);
      }
    }
  }

  // Invoke the TierHelper forced Flush.
  private void pushBlocktoWAL(BucketRegion region, BlockKey lastKey, BlockValue blockValue) {
    TierHelper.forcedOverflowToNextStorageTier(region, lastKey, blockValue);
  }

  @Override
  public String getId() {
    return this.getClass().getName();
  }
}
