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
package io.ampool.tierstore.internal;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.store.StoreHandler;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.SharedTierStore;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.TierStoreWriter;
import io.ampool.utils.TimestampUtil;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.Logger;

/**
 * The thread that handles evicting/expiring the data/records from the tier to subsequent tier. In
 * case no next tier is configured it is assumed as /dev/null and the respective records are
 * deleted/expired from the tier. Once deleted, these records cannot be recovered.
 * <p>
 * A separate thread is launched per tier-store that keeps on monitoring for all tables at the
 * specified location. At the moment the granularity of data movement is at the provided
 * time-partitioning interval per table per tier. The complete data in that time-partition, from the
 * tier, is moved to the subsequent tier atomically. The complete time-partition in this tier is
 * written as a single unit (say single ORC file) in the next; thus it compacts all chunks/groups
 * for the time-partition in this tier.
 * <p>
 */
public class TierEvictorThread extends Thread {
  private static final Logger logger = LogService.getLogger();
  private static int fixedWaitTimeSecs = Integer.getInteger("ampool.tier.evictor.interval", 5);
  private final FileSystem fs;
  private final Path basePath;
  private String tierName;

  private static final long TO_MS = 1_000L;
  private TierStore thisTierStore;

  /* only for tests */
  private static boolean TEST_EVICT = false;
  private static Map<String, Long> TEST_TIER_EVICT_INTERVAL = new HashMap<>();

  /**
   * Constructor.. constructs the thread that will take care of evicting the required data from
   * _this_ tier to the subsequent tier.
   *
   * @param tierStore the tier-store
   * @param fs the root file-system of the tier
   * @param path the base path indicating the storage location for the tier
   */
  public TierEvictorThread(final TierStore tierStore, final FileSystem fs, final String path) {
    this.thisTierStore = tierStore;
    this.tierName = tierStore.getName();
    this.fs = fs;
    this.basePath = new Path(path);
    setName("TierEvictorThread_" + this.tierName);
    setDaemon(true);
  }

  /**
   * If this thread was constructed using a separate <code>Runnable</code> run object, then that
   * <code>Runnable</code> object's <code>run</code> method is called; otherwise, this method does
   * nothing and returns.
   * <p>
   * Subclasses of <code>Thread</code> should override this method.
   *
   * @see #start()
   * @see #stop()
   */
  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        /* sleep for a fixed time */
        Thread.sleep(getTimeToSleep());
        movePartitionsToNextTier();
      } catch (InterruptedException ie) {
        break;
      } catch (Exception e) {
        logger.warn("{}: Got exception while processing.", getName(), e);
      }
    }
    logger.info("Exiting {}...", this.getName());
  }

  /**
   * Move all the matching time-partitions from this tier to the next tier, of all tables, if the
   * respective time-expiration window is over. All the tables, all buckets, and all time-partitions
   * within a table are processed sequentially.
   *
   * The processing is skipped in case GemFireCache is unavailable.
   *
   * @throws IOException in case there was an exception while reading or writing
   */
  void movePartitionsToNextTier() throws IOException {
    if (!fs.exists(basePath)) {
      logger.debug("No files to process at location: {}", basePath);
      return;
    }

    /* iterate through all tables from the base-path to check what data needs to be moved */
    final FileStatus[] tables = fs.listStatus(basePath);
    for (final FileStatus tableFS : tables) {
      final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache == null) {
        logger.info("{}: Seems GemFireCache is unavailable; skipping..", getName());
        break;
      }
      final String tableName = tableFS.getPath().getName();
      final TableDescriptor td = MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
      final Map<String, TierStoreConfiguration> tiers = td != null && td instanceof FTableDescriptor
          ? ((FTableDescriptor) td).getTierStores() : null;
      if (tiers == null || tiers.isEmpty()) {
        logger.debug(
            "{}: Skipping table= {}; either it is not an FTable or does not have any tiers.",
            getName());
        continue;
      }
      final Properties tierProps = tiers.get(tierName).getTierProperties();
      final Iterator<Map.Entry<String, TierStoreConfiguration>> tierIterator =
          tiers.entrySet().iterator();
      while (tierIterator.hasNext()) {
        final Map.Entry<String, TierStoreConfiguration> next = tierIterator.next();
        if (next.getKey().equals(tierName)) {
          break;
        }
      }
      long[] attributes = getTableAttributes(tierProps);
      final TierStore nextTierStore = tierIterator.hasNext()
          ? StoreHandler.getInstance().getTierStore(tierIterator.next().getKey()) : null;
      /* iterate on all available buckets for the table */
      if (tableFS.isDirectory()) {
        final PartitionedRegion region =
            PartitionedRegionHelper.getPartitionedRegion(tableName, cache);
        if (region == null) {
          logger.info("Region= {} does not exist.. skipping.", tableName);
          continue;
        }
        final FileStatus[] tableBuckets = fs.listStatus(tableFS.getPath());
        for (FileStatus bucketFS : tableBuckets) {
          final String bucketId = bucketFS.getPath().getName();
          final int bid = Integer.parseInt(bucketId);
          final FileStatus[] bucketTimeParts = fs.listStatus(bucketFS.getPath());
          /*
           * TODO: maintain and check/optimize on last processed time-partition per table per bucket
           */
          for (FileStatus timePartFS : bucketTimeParts) {
            final String timePartId = timePartFS.getPath().getName();
            long l = Long.parseLong(timePartId);
            /* move the complete time-partition to next tier upon expiry */
            if ((l * attributes[1]) < (TimestampUtil.getCurrentTime() - attributes[0])) {
              final String[] unitPath =
                  Paths.get(tableName, bucketId, timePartId).toString().split("/");
              final int batchSize = (int) tierProps.getOrDefault("move-batch-size", 1_000);
              final BucketRegion br = region.getDataStore().getLocalBucketById(bid);
              moveUnitToNextTier(br,
                  nextTierStore == null ? null : nextTierStore.getConverterDescriptor(tableName),
                  nextTierStore, batchSize, timePartFS, unitPath);
            } else {
              logger.trace("{}: Not moving any data for table= {}, bucketId= {}; no expiration.",
                  getName(), tableName, bucketId);
            }
          }
        }
      }
    }
  }

  /**
   * Return true if the data from this tier should be written to the next tier; false otherwise. The
   * data is written to the next tier only if it is non-null. The next tier could be either local or
   * shared or the current bucket could be primary or secondary. Following are the combinations and
   * respective return values. - Local tier + Primary : true - Local tier + Secondary: true - Shared
   * tier+ Primary : true - Shared tier+ Secondary: false The data is not written to the next
   * tier-store if it is secondary.
   *
   * @param nextTierStore the next tier store
   * @param isPrimary whether the bucket being processed is primary
   * @return false if the next tier store is shared and processing secondary bucket; true otherwise
   */
  public static boolean shouldWriteToNextTier(final TierStore nextTierStore,
      final boolean isPrimary) {
    return nextTierStore != null && (!(nextTierStore instanceof SharedTierStore) || isPrimary);
  }

  /**
   * Move complete data from a time-partition to the next tier as a single unit.
   *
   * @param br the bucket region
   * @param cd the converter descriptor
   * @param nextTierStore the next tier store
   * @param batchSize the batch-size to be used for writing to the next tier
   * @param unitBaseLocation the file-system base location for the unit
   * @param unitPath the absolute path for the unit to be moved (table/bucket/time-part)
   * @throws IOException in case there were errors reading or writing
   */
  private void moveUnitToNextTier(final BucketRegion br, final ConverterDescriptor cd,
      final TierStore nextTierStore, final int batchSize, final FileStatus unitBaseLocation,
      final String... unitPath) throws IOException {
    int movedCount = 0;
    boolean processingError = false;
    final boolean isPrimary = br.getBucketAdvisor().isPrimary();
    if (shouldWriteToNextTier(nextTierStore, isPrimary)) {
      Properties props = new Properties();
      try {
        final TierStoreReader reader = thisTierStore.getReader(props, unitPath);
        final TierStoreWriter writer = nextTierStore.getWriter(props, unitPath);
        reader.setConverterDescriptor(cd);
        writer.setConverterDescriptor(cd);
        writer.openChunkWriter(props);
        final Iterator<StoreRecord> srItr = reader.iterator();
        List<StoreRecord> srs = new ArrayList<>(batchSize);
        while (srItr.hasNext()) {
          movedCount++;
          srs.add(srItr.next());
          if (srs.size() == batchSize) {
            writer.writeChunk(srs);
            srs.clear();
          }
        }
        if (srs.size() > 0) {
          writer.writeChunk(srs);
          srs.clear();
        }
        writer.closeChunkWriter();
        processingError = false;
      } catch (IOException e) {
        logger.error("{}: Error while moving data to tier= {} for: {}", getName(),
            nextTierStore.getName(), unitPath, e);
        processingError = true;
      }
    } else {
      logger.debug("Skipped data movement to next tier; nextTier= {}, isShared= {}, isPrimary= {}",
          nextTierStore == null ? "<null>" : nextTierStore.getName(),
          (nextTierStore instanceof SharedTierStore), isPrimary);
    }
    if (processingError) {
      logger.info("{}: Skipping deletion as error occurred while processing.", getName());
    } else {
      this.thisTierStore.delete(unitPath);
      logger.debug("Moved {} records from `{}` to `{}`", movedCount, tierName,
          nextTierStore == null ? "<null>" : nextTierStore.getName());
    }
  }

  /**
   * Get the required table attributes like time-to-expire and partitioning-interval. For tests,
   * when enabled via TEST_EVICT, returns the values the map.
   *
   * @param tierProps the tier properties
   * @return an array with time-to-expire and partitioning-interval
   */
  private long[] getTableAttributes(final Properties tierProps) {
    return TEST_EVICT && TEST_TIER_EVICT_INTERVAL.get(tierName) != null
        ? new long[] {TEST_TIER_EVICT_INTERVAL.get(tierName) * TO_MS,
            TierStoreConfiguration.DEFAULT_TIER_PARTITION_INTERVAL_MS}
        /* get the required information from table descriptor */
        : new long[] {(long) tierProps.get(TierStoreConfiguration.TIER_TIME_TO_EXPIRE),
            (long) tierProps.get(TierStoreConfiguration.TIER_PARTITION_INTERVAL)};
  }

  private long getTimeToSleep() {
    return fixedWaitTimeSecs * 1_000;
  }
}
