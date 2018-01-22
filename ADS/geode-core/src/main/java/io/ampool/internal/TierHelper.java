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

package io.ampool.internal;

import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.store.StoreHandler;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.FTableBucketRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TableBucketRegion;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.lru.LRUEntry;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;


public class TierHelper {
  private static final Logger logger = LogService.getLogger();
  private static final int WAL_RETRY_COUNT = Integer.getInteger("ampool.retry.wal.write", 5);

  /**
   * Overflow the entries (both key and value) to the next tier. Once the entry has been put
   * successfully in the tier, destroy the entry completely from in-memory to free the required
   * space. The subsequent scans would eventually fetch the required data from respective tiers.
   * Gets may not be supported on such overflowed data.
   * <p>
   * This is different from typical Geode overflow, where only value is removed, keeping key (with
   * the respective DiskEntry) still occupying the memory.
   *
   * @param region the region
   * @param entry the entry to be moved to next tier
   * @return the size of the entry i.e. the space to be freed after removing this entry
   */
  public static int overflowToNextTier(final LocalRegion region, final LRUEntry entry) {
    synchronized (entry) {
      /**
       * need explicit lock on key as the block-value (in FTable) is updated inside the block
       * synchronized on the key (BucketRegion::setBlockKeyVal).
       */
      synchronized (entry.getKey()) {
        if (entry.isInUseByTransaction() || entry.isDestroyedOrRemoved()) {
          if (logger.isTraceEnabled(LogMarker.LRU)) {
            logger.trace(LogMarker.LRU,
                "Not moving to the next tier; either it is in-use or already removed. Key={}",
                entry.getKey());
          }
          return 0;
        }
        /** do not destroy if the entry was not written successfully to the next tier **/
        if (writeToNextTier(region, entry)) {
          if (logger.isTraceEnabled(LogMarker.LRU)) {
            logger.trace(LogMarker.LRU,
                "Not moving to the next tier; failed to write entry to the next tier. Region={}, Key={}",
                region, entry.getKey());
          }
          return 0;
        }
        /** now safe to destroy the entry once it is written to the next tier **/
        if (region.evictDestroy(entry)) {
          return entry.getEntrySize();
        } else {
          return 0;
        }
      }
    }
  }


  /**
   * Overflow the entries (both key and value) to the next tier. Once the entry has been put
   * successfully in the tier, destroy the entry completely from in-memory to free the required
   * space. The subsequent scans would eventually fetch the required data from respective tiers.
   * Gets may not be supported on such overflowed data.
   * <p>
   * This is different from typical Geode overflow, where only value is removed, keeping key (with
   * the respective DiskEntry) still occupying the memory.
   *
   * @param region the region
   * @param entry the entry to be moved to next tier
   * @return the size of the entry i.e. the space to be freed after removing this entry
   */
  public static int overflowToNextStorageTier(final LocalRegion region, final LRUEntry entry) {
    synchronized (entry) {
      if (entry.isInUseByTransaction() || entry.isDestroyedOrRemoved()) {
        if (logger.isTraceEnabled(LogMarker.LRU)) {
          logger.trace(LogMarker.LRU,
              "Not moving to the next tier; either it is in-use or already removed. Key={}",
              entry.getKey());
        }
        return 0;
      }

      final Object key = entry.getKey();
      final Object value = entry._getValue();
      BlockValue blockValue;
      if (value instanceof BlockValue) {
        blockValue = (BlockValue) value;
      } else if (value instanceof VMCachedDeserializable) {
        // FIXME: Secondary should also receive BlockValue instead of VMCachedDeserializable
        blockValue =
            (BlockValue) ((VMCachedDeserializable) value).getDeserializedValue(region, entry);
      } else {
        logger.warn("Unknown value found for key= {}; value= {}", entry.getKey(), value);
        return 0;
      }

      // if (entry.testEvicted() || blockValue.isEvicted()) {
      // return 0;
      // }
      /**
       * need explicit lock on key as the block-value (in FTable) is updated inside the block
       * synchronized on the key (BucketRegion::setBlockKeyVal).
       */
      synchronized (key) {
        /**
         * delete the entry and then write to the WAL with retries.. it is this way since once we
         * write to the WAL it cannot be undone. NOTE: If the entry cannot be written to the WAL
         * even after retries then it will be lost.
         */
        final int size = entry.getEntrySize();
        if (region.evictDestroy(entry)) {
          entry.setEvicted();
          if (!blockValue.isEvicted()) {
            FTableBucketRegion br = (FTableBucketRegion) region;
            br.incEvictions(blockValue.getCurrentIndex() - 1);
            br.incActualCount(-1 * blockValue.getCurrentIndex());
            /** now write key-value to the WAL with fixed number of retries **/
            int i = 0;
            boolean status = false;
            while (i < WAL_RETRY_COUNT) {
              status = writeToNextStorageTier(br, (IMKey) key, blockValue);
              if (!status) {
                break;
              }
              i++;
            }
            /** log error if the entry was not written successfully to the next tier **/
            if (status) {
              logger.error("Failed to write to the storage tier. Region= {}, Key= {}",
                  region.getName(), entry.getKey());
            }
          }
          return size;
        } else {
          logger.info("Failed to evict entry; Region= {}, Key= {}", region.getName(),
              entry.getKey());
          return 0;
        }
      }
    }
  }



  public static int forcedOverflowToNextStorageTier(LocalRegion region, BlockKey blockKey,
      BlockValue blockValue) {

    final Object key = blockKey;
    final Object value = blockValue;
    /**
     * need explicit lock on key as the block-value (in FTable) is updated inside the block
     * synchronized on the key (BucketRegion::setBlockKeyVal).
     */
    synchronized (key) {
      /**
       * delete the entry and then write to the WAL with retries.. it is this way since once we
       * write to the WAL it cannot be undone. NOTE: If the entry cannot be written to the WAL even
       * after retries then it will be lost.
       */
      if (!blockValue.isEvicted()) {
        /** now write key-value to the WAL with fixed number of retries **/
        int i = 0;
        boolean status = false;
        while (i < WAL_RETRY_COUNT) {
          status = writeToNextStorageTier((BucketRegion) region, (IMKey) key, blockValue);
          if (!status) {
            break;
          }
          i++;
        }
        /** log error if the entry was not written successfully to the next tier **/
        if (status) {
          logger.error("Failed to write to the storage tier. Region= {}, Key= {}", region.getName(),
              blockKey);
        }
      }
      /* Return 0 on success */
      return 0;
    }
  }



  /**
   * A simple (crude) implementation of writing the entries to a file, both key and value.
   * <p>
   * The file is opened and closed here, every time, but tier-write should optimize that. This will
   * be eventually replaced by the write for the respective tier. For now, each bucket region has a
   * file for all of it's overflowed/moved entries. Here these are maintained in (insertion)
   * sequence but the actual tier-write implementation can decide that.
   *
   * @param region the region
   * @param entry the entry to be written
   * @return true if the entry was successfully written to the file; false otherwise
   */
  private static boolean writeToNextTier(final LocalRegion region, final LRUEntry entry) {

    /** TODO: Creation/closing of the file should be done only once per region.. **/
    try (

        FileOutputStream fos = new FileOutputStream(getTierFileNameKeys(region), true);
        BufferedOutputStream bos = new BufferedOutputStream(fos, 32768);
        DataOutputStream dos = new DataOutputStream(bos);
        SeekableByteChannel bc =
            Files.newByteChannel(new File(getTierFileNameValues(region)).toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);) {
      Object value = entry._getValue();
      byte[] valueBytes = value instanceof byte[] ? (byte[]) value
          : value instanceof CachedDeserializable
              ? ((CachedDeserializable) value).getSerializedValue() : null;
      if (valueBytes != null) {
        long pos = bc.position();
        bc.write(ByteBuffer.wrap(valueBytes));
        byte[] keyBytes = IMKey.getBytes(entry.getKey());
        DataSerializer.writeObject(new MKeyInfo(keyBytes, valueBytes.length, pos), dos);
      } else {
        logger.warn("Unknown value found for key= {}; value= {}", entry.getKey(), value);
      }
    } catch (IOException e) {
      logger.warn("Writing to the tier failed for key={}; skipping eviction.", entry.getKey(), e);
      return true;
    }

    return false;
  }


  private static boolean writeToNextStorageTier(final BucketRegion region, final IMKey key,
      final BlockValue blockValue) {
    StoreHandler storeHandler = MCacheFactory.getAnyInstance().getStoreHandler();
    try {
      storeHandler.append(region.getDisplayName(), region.getId(), key, blockValue);
      blockValue.setEvicted(true);
    } catch (IOException e) {
      logger.warn("Writing to the tier failed for key={}.", key, e);
      return true;
    }
    return false;
  }



  public static String getTierFileName(LocalRegion region) {
    final String suffix = region instanceof BucketRegion
        ? ((BucketRegion) region).getBucketAdvisor().isPrimary() ? "_primary" : "_secondary" : "";
    return region.getDiskDirs()[0].getAbsoluteFile() + "/" + region.getName() + suffix;
  }

  public static String getTierFileNameKeys(LocalRegion region) {
    return getTierFileName(region) + ".keys";
  }

  public static String getTierFileNameValues(LocalRegion region) {
    return getTierFileName(region) + ".values";
  }

  /**
   * A simple class to wrap the required key-information..
   */
  public static final class MKeyInfo implements DataSerializableFixedID {
    byte[] keyBytes;
    int valueLength;
    long valueOffset;

    public MKeyInfo() {}

    MKeyInfo(final byte[] bytes, final int valueLength, final long valueOffset) {
      this.keyBytes = bytes;
      this.valueLength = valueLength;
      this.valueOffset = valueOffset;
    }

    @Override
    public int getDSFID() {
      return NO_FIXED_ID;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeByteArray(keyBytes, out);
      DataSerializer.writePrimitiveInt(valueLength, out);
      DataSerializer.writePrimitiveLong(valueOffset, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.keyBytes = DataSerializer.readByteArray(in);
      this.valueLength = DataSerializer.readPrimitiveInt(in);
      this.valueOffset = DataSerializer.readPrimitiveLong(in);
    }

    @Override
    public Version[] getSerializationVersions() {
      return new Version[0];
    }
  }
}
