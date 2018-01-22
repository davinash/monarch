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

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.exceptions.MException;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import org.apache.geode.CancelException;
import org.apache.geode.DeltaSerializationException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionReinitializedException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.partitioned.RedundancyAlreadyMetException;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class FTableBucketRegion extends TableBucketRegion {

  private static final Logger logger = LogService.getLogger();

  public FTableBucketRegion(String regionName, RegionAttributes attrs, LocalRegion parentRegion,
                            GemFireCacheImpl cache, InternalRegionArguments internalRegionArgs) {
    super(regionName, attrs, parentRegion, cache, internalRegionArgs);
  }

  /**
   * Get the descriptor identifying this table-region.
   *
   * @return the descriptor
   */
  @Override
  public FTableDescriptor getDescriptor() {
    return (FTableDescriptor) super.getDescriptor();
  }

  @Override
  public Region createSubregion(String subregionName, RegionAttributes attrs,
                                InternalRegionArguments internalRegionArgs)
          throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException {
    checkReadiness();
    LocalRegion newRegion = null;
    RegionAttributes regionAttributes = attrs;
    attrs = cache.invokeRegionBefore(this, subregionName, attrs, internalRegionArgs);
    final InputStream snapshotInputStream = internalRegionArgs.getSnapshotInputStream();
    final boolean getDestroyLock = internalRegionArgs.getDestroyLockFlag();
    final InternalDistributedMember imageTarget = internalRegionArgs.getImageTarget();
    try {
      if (getDestroyLock) {
        acquireDestroyLock();
      }
      LocalRegion existing = null;
      try {
        if (isDestroyed()) {
          if (this.reinitialized_old) {
            throw new RegionReinitializedException(toString(), getFullPath());
          }
          throw new RegionDestroyedException(toString(), getFullPath());
        }
        validateRegionName(subregionName, internalRegionArgs);

        validateSubregionAttributes(regionAttributes);
        String regionPath = calcFullPath(subregionName, this);

        // lock down the subregionsLock
        // to prevent other threads from adding a region to it in toRegion
        // but don't wait on initialization while synchronized (distributed
        // deadlock)
        synchronized (this.subregionsLock) {

          existing = (LocalRegion) this.subregions.get(subregionName);

          if (existing == null) {
            if (regionAttributes.getScope().isDistributed()
                    && internalRegionArgs.isUsedForPartitionedRegionBucket()) {
              final PartitionedRegion pr = internalRegionArgs.getPartitionedRegion();
              internalRegionArgs.setUserAttribute(pr.getUserAttribute());
              if (pr.isShadowPR()) {
                newRegion = new BucketRegionQueue(subregionName, regionAttributes, this, this.cache,
                        internalRegionArgs);
              } else {
                newRegion = new FTableBucketRegion(subregionName, regionAttributes, this,
                        this.cache, internalRegionArgs);
              }
            } else if (regionAttributes.getPartitionAttributes() != null) {
              newRegion = new FTablePartitionedRegion(subregionName, regionAttributes, this,
                      this.cache, internalRegionArgs);
            } else {
              boolean local = regionAttributes.getScope().isLocal();
              newRegion = local
                      ? new TableLocalRegion(subregionName, regionAttributes, this, this.cache,
                      internalRegionArgs)
                      : new TableDistributedRegion(subregionName, regionAttributes, this, this.cache,
                      internalRegionArgs);
            }
            Object o = this.subregions.putIfAbsent(subregionName, newRegion);

            Assert.assertTrue(o == null);

            Assert.assertTrue(!newRegion.isInitialized());

            //
            if (logger.isDebugEnabled()) {
              logger.debug("Subregion created: {}", newRegion.getFullPath());
            }
            if (snapshotInputStream != null || imageTarget != null
                    || internalRegionArgs.getRecreateFlag()) {
              this.cache.regionReinitialized(newRegion); // fix for bug 33534
            }

          } // endif: existing == null
        } // end synchronization
      } finally {
        if (getDestroyLock) {
          releaseDestroyLock();
        }
      }

      // Fix for bug 42127 - moved to outside of the destroy lock.
      if (existing != null) {
        // now outside of synchronization we must wait for appropriate
        // initialization on existing region before returning a reference to
        // it
        existing.waitOnInitialization();
        // fix for bug 32570
        throw new RegionExistsException(existing);
      }

      boolean success = false;
      try {
        newRegion.checkReadiness();
        this.cache.setRegionByPath(newRegion.getFullPath(), newRegion);
        if (regionAttributes instanceof UserSpecifiedRegionAttributes) {
          internalRegionArgs
                  .setIndexes(((UserSpecifiedRegionAttributes) regionAttributes).getIndexes());
        }
        newRegion.initialize(snapshotInputStream, imageTarget, internalRegionArgs); // releases
        // initialization
        // Latches
        // register the region with resource manager to get memory events
        if (!newRegion.isInternalRegion()) {
          if (!newRegion.isDestroyed) {
            cache.getResourceManager()
                    .addResourceListener(InternalResourceManager.ResourceType.MEMORY, newRegion);

            if (!newRegion.getOffHeap()) {
              newRegion.initialCriticalMembers(
                      cache.getResourceManager().getHeapMonitor().getState().isCritical(),
                      cache.getResourceAdvisor().adviseCritialMembers());
            } else {
              newRegion.initialCriticalMembers(
                      cache.getResourceManager().getHeapMonitor().getState().isCritical()
                              || cache.getResourceManager().getOffHeapMonitor().getState().isCritical(),
                      cache.getResourceAdvisor().adviseCritialMembers());
            }

            // synchronization would be done on ManagementAdapter.regionOpLock
            // instead of destroyLock in LocalRegion? ManagementAdapter is one
            // of the Resource Event listeners

            InternalDistributedSystem system = this.cache.getDistributedSystem();
            system.handleResourceEvent(ResourceEvent.REGION_CREATE, newRegion);
          }
        }
        success = true;
      } catch (CancelException | RegionDestroyedException | RedundancyAlreadyMetException e) {
        // don't print a call stack
        throw e;
      } catch (final RuntimeException validationException) {
        logger
                .warn(
                        LocalizedMessage.create(
                                LocalizedStrings.LocalRegion_INITIALIZATION_FAILED_FOR_REGION_0, getFullPath()),
                        validationException);
        throw validationException;
      } finally {
        if (!success) {
          this.cache.setRegionByPath(newRegion.getFullPath(), null);
          initializationFailed(newRegion);
          cache.getResourceManager(false).removeResourceListener(newRegion);
        }
      }

      newRegion.postCreateRegion();
    } finally {
      // make sure region initialization latch is open regardless
      // before returning;
      // if the latch is not open at this point, then an exception must
      // have occurred
      if (newRegion != null && !newRegion.isInitialized()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Region initialize latch is closed, Error must have occurred");
        }
      }
    }

    cache.invokeRegionAfter(newRegion);
    return newRegion;
  }

  // Entry (Put/Create) rules
  // If this is a primary for the bucket
  // 1) apply op locally, aka update or create entry
  // 2) distribute op to bucket secondaries and bridge servers with synchrony on local entry
  // 3) cache listener with synchrony on entry
  // Else not a primary
  // 1) apply op locally
  // 2) update local bs, gateway
  // This is a copy of BucketRegion::virtualPut + ampool changes.
  @Override
  protected boolean virtualPut(EntryEventImpl event, boolean ifNew, boolean ifOld,
                               Object expectedOldValue, boolean requireOldValue, long lastModified,
                               boolean overwriteDestroyed) throws TimeoutException, CacheWriterException {

    /* synchronize the key generation/merge/put */
    synchronized (this) {
      setBlockKeyVal(event);

      beginLocalWrite(event);

      try {
        if (this.getPartitionedRegion().isParallelWanEnabled()) {
          handleWANEvent(event);
        }
        if (!hasSeenEvent(event)) {
          forceSerialized(event);
          RegionEntry oldEntry = this.entries.basicPut(event, lastModified, ifNew, ifOld,
                  expectedOldValue, requireOldValue, overwriteDestroyed);
          return oldEntry != null;
        }
        if (event.getDeltaBytes() != null && event.getRawNewValue() == null
                && !hasSeenEvent(event)) {
          // This means that this event has delta bytes but no full value.
          // Request the full value of this event.
          // The value in this vm may not be same as this event's value.
          throw new InvalidDeltaException(
                  "Cache encountered replay of event containing delta bytes for key " + event.getKey());
        }
        // Forward the operation and event messages
        // to members with bucket copies that may not have seen the event. Their
        // EventTrackers will keep them from applying the event a second time if
        // they've already seen it.
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "BR.virtualPut: this cache has already seen this event {}",
                  event);
        }
        if (!getConcurrencyChecksEnabled() || event.hasValidVersionTag()) {
          distributeUpdateOperation(event, lastModified);
        }
        return true;
      } finally {
        endLocalWrite(event);
      }
    }
  }



  private void setBlockKeyVal(EntryEventImpl entryEvent) {
    if (this.getBucketAdvisor().isPrimary()) {
      /** import data case.. nothing to be done **/
      if (entryEvent.getKey() instanceof BlockKey) {
        return;
      }
      final Pair<Object, Object> lastEntry =
              ((RowTupleConcurrentSkipListMap) this.entries.getInternalMap()).lastEntryPair();
      BlockKey blockKey = null;
      BlockValue blockValue = null;
      long timestamp = System.currentTimeMillis();
      if (lastEntry == null) { // this is first entry
        blockKey = new BlockKey(timestamp, updateSequenceNumber(1), getId());
        blockValue = new BlockValue(getDescriptor().getBlockSize());
        blockKey = appendToBlockValue(entryEvent, getDescriptor(), blockKey, blockValue, timestamp);
      } else { // entries exist in map
        final BlockKey lastEntryKey = (BlockKey) lastEntry.getFirst();
        /* reset the sequence-number in case bucket was secondary/moved here/recovered.. */
        if (getSequenceNumber() != lastEntryKey.getBlockSequenceID() + 1) {
          setSequenceNumber(lastEntryKey.getBlockSequenceID() + 1);
        }
        /** make sure that the entry being evicted is not used to append the value **/
        synchronized (lastEntryKey) {
          Object lastEntryValue = ((RegionEntry) lastEntry.getSecond())._getValue();
          boolean blockEvicted =
                  lastEntryValue instanceof BlockValue && ((BlockValue) lastEntryValue).isEvicted();
          if (blockEvicted) {
            logger.debug("Block already evicted: region= {}, key= {}", this.getRegion().getName(),
                    lastEntryKey);
          }
          boolean isDeltaRequired = false;
          /* recover the value from disk if it is not yet available in map */
          if (lastEntryValue == null) {
            lastEntryValue = get(lastEntryKey);
          }
          if (Token.isInvalidOrRemoved(lastEntryValue) || blockEvicted) { // invalid token
            blockKey = new BlockKey(timestamp, updateSequenceNumber(1), getId());
            blockValue = new BlockValue(getDescriptor().getBlockSize());
          } else { // valid entry exists
            BlockValue lastBlockValue = null;
            if (lastEntryValue instanceof VMCachedDeserializable) {
              lastBlockValue =
                      (BlockValue) ((VMCachedDeserializable) lastEntryValue).getDeserializedValue(
                              entryEvent.getRegion(), (RegionEntry) lastEntry.getSecond());
            } else {
              lastBlockValue = (BlockValue) lastEntryValue;
            }
            if (lastBlockValue.getCurrentIndex() >= getDescriptor().getBlockSize()) { // block full
              blockKey = new BlockKey(timestamp, updateSequenceNumber(1), getId());
              blockValue = new BlockValue(getDescriptor().getBlockSize());
            } else { // same block add in the existing entry
              blockKey = lastEntryKey;
              blockValue = lastBlockValue;
              isDeltaRequired = true;
            }
          }
          blockValue.resetDelta();
          blockKey =
                  appendToBlockValue(entryEvent, getDescriptor(), blockKey, blockValue, timestamp);
          boolean deltaPropagation = this.getSystem().getConfig().getDeltaPropagation();
          if (isDeltaRequired && deltaPropagation && blockValue.hasDelta()) {
            HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
            try {
              blockValue.toDelta(hdos);
            } catch (IOException e) {
              throw new DeltaSerializationException(e);
            }
            entryEvent.setDeltaBytes(hdos.toByteArray());
          }
        }
      }
      entryEvent.getKeyInfo().setKey(blockKey);
    } else {
      // nothing to be done, at the moment, in case of secondary..
    }
  }

  private BlockKey appendToBlockValue(EntryEventImpl entryEvent, FTableDescriptor fTableDescriptor,
                                      BlockKey blockKey, BlockValue blockValue, long timestamp) {
    if (hasSeenEvent(entryEvent)) {
      return blockKey;
    }
    Object value = entryEvent.getValue();
    int offsetToStoreInsertionTS = fTableDescriptor.getOffsetToStoreInsertionTS();
    if (value instanceof byte[]) {
      System.arraycopy(Bytes.toBytes(timestamp), 0, (byte[]) value, offsetToStoreInsertionTS,
              Bytes.SIZEOF_LONG);
    } else {
      if (value instanceof VMCachedDeserializable) {
        value = ((VMCachedDeserializable) value).getDeserializedForReading();
      }
      for (byte[] bytes : ((BlockValue) value).getRecords()) {
        System.arraycopy(Bytes.toBytes(timestamp), 0, bytes, offsetToStoreInsertionTS,
                Bytes.SIZEOF_LONG);
      }
    }
    blockValue.checkAndAddRecord(value, fTableDescriptor);

    /* close the block if reached past block-size */
    if (blockValue.size() >= fTableDescriptor.getBlockSize()) {
      blockValue.close(fTableDescriptor);
    }
    entryEvent.setNewValue(blockValue);
    return blockKey;
  }

  private boolean isBlockWithOlderEncodingScheme(byte encoding, FTableDescriptor fTableDescriptor) {
    byte[] storageFormatterIdentifiers = fTableDescriptor.getStorageFormatterIdentifiers();
    if (encoding < storageFormatterIdentifiers[1]) {
      // older block
      return true;
    }
    if (encoding == storageFormatterIdentifiers[1])
      return false;

    // What should we do if block encoding is greater that descriptor encoding
    // This should never happen
    throw new MException("Corrput block found");
  }


  static int calcMemSize(Object value) {
    if (value == null || value instanceof Token) {
      return 0;
    }
    if (!(value instanceof byte[]) && !(value instanceof CachedDeserializable)
            && !(value instanceof org.apache.geode.Delta) && !(value instanceof GatewaySenderEventImpl)
            && !(value instanceof BlockValue)) {
      // ezoerner:20090401 it's possible this value is a Delta
      throw new InternalGemFireError(
              "DEBUG: calcMemSize: weird value (class " + value.getClass() + "): " + value);
    }
    try {
      return CachedDeserializableFactory.calcMemSize(value);
    } catch (IllegalArgumentException e) {
      return 0;
    }
  }

  /* TODO: need to update actual count and size for delete/update on FTable */
  private final AtomicLong actualCount = new AtomicLong(0L);
  private final AtomicLong lastSize = new AtomicLong(0L);

  public long getActualCount() {
    return actualCount.get() == 0 ? updateCount() : actualCount.get();
  }

  @SuppressWarnings("unchecked")
  private long updateCount() {
    Map<Object, Object> iMap = (Map<Object, Object>) this.getRegionMap().getInternalMap();
    /* count the total records from all blocks */
    long totalCount = iMap.values().stream().filter(Objects::nonNull)
            .map(e -> ((RegionEntry) e)._getValue()).filter(e -> !(e instanceof Token))
            .map(e -> e instanceof VMCachedDeserializable
                    ? ((VMCachedDeserializable) e).getDeserializedForReading() : e)
            .filter(e -> e instanceof BlockValue).mapToLong(e -> ((BlockValue) e).getCurrentIndex())
            .sum();
    actualCount.getAndAdd(totalCount);
    return totalCount;
  }

  public void incActualCount(final long delta) {
    this.actualCount.addAndGet(delta);
  }

  /**
   * Updates the bucket size.
   */
  void updateBucket2Size(int oldSize, int newSize, SizeOp op) {

    int memoryDelta = op.computeMemoryDelta(oldSize, newSize);

    incActualCount(op == SizeOp.DESTROY ? 0L : 1L);
    if (op == SizeOp.UPDATE) {
      memoryDelta = newSize - (int) lastSize.get();
    }
    lastSize.set(newSize);


    if (memoryDelta == 0)
      return;
    // do the bigger one first to keep the sum > 0
    updateBucketMemoryStats(memoryDelta);
  }


  private AtomicLong sequenceNumber = new AtomicLong();

  public long updateSequenceNumber(long delta) {
    return this.sequenceNumber.getAndAdd(delta);
  }

  /**
   * Get the current incremental sequence id that is used to generate unique key.
   *
   * @return the current sequence id
   */
  private long getSequenceNumber() {
    return this.sequenceNumber.get();
  }

  /**
   * Set the specified value as current sequence id.
   *
   * @param newValue the new value to be assigned as sequence id
   */
  private void setSequenceNumber(final long newValue) {
    this.sequenceNumber.set(newValue);
  }

  @Override
  public void destroyRegion(Object aCallbackArgument) {
    super.destroyRegion(aCallbackArgument);
    MCache anyInstance = MonarchCacheImpl.getInstance();
    anyInstance.getStoreHandler().deleteBucket(this.getDisplayName(), this.getId());
  }

}
