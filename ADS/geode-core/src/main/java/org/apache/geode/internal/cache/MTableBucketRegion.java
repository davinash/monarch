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

import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.internal.IBitMap;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.table.internal.MTableStorageFormatter;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.CacheLoaderException;
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
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.partitioned.RedundancyAlreadyMetException;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.util.concurrent.FutureResult;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MTableBucketRegion extends TableBucketRegion {

  private static final Logger logger = LogService.getLogger();

  public MTableBucketRegion(String regionName, RegionAttributes attrs, LocalRegion parentRegion,
      GemFireCacheImpl cache, InternalRegionArguments internalRegionArgs) {
    super(regionName, attrs, parentRegion, cache, internalRegionArgs);
  }

  /**
   * Return serialized form of an entry
   * <p>
   * Horribly plagiarized from the similar method in LocalRegion
   *
   * @param keyInfo
   * @param generateCallbacks
   * @param clientEvent holder for the entry's version information
   * @param returnTombstones TODO
   * @return serialized (byte) form
   * @throws IOException if the result is not serializable
   * @see LocalRegion#get(Object, Object, boolean, EntryEventImpl)
   */
  public RawValue getSerialized(KeyInfo keyInfo, boolean generateCallbacks, boolean doNotLockEntry,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws IOException {
    checkReadiness();
    checkForNoAccess();
    CachePerfStats stats = getCachePerfStats();
    long start = stats.startGet();

    boolean miss = true;
    try {
      RawValue valueBytes = NULLVALUE;
      boolean isCreate = false;
      RawValue result =
          getSerialized(keyInfo.getKey(), true, doNotLockEntry, clientEvent, returnTombstones);
      isCreate =
          result == NULLVALUE || (result.getRawValue() == Token.TOMBSTONE && !returnTombstones);
      miss = (result == NULLVALUE || Token.isInvalid(result.getRawValue()));
      if (result != NULLVALUE
          && AmpoolTableRegionAttributes.isAmpoolMTable(this.getCustomAttributes())) {
        MTableDescriptor descriptor = (MTableDescriptor) MTableUtils
            .getTableDescriptor((MonarchCacheImpl) this.getCache(), this.getDisplayName());
        if (descriptor.getCacheLoaderClassName() != null) {
          IBitMap bitMap =
              MTableStorageFormatter.readBitMap(descriptor, (byte[]) result.getDeserialized(false));
          for (Integer columnPosition : ((MTableKey) keyInfo.getKey()).getColumnPositions()) {
            if (false == bitMap.get(columnPosition)) {
              result = NULLVALUE;
              miss = true;
              break;
            }
          }
        }
      }
      if (miss) {
        // if scope is local and there is no loader, then
        // don't go further to try and get value
        if (hasServerProxy() || basicGetLoader() != null) {
          if (doNotLockEntry) {
            return REQUIRES_ENTRY_LOCK;
          }
          Object value = nonTxnFindObject(keyInfo, isCreate, generateCallbacks,
              result.getRawValue(), true, true, requestingClient, clientEvent, false);
          if (value != null) {
            result = new RawValue(value);
          }
        } else { // local scope with no loader, still might need to update stats
          if (isCreate) {
            recordMiss(null, keyInfo.getKey());
          }
        }
      }
      return result; // changed in 7.0 to return RawValue(Token.INVALID) if the entry is invalid
    } finally {
      stats.endGet(start, miss);
    }
  } // getSerialized

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
                newRegion = new MTableBucketRegion(subregionName, regionAttributes, this,
                    this.cache, internalRegionArgs);
              }
            } else if (regionAttributes.getPartitionAttributes() != null) {
              newRegion = new MTableRegion(subregionName, regionAttributes, this, this.cache,
                  internalRegionArgs);
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

  /**
   * optimized to only allow one thread to do a search/load, other threads wait on a future
   *
   * @param keyInfo
   * @param p_isCreate true if call found no entry; false if updating an existing entry
   * @param generateCallbacks
   * @param p_localValue the value retrieved from the region for this object.
   * @param disableCopyOnRead if true then do not make a copy
   * @param preferCD true if the preferred result form is CachedDeserializable
   * @param clientEvent the client event, if any
   * @param returnTombstones whether to return tombstones
   */
  @Retained
  public Object nonTxnFindObject(KeyInfo keyInfo, boolean p_isCreate, boolean generateCallbacks,
      Object p_localValue, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws TimeoutException, CacheLoaderException {
    final Object key = keyInfo.getKey();

    Object localValue = p_localValue;
    boolean isCreate = p_isCreate;
    Object[] valueAndVersion = null;
    @Retained
    Object result = null;
    FutureResult thisFuture = new FutureResult(this.stopper);
    Future otherFuture = (Future) this.getFutures.putIfAbsent(keyInfo.getKey(), thisFuture);
    // only one thread can get their future into the map for this key at a time
    if (otherFuture != null) {
      try {
        valueAndVersion = (Object[]) otherFuture.get();
        if (valueAndVersion != null) {
          result = valueAndVersion[0];
          if (clientEvent != null) {
            clientEvent.setVersionTag((VersionTag) valueAndVersion[1]);
          }
          if (!preferCD && result instanceof CachedDeserializable) {
            CachedDeserializable cd = (CachedDeserializable) result;
            // fix for bug 43023
            if (!disableCopyOnRead && isCopyOnRead()) {
              result = cd.getDeserializedWritableCopy(null, null);
            } else {
              result = cd.getDeserializedForReading();
            }

          } else if (!disableCopyOnRead) {
            result = conditionalCopy(result);
          }
          // what was a miss is now a hit
          RegionEntry re = null;
          if (isCreate) {
            re = basicGetEntry(keyInfo.getKey());
            updateStatsForGet(re, true);
          }
          return result;
        }
        // if value == null, try our own search/load
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // TODO check a CancelCriterion here?
        return null;
      } catch (ExecutionException e) {
        // unexpected since there is no background thread
        InternalGemFireError err = new InternalGemFireError(
            LocalizedStrings.LocalRegion_UNEXPECTED_EXCEPTION.toLocalizedString());
        err.initCause(err);
        throw err;
      }
    }
    // didn't find a future, do one more probe for the entry to catch a race
    // condition where the future was just removed by another thread
    try {
      boolean partitioned = this.getDataPolicy().withPartitioning();
      if (!partitioned) {
        localValue = getDeserializedValue(null, keyInfo, isCreate, disableCopyOnRead, preferCD,
            clientEvent, false, false);

        if (localValue != null
            && AmpoolTableRegionAttributes.isAmpoolMTable(this.getCustomAttributes())) {
          MTableDescriptor descriptor = (MTableDescriptor) MTableUtils
              .getTableDescriptor((MonarchCacheImpl) this.getCache(), this.getDisplayName());
          if (descriptor.getCacheLoaderClassName() != null) {
            IBitMap bitMap = MTableStorageFormatter.readBitMap(descriptor, (byte[]) localValue);
            for (Integer columnPosition : ((MTableKey) key).getColumnPositions()) {
              if (false == bitMap.get(columnPosition)) {
                localValue = null;
                break;
              }
            }
          }
        }

        // stats have now been updated
        if (localValue != null && !Token.isInvalid(localValue)) {
          result = localValue;
          return result;
        }
        isCreate = localValue == null;
        result = findObjectInSystem(keyInfo, isCreate, null, generateCallbacks, localValue,
            disableCopyOnRead, preferCD, requestingClient, clientEvent, returnTombstones);

      } else {

        // This code was moved from PartitionedRegion.nonTxnFindObject(). That method has been
        // removed.
        // For PRs we don't want to deserialize the value and we can't use findObjectInSystem
        // because
        // it can invoke code that is transactional.
        result =
            getSharedDataView().findObject(keyInfo, this, isCreate, generateCallbacks, localValue,
                disableCopyOnRead, preferCD, requestingClient, clientEvent, returnTombstones);
      }

      if (result == null && localValue != null) {
        if (localValue != Token.TOMBSTONE || returnTombstones) {
          result = localValue;
        }
      }
      // findObjectInSystem does not call conditionalCopy
    } finally {
      if (result != null) {
        VersionTag tag = (clientEvent == null) ? null : clientEvent.getVersionTag();
        thisFuture.set(new Object[] {result, tag});
      } else {
        thisFuture.set(null);
      }
      this.getFutures.remove(keyInfo.getKey());
    }
    if (!disableCopyOnRead) {
      result = conditionalCopy(result);
    }
    return result;
  }

}
