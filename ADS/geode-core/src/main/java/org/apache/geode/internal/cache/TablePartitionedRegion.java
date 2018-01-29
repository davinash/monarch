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

import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.internal.IBitMap;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.table.internal.MTableStorageFormatter;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import org.apache.geode.CancelException;
import org.apache.geode.cache.*;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.partitioned.RedundancyAlreadyMetException;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

public class TablePartitionedRegion extends PartitionedRegion {
  // TODO: move all required changes from local region here
  private static final Logger logger = LogService.getLogger();
  private TableDescriptor descriptor;


  protected TablePartitionedRegion(String regionName, RegionAttributes attrs,
      LocalRegion parentRegion, GemFireCacheImpl cache,
      InternalRegionArguments internalRegionArgs) {
    super(regionName, attrs, parentRegion, cache, internalRegionArgs);
  }

  /**
   * Get the descriptor identifying this table-region.
   *
   * @return the descriptor
   */
  public TableDescriptor getDescriptor() {
    return this.descriptor == null
        ? descriptor = MTableUtils.getTableDescriptor((MonarchCacheImpl) cache, getDisplayName())
        : descriptor;
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
      if (getDestroyLock)
        acquireDestroyLock();
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
              newRegion = new MTablePartitionedRegion(subregionName, regionAttributes, this,
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
        if (getDestroyLock)
          releaseDestroyLock();
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

  @Override
  public void destroyRegion(Object aCallbackArgument)
      throws CacheWriterException, TimeoutException {
    super.destroyRegion(aCallbackArgument);
    MCache anyInstance = MonarchCacheImpl.getInstance();
    if (anyInstance != null && !anyInstance.isClosed()) {
      anyInstance.getStoreHandler().deleteTable(this.getDisplayName());
    }
  }


  /**
   * override the one in LocalRegion since we don't need to do getDeserialized.
   */
  // From: LocalRegion.java
  @Override
  public Object get(Object key, Object aCallbackArgument, boolean generateCallbacks,
      boolean disableCopyOnRead, boolean preferCD, ClientProxyMembershipID requestingClient,
      EntryEventImpl clientEvent, boolean returnTombstones, boolean opScopeIsLocal,
      boolean retainResult) throws TimeoutException, CacheLoaderException {
    assert !retainResult || preferCD;
    validateKey(key);
    validateCallbackArg(aCallbackArgument);
    checkReadiness();
    checkForNoAccess();
    discoverJTA();
    CachePerfStats stats = getCachePerfStats();
    long start = stats.startGet();
    boolean isMiss = true;
    try {
      KeyInfo keyInfo = getKeyInfo(key, aCallbackArgument);
      Object value = getDataView().getDeserializedValue(keyInfo, this, true, disableCopyOnRead,
          preferCD, clientEvent, returnTombstones, retainResult);
      final boolean isCreate = value == null;
      isMiss = value == null || Token.isInvalid(value)
          || (!returnTombstones && value == Token.TOMBSTONE);
      // Note: if the value was Token.DESTROYED then getDeserialized
      // returns null so we don't need the following in the above expression:
      // || (isRegInterestInProgress() && Token.isDestroyed(value))
      // because (value == null) will be true in this case.

      // AMPOOL SPECIFIC CHANGES START HERE
      if (value != null && AmpoolTableRegionAttributes.isAmpoolMTable(this.customAttributes)) {
        MTableDescriptor descriptor = (MTableDescriptor) MTableUtils
            .getTableDescriptor((MonarchCacheImpl) this.getCache(), this.getDisplayName());
        if (descriptor.getCacheLoaderClassName() != null) {
          IBitMap bitMap = MTableStorageFormatter.readBitMap(descriptor, (byte[]) value);
          for (Integer columnPosition : ((MTableKey) key).getColumnPositions()) {
            if (false == bitMap.get(columnPosition)) {
              isMiss = true;
              value = null;
              break;
            }
          }
        }
      }
      // AMPOOL SPECIFIC CHANGES ENDS HERE
      if (isMiss) {
        // to fix bug 51509 raise the precedence of opScopeIsLocal
        // if scope is local and there is no loader, then
        // don't go further to try and get value
        if (!opScopeIsLocal
            && ((getScope().isDistributed()) || hasServerProxy() || basicGetLoader() != null)) {
          // serialize search/load threads if not in txn
          value = getDataView().findObject(keyInfo, this, isCreate, generateCallbacks, value,
              disableCopyOnRead, preferCD, requestingClient, clientEvent, returnTombstones);
          if (!returnTombstones && value == Token.TOMBSTONE) {
            value = null;
          }
        } else { // local scope with no loader, still might need to update stats
          if (isCreate) {
            recordMiss(null, key);
          }
          value = null;
        }
      }
      return value;
    } finally {
      stats.endGet(start, isMiss);
    }
  }
}
