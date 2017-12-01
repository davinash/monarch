package org.apache.geode.internal.cache;

import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import org.apache.geode.CancelException;
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
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

public class TableDistributedRegion extends DistributedRegion {

  private static final Logger logger = LogService.getLogger();

  protected TableDistributedRegion(String regionName, RegionAttributes attrs,
      LocalRegion parentRegion, GemFireCacheImpl cache,
      InternalRegionArguments internalRegionArgs) {
    super(regionName, attrs, parentRegion, cache, internalRegionArgs);
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

                if (AmpoolTableRegionAttributes.isAmpoolFTable(attrs.getCustomAttributes())) {
                  newRegion = new FTableBucketRegion(subregionName, regionAttributes, this,
                      this.cache, internalRegionArgs);
                } else {
                  newRegion = new MTableBucketRegion(subregionName, regionAttributes, this,
                      this.cache, internalRegionArgs);
                }
              }
            } else if (regionAttributes.getPartitionAttributes() != null) {
              if (AmpoolTableRegionAttributes.isAmpoolFTable(attrs.getCustomAttributes())) {
                newRegion = new FTableRegion(subregionName, regionAttributes, this, this.cache,
                    internalRegionArgs);
              } else {
                newRegion = new MTableRegion(subregionName, regionAttributes, this, this.cache,
                    internalRegionArgs);
              }
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
}
