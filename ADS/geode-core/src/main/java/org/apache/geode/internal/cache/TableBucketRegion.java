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

import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class TableBucketRegion extends BucketRegion {
  private static final Logger logger = LogService.getLogger();


  public TableBucketRegion(String regionName, RegionAttributes attrs, LocalRegion parentRegion,
      GemFireCacheImpl cache, InternalRegionArguments internalRegionArgs) {
    super(regionName, attrs, parentRegion, cache, internalRegionArgs);
  }


  /* TODO: need to update actual count and size for delete/update on FTable */
  private final AtomicLong actualCount = new AtomicLong(0L);
  private final AtomicLong lastSize = new AtomicLong(0L);


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

  @Override
  public Region createSubregion(String subregionName, RegionAttributes attrs,
      InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException {
    return null;
  }

  // @Override
  // public void fillInProfile(DistributionAdvisor.Profile profile) {
  // super.fillInProfile(profile);
  // ((CacheDistributionAdvisor.CacheProfile)profile).customRegionAttributes =
  // getCustomAttributes();
  //
  // }

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

    beginLocalWrite(event);

    try {
      if (super.getPartitionedRegion().isParallelWanEnabled()) {
        handleWANEvent(event);
      }
      if (!hasSeenEvent(event)) {
        forceSerialized(event);
        RegionEntry oldEntry = this.entries.basicPut(event, lastModified, ifNew, ifOld,
            expectedOldValue, requireOldValue, overwriteDestroyed);
        return oldEntry != null;
      }
      if (event.getDeltaBytes() != null && event.getRawNewValue() == null) {
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


  public Pair<Object, Object> getStartEndKeys() {
    return this.entries.getStartEndKey();
  }

  public long getActualCount() {
    return size();
  }
}
