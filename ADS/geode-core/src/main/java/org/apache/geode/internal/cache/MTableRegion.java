package org.apache.geode.internal.cache;

import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class MTableRegion extends TableRegion {

  private static final Logger logger = LogService.getLogger();

  public MTableRegion(String regionName, RegionAttributes attrs, LocalRegion parentRegion,
      GemFireCacheImpl cache, InternalRegionArguments internalRegionArgs) {
    super(regionName, attrs, parentRegion, cache, internalRegionArgs);
  }

  @Override
  public Region createSubregion(String subregionName, RegionAttributes attrs,
      InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException {
    return super.createSubregion(subregionName, attrs, internalRegionArgs);
  }

  @Override
  public Object get(Object key, Object aCallbackArgument, boolean generateCallbacks,
      boolean disableCopyOnRead, boolean preferCD, ClientProxyMembershipID requestingClient,
      EntryEventImpl clientEvent, boolean returnTombstones, boolean opScopeIsLocal,
      boolean retainResult) throws TimeoutException, CacheLoaderException {
    return super.get(key, aCallbackArgument, generateCallbacks, disableCopyOnRead, preferCD,
        requestingClient, clientEvent, returnTombstones, opScopeIsLocal, retainResult);
  }
}
