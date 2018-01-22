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

import org.apache.geode.cache.*;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class MTablePartitionedRegion extends TablePartitionedRegion {

  private static final Logger logger = LogService.getLogger();

  public MTablePartitionedRegion(String regionName, RegionAttributes attrs,
                                 LocalRegion parentRegion, GemFireCacheImpl cache,
                                 InternalRegionArguments internalRegionArgs) {
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
