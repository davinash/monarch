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

/**
 * Default RegionMapFactory implementation which creates the default region maps
 */
public class RegionMapFactoryImpl implements RegionMapFactory {
  /**
   * Creates a RegionMap that is stored in the VM.
   * 
   * @param localRegion the region that will be the owner of the map
   * @param internalRegionArgs internal region args
   * @return region map instance
   */
  @Override
  public RegionMap create(final LocalRegion localRegion,
      final InternalRegionArguments internalRegionArgs) {
    RegionMap.Attributes ma = new RegionMap.Attributes();
    ma.statisticsEnabled = localRegion.getStatisticsEnabled();
    ma.loadFactor = localRegion.getLoadFactor();
    ma.initialCapacity = localRegion.getInitialCapacity();
    ma.concurrencyLevel = localRegion.getConcurrencyLevel();
    if (localRegion.isProxy()) {
      return new ProxyRegionMap(localRegion, ma, internalRegionArgs);
    } else if (localRegion.getEvictionController() != null) {
      return new VMLRURegionMap(localRegion, ma, internalRegionArgs);
    } else {
      return new VMRegionMap(localRegion, ma, internalRegionArgs);
    }
  }

  /**
   * Creates a RegionMap that is stored in the VM. Called during DiskStore recovery before the
   * region actually exists.
   * 
   * @param placeHolderDiskRegion the place holder disk region that will be the owner of the map
   * @param internalRegionArgs internal region args
   * @return region map instance
   */
  @Override
  public RegionMap create(final PlaceHolderDiskRegion placeHolderDiskRegion,
      final InternalRegionArguments internalRegionArgs) {
    RegionMap.Attributes ma = new RegionMap.Attributes();
    ma.statisticsEnabled = placeHolderDiskRegion.getStatisticsEnabled();
    ma.loadFactor = placeHolderDiskRegion.getLoadFactor();
    ma.initialCapacity = placeHolderDiskRegion.getInitialCapacity();
    ma.concurrencyLevel = placeHolderDiskRegion.getConcurrencyLevel();
    if (placeHolderDiskRegion.getLruAlgorithm() != 0) {
      return new VMLRURegionMap(placeHolderDiskRegion, ma, internalRegionArgs);
    } else {
      return new VMRegionMap(placeHolderDiskRegion, ma, internalRegionArgs);
    }
  }
}
