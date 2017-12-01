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

import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.lang.StringUtils;

/**
 * Implement this interface to override the default region map associations
 */
public interface RegionMapFactory {
  /**
   * Creates a RegionMap that is stored in the VM.
   *
   * @param localRegion the region that will be the owner of the map
   * @param internalRegionArgs internal region args
   * @return region map instance
   */
  RegionMap create(final LocalRegion localRegion, final InternalRegionArguments internalRegionArgs);

  /**
   * Creates a RegionMap that is stored in the VM. Called during DiskStore recovery before the
   * region actually exists.
   *
   * @param placeHolderDiskRegion the place holder disk region that will be the owner of the map
   * @param internalRegionArgs internal region args
   */
  RegionMap create(final PlaceHolderDiskRegion placeHolderDiskRegion,
      final InternalRegionArguments internalRegionArgs);

  /**
   * Gets the region map factory instance if any custom region map factory set then the instance of
   * the custom region map factory is created otherwise default instance is created.
   */
  static RegionMapFactory getRegionMapFactory(final String regionMapFactory) {
    RegionMapFactory instance;
    try {
      if (StringUtils.isBlank(regionMapFactory)) {
        instance = new RegionMapFactoryImpl();
      } else {
        instance =
            (RegionMapFactory) ClassPathLoader.getLatest().forName(regionMapFactory).newInstance();
      }
    } catch (Exception ex) {
      throw new RuntimeException("Failed to create region map factory " + ex);
    }
    return instance;
  }

}
