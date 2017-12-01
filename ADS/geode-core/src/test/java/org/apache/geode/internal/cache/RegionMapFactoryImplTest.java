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


import static org.junit.Assert.assertTrue;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category(UnitTest.class)
public class RegionMapFactoryImplTest {
  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {
    try {
      Cache cache = CacheFactory.getAnyInstance();
      cache.close();
    } catch (Exception ex) {
      // cache is already closed
    }

  }

  @Test
  public void validateRegionMap() {
    Properties properties = new Properties();
    properties.setProperty("mcast-port", "0");
    CacheFactory cacheFactory = new CacheFactory(properties);
    Cache cache = cacheFactory.create();
    Region<Object, Object> replicatedRegion =
        cache.createRegionFactory(RegionShortcut.REPLICATE).create("ReplicatedRegion");
    assertTrue(((LocalRegion) replicatedRegion).entries instanceof VMRegionMap);
    replicatedRegion.destroyRegion();

    Region<Object, Object> partitionRegion =
        cache.createRegionFactory(RegionShortcut.PARTITION).create("PartitionRegion");

    assertTrue(((LocalRegion) partitionRegion).entries instanceof VMRegionMap);
    partitionRegion.destroyRegion();

    Region<Object, Object> partitionRegionLRU =
        cache.createRegionFactory(RegionShortcut.PARTITION_OVERFLOW).create("PartitionRegionLRU");

    assertTrue(((LocalRegion) partitionRegionLRU).entries instanceof VMLRURegionMap);
    partitionRegionLRU.destroyRegion();
    cache.close();
  }


  @Test
  public void validateRegionMapAfterRecovery() {
    Properties properties = new Properties();
    properties.setProperty("mcast-port", "0");
    CacheFactory cacheFactory = new CacheFactory(properties);
    Cache cache = cacheFactory.create();
    Region<Object, Object> replicatedRegion =
        cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).create("ReplicatedRegion");
    replicatedRegion.put("1", "1");
    replicatedRegion.put("2", "2");
    assertTrue(((LocalRegion) replicatedRegion).entries instanceof VMRegionMap);


    Region<Object, Object> partitionRegion =
        cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT).create("PartitionRegion");
    partitionRegion.put("1", "1");
    partitionRegion.put("2", "2");
    assertTrue(((LocalRegion) partitionRegion).entries instanceof VMRegionMap);

    Region<Object, Object> partitionRegionLRU =
        cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT_OVERFLOW)
            .create("PartitionRegionLRU");
    partitionRegionLRU.put("1", "1");
    partitionRegionLRU.put("2", "2");
    assertTrue(((LocalRegion) partitionRegionLRU).entries instanceof VMLRURegionMap);
    cache.close();

    cache = cacheFactory.create();
    replicatedRegion =
        cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).create("ReplicatedRegion");
    assertTrue(((LocalRegion) replicatedRegion).entries instanceof VMRegionMap);
    replicatedRegion.destroyRegion();

    partitionRegion =
        cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT).create("PartitionRegion");
    assertTrue(((LocalRegion) partitionRegion).entries instanceof VMRegionMap);
    partitionRegion.destroyRegion();

    partitionRegionLRU = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT_OVERFLOW)
        .create("PartitionRegionLRU");
    assertTrue(((LocalRegion) partitionRegionLRU).entries instanceof VMLRURegionMap);
    partitionRegionLRU.destroyRegion();
    cache.close();

  }

}
