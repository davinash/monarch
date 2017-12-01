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
package io.ampool.client;

import io.ampool.monarch.table.functions.TestDUnitBase;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class AmpoolClientDUnitTest {
  private static final TestDUnitBase TEST_BASE = new TestDUnitBase();

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_BASE.preSetUp();
    TEST_BASE.postSetUp();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    TEST_BASE.preTearDownCacheTestCase();
  }

  // /**
  // * The underlying Geode cache is created even if the specified locator (host/port) is
  // unavailable
  // * at the moment. Just make sure that no exceptions are thrown in such cases.
  // */
  // @Test
  // public void testInvalidHost() {
  // final AmpoolClient ac = new AmpoolClient("my-junk-host", 1234);
  //
  // /* even though the locator is not available, at that moment, cache is still created */
  // assertNotNull(ac);
  // assertNotNull(ac.get.getRegion(MTableUtils.getMetaRegionName()));
  // }

  // @Test
  // public void testValidClient() {
  // final int port = Integer.parseInt(TEST_BASE.getLocatorPort());
  // final AmpoolClient ac = new AmpoolClient("localhost", port);
  // assertNotNull(ac);
  // assertTrue(ac.isConnected());
  // assertNotNull(ac.getgetRegion(MTableUtils.getMetaRegionName()));
  //
  // /* just make sure that the correct objects are wrapped inside client */
  // final MClientCache cc = MClientCacheFactory.getAnyInstance();
  // assertSame(cc.getAdmin(), ac.getAdmin());
  // assertSame(MonarchCacheImpl.getGeodeCacheInstance(), ac.getGeodeCache());
  // }
}
