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
package io.ampool.monarch.table;

import io.ampool.monarch.table.internal.AMPLJUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.experimental.categories.Category;


@Category(MonarchTest.class)
public class MTableNotExistingDistributedSystemDUnitTest extends AMPLJUnit4CacheTestCase {
  public MTableNotExistingDistributedSystemDUnitTest() {
    super();
  }
  /*
   * Test Description Try connection to server which is yet not start
   */


  // @Test
  // public void testConnectionWithNullConfiguration() {
  // Exception expectedException = null;
  // MClientCache clientCache = null;
  // try {
  // clientCache = new MClientCacheFactory().create((MConfiguration) null);
  // } catch (MCacheInvalidConfigurationException mic) {
  // expectedException = mic;
  // } finally {
  // if (clientCache != null) {
  // clientCache.close();
  // }
  // }
  // assertTrue(expectedException instanceof MCacheInvalidConfigurationException);
  // }

  // @Ignore
  // @Test
  // public void testConnectionWithNonExistentDistributedSystem() {
  // MConfiguration mconf = MConfiguration.create();
  // mconf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, "999.999.999.999");
  // mconf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, 9999);
  //
  // Exception expectedException = null;
  // MClientCache clientCache = null;
  // try {
  // clientCache = new MClientCacheFactory().create(mconf);
  // } catch (MCacheClosedException e) {
  // expectedException = e;
  // } finally {
  // if (clientCache != null) {
  // clientCache.close();
  // }
  // }
  // assertTrue(expectedException instanceof MCacheClosedException);
  // }
}
