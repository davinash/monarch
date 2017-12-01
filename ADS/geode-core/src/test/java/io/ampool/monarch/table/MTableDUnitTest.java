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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MCacheClosedException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class MTableDUnitTest extends MTableDUnitHelper {
  public MTableDUnitTest() {
    super();
  }

  final int numOfEntries = 3;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();

    /*
     * startServerOn(vm0, DUnitLauncher.getLocatorString()); startServerOn(vm1,
     * DUnitLauncher.getLocatorString()); startServerOn(vm2, DUnitLauncher.getLocatorString());
     */

    /*
     * createClientCache(vm3); createClientCache();
     */
  }


  @Override
  public void tearDown2() throws Exception {
    // closeMClientCache();
    // closeMClientCache(this.vm3);
    super.tearDown2();
  }

  @Test
  public void testMClientCachegetAnyInstance() {
    Exception e = null;
    try {
      new MClientCacheFactory().getAnyInstance();
    } catch (MCacheClosedException mce) {
      e = mce;
    }
    assertTrue(e instanceof MCacheClosedException);
  }

  @Test
  public void testMClientCachegetAnyInstanceClientAccessingServerCache() {
    startServerOn(vm0, DUnitLauncher.getLocatorString());

    this.vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance();
        Exception e = null;
        try {
          MClientCacheFactory.getAnyInstance();
        } catch (IllegalStateException ise) {
          e = ise;
        }
        assertTrue(e instanceof IllegalStateException);
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    });
  }
}

// @Test
// public void testMClientCachebasicCreateAPI() {
// startServerOn(vm0, DUnitLauncher.getLocatorString());
// this.vm0.invoke(new SerializableCallable() {
// @Override
// public Object call() throws Exception {
// MCacheFactory.getAnyInstance();
// Exception e = null;
// try {
// new MClientCacheFactory().create((MConfiguration) null);
// } catch (IllegalStateException ise) {
// e = ise;
// }
// assertTrue(e instanceof IllegalStateException);
// MCacheFactory.getAnyInstance().close();
// return null;
// }
// });
// }

// @Test
// public void testSocketBufferSizes() {
// startServerOn(vm0, DUnitLauncher.getLocatorString());
// MConfiguration mconf = MConfiguration.create();
// mconf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, "127.0.0.1");
// mconf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, getLocatorPort());
// mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG,
// "MTableClient-testSocketBufferSizes.log");
// mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_SOCK_BUFFER_SIZE, "32767");
// MClientCache clientCache = new MClientCacheFactory().create(mconf);
//
// MClientCache clientCache = new MClientCacheFactory()
// .addPoolLocator("127.0.0.1", getLocatorPort()).set("log-file",
// "MTableClient-testSocketBufferSizes.log")
// .setPoolSocketBufferSize()
// assertEquals(32767, clientCache.getSocketBufferSize());
// clientCache.close();
// mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_SOCK_BUFFER_SIZE, "777777");
// clientCache = new MClientCacheFactory().create(mconf);
// assertEquals(777777, clientCache.getSocketBufferSize());
// clientCache.close();
// }
// }
