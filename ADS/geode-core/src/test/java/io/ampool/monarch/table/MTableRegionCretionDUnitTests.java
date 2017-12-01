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

import io.ampool.internal.RegionDataOrder;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CustomRegionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MonarchTest.class)
public class MTableRegionCretionDUnitTests extends MTableDUnitHelper {
  Host host = Host.getHost(0);
  VM vm_0 = host.getVM(0);
  VM vm_1 = host.getVM(1);
  VM vm_2 = host.getVM(2);
  VM vm_3 = host.getVM(3);

  public MTableRegionCretionDUnitTests() {
    super();
  }

  public void cleanUpAllVMs() throws Exception {
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache c = null;
        try {
          c = MCacheFactory.getAnyInstance();
          if (c != null) {
            c.close();
          }
        } catch (CacheClosedException e) {
        }
        return null;
      }
    });
  }

  public Object restartServerOn(VM vm) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache c = null;
        try {
          c = MCacheFactory.getAnyInstance();
          int serverRestarted = 0;
          assertTrue(c.isServer());
          for (CacheServer server : c.getCacheServers()) {
            assertTrue(server.isRunning());
            server.stop();
            server.start();
            assertTrue(server.isRunning());
          }
          assertTrue(c.isServer());
        } catch (CacheClosedException cce) {
        }
        return null;
      }
    });
  }

  public void closeMClientCache() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    if (clientCache != null) {
      clientCache.close();
      clientCache = null;
    }
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }


  public void initVM(VM vm, final String locators) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("info"));
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
        MCache c = null;
        try {
          c = MCacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        c = MCacheFactory.create(getSystem(props));
        CacheServer s = c.addCacheServer();
        s.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
        s.start();
        return null;
      }
    });
  }

  private void createRegionOnServer(VM vm, final boolean isRowTuple, final boolean isOrdered) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        try {
          MCache c = MCacheFactory.getAnyInstance();
          RegionFactory rf = c.createRegionFactory(RegionShortcut.PARTITION);
          if (isRowTuple) {
            AmpoolTableRegionAttributes customAttributes = new AmpoolTableRegionAttributes();
            if (isOrdered) {

              customAttributes.setRegionDataOrder(RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED);
              rf.setCustomAttributes(customAttributes);
              // rf.setRegionDataOrder(RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED);
            } else {
              customAttributes.setRegionDataOrder(RegionDataOrder.ROW_TUPLE_UNORDERED);
              rf.setCustomAttributes(customAttributes);
              // rf.setRegionDataOrder(RegionDataOrder.ROW_TUPLE_UNORDERED);
            }
          }
          Region r = rf.create("REGION_WITH_ROW_TUPLE");
        } catch (Exception cce) {
          throw cce;
        }
        return null;
      }
    });
  }


  private void verifyRegionOnServer(VM vm, final RegionDataOrder regionDataOrder) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        try {
          MCache c = MCacheFactory.getAnyInstance();
          Region r = c.getRegion("REGION_WITH_ROW_TUPLE");
          if (r == null) {
            org.junit.Assert.fail("Region Creation failed");
          }
          if (regionDataOrder == null) {
            assertEquals(null, r.getAttributes().getCustomAttributes());
          } else {
            assertTrue(((AmpoolTableRegionAttributes) r.getAttributes().getCustomAttributes())
                .getRegionDataOrder().equals(regionDataOrder));
          }

        } catch (Exception cce) {
          throw cce;
        }
        return null;
      }
    });
  }

  /**
   * TEST DESCRIPTION: 1. Start Locator 2. Start 3 Data Node Server 3. Create Region with Row-Tuple
   * FALSE on 3 Data Node Servers 4. Verify if the Row-Tuple Property of the Region is FALSE.
   */
  @Test
  public void testRegionWithoutRowTupleRegion() throws Exception {
    initVM(this.vm_0, DUnitLauncher.getLocatorString());
    initVM(this.vm_1, DUnitLauncher.getLocatorString());
    initVM(this.vm_2, DUnitLauncher.getLocatorString());

    createRegionOnServer(this.vm_0, false, false);
    createRegionOnServer(this.vm_1, false, false);
    createRegionOnServer(this.vm_2, false, false);

    verifyRegionOnServer(this.vm_0, null);

    verifyRegionOnServer(this.vm_1, null);
    verifyRegionOnServer(this.vm_2, null);

    cleanUpAllVMs();
  }

  /**
   * TEST DESCRIPTION: 1. Start Locator 2. Start 3 Data Node Server 3. Create Region with Row-Tuple
   * TRUE on 3 Data Node Servers 4. Verify if the Row-Tuple Property of the Region is TRUE.
   */

  @Test
  public void testRegionWithRowTupleRegion() throws Exception {
    initVM(this.vm_0, DUnitLauncher.getLocatorString());
    initVM(this.vm_1, DUnitLauncher.getLocatorString());
    initVM(this.vm_2, DUnitLauncher.getLocatorString());

    createRegionOnServer(this.vm_0, true, true);
    createRegionOnServer(this.vm_1, true, true);
    createRegionOnServer(this.vm_2, true, true);

    verifyRegionOnServer(this.vm_0, RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED);
    verifyRegionOnServer(this.vm_1, RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED);
    verifyRegionOnServer(this.vm_2, RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED);

    restartServerOn(this.vm_1);
    verifyRegionOnServer(this.vm_1, RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED);

    cleanUpAllVMs();
  }

  /**
   * TEST DESCRIPTION: 1. Start Locator 2. Start 3 Data Node Server 3. Create Region with Row-Tuple
   * TRUE on 3 Data Node Servers 4. Verify if the Row-Tuple Property of the Region is TRUE.
   */

  @Test
  public void testRegionWithRowTupleUnorderedRegion() throws Exception {
    initVM(this.vm_0, DUnitLauncher.getLocatorString());
    initVM(this.vm_1, DUnitLauncher.getLocatorString());
    initVM(this.vm_2, DUnitLauncher.getLocatorString());

    createRegionOnServer(this.vm_0, true, false);
    createRegionOnServer(this.vm_1, true, false);
    createRegionOnServer(this.vm_2, true, false);

    verifyRegionOnServer(this.vm_0, RegionDataOrder.ROW_TUPLE_UNORDERED);
    verifyRegionOnServer(this.vm_1, RegionDataOrder.ROW_TUPLE_UNORDERED);
    verifyRegionOnServer(this.vm_2, RegionDataOrder.ROW_TUPLE_UNORDERED);

    cleanUpAllVMs();
  }

  /**
   * TEST DESCRIPTION: 1. Start Locator 2. Start 3 Data Node Server 3. Create Region with Row-Tuple
   * TRUE on 2 Data Node Servers 4. Create Region with Same Name on 3rd Data Node Server with
   * Row-Tuple FALSE 5. Region Creation on 3rd node should fail with Mismatch of Configuration.
   */

  @Test
  public void testRegionWithRowTupleConfigMisMatch() throws Exception {
    try {
      initVM(this.vm_0, DUnitLauncher.getLocatorString());
      initVM(this.vm_1, DUnitLauncher.getLocatorString());
      initVM(this.vm_2, DUnitLauncher.getLocatorString());

      createRegionOnServer(this.vm_0, true, true);
      createRegionOnServer(this.vm_1, true, true);
      try {
        createRegionOnServer(this.vm_2, false, false);
        org.junit.Assert
            .fail("Region with Row-Tuple should have same configuration on all the nodes.");
      } catch (Exception ie) {
      }
    } finally {
      cleanUpAllVMs();
    }
  }

  /**
   * TEST DESCRIPTION: 1. Start Locator 2. Start 3 Data Node Server 3. Create Region with Row-Tuple
   * TRUE on 2 Data Node Servers 4. Create Region with Same Name on 3rd Data Node Server with
   * Row-Tuple FALSE 5. Region Creation on 3rd node should fail with Mismatch of Configuration.
   */

  @Test
  public void testRegionWithRowTupleUnorederedConfigMisMatch() throws Exception {
    try {
      initVM(this.vm_0, DUnitLauncher.getLocatorString());
      initVM(this.vm_1, DUnitLauncher.getLocatorString());
      initVM(this.vm_2, DUnitLauncher.getLocatorString());

      createRegionOnServer(this.vm_0, true, false);
      createRegionOnServer(this.vm_1, true, false);
      try {
        createRegionOnServer(this.vm_2, false, false);
        org.junit.Assert
            .fail("Region with Row-Tuple should have same configuration on all the nodes.");
      } catch (Exception ie) {
      }
    } finally {
      cleanUpAllVMs();
    }
  }
}
