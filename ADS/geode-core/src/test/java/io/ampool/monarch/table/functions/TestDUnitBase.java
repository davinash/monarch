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

package io.ampool.monarch.table.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.AMPLJUnit4CacheTestCase;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Example test to create separate VMs (server and client) and insert some data in via server and
 * verify the same from client.
 * <p>
 * To use copy this test to the correct location with proper dependencies and execute
 * `testExample_1'.
 * <p>
 */

public class TestDUnitBase extends AMPLJUnit4CacheTestCase {
  /**
   * the default server host and port, to connect
   **/
  private static final String SERVER_HOST = "localhost";

  /**
   * Wrapper enum for server and client to return the required cache.
   */
  public enum CacheType {
    SERVER(true), CLIENT(false);
    private boolean isServer;

    /**
     * C'tor for cache type.
     *
     * @param isServer true if the type is server; false otherwise
     */
    CacheType(final boolean isServer) {
      this.isServer = isServer;
    }

    /**
     * Get the required cache for server and client.
     *
     * @param props the properties for creating the cache
     * @return the cache
     */
    MCache getCache(final Properties props) {
      MCache c;
      if (isServer) {
        try {
          c = MCacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
          c = new MCacheFactory(props).set("mcast-port", "0").create();
          CacheServer cs = c.addCacheServer();
          cs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
          try {
            cs.start();
          } catch (IOException e) {
            System.out.println("Unable to start MCache Server.");
          }
        }
      } else {
        props.remove(DistributionConfig.LOCATORS_NAME);
        c = (MCache) new MClientCacheFactory(props)
            .addPoolLocator(SERVER_HOST, DistributedTestUtils.getDUnitLocatorPort()).create();
      }
      return c;
    }
  }

  /**
   * the geode cache
   **/
  private static MCache cache;
  protected int numberOfVms = 4;
  List<VM> vmList = new ArrayList<>(getNumberOfVms());
  List<VM> serverList = new ArrayList<>(getNumberOfVms());
  private VM serverVm;
  private VM clientVm;

  public TestDUnitBase() {
    super();
    // Host host = Host.getHost(0);
    // for (int i = 0; i < numberOfVms; i++) {
    // vmList.add(host.getVM(i));
    // }
  }

  @Override
  public void preSetUp() throws Exception {
    DUnitLauncher.launchIfNeeded();
    disconnectAllFromDS();
    super.preSetUp();
  }

  public void postSetUp() throws Exception {
    super.postSetUp();
    Host host = Host.getHost(0);
    for (int i = 0; i < numberOfVms; i++) {
      vmList.add(host.getVM(i));
    }

    this.serverVm = vmList.get(0);
    createOnVm(serverVm, CacheType.SERVER);
    createOnVm(vmList.get(2), CacheType.SERVER);
    createOnVm(vmList.get(3), CacheType.SERVER);
    this.clientVm = vmList.get(1);
    createOnVm(clientVm, CacheType.CLIENT);
    serverList.add(serverVm);
    serverList.add(vmList.get(2));
    serverList.add(vmList.get(3));
  }

  public void restart() {
    /** stop all servers.. **/
    for (VM vm : serverList) {
      vm.invoke(new SerializableRunnable() {
        @Override
        public void run() throws Exception {
          MCacheFactory.getAnyInstance().getCacheServers().forEach(CacheServer::stop);
          MCacheFactory.getAnyInstance().close();
        }
      });
    }
    /** start the servers in async mode and wait for completion.. **/
    serverList.parallelStream().map(v -> createOnVmAsync(v, CacheType.SERVER, new Properties()))
        .forEach(task -> {
          try {
            task.join();
          } catch (InterruptedException ie) {
            ////
          }
        });
  }

  protected int getNumberOfVms() {
    return numberOfVms;
  }

  public void createOnVm(final VM vm, final CacheType type) throws RMIException {
    createOnVm(vm, type, new Properties());
  }

  public void createOnVm(final VM vm, final CacheType type, final Properties props)
      throws RMIException {
    props.setProperty(DistributionConfig.LOCATORS_NAME, this.getLocatorString());
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
    props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
    vm.invoke(new SerializableRunnable("Creating MCache " + type.toString()) {
      private static final long serialVersionUID = 1L;

      @Override
      public void run() {
        cache = type.getCache(props);
        MCacheFactory.getAnyInstance();
      }
    });
  }

  public AsyncInvocation createOnVmAsync(final VM vm, final CacheType type, final Properties props)
      throws RMIException {
    props.setProperty(DistributionConfig.LOCATORS_NAME, this.getLocatorString());
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
    props.setProperty(DistributionConfig.LOG_FILE_NAME, "system_after_restart.log");
    return vm.invokeAsync(new SerializableRunnable("Creating MCache " + type.toString()) {
      private static final long serialVersionUID = 1L;

      @Override
      public void run() {
        cache = type.getCache(props);
        MCacheFactory.getAnyInstance();
      }
    });
  }

  public VM getVm(final int index) {
    return vmList.get(index);
  }

  public List<VM> getServers() {
    return serverList;
  }

  /**
   * The utility method to create regions on server.
   *
   * @param regionNames an array of region names
   * @throws RMIException
   */
  public void createRegionOnServer(final String... regionNames) throws RMIException {
    for (VM serverVm : serverList) {
      serverVm.invoke(new SerializableRunnable() {
        @Override
        public void run() {
          for (final String region : regionNames) {
            try {
              PartitionAttributesFactory paf = new PartitionAttributesFactory();
              paf.setRedundantCopies(2);
              cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
                  .setPartitionAttributes(paf.create()).create(region);
            } catch (RegionExistsException ree) {
              // ignore..
            }
          }
        }
      });
    }
  }

  public void destroyRegionOnServer(final String... regionNames) throws RMIException {
    for (VM serverVm : serverList) {
      serverVm.invoke(new SerializableRunnable() {
        @Override
        public void run() {
          for (final String region : regionNames) {
            Region<Object, Object> r = cache.getRegion(region);
            if (r != null) {
              r.destroyRegion();
            }
          }
        }
      });
    }
  }

  /**
   * The utility method to put some key-value pairs on server.
   *
   * @param regionName the region name where to put the key-value pairs
   * @param key the key to be put in monarch region
   * @param value the value to be put against the specified key
   */
  public void putInRegionOnServer(final String regionName, final Object key, final Object value)
      throws RMIException {
    serverVm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region<Object, Object> r = cache.getRegion(regionName);
        if (r == null) {
          r = cache.createRegionFactory().create(regionName);
        }
        r.put(key, value);
        // r.putAll(map);
      }
    });
  }

  /**
   * Helper method to provide locator port.
   *
   * @return the locator port
   */
  public String getLocatorPort() {
    return DUnitLauncher.getLocatorString().replaceAll("[^0-9]", "");
  }

  /**
   * Helper method to provide locator string (format -- host[port])
   *
   * @return the locator string
   */
  public String getLocatorString() {
    return DUnitLauncher.getLocatorString();
  }



  /**
   * Provide the client-cache..
   *
   * @return the instance of client cache
   */
  public MClientCache getClientCache() {
    return new MClientCacheFactory().addPoolLocator("127.0.0.1", Integer.parseInt(getLocatorPort()))
        .create();
  }
}
