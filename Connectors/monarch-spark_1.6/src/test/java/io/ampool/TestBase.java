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
package io.ampool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import io.ampool.monarch.AsyncInvocation;
import io.ampool.monarch.AvailablePortHelper;
import io.ampool.monarch.CacheTestCase;
import io.ampool.monarch.Host;
import io.ampool.monarch.RMIException;
import io.ampool.monarch.SerializableRunnable;
import io.ampool.monarch.VM;
import io.ampool.monarch.standalone.DUnitLauncher;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.junit.Before;

/**
 * Created on: 2016-03-01
 * Since version: 0.3.2.0
 */
public class TestBase extends CacheTestCase {
  /**
   * the default server host and port, to connect
   **/
  private static final String SERVER_HOST = "localhost";

  /**
   * Wrapper enum for server and client to return the required cache.
   */
  public enum CacheType {
    SERVER(true),
    CLIENT(false);
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
            MCacheFactory.getAnyInstance();
          } catch (IOException e) {
            System.out.println("Unable to start Cache Server.");
          }
        }
      } else {
        props.remove(DistributionConfig.LOCATORS_NAME);
        c = (MCache) new MClientCacheFactory(props).addPoolLocator(SERVER_HOST, getDUnitLocatorPort()).create();
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

  public TestBase() {
    this("TestBase");
  }
  /**
   * Constructor..
   *
   * @param name name
   */
  public TestBase(String name) {
    super(name);
    Host host = Host.getHost(0);
    for (int i = 0; i < numberOfVms; i++) {
      vmList.add(host.getVM(i));
    }
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
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
  public void restart() throws RMIException {
    /** stop all servers.. **/
    for (VM vm : serverList) {
      vm.invoke(new SerializableRunnable() {
        @Override
        public void run() {
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
  public AsyncInvocation createOnVmAsync(final VM vm, final CacheType type, final Properties props)
    {
    props.setProperty(DistributionConfig.LOCATORS_NAME, this.getLocatorString());
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
    props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
    return vm.invokeAsync(new SerializableRunnable("Creating Cache " + type.toString()) {
      private static final long serialVersionUID = 1L;

      @Override
      public void run() {
        cache = type.getCache(props);
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
    vm.invoke(new SerializableRunnable("Creating Cache " + type.toString()) {
      private static final long serialVersionUID = 1L;

      @Override
      public void run() {
        cache = type.getCache(props);
      }
    });
  }

  public VM getVm(final int index) {
    return vmList.get(index);
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
              cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT).setPartitionAttributes(paf.create()).create(region);
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
  public void putInRegionOnServer(final String regionName, final Object key, final Object value) throws RMIException {
    serverVm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region<Object, Object> r = cache.getRegion(regionName);
        if (r == null) {
          r = cache.createRegionFactory().create(regionName);
        }
        r.put(key, value);
//        r.putAll(map);
      }
    });
  }

  public void assertOnClient(final String regionName, final String key, final Object value) throws RMIException {
    clientVm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region region = cache.getRegion(regionName);
        if (region == null) {
          region = ((ClientCache) cache)
            .createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create(regionName);
        }
        assertNotNull("Region should be present on client.", region);

        assertEquals("Incorrect value for key: " + key, String.valueOf(value), String.valueOf(region.get(key)));
      }
    });

  }

  public void assertOnServer(final String regionName, boolean notNull) throws RMIException {
    serverVm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region region = cache.getRegion(regionName);
        if (notNull) {
          assertNotNull("Region should be present on server.", region);
        } else {
          assertNull("Region should not be present on server.", region);
        }
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
   * Example test to create separate server and client VMs.
   *   Create and put some data on server and assert that the
   *   same data is available from client VM as well.
   *
   * @throws Exception
   */
//  @Test(enabled = false)
  public void testExample_1() throws Exception {
    /** expected data **/
    final String regionName = "my_region";
    final Map<String, String> map = new HashMap<String, String>(2) {{
      put("key_1", "value_1");
      put("key_2", "value_2");
    }};

    /** create cache on one VM as server and other as client **/
    final VM serverVm = vmList.get(0);
    final VM clientVm = vmList.get(1);

    createOnVm(serverVm, CacheType.SERVER);
    createOnVm(clientVm, CacheType.CLIENT);

    /** create region and add some data on server **/
    serverVm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        cache.createRegionFactory().create(regionName).putAll(map);
      }
    });

    /** assert that the data added on server is available on client as well **/
    clientVm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region region = ((ClientCache) cache)
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .create(regionName);
        assertNotNull("Region should be present on client.", region);

        for (Map.Entry<String, String> entry : map.entrySet()) {
          assertEquals("Incorrect value for key: " + entry.getKey(),
            entry.getValue(), region.get(entry.getKey()));
        }
      }
    });
  }
}
