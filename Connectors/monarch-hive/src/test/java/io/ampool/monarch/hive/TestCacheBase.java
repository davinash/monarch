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

package io.ampool.monarch.hive;

import io.ampool.monarch.CacheTestCase;
import io.ampool.monarch.Host;
import io.ampool.monarch.RMIException;
import io.ampool.monarch.SerializableRunnable;
import io.ampool.monarch.VM;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Example test to create separate VMs (server and client) and
 *   insert some data in via server and verify the same from client.
 * <p>
 * To use copy this test to the correct location with proper
 *   dependencies and execute `testExample_1'.
 * <p>
 * Created on: 2015-11-04
 * Since version: 0.2.0.0
 */
@Ignore
public class TestCacheBase extends CacheTestCase {
  /**
   * the default server host and port, to connect
   **/
  private static final String SERVER_HOST = "localhost";
  private static final int SERVER_PORT = 10240;

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
    Cache getCache(final Properties props) {
      Cache c;
      if (isServer) {
        c = new CacheFactory(props).set("mcast-port", "0").create();
        CacheServer cs = c.addCacheServer();
        cs.setPort(SERVER_PORT);
        cs.setBindAddress(SERVER_HOST);
        try {
          cs.start();
        } catch (IOException e) {
//          System.out.println("Unable to start Cache Server.");
        }
      } else {
        c = (Cache) new ClientCacheFactory(props)
          .addPoolServer(SERVER_HOST, SERVER_PORT).create();
      }
      return c;
    }
  }

  /**
   * the geode cache
   **/
  private static Cache cache;

  /**
   * Constructor..
   *
   * @param name name
   */
  public TestCacheBase(String name) throws RMIException {
    this(name, 1, 1);
  }

  private int numberOfServers = 1;
  private int numberOfClients = 1;
  private List<VM> clientList;
  private List<VM> serverList;
  List<VM> vmList = new ArrayList<>(5);

  public TestCacheBase(String name, final int numberOfServers, final int numberOfClients) throws RMIException {
    super(name);
    this.numberOfServers = numberOfServers;
    this.numberOfClients = numberOfClients;
    clientList = new ArrayList<>(numberOfClients);
    serverList = new ArrayList<>(numberOfServers);
    final Host host = Host.getHost(0);
    VM vm;
    for (int i = 0; i < numberOfServers; i++) {
      vm = createOnVm(host.getVM(i), CacheType.SERVER);
      serverList.add(vm);
      vmList.add(vm);
    }
    for (int i = 0, j = numberOfServers; i < numberOfClients; i++, j++) {
      vm = createOnVm(host.getVM(j), CacheType.CLIENT);
      clientList.add(vm);;
      vmList.add(vm);
    }

  }


  protected int getNumberOfClients() {
    return numberOfClients;
  }
  protected int getNumberOfServers() {
    return numberOfServers;
  }

  public VM createOnVm(final VM vm, final CacheType type) throws RMIException {
    return createOnVm(vm, type, null);
  }

  public VM createOnVm(final VM vm, final CacheType type, final Properties props)
    throws RMIException {
    vm.invoke(new SerializableRunnable("Creating Cache " + type.toString()) {
      private static final long serialVersionUID = 1L;

      @Override
      public void run() {
        cache = type.getCache(props);
      }
    });
    return vm;
  }

  public VM getServer() {
    return getServer(0);
  }

  public VM getServer(final int index) {
    return serverList.get(index);
  }
  public VM getClient() {
    return getClient(0);
  }

  public VM getClient(final int index) {
    return clientList.get(index);
  }

  public VM getVm(final CacheType cacheType) {
    return cacheType == CacheType.CLIENT ? clientList.get(0) : serverList.get(0);
  }
  public void executeOn(final CacheType cacheType, final SerializableRunnable code) throws RMIException {
    VM vm = getVm(cacheType);
    vm.invoke(code);
  }

  public Object executeOn(final CacheType cacheType, final String objectType, final String methodName, final Object...args) throws RMIException {
    final VM vm = getVm(cacheType);
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Object o = cache.createRegionFactory();
//        System.out.println("From invoke.. o = " + o);

        final Object obj;
        if ("cache".equals(objectType)) {
          obj = cache;
        } else {
          obj = cache.getRegion((String) args[0]);
        }
        final Object x = null;
        try {
//          x = vm.invoke(obj, methodName, args);
//          System.out.println("x = " + x);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    Object o = vm.invoke(cache, "createRegionFactory");
//    System.out.println("in executeOn:: o = " + o);
    return null;
  }

  public abstract class MyCode extends SerializableRunnable {
    public Cache myCache;
    public Region<Object, Object> region;
    public String regionName;

    public MyCode(final String regionName) {
      this.regionName = regionName;
    }
    @Override
    public void run() {
//      System.out.println("MyCode::SerializableRunnable = " + cache);
      this.myCache = cache;
    }
    public Cache getCache() {
      return myCache;
    }
    abstract public void myCode();
  }
  @Test
  public void testExample_2() throws Exception {
    /** expected data **/
    final String regionName = "my_region";
    final Map<String, String> map = new HashMap<String, String>(2) {{
      put("key_1", "value_1");
      put("key_2", "value_2");
    }};

    getServer().invoke(new SerializableRunnable("my_region") {
      @Override
      public void run() {
        Object o;
        o = cache.createRegionFactory();
//        Object o = this.myCache.createRegionFactory().create(regionName);
//        System.out.println("rxxx o = " + o);
//        Region region = cache.getRegion(regionName);
//        System.out.println("region o = " + region);
//        region.putAll(map);
      }
    });

//    getClient().invoke(new MyCode("my_region") {
//      @Override
//      public void myCode() {
//        Region region = ((ClientCache) cache)
//          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
//          .create(regionName);
//        assertNotNull("Region should be present on client.", region);
//
//        for (Map.Entry<String, String> entry : map.entrySet()) {
//          System.out.println("regionEntry = " + region.get(entry.getKey()));
//          assertEquals("Incorrect value for key: " + entry.getKey(),
//            entry.getValue(), region.get(entry.getKey()));
//        }
//
//      }
//    });

    /** assert that the data added on server is available on client as well **/
//    clientVm.invoke(new SerializableRunnable() {
//      @Override
//      public void run() {
//        Region region = ((ClientCache) cache)
//          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
//          .create(regionName);
//        assertNotNull("Region should be present on client.", region);
//
//        for (Map.Entry<String, String> entry : map.entrySet()) {
//          assertEquals("Incorrect value for key: " + entry.getKey(),
//            entry.getValue(), region.get(entry.getKey()));
//        }
//      }
//    });
  }

  /**
   * Example test to create separate server and client VMs.
   *   Create and put some data on server and assert that the
   *   same data is available from client VM as well.
   *
   * @throws Exception
   */
//  @Test
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

//    createOnVm(serverVm, CacheType.SERVER);
//    createOnVm(clientVm, CacheType.CLIENT);

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