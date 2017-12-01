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

import io.ampool.monarch.AvailablePortHelper;
import io.ampool.monarch.CacheTestCase;
import io.ampool.monarch.Host;
import io.ampool.monarch.RMIException;
import io.ampool.monarch.SerializableRunnable;
import io.ampool.monarch.VM;
import io.ampool.monarch.standalone.DUnitLauncher;
import io.ampool.monarch.functions.MonarchCreateRegionFunction;
import io.ampool.monarch.functions.MonarchGetAllFunction;
import io.ampool.monarch.hive.functions.MonarchGetRegionFunction;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.junit.Before;
import org.junit.Ignore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

//import io.ampool.store.hive.HiveStore;
//import io.ampool.store.hive.HiveStoreConfig;

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
public class MonarchDUnitBase extends CacheTestCase {
  /**
   * the default server host and port, to connect
   **/
  private static final String SERVER_HOST = "localhost";

  /**
   * hiveConfig
   **/
//  public final String HIVETEST_ENV_HIVEHOSTNAME = "HIVETEST_ENV_HIVEHOSTNAME";
//  public final String HIVETEST_ENV_HIVEPORT = "HIVETEST_ENV_HIVEPORT";
//  public final String HIVETEST_ENV_HIVEUSERNAME = "HIVETEST_ENV_HIVEUSERNAME";
//  public final String HIVETEST_ENV_HIVEPASSWD = "HIVETEST_ENV_HIVEPASSWD";

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
            FunctionService.registerFunction(new MonarchCreateRegionFunction());
            FunctionService.registerFunction(new MonarchGetRegionFunction());
            FunctionService.registerFunction(new MonarchGetAllFunction());
          } catch (IOException e) {
            System.out.println("Unable to start Cache Server." + e.getMessage());
            e.printStackTrace();
          }
        }
      } else {
        props.remove(DistributionConfig.LOCATORS_NAME);
        c = (MCache) new MClientCacheFactory(props).setPoolReadTimeout(50000).addPoolLocator(SERVER_HOST, getDUnitLocatorPort()).create();
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

  public MonarchDUnitBase() {
    this("MonarchDUnitBase");
  }
  /**
   * Constructor..
   *
   * @param name name
   */
  public MonarchDUnitBase(String name) {
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
  @Test(enabled = false)
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

  /**
   * Example test to use this class as member in other tests
   *   rather than extending this.
   *
   * @throws Exception
   */
  @Test(enabled = false)
  public void testExample_2() throws Exception {
    /** create and setup **/
    MonarchDUnitBase tcb = new MonarchDUnitBase("my_tt");
    tcb.setUp();

    final String regionName = "r1";
    tcb.createRegionOnServer(regionName);
    tcb.putInRegionOnServer(regionName, "my_key", "my_value");
    tcb.assertOnClient(regionName, "my_key", "my_value");

    /** once done call clean-up **/
    tcb.tearDown2();
  }

//  public void createDBOnHiveServer(HiveStoreConfig hiveStoreConfig) {
//    try {
//      Class.forName(hiveStoreConfig.get("hive.drivername"));
//    } catch (ClassNotFoundException e) {
//      e.printStackTrace();
//    }
//    try {
//      Connection connection = DriverManager.getConnection("jdbc:hive2://" +
//                      hiveStoreConfig.get("hiveserver.host") + ":" +
//                      hiveStoreConfig.getInt("hiveserver.port") + "/",
//              hiveStoreConfig.get("hive.user"),
//              hiveStoreConfig.get("hive.password"));
//      Statement stmt = connection.createStatement();
//
//      stmt.execute("CREATE DATABASE IF NOT EXISTS " + hiveStoreConfig.get("hive.dbname"));
//      stmt.close();
//    } catch (SQLException sqle) {
////      sqle.printStackTrace();
//    }
//  }
//
//  public void dropDBOnHiveServer(HiveStoreConfig hiveStoreConfig) {
//    try {
//      Class.forName(hiveStoreConfig.get("hive.drivername"));
//    } catch (ClassNotFoundException e) {
//      e.printStackTrace();
//    }
//    try {
//      Connection connection = DriverManager.getConnection("jdbc:hive2://" +
//                      hiveStoreConfig.get("hiveserver.host") + ":" +
//                      hiveStoreConfig.getInt("hiveserver.port") + "/",
//              hiveStoreConfig.get("hive.user"),
//              hiveStoreConfig.get("hive.password"));
//      Statement stmt = connection.createStatement();
//
//      stmt.execute("DROP DATABASE IF EXISTS " + hiveStoreConfig.get("hive.dbname"));
//      stmt.close();
//    } catch (SQLException sqle) {
////      sqle.printStackTrace();
//    }
//  }
//
//  public void dropTableOnHiveServer(HiveStoreConfig hiveStoreConfig, String tableName) {
//    try {
//      Class.forName(hiveStoreConfig.get("hive.drivername"));
//    } catch (ClassNotFoundException e) {
//      e.printStackTrace();
//    }
//    try {
//      Connection connection = DriverManager.getConnection("jdbc:hive2://" +
//        hiveStoreConfig.get("hiveserver.host") + ":" +
//        hiveStoreConfig.getInt("hiveserver.port") + "/" + hiveStoreConfig.get("hive.dbname"),
//        hiveStoreConfig.get("hive.user"),
//        hiveStoreConfig.get("hive.password"));
//      Statement stmt = connection.createStatement();
//
//      stmt.execute("DROP TABLE IF EXISTS " + HiveStore.HIVE_STORE_TABLE_PREFIX + tableName);
//      stmt.close();
//    } catch (SQLException sqle) {
////      sqle.printStackTrace();
//    }
//  }
//
//  public void verifyDataInHiveSouthTable(HiveStoreConfig hiveStoreConfig, String tableName, List<String> testData) {
//    try {
//      Class.forName(hiveStoreConfig.get("hive.drivername"));
//    } catch (ClassNotFoundException e) {
//      e.printStackTrace();
//    }
//    try {
//      ResultSet rs = null;
//      Connection connection = DriverManager.getConnection("jdbc:hive2://" +
//        hiveStoreConfig.get("hiveserver.host") + ":" +
//        hiveStoreConfig.getInt("hiveserver.port") + "/" + hiveStoreConfig.get("hive.dbname"),
//        hiveStoreConfig.get("hive.user"),
//        hiveStoreConfig.get("hive.password"));
//      Statement stmt = connection.createStatement();
//
//      rs = stmt.executeQuery("select count(*) from " + HiveStore.HIVE_STORE_TABLE_PREFIX + tableName);
//      assertNotNull(rs.next());
//      int num_rows = rs.getInt(1);
//      assertEquals(1, num_rows);
//      stmt.close();
//
//      stmt = connection.createStatement();
//      rs = stmt.executeQuery("select * from " + HiveStore.HIVE_STORE_TABLE_PREFIX + tableName);
//      ResultSetMetaData rsmd = rs.getMetaData();
//      int columnsNumber = rsmd.getColumnCount();
//      System.out.println("columnsNumber = " + columnsNumber);
//      while (rs.next()) {
//        for (int i = 1; i < columnsNumber; i++) {
//          Object expected = testData.get(i-1);
//          Object actual = rs.getObject(i);
//          System.out.println("Expected = " + expected.toString() + " " + "Actual = " + actual.toString());
//          assertEquals(expected.toString(), actual.toString());
//        }
//      }
//      stmt.close();
//      connection.close();
//
//    } catch (SQLException sqle) {
//      //sqle.printStackTrace();
//    }
//  }
}