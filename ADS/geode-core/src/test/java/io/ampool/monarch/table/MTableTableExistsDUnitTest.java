package io.ampool.monarch.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.MTableUtils;

import org.apache.geode.cache.CacheClosedException;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

@Category(MonarchTest.class)
public class MTableTableExistsDUnitTest extends MTableDUnitHelper {
  public MTableTableExistsDUnitTest() {
    super();
  }

  private List<VM> allServerVms = null;

  private final int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "MTableTableExistsDUnitTest";
  private final String COLUMN_NAME_PREFIX = "COLUMN";

  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    allServerVms = new ArrayList<>(Arrays.asList(this.vm0, this.vm1, this.vm2));
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);

    allServerVms.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    }));

    super.tearDown2();
  }

  private void stopAllCacheServers() {
    allServerVms.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        Iterator iter = MCacheFactory.getAnyInstance().getCacheServers().iterator();
        if (iter.hasNext()) {
          CacheServer server = (CacheServer) iter.next();
          server.stop();
        }
        cache.close();
        return null;
      }
    }));
  }

  private void startAllServerAsync(final String locatorString) {
    AsyncInvocation vm0Task = vm0.invokeAsync(new SerializableCallable() {
      @Override
      public Object call() throws Exception {

        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system_after_restart.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locatorString);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");

        MCache cache = null;
        try {
          cache = MCacheFactory.getAnyInstance();
          cache.close();
        } catch (CacheClosedException cce) {
        }
        cache = MCacheFactory.create(getSystem(props));
        CacheServer s = cache.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        System.out.println("MTableDiskPersistDUnitTest.call.VM0.PORT = " + port);
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        return null;
      }
    });


    AsyncInvocation vm1Task = vm1.invokeAsync(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system_after_restart.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locatorString);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
        MCache cache = null;
        try {
          cache = MCacheFactory.getAnyInstance();
          cache.close();
        } catch (CacheClosedException cce) {
        }
        cache = MCacheFactory.create(getSystem(props));
        CacheServer s = cache.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        System.out.println("MTableDiskPersistDUnitTest.call.VM1.PORT = " + port);
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        return null;
      }
    });

    AsyncInvocation vm2Task = vm2.invokeAsync(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system_after_restart.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locatorString);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
        MCache cache = null;
        try {
          cache = MCacheFactory.getAnyInstance();
          cache.close();
        } catch (CacheClosedException cce) {
        }
        cache = MCacheFactory.create(getSystem(props));
        CacheServer s = cache.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        System.out.println("MTableDiskPersistDUnitTest.call.VM2.PORT = " + port);
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        return null;
      }
    });


    try {
      vm0Task.join();
      vm1Task.join();
      vm2Task.join();

    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void restart() {
    closeMClientCache();
    stopAllCacheServers();

    startAllServerAsync(DUnitLauncher.getLocatorString());
    createClientCache();

  }

  public void createTable(final boolean ordered) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  private void createTableOn(VM vm, final boolean ordered) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(ordered);
        return null;
      }
    });
  }

  private void verifyTablesExistsFrom(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        assertTrue(clientCache.getAdmin().tableExists(TABLE_NAME));
        return null;
      }
    });
  }

  /*
   * TEST 1. Create table from Client 1 2. Check if exists pass 2.
   */
  @Test
  public void testCreateTableAndcheckExists() {
    createTableOn(this.client1, true);
    verifyTablesExistsFrom(this.client1);
    restart();
    verifyTablesExistsFrom(this.client1);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  /*
   * TEST 1. Create table from client1 2. Verifies that table exists API working properly form taht
   * client 3. Try doing table Exists from new client ( which is localhost )
   */
  @Test
  public void testCreateFromOneClientAndExistsFromAnotherClient() {
    createTableOn(this.client1, true);
    verifyTablesExistsFrom(this.client1);

    /* check the value from region */
    this.vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        Region r = cache.getRegion(MTableUtils.AMPL_META_REGION_NAME);
        assertEquals(1, r.size());
        assertNotNull(r);
        assertTrue(r.get(TABLE_NAME) != null);
        return null;
      }
    });

    restart();

    /* client 2 operations */
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    assertTrue(clientCache.getAdmin().tableExists(TABLE_NAME));

  }


  @Test
  public void testSimpleReplicatedPutGetTest() {

    new ArrayList<>(Arrays.asList(vm0, vm1, vm2)).forEach((VM) -> {
      VM.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          MCache cache = MCacheFactory.getAnyInstance();
          Region r =
              cache.createRegionFactory(RegionShortcut.REPLICATE).create("REPLICATED_REGION");
          return null;
        }
      });
    });

    this.client1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCache cache = ClientCacheFactory.getAnyInstance();
        Region r =
            cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("REPLICATED_REGION");
        r.put("KEY", "VALUE");
        return null;
      }
    });

    new ArrayList<>(Arrays.asList(vm0, vm1, vm2)).forEach((VM) -> {
      VM.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          MCache cache = MCacheFactory.getAnyInstance();
          Region r = cache.getRegion("REPLICATED_REGION");
          assertNotNull(r);
          // assertEquals(1, r.size());
          return null;
        }
      });
    });
  }

  @Test
  public void testTableExistsOnPeers() {
    /* Create table on vm0 */
    this.vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        MTable table = cache.getAdmin().createTable("VM_TABLE",
            new MTableDescriptor().addColumn(Bytes.toBytes("COL1")));
        assertNotNull(table);
        return null;
      }
    });
    /* Verify getTable and table Exists and Region entries */
    new ArrayList<>(Arrays.asList(vm1, vm2)).forEach((VM) -> {
      VM.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          MCache cache = MCacheFactory.getAnyInstance();
          assertNotNull(cache);
          Region r = cache.getRegion(MTableUtils.AMPL_META_REGION_NAME);
          assertNotNull(r);
          assertEquals(1, r.size());
          assertNotNull(r.get("VM_TABLE"));

          assertTrue(MCacheFactory.getAnyInstance().getAdmin().tableExists("VM_TABLE"));
          assertNotNull(MCacheFactory.getAnyInstance().getTable("VM_TABLE"));
          return null;
        }
      });
    });
  }
}
