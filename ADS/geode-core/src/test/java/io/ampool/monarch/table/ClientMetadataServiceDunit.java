package io.ampool.monarch.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * Test retrieving ClientMetaDataService in Geode & RowTuple Validate changes made to
 * ClientMetaDataService for MTable
 *
 */
public class ClientMetadataServiceDunit extends MTableDUnitHelper {
  public ClientMetadataServiceDunit() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();
  Host host = Host.getHost(0);
  private VM vm0 = host.getVM(0);
  private VM vm1 = host.getVM(1);
  private VM vm2 = host.getVM(2);
  private VM client = host.getVM(3);

  protected final int NUM_OF_COLUMNS = 10;
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 10;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected final int LATEST_TIMESTAMP = 300;
  protected final int MAX_VERSIONS = 5;
  protected final int TABLE_MAX_VERSIONS = 7;

  protected final int MTABLE_REGION_DEFAULT_PARTITION_REDUDANT_COPIES = 1;
  final int numOfEntries = 10;

  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }

  protected void createTable(String tableName, boolean persistentEnabled) {

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
    tableDescriptor.setRedundantCopies(1);
    if (persistentEnabled) {
      tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    }

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createTable(tableName, tableDescriptor);
    assertEquals(mtable.getName(), tableName);
  }

  protected void createTableOn(VM vm, String tableName, boolean persistentEnabled) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(tableName, persistentEnabled);
        return null;
      }
    });
  }

  private void createRegionOnServer(VM vm, String regionName) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        try {
          MCache c = MCacheFactory.getAnyInstance();
          RegionFactory rf = c.createRegionFactory(RegionShortcut.PARTITION);
          Region r = rf.create(regionName);
          assertNotNull(r);
        } catch (Exception cce) {
          throw cce;
        }
        return null;
      }
    });
  }

  public void startClientOn(VM vm, String regionName) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        startClientOn(regionName);
        return null;
      }
    });
  }

  public void startClientOn(String regionName) {
    MClientCache cache =
        new MClientCacheFactory().addPoolLocator("127.0.0.1", getLocatorPort()).create();
    Region region = ClientCacheFactory.getAnyInstance()
        .createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
  }

  private void createTableFromClient(VM vm, String tableName, boolean persistenTable) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          createTable(tableName, persistenTable);
        } catch (CacheClosedException cce) {
        }

        return null;
      }
    });

  }

  private void doPutGetOperationFromClient(VM vm, String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {

          MTable mtable = MClientCacheFactory.getAnyInstance().getTable(tableName);
          Random randomGenerator = new Random();
          for (int i = 0; i < numOfEntries; i++) {
            String key1 = randomGenerator.nextInt(Integer.MAX_VALUE) + "RowKey";
            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("User" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }

        } catch (CacheClosedException cce) {
        }

        return null;
      }
    });
  }

  private void doGetFromClient(String tableName, final boolean order) {
    try {

      MTable mtable = MClientCacheFactory.getAnyInstance().getTable(tableName);
      // gets
      for (int i = 0; i < numOfEntries; i++) {

        String key1 = "RowKey" + i;
        Put myput = new Put(Bytes.toBytes(key1));
        myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("User" + i));
        myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
        myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
        myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
      }

    } catch (CacheClosedException cce) {
    }
  }

  private void doGetFromClient(VM vm, String tableName, final boolean order) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doGetFromClient(tableName, order);
        return null;
      }
    });
  }

  public void doPutsFromClient(VM vm, String regionName) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        doPuts(regionName);
        return null;
      }
    });

  }

  public void doPuts(String regionName) {
    Region region = ClientCacheFactory.getAnyInstance().getRegion(regionName);
    String rowKey = "key";
    String value = "value";
    for (int i = 0; i < numOfEntries; i++) {
      region.put(rowKey + i, value + i);
    }
  }

  public void getClientMetadataService(VM vm, String regionName) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        getClientMetadataService(regionName);
        return null;
      }
    });

  }

  public Map<ServerLocation, HashSet<Integer>> getClientMetadataService(String regionName) {
    MonarchCacheImpl client = ((MonarchCacheImpl) MClientCacheFactory.getAnyInstance());
    ClientMetadataService cms = client.getClientMetadataService();
    Region region = client.getRegion(regionName);

    int serverBucketMapsCount = 0;
    HashMap<ServerLocation, HashSet<Integer>> serverBucketMap =
        cms.groupByServerToAllBuckets(region, true);
    for (Map.Entry<ServerLocation, HashSet<Integer>> map : serverBucketMap.entrySet()) {
      System.out.print(" Server - " + map.getKey().getHostName());
      Iterator it = map.getValue().iterator();
      while (it.hasNext()) {
        System.out.print(" | bucketId : " + it.next());
        System.out.println();
        serverBucketMapsCount++;
      }
    }
    assertTrue(serverBucketMapsCount > 0);
    return serverBucketMap;
  }

  private void closeClientCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        closeClientCache();
        return null;
      }
    });

  }

  private void closeClientCache() {
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    if (cache != null) {
      cache.close();
    }
  }

  public void createClientCache() {
    String testName = getTestMethodName();
    String logFileName = testName + "-client.log";

    MClientCache clientCache = new MClientCacheFactory().set("log-file", logFileName)
        .addPoolLocator("127.0.0.1", getLocatorPort()).create();
  }

  public void createClientCache(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        createClientCache();
        return null;
      }
    });
  }

  /**
   * TEST Description - Get ClientMetaService for default geode region return unless all buckets are
   * utilized Create region Add data to region Make call to get ClientMetaService object Failure at
   * first attempt is expected as this object gets created lazily. It will continue to fail as all
   * buckets are not created
   */
  @Test
  public void testGetCMDServiceInGeode() {
    String REGION_NAME = "geodeRegionName";
    createRegionOnServer(this.vm0, REGION_NAME);
    createRegionOnServer(this.vm1, REGION_NAME);
    createRegionOnServer(this.vm2, REGION_NAME);
    startClientOn(this.client, REGION_NAME);
    startClientOn(REGION_NAME);
    doPutsFromClient(this.client, REGION_NAME);
    try {
      getClientMetadataService(REGION_NAME);
      fail("NPE expected");
    } catch (Exception e) {
      assertTrue(e instanceof NullPointerException);
    }

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }

    try {
      getClientMetadataService(REGION_NAME);
      fail("NPE expected");
    } catch (Exception e) {
      assertTrue(e instanceof NullPointerException);
    }

    try {
      getClientMetadataService(REGION_NAME);
      fail("NPE expected");
    } catch (Exception e) {
      assertTrue(e instanceof NullPointerException);
    }

    closeClientCache();
    closeClientCache(client);
  }


  /**
   * TEST Description - Get ClientMetaService for MTable region Create region Add data to region
   * Make call to get ClientMetaService object Failure at first attempt is expected as this object
   * gets created lazily. Check bucket distribution
   */
  @Test
  public void testGetCMDServiceInMtable() {
    String MTABLE_NAME = "mtable";
    createClientCache(this.client);
    createClientCache();

    createTableFromClient(this.client, MTABLE_NAME, true);
    doPutGetOperationFromClient(this.client, MTABLE_NAME);
    doGetFromClient(MTABLE_NAME, true);

    try {
      getClientMetadataService(MTABLE_NAME);
    } catch (Exception e) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
    }
    Map<ServerLocation, HashSet<Integer>> serverTiBucketMap = getClientMetadataService(MTABLE_NAME);
    assertNotNull(serverTiBucketMap);
    assertTrue(serverTiBucketMap.keySet().size() > 0);

    closeMClientCache();
    closeMClientCache(this.client);

  }
}
