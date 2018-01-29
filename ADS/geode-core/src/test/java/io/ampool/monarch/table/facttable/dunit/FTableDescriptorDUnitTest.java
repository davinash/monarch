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

package io.ampool.monarch.table.facttable.dunit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MExpirationAction;
import io.ampool.monarch.table.MExpirationAttributes;
import io.ampool.monarch.table.MTableColumnType;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.internal.FTableImpl;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.monarch.table.internal.AbstractTableDescriptor;
import io.ampool.monarch.table.internal.MPartitionResolver;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

@Category(FTableTest.class)
public class FTableDescriptorDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();
  private Host host = Host.getHost(0);
  private VM server1 = host.getVM(0);
  private VM server2 = host.getVM(1);
  private VM server3 = host.getVM(2);
  private VM client1 = host.getVM(3);

  private List<VM> allServerVms = null;

  // Default configuration for FTableDescriptor
  private final static int DEFAULT_REPLICAS = 2;
  private final static MTableType DEFAULT_TYPE = MTableType.UNORDERED;
  private final static int DEFAULT_SPLITS = 113;
  private final static int DEFAULT_TOTAL_COLUMNS = 10;
  private final static int TOTAL_INTERNAL_COLUMNS = 1;
  private final static int TOTAL_ADDITIONAL_COLUMNS = 6;
  private final static boolean DEFAULT_IS_DISK_PERSISTENCE = true;
  private final static int INVALID_REPLICAS = 100;
  private final static int INVALID_SPLITS1 = 1000;
  private final static int INVALID_SPLITS2 = 0;
  private static final String COLUMN_NAME_PREFIX = "COL";
  private static final String TABLE_NAME = "FTableDescriptorDUnitTest";

  private MDiskWritePolicy DEFAULT_DISK_WRITE_POLICY = MDiskWritePolicy.ASYNCHRONOUS;
  private MEvictionPolicy DEFAULT_EVICTION_POLICY = MEvictionPolicy.OVERFLOW_TO_TIER;
  private MExpirationAttributes DEFAULT_EXPIRATION_ATTRIBUTES =
      new MExpirationAttributes(0, MExpirationAction.DESTROY);

  public FTableDescriptorDUnitTest() {}

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    allServerVms = new ArrayList<>(Arrays.asList(this.server1, this.server2, this.server3));
    startServerOn(this.server1, DUnitLauncher.getLocatorString());
    startServerOn(this.server2, DUnitLauncher.getLocatorString());
    startServerOn(this.server3, DUnitLauncher.getLocatorString());

    createClientCache(this.client1);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache(this.client1);
    closeMClientCache();

    allServerVms.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    }));
    super.tearDown2();
  }

  public void doPutFromClient1(VM vm, final int locatorPort) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        FTableDescriptor td = new FTableDescriptor();
        td.setRedundantCopies(DEFAULT_REPLICAS);
        td.setTotalNumOfSplits(DEFAULT_SPLITS);

        { // setting invalid #splits and validating the exception
          Exception expectedException = null;
          try {
            td.setTotalNumOfSplits(INVALID_SPLITS1);
          } catch (IllegalArgumentException iae) {
            expectedException = iae;
          }
          assertTrue(expectedException instanceof IllegalArgumentException);
        }

        { // setting invalid #splits and validating the exception
          Exception expectedException = null;
          try {
            td.setTotalNumOfSplits(INVALID_SPLITS2);
          } catch (IllegalArgumentException iae) {
            expectedException = iae;
          }
          assertTrue(expectedException instanceof IllegalArgumentException);
        }

        {
          // setting invalid eviction-policy and validating the exception
          Exception expectedException = null;
          try {
            td.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_DISK);
          } catch (IllegalArgumentException iae) {
            expectedException = iae;
          }
          assertTrue(expectedException instanceof IllegalArgumentException);
        }

        td.setPartitioningColumn(Bytes.toBytes("column-5"));
        td.setPartitionResolver(new FTableDescriptorDUnitTest.SampleFTablePartitionResolver());
        for (int i = 1; i <= DEFAULT_TOTAL_COLUMNS - TOTAL_ADDITIONAL_COLUMNS; i++) {
          td.addColumn(Bytes.toBytes("column-" + i));
        }

        td.addColumn(Bytes.toBytes("ByteArrayColumn"));
        td.addColumn("StringColumn");
        td.addColumn("BasicObjectColumn1", BasicTypes.INT);
        td.addColumn(Bytes.toBytes("BasicObjectColumn2"), BasicTypes.FLOAT);
        td.addColumn("DOJ-1", new MTableColumnType("DATE"));
        td.addColumn("DOJ-2", new MTableColumnType("DATE"));

        MClientCacheFactory.getAnyInstance().getAdmin().createFTable("key1", td);

        return null;
      }
    });
  }

  public void doGetFromClient2(final int locatorPort) {
    FTableDescriptor ftd = MClientCacheFactory.getAnyInstance().getFTableDescriptor("key1");
    if (ftd != null) {
      // Debug statements
      {
        System.out.println(
            "FTableDescriptorDUnitTest.Client-2.GET  #Copies = " + ftd.getRedundantCopies());
        // System.out.println("FTableDescriptorDUnitTest.Client-2.GET #Table Type = " +
        // ftd.getTableType());
        System.out.println(
            "FTableDescriptorDUnitTest.Client-2.GET  #Splits = " + ftd.getTotalNumOfSplits());
        System.out.println("FTableDescriptorDUnitTest.Client-2.GET  #ColumnDesc_Map.size = "
            + ftd.getColumnDescriptorsMap().size());
      }
      verifyTableDescriptor(ftd);
      System.out.println("FTableDescriptorDUnitTest.doGetFromClient2 Success!");
    } else {
      fail("Error");
    }
  }

  public void verifyTableDescriptor(FTableDescriptor ftd) {
    Assert.assertEquals(DEFAULT_REPLICAS, ftd.getRedundantCopies());
    // Assert.assertEquals(DEFAULT_TYPE, ftd.getTableType());
    Assert.assertEquals(DEFAULT_SPLITS, ftd.getTotalNumOfSplits());
    Assert.assertEquals(DEFAULT_TOTAL_COLUMNS + TOTAL_INTERNAL_COLUMNS,
        ftd.getColumnDescriptorsMap().size());

    // System.out.println("NNN FTableDescriptorDUnitTest.verifyTableDescriptor ==> " +
    // ftd.getRecoveryDiskStore());
    Assert.assertEquals(MTableUtils.DEFAULT_FTABLE_DISK_STORE_NAME, ftd.getRecoveryDiskStore());

    // System.out.println("NNN FTableDescriptorDUnitTest.verifyTableDescriptor ==> " +
    // ftd.getDiskWritePolicy().name());
    Assert.assertEquals(DEFAULT_DISK_WRITE_POLICY.name(), ftd.getDiskWritePolicy().name());

    // System.out.println("NNN FTableDescriptorDUnitTest.verifyTableDescriptor ==> " +
    // ftd.getEvictionPolicy().name());
    Assert.assertEquals(DEFAULT_EVICTION_POLICY.name(), ftd.getEvictionPolicy().name());

    // System.out.println("NNN FTableDescriptorDUnitTest.verifyTableDescriptor ==> " +
    // ftd.getExpirationAttributes().toString());
    Assert.assertEquals(DEFAULT_EXPIRATION_ATTRIBUTES.getTimeout(),
        ftd.getExpirationAttributes().getTimeout());
    Assert.assertEquals(DEFAULT_EXPIRATION_ATTRIBUTES.getAction(),
        ftd.getExpirationAttributes().getAction());

    Assert.assertEquals(DEFAULT_IS_DISK_PERSISTENCE, ftd.isDiskPersistenceEnabled());

  }

  public void VerifyFTDescOnServer() {
    verifyTableDescriptor(MCacheFactory.getAnyInstance().getFTableDescriptor("key1"));
  }

  /**
   * Test: Verify FTableDescriptor serialization/deserialization across client and servers.
   * TestCase: Put a FTableDescriptor from one client and get/verify it from another client
   * <p>
   * Verify FTableDescriptor config using different public APIs
   */
  @Test
  public void testMultipleClientFTDPutGet() {

    doPutFromClient1(client1, getLocatorPort());

    server1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        VerifyFTDescOnServer();
        return null;
      }
    });

    server2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        VerifyFTDescOnServer();
        return null;
      }
    });

    server3.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        VerifyFTDescOnServer();
        return null;
      }
    });

    doGetFromClient2(getLocatorPort());
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
    AsyncInvocation vm0Task = server1.invokeAsync(new SerializableCallable() {
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
        System.out.println("FTableDescriptorDUintTest.call.server1.PORT = " + port);
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        return null;
      }
    });

    AsyncInvocation vm1Task = server2.invokeAsync(new SerializableCallable() {
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
        System.out.println("FTableDescriptorDUintTest.call.server2.PORT = " + port);
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        return null;
      }
    });

    AsyncInvocation vm2Task = server3.invokeAsync(new SerializableCallable() {
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
        System.out.println("FTableDescriptorDUintTest.call.server3.PORT = " + port);
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

  private void verifyMetaRegion(boolean isClient) {

    FTableDescriptor ftd = null;
    if (isClient == true) {
      FTable table = MClientCacheFactory.getAnyInstance().getFTable(TABLE_NAME);
      assertNotNull(table);
      ftd = MClientCacheFactory.getAnyInstance().getFTableDescriptor(TABLE_NAME);
    } else {
      FTable table = MCacheFactory.getAnyInstance().getFTable(TABLE_NAME);
      assertNotNull(table);
      ftd = MCacheFactory.getAnyInstance().getFTableDescriptor(TABLE_NAME);
    }
    assertNotNull(ftd);
    System.out.println("MMM " + ftd.isDiskPersistenceEnabled());
    verifyTableDescriptor(ftd);
  }

  /**
   * Tests meta-region(FTableDescriptor shortned as FTD) and user-table recovery after restart.
   */
  @Test
  public void testFTDRecovery() {
    System.out.println("Creating user-table (Meta-region) from client and verifying it on servers");
    doCreateFTable();

    System.out.println("----------------------- RESTARTING SERVERS ----------------------------");

    // After fix for GEN-2139, adding conditional restart of servers
    FTableDescriptor fTableDescriptor = getFTableDescriptor();
    if (((AbstractTableDescriptor) fTableDescriptor).isDiskPersistenceEnabled()) {

      restart();
      System.out.println("----------------------- RESTARTING SERVERS DONE -----------------------");

      // verify recovery from server
      server1.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          verifyMetaRegion(false);
          return null;
        }
      });

      server2.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          verifyMetaRegion(false);
          return null;
        }
      });

      server3.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          verifyMetaRegion(false);
          return null;
        }
      });

      // Verify recovery from client: meta-region (FTableDescriptor) and user-table after server
      // restart
      FTable table = MClientCacheFactory.getAnyInstance().getFTable(TABLE_NAME);
      assertNotNull(table);

      verifyMetaRegion(true);
    }

  }
  /////////////// Reconnect related code changes

  private static FTable createFTable(final FTableDescriptor tableDescriptor) {
    System.out.println(
        "FTableDescriptorDUnitTest.createFTable :: " + "Creating Ftable:---- " + tableDescriptor);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    // System.out.println("CreateMTableDUnitTest.createFTable :: " + "isCacheClosed: " +
    // clientCache.isClosed());
    // MTableDescriptor tableDescriptor = getFTableDescriptor(splits);

    FTable fTable = null;
    try {
      fTable = clientCache.getAdmin().createFTable(TABLE_NAME, tableDescriptor);
    } catch (Exception e) {
      System.out.println("CreateMTableDUnitTest.createFTable :: " + "Throwing from test");
      throw e;
    }
    System.out.println("CreateMTableDUnitTest.createFTable :: " + "FTable is " + fTable);
    return fTable;
  }

  private static void verifyTableOnServer(final FTableDescriptor tableDescriptor) {
    final MCache serverCache = MCacheFactory.getAnyInstance();
    assertNotNull(serverCache);
    FTable fTable = null;
    int retries = 0;
    do {
      fTable = serverCache.getFTable(TABLE_NAME);
      if (retries > 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      retries++;
      // if (retries > 1) {
      // System.out.println("CreateMTableDUnitTest.verifyTableOnServer :: " + "Attempting to fetch
      // table... Attempt " + retries);
      // }
    } while (fTable == null && retries < 500);
    assertNotNull(fTable);
    final Region<Object, Object> mRegion = ((FTableImpl) fTable).getTableRegion();
    String path = mRegion.getFullPath();
    assertTrue(path.contains(TABLE_NAME));

    // Verify disk persistence
    assertEquals(tableDescriptor.isDiskPersistenceEnabled(),
        mRegion.getAttributes().getDataPolicy().withPersistence());
  }

  private static FTableDescriptor getFTableDescriptor() {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    tableDescriptor.setTotalNumOfSplits(DEFAULT_SPLITS);
    for (int colmnIndex = 0; colmnIndex < DEFAULT_TOTAL_COLUMNS; colmnIndex++) {
      if (colmnIndex == 0) {
        tableDescriptor.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    tableDescriptor.setRedundantCopies(2);

    return tableDescriptor;
  }

  private void doCreateFTable() {
    final FTable table = FTableDescriptorDUnitTest.createFTable(getFTableDescriptor());
    assertNotNull(table);
    server1.invoke(() -> {
      System.out.println(
          "CreateMTableDUnitTest.testMTableCreationNormal :: " + "before Verifying on server1");
      FTableDescriptorDUnitTest.verifyTableOnServer(getFTableDescriptor());
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on server1");
    });
    server2.invoke(() -> {
      System.out.println(
          "CreateMTableDUnitTest.testMTableCreationNormal :: " + "before Verifying on server2");
      FTableDescriptorDUnitTest.verifyTableOnServer(getFTableDescriptor());
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on server2");
    });
    server3.invoke(() -> {
      System.out.println(
          "CreateMTableDUnitTest.testMTableCreationNormal :: " + "before Verifying on server3");
      FTableDescriptorDUnitTest.verifyTableOnServer(getFTableDescriptor());
      System.out
          .println("CreateMTableDUnitTest.testMTableCreationNormal :: " + "Verified on server3");
    });
  }

  private void crashMemberToGenerateForcedDisconnectException() {
    System.out.println("Before ForcedDisconnect -> Crash a node in Distributed System");
    server1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        InternalDistributedSystem distributedSystem =
            (InternalDistributedSystem) MCacheFactory.getAnyInstance().getDistributedSystem();
        assertNotNull(distributedSystem);
        MembershipManagerHelper.crashDistributedSystem(distributedSystem);
        return null;
      }
    });
  }

  /*
   * private void getReconnectedCache() {
   * System.out.println("After ForcedDisconnect -> Reconnect the MCache"); vm0.invoke(new
   * SerializableCallable() {
   *
   * @Override public Object call() throws Exception { cacheBeforeReconnect =
   * cacheBeforeReconnect.getReconnectedCache(); return null; } }); }
   */

  public static class SampleFTablePartitionResolver extends MPartitionResolver {

    @Override
    public Object getDistributionObject(EntryOperation keyColumn) {
      System.out.println(
          "SampleFTablePartitionResolver.getDistributionObject PARTITION_RESOLVER CALLED!!!");
      FTableDescriptor ftd = MCacheFactory.getAnyInstance().getFTableDescriptor("key-1");
      return PartitionedRegionHelper.getHashKey(keyColumn, ftd.getTotalNumOfSplits());
    }

    @Override
    public String getName() {
      return this.getClass().getName();
    }
  }

}
