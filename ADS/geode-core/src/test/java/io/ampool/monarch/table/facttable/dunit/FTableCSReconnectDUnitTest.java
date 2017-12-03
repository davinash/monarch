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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Properties;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(FTableTest.class)
public class FTableCSReconnectDUnitTest extends MTableDUnitHelper {

  public FTableCSReconnectDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  private final int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "FTableCSReconnectDUnitTest";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 10;
  private final String COLUMN_NAME_PREFIX = "COLUMN";
  private static MCache cacheBeforeReconnect = null;
  private static List<byte[]> keys;
  private final int TOTAL_SPLITS = 113;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, DUnitLauncher.getLocatorString());
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
        props.setProperty(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME, "false");
        props.setProperty(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "true");
        props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        try {
          cacheBeforeReconnect = MCacheFactory.getAnyInstance();
          cacheBeforeReconnect.close();
        } catch (CacheClosedException cce) {
        }
        cacheBeforeReconnect = MCacheFactory.create(getSystem(props));
        CacheServer s = cacheBeforeReconnect.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        return port;
      }
    });

    // create client cache in a running process.
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    });
    // close the client cache
    closeMClientCache();
    super.tearDown2();
  }

  private void crashMemberToGenerateForcedDisconnectException() {
    System.out.println("Before ForcedDisconnect -> Crash a node in Distributed System");
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        InternalDistributedSystem distributedSystem =
            (InternalDistributedSystem) cacheBeforeReconnect.getDistributedSystem();
        assertNotNull(distributedSystem);
        MembershipManagerHelper.crashDistributedSystem(distributedSystem);
        return null;
      }
    });
  }

  private void getReconnectedCache() {
    System.out.println("After ForcedDisconnect -> Reconnect the MCache");
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        cacheBeforeReconnect = cacheBeforeReconnect.getReconnectedCache();
        return null;
      }
    });
  }

  private void verifyMetaRegion() {
    MTable table = MCacheFactory.getAnyInstance().getMTable(TABLE_NAME);
    assertNotNull(table);

    /* Check if the table exists in Meta Region */
    FTableDescriptor ftd = MCacheFactory.getAnyInstance().getFTableDescriptor(TABLE_NAME);
    assertNotNull(ftd);

    System.out
        .println("NNN .verifyMetaRegion DISK_WRITE_POLICY = " + ftd.getDiskWritePolicy().name());
    System.out.println("NNN .verifyMetaRegion EVICTION_POLICY = " + ftd.getEvictionPolicy().name());
    System.out.println("NNN .verifyMetaRegion Partitioning_Column = "
        + Bytes.toString(ftd.getPartitioningColumn().getByteArray()));
    System.out.println(
        "NNN .verifyMetaRegion PARTITION_RESOLVER = " + ftd.getPartitionResolver().getName());
    System.out.println(
        "NNN .verifyMetaRegion EXPIRATION_ATTR = " + ftd.getExpirationAttributes().getAction() + " "
            + ftd.getExpirationAttributes().getTimeout());
    System.out.println("NNN .verifyMetaRegion DISKSTORE_NAME = " + ftd.getRecoveryDiskStore());
  }

  private void verifyServerDataAfterRecovery() {
    System.out.println("After ForcedDisconnect -> Verify Meta-region (FTableDescriptor) Operation");
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyServerData();
        return null;
      }
    });
  }

  private void verifyServerData() {
    final MCache serverCache = MCacheFactory.getAnyInstance();
    assertNotNull(serverCache);
    FTable mtable = null;
    int retries = 0;
    do {
      mtable = serverCache.getFTable(TABLE_NAME);
      if (retries > 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      retries++;
    } while (mtable == null && retries < 500);
    assertNotNull(mtable);
    final Region<Object, Object> mregion = ((ProxyFTableRegion) mtable).getTableRegion();
    String path = mregion.getFullPath();
    assertTrue(path.contains(TABLE_NAME));

    /* Check if the table exists in Meta Region */
    FTableDescriptor ftd = MCacheFactory.getAnyInstance().getFTableDescriptor(TABLE_NAME);
    assertNotNull(ftd);

    // To verify disk persistence
    // assertEquals(tableDescriptor.isDiskPersistenceEnabled(),
    // mregion.getAttributes().getDataPolicy().withPersistence());
  }

  private void verifyFTableDescriptor(FTableDescriptor ftd) {
    System.out
        .println("NNN .verifyMetaRegion DISK_WRITE_POLICY = " + ftd.getDiskWritePolicy().name());
    System.out.println("NNN .verifyMetaRegion EVICTION_POLICY = " + ftd.getEvictionPolicy().name());
    System.out.println("NNN .verifyMetaRegion Partitioning_Column = "
        + Bytes.toString(ftd.getPartitioningColumn().getByteArray()));
    if (ftd.getPartitionResolver() != null)
      System.out.println(
          "NNN .verifyMetaRegion PARTITION_RESOLVER = " + ftd.getPartitionResolver().getName());

    System.out.println(
        "NNN .verifyMetaRegion EXPIRATION_ATTR = " + ftd.getExpirationAttributes().getAction() + " "
            + ftd.getExpirationAttributes().getTimeout());
    System.out.println("NNN .verifyMetaRegion DISKSTORE_NAME = " + ftd.getRecoveryDiskStore());
    System.out
        .println("MCacheServerReconnectDUnitTest.verifyServerData IS_DISK_PERSISTANCE_ENABLED = "
            + ftd.isDiskPersistenceEnabled());
  }

  private FTable createFTable(final FTableDescriptor tableDescriptor) {
    System.out.println(
        "FTableDescriptorDUnitTest.createFTable :: " + "Creating Ftable:---- " + tableDescriptor);
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

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

  private FTableDescriptor getFTableDescriptor() {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    tableDescriptor.setTotalNumOfSplits(TOTAL_SPLITS);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      if (colmnIndex == 0) {
        tableDescriptor.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    tableDescriptor.setRedundantCopies(2);

    return tableDescriptor;
  }

  private void createFTable() {
    final FTable table = createFTable(getFTableDescriptor());
    assertNotNull(table);
  }

  private void doCreateFTable() {
    System.out.println("Before ForcedDisconnect -> Creating Table and Verify FTableDescriptor.");
    createFTable();
  }

  /**
   * Reconnect Dunit test-case. Verify that when a cache-server reconnects to the DS, it creates the
   * meta-region and data populated in it (FTableDescriptor) is as expected.
   */
  @Test
  public void testFTableDescriptorInReconnectCase() throws InterruptedException {
    // 1. create a table from a client
    doCreateFTable();

    // 2. simulate force-disconnect exception by explicitly crashing member
    crashMemberToGenerateForcedDisconnectException();
    getReconnectedCache();

    /* wait till reconnect happens i.e. 60 seconds */
    Thread.sleep(65_000);

    // verify metaregion and its data at server-side
    verifyServerDataAfterRecovery();

    // verifymeta-region and its data at client side
  }

}
