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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.exceptions.MTableExistsException;
import io.ampool.monarch.table.internal.MTableUtils;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Category(MonarchTest.class)
public class MCacheServerReconnectDUnitTest extends MTableDUnitHelper {

  public MCacheServerReconnectDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  private final int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "MCacheServerReconnectDUnitTest";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 10;
  private final String COLUMN_NAME_PREFIX = "COLUMN";
  private static MCache cacheBeforeReconnect = null;
  private static List<byte[]> keys;

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
        return port;
      }
    });
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
    super.tearDown2();
  }

  private void createTable() {
    MCache serverCache = MCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1).setMaxVersions(1);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    MTable table = null;
    try {
      table = serverCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    } catch (MTableExistsException e) {
      System.out.println("MCacheServerReconnectDUnitTest.createTable :: " + "Got exception....");
      e.printStackTrace();
    }
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  private List<byte[]> doPuts(final String tableName) {
    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);

    List<byte[]> allKeys = new ArrayList<byte[]>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> {
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    return allKeys;
  }

  private void doGets() {
    MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    keys.forEach((K) -> {
      Get get = new Get(K);
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1 /* GEN-1696 */, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (Cell cell : row) {
        Assert.assertNotEquals(NUM_OF_COLUMNS + 1 /* GEN-1696 */, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        // GEN-1696
        if (columnIndex == NUM_OF_COLUMNS) {
          expectedColumnName = Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME);
          exptectedValue = result.getRowId();
        }

        if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out.println("actualColumnName   => " + Arrays.toString(cell.getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) cell.getColumnValue())) {
          System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
          System.out
              .println("actualValue    => " + Arrays.toString((byte[]) cell.getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex++;
      }
    });
  }

  static DistributedSystem savedSystem;

  private void doCreateTableAndDoSomePuts() {
    System.out.println("Before ForcedDisconnect -> Creating Table and Verify Puts and Gets");
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheServerReconnectDUnitTest.savedSystem =
            MCacheFactory.getAnyInstance().getDistributedSystem();
        createTable();
        keys = doPuts(TABLE_NAME);
        doGets();
        return null;
      }
    });
  }

  private void crashMemberToGenerateForcedDisconnectException() {
    forceDisconnect(vm0);
    DistributedMember newdm =
        (DistributedMember) this.vm0.invoke(new SerializableCallable("wait for reconnect(1)") {
          public Object call() {
            final DistributedSystem ds = MCacheServerReconnectDUnitTest.savedSystem;
            MTableReconnectDUnitTest.savedSystem = null;
            Wait.waitForCriterion(new WaitCriterion() {
              public boolean done() {
                return ds.isReconnecting();
              }

              public String description() {
                return "waiting for ds to begin reconnecting";
              }
            }, 30000, 1000, true);
            LogWriterUtils.getLogWriter().info("entering reconnect wait for " + ds);
            LogWriterUtils.getLogWriter().info("ds.isReconnecting() = " + ds.isReconnecting());
            boolean failure = true;
            try {
              ds.waitUntilReconnected(60, TimeUnit.SECONDS);
              MCacheServerReconnectDUnitTest.savedSystem = ds.getReconnectedSystem();
              InternalLocator locator = (InternalLocator) Locator.getLocator();
              assertTrue("Expected system to be restarted", ds.getReconnectedSystem() != null);
              assertTrue("Expected system to be running", ds.getReconnectedSystem().isConnected());
              // assertTrue("Expected there to be a locator", locator != null);
              // assertTrue("Expected locator to be restarted", !locator.isStopped());
              failure = false;
              return ds.getReconnectedSystem().getDistributedMember();
            } catch (InterruptedException e) {
              LogWriterUtils.getLogWriter().warning("interrupted while waiting for reconnect");
              return null;
            } finally {
              if (failure) {
                ds.disconnect();
              }
            }
          }
        });
  }

  public boolean forceDisconnect(VM vm) {
    return (Boolean) vm.invoke(new SerializableCallable("crash distributed system") {
      public Object call() throws Exception {
        IgnoredException.addIgnoredException(
            "org.apache.geode.ForcedDisconnectException||Possible loss of quorum");
        // since the system will disconnect and attempt to reconnect
        // a new system the old reference to DTC.system can cause
        // trouble, so we first null it out.
        nullSystem();
        final DistributedSystem msys = InternalDistributedSystem.getAnyInstance();
        // final Locator oldLocator = Locator.getLocator();
        MembershipManagerHelper.crashDistributedSystem(msys);
        /*
         * if (oldLocator != null) { WaitCriterion wc = new WaitCriterion() { public boolean done()
         * { return msys.isReconnecting(); } public String description() { return
         * "waiting for locator to start reconnecting: " + oldLocator; } };
         * Wait.waitForCriterion(wc, 10000, 50, true); }
         */
        return true;
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

  private void performGetOperationAfterForcedDisconnectAndReconnect() {
    System.out.println("After ForcedDisconnect -> Verify Get Operation");
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doGets();
        return null;
      }
    });
  }

  @Test
  public void testCacheServerReconnect() {
    doCreateTableAndDoSomePuts();
    crashMemberToGenerateForcedDisconnectException();
    getReconnectedCache();
    performGetOperationAfterForcedDisconnectAndReconnect();
  }

  @Test
  public void testCacheServerReconnect2() {
    doCreateTableAndDoSomePuts();
    crashMemberToGenerateForcedDisconnectException();
    getReconnectedCache();
    performGetOperationAfterForcedDisconnectAndReconnect();
  }
}
