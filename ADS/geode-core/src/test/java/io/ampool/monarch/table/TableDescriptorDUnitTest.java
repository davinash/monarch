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

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.AbstractTableDescriptor;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.Table;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;

import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@Category(MonarchTest.class)
public class TableDescriptorDUnitTest extends MTableDUnitHelper {
  private static final int NUM_COLS = 5;
  private static final String TABLE_NAME = "testTable";
  List<VM> allServers = new ArrayList(3);


  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    allServers.add(vm0);
    allServers.add(vm1);
    allServers.add(vm2);

    createClientCache(this.client1);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);

    allServers.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    }));

    super.tearDown2();
  }

  private TableDescriptor getMTableDescriptor() {
    TableDescriptor td = new MTableDescriptor();
    for (int i = 0; i < NUM_COLS; i++) {
      td.addColumn("COL_" + i);
    }
    return td;
  }

  private Table createMTable(String name, TableDescriptor td) {
    return MClientCacheFactory.getAnyInstance().getAdmin().createMTable(name,
        (MTableDescriptor) td);
  }

  private TableDescriptor getFTableDescriptor() {
    TableDescriptor td = new FTableDescriptor();
    for (int i = 0; i < NUM_COLS; i++) {
      td.addColumn("COL_" + i);
    }
    return td;
  }

  private Table createFTable(String name, TableDescriptor td) {
    return MClientCacheFactory.getAnyInstance().getAdmin().createFTable(name,
        (FTableDescriptor) td);
  }

  private void deleteTable(String name, TableDescriptor td) {
    if (td instanceof FTableDescriptor) {
      MClientCacheFactory.getAnyInstance().getAdmin().deleteFTable(name);
    } else {
      MClientCacheFactory.getAnyInstance().getAdmin().deleteMTable(name);
    }
  }

  private void checkPRAttributesOnServers(String name, Integer localMaxMemory,
      Integer localMacMemoryPct, Long totalMaxMemory, boolean isMtable) {
    allServers.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        TableDescriptor td = null;
        Region r = MCacheFactory.getAnyInstance().getRegion(name);
        if (isMtable) {
          td = MCacheFactory.getAnyInstance().getMTable(name).getTableDescriptor();
        } else {
          td = MCacheFactory.getAnyInstance().getFTable(name).getTableDescriptor();
        }

        if (localMaxMemory != null) {
          assertEquals(localMaxMemory.intValue(), td.getLocalMaxMemory());
          assertEquals(localMaxMemory.intValue(),
              r.getAttributes().getPartitionAttributes().getLocalMaxMemory());
        }

        if (localMacMemoryPct != null) {
          assertEquals(localMacMemoryPct.intValue(), td.getLocalMaxMemoryPct());
          assertNotEquals(0, r.getAttributes().getPartitionAttributes().getLocalMaxMemory());
          assertEquals(
              (int) (((float) localMacMemoryPct.intValue() / 100)
                  * (Runtime.getRuntime().maxMemory() / (1024 * 1024))),
              r.getAttributes().getPartitionAttributes().getLocalMaxMemory());
        }
        return null;
      }
    }));
  }

  private void checkPRAttributesOnClient(String tableName, VM vm, Integer localMaxMemory,
      Integer locaMaxMemoryPct, Long totalMaxMemory, boolean isMtable) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        TableDescriptor td = null;
        if (isMtable) {
          td = MCacheFactory.getAnyInstance().getMTable(tableName).getTableDescriptor();
        } else {
          td = MCacheFactory.getAnyInstance().getFTable(tableName).getTableDescriptor();
        }

        if (localMaxMemory != null) {
          assertEquals(localMaxMemory.intValue(), td.getLocalMaxMemory());
        }
        if (locaMaxMemoryPct != null) {
          assertEquals(locaMaxMemoryPct.intValue(), td.getLocalMaxMemoryPct());
        }
        return null;
      }
    });
  }

  private void checkSizeOnAllServers(int sizeLimit, String name) {
    for (VM vm : allServers) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          int size = 0;
          TableDescriptor td = MCacheFactory.getAnyInstance().getTableDescriptor(name);
          int totalBuckets = td.getTotalNumOfSplits();

          Region region = MCacheFactory.getAnyInstance().getRegion(name);
          for (int i = 0; i < totalBuckets; i++) {
            BucketRegion br = ((PartitionedRegion) region).getDataStore().getLocalBucketById(i);
            if (br == null)
              continue;
            RowTupleConcurrentSkipListMap internalMap =
                (RowTupleConcurrentSkipListMap) br.getRegionMap().getInternalMap();
            Map realMap = internalMap.getInternalMap();
            Iterator<Map.Entry<IMKey, RegionEntry>> itr = realMap.entrySet().iterator();
            while (itr.hasNext()) {
              Map.Entry<IMKey, RegionEntry> entry = itr.next();
              Object val = entry.getValue()._getValue();
              if (val instanceof byte[]) {
                size += ((byte[]) val).length;
              }
            }
          }
          assertTrue(sizeLimit - size >= 0);
          return null;
        }
      });
    }
  }


  private void checkRecoveryDelayOnAllServers(long recoveryDelay, long startUpRecoveryDelay,
      String tableName) {
    for (VM vm : allServers) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          TableDescriptor td = MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
          Region region = MCacheFactory.getAnyInstance().getRegion(tableName);

          assertEquals(recoveryDelay, td.getRecoveryDelay());
          assertEquals(recoveryDelay,
              region.getAttributes().getPartitionAttributes().getRecoveryDelay());

          assertEquals(startUpRecoveryDelay, td.getStartupRecoveryDelay());
          assertEquals(startUpRecoveryDelay,
              region.getAttributes().getPartitionAttributes().getStartupRecoveryDelay());

          return null;
        }
      });
    }
  }

  private void restartServers() {
    for (VM vm : allServers) {
      stopServerOn(vm);
    }
    for (VM vm : allServers) {
      startServerOn(vm, DUnitLauncher.getLocatorString());
    }
  }

  public Object stopServerOn(VM vm) {
    System.out.println("Stopping server on VM: " + vm.toString());
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          Iterator iter = MCacheFactory.getAnyInstance().getCacheServers().iterator();
          if (iter.hasNext()) {
            CacheServer server = (CacheServer) iter.next();
            server.stop();
          }
        } catch (CacheClosedException e) {
          System.out.println("MCache is closed.");
        } catch (Exception e) {
          fail("failed while stopServer()" + e);
        }
        return null;
      }
    });
  }

  public Object startServerOn(VM vm, final String locators) {
    System.out.println("Starting server on VM: " + vm.toString());
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
        MCache c = null;
        try {
          c = MCacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        c = MCacheFactory.create(getSystem(props));
        CacheServer s = c.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        // registerFunction();
        return port;
      }
    });
  }

  @Test
  public void testLocalMaxMemoryMTable() {
    TableDescriptor td = getMTableDescriptor();
    td.setLocalMaxMemory(53);
    assertEquals(53, td.getLocalMaxMemory());

    Table table = createMTable(TABLE_NAME, td);
    checkPRAttributesOnServers(TABLE_NAME, 53, null, null, true);
    checkPRAttributesOnClient(TABLE_NAME, vm3, 53, null, null, true);
  }

  @Test
  public void testLocalMaxMemoryPctMTable() {
    TableDescriptor td = getMTableDescriptor();
    td.setLocalMaxMemoryPct(53);
    assertEquals(53, td.getLocalMaxMemoryPct());

    Table table = createMTable(TABLE_NAME, td);
    checkPRAttributesOnServers(TABLE_NAME, null, 53, null, true);
    checkPRAttributesOnClient(TABLE_NAME, vm3, null, 53, null, true);
  }


  @Test
  public void testLocalMaxMemoryFTable() {
    TableDescriptor td = getFTableDescriptor();
    td.setLocalMaxMemory(54);
    assertEquals(54, td.getLocalMaxMemory());

    Table table = createFTable(TABLE_NAME, td);
    checkPRAttributesOnServers(TABLE_NAME, 54, null, null, false);
    checkPRAttributesOnClient(TABLE_NAME, vm3, 54, null, null, false);
  }

  @Test
  public void testLocalMaxMemoryPctFTable() {
    TableDescriptor td = getFTableDescriptor();
    td.setLocalMaxMemoryPct(54);
    assertEquals(54, td.getLocalMaxMemoryPct());

    Table table = createFTable(TABLE_NAME, td);
    checkPRAttributesOnServers(TABLE_NAME, null, 54, null, false);
    checkPRAttributesOnClient(TABLE_NAME, vm3, null, 54, null, false);
  }

  @Test
  public void testAttributeResetMTable() {
    // TODO: this can be a Junit
    TableDescriptor td = getMTableDescriptor();
    td.setLocalMaxMemory(53);
    assertEquals(53, td.getLocalMaxMemory());
    assertEquals(AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY_PCT, td.getLocalMaxMemoryPct());

    td.setLocalMaxMemoryPct(54);
    assertEquals(54, td.getLocalMaxMemoryPct());
    assertEquals(AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY, td.getLocalMaxMemory());

    td.setLocalMaxMemory(55);
    assertEquals(55, td.getLocalMaxMemory());
    assertEquals(AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY_PCT, td.getLocalMaxMemoryPct());
  }

  @Test
  public void testAttributeResetFTable() {
    // TODO: this can be a Junit
    TableDescriptor td = getFTableDescriptor();
    td.setLocalMaxMemory(53);
    assertEquals(53, td.getLocalMaxMemory());
    assertEquals(AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY_PCT, td.getLocalMaxMemoryPct());

    td.setLocalMaxMemoryPct(54);
    assertEquals(54, td.getLocalMaxMemoryPct());
    assertEquals(AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY, td.getLocalMaxMemory());

    td.setLocalMaxMemory(55);
    assertEquals(55, td.getLocalMaxMemory());
    assertEquals(AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY_PCT, td.getLocalMaxMemoryPct());
  }

  @Test
  public void testInvalidValuesLocalMaxMemory() {
    Exception e = null;
    TableDescriptor mtd = getMTableDescriptor();
    try {
      mtd.setLocalMaxMemory(-2);
    } catch (Exception e1) {
      e = e1;
    }
    assertNotNull(e);
    assertTrue(e instanceof IllegalArgumentException);
    e = null;

    TableDescriptor ftd = getFTableDescriptor();
    try {
      ftd.setLocalMaxMemory(-2);
    } catch (Exception e1) {
      e = e1;
    }
    assertNotNull(e);
    assertTrue(e instanceof IllegalArgumentException);
  }

  @Test
  public void testInvalidValuesLocalMaxMemoryPct() {
    Exception e = null;
    TableDescriptor mtd = getMTableDescriptor();
    try {
      mtd.setLocalMaxMemoryPct(-2);
    } catch (Exception e1) {
      e = e1;
    }
    assertNotNull(e);
    assertTrue(e instanceof IllegalArgumentException);
    e = null;

    try {
      mtd.setLocalMaxMemoryPct(91);
    } catch (Exception e1) {
      e = e1;
    }
    assertNotNull(e);
    assertTrue(e instanceof IllegalArgumentException);
    e = null;

    TableDescriptor ftd = getFTableDescriptor();
    try {
      ftd.setLocalMaxMemoryPct(-2);
    } catch (Exception e1) {
      e = e1;
    }
    assertNotNull(e);
    assertTrue(e instanceof IllegalArgumentException);
    e = null;

    try {
      ftd.setLocalMaxMemoryPct(91);
    } catch (Exception e1) {
      e = e1;
    }
    assertNotNull(e);
    assertTrue(e instanceof IllegalArgumentException);

  }

  @Test
  public void testLocalMemoryLimitMTableOverflow() {
    TableDescriptor mtd = getMTableDescriptor();
    mtd.setLocalMaxMemory(1);
    mtd.setRedundantCopies(0);
    mtd.setTotalNumOfSplits(10);
    mtd.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_DISK);
    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    int size = 0;
    int cnt = 0;
    while (size < (10 * mtd.getLocalMaxMemory() * 1024 * 1024)) {
      Put put = new Put(Bytes.toBytes(cnt++));
      for (int j = 0; j < NUM_COLS; j++) {
        put.addColumn("COL_" + j, new byte[100]);
      }
      size += (100 * NUM_COLS);
      table.put(put);
    }

    checkSizeOnAllServers(mtd.getLocalMaxMemory() * 1024 * 1024, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testLocalMemoryLimitMTableLocalDestroy() {
    TableDescriptor mtd = getMTableDescriptor();
    mtd.setLocalMaxMemory(1);
    mtd.setRedundantCopies(1);
    mtd.setTotalNumOfSplits(113);
    mtd.setEvictionPolicy(MEvictionPolicy.LOCAL_DESTROY);
    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    int size = 0;
    int cnt = 0;
    while (size < (10 * mtd.getLocalMaxMemory() * 1024 * 1024)) {
      Put put = new Put(Bytes.toBytes(cnt++));
      for (int j = 0; j < NUM_COLS; j++) {
        put.addColumn("COL_" + j, new byte[100]);
      }
      size += (100 * NUM_COLS);
      table.put(put);
    }

    checkSizeOnAllServers(mtd.getLocalMaxMemory() * 1024 * 1024, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testLocalMemoryLimitFTableOverflow() {
    TableDescriptor mtd = getFTableDescriptor();
    mtd.setLocalMaxMemory(1);
    mtd.setRedundantCopies(0);
    mtd.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    FTable table = (FTable) createFTable(TABLE_NAME, mtd);
    int size = 0;
    int cnt = 0;
    while (size < (10 * mtd.getLocalMaxMemory() * 1024 * 1024)) {
      Record record = new Record();
      for (int j = 0; j < NUM_COLS; j++) {
        record.add("COL_" + j, new byte[100]);
      }
      size += (100 * NUM_COLS);
      table.append(record);
    }

    checkSizeOnAllServers(mtd.getLocalMaxMemory() * 1024 * 1024, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testLocalMemoryLimitFableLocalDestroy() {
    TableDescriptor mtd = getFTableDescriptor();
    mtd.setLocalMaxMemory(1);
    mtd.setRedundantCopies(0);
    mtd.setEvictionPolicy(MEvictionPolicy.LOCAL_DESTROY);
    FTable table = (FTable) createFTable(TABLE_NAME, mtd);
    int size = 0;
    int cnt = 0;
    while (size < (10 * mtd.getLocalMaxMemory() * 1024 * 1024)) {
      Record record = new Record();
      for (int j = 0; j < NUM_COLS; j++) {
        record.add("COL_" + j, new byte[100]);
      }
      size += (100 * NUM_COLS);
      table.append(record);
    }

    checkSizeOnAllServers(mtd.getLocalMaxMemory() * 1024 * 1024, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testRecoveryDelayMTableDefault() {
    TableDescriptor mtd = getMTableDescriptor();
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, mtd.getRecoveryDelay());

    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        table.getTableDescriptor().getRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testRecoveryDelayFTableDefault() {
    TableDescriptor ftd = getFTableDescriptor();
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, ftd.getRecoveryDelay());

    FTable table = (FTable) createFTable(TABLE_NAME, ftd);
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        table.getTableDescriptor().getRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    deleteTable(TABLE_NAME, ftd);
  }

  @Test
  public void testRecoveryDelayMTableNonDefault() {
    TableDescriptor mtd = getMTableDescriptor();
    mtd.setRecoveryDelay(2000);
    assertEquals(2000, mtd.getRecoveryDelay());

    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    assertEquals(2000, table.getTableDescriptor().getRecoveryDelay());

    checkRecoveryDelayOnAllServers(2000, PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testRecoveryDelayFTableNonDefault() {
    TableDescriptor ftd = getFTableDescriptor();
    ftd.setRecoveryDelay(2000);
    assertEquals(2000, ftd.getRecoveryDelay());

    FTable table = (FTable) createFTable(TABLE_NAME, ftd);
    assertEquals(2000, table.getTableDescriptor().getRecoveryDelay());

    checkRecoveryDelayOnAllServers(2000, PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        TABLE_NAME);
    deleteTable(TABLE_NAME, ftd);
  }


  @Test
  public void testRecoveryDelayMTableDefaultServerRestart() {
    TableDescriptor mtd = getMTableDescriptor();
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, mtd.getRecoveryDelay());

    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        table.getTableDescriptor().getRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    restartServers();
    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);

    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testRecoveryDelayFTableDefaultServerRestart() {
    TableDescriptor ftd = getFTableDescriptor();
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, ftd.getRecoveryDelay());

    FTable table = (FTable) createFTable(TABLE_NAME, ftd);
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        table.getTableDescriptor().getRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    restartServers();
    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    deleteTable(TABLE_NAME, ftd);
  }

  @Test
  public void testRecoveryDelayMTableNonDefaultServerRestart() {
    TableDescriptor mtd = getMTableDescriptor();
    mtd.setRecoveryDelay(2000);
    assertEquals(2000, mtd.getRecoveryDelay());

    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    assertEquals(2000, table.getTableDescriptor().getRecoveryDelay());

    checkRecoveryDelayOnAllServers(2000, PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        TABLE_NAME);
    restartServers();
    checkRecoveryDelayOnAllServers(2000, PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testRecoveryDelayFTableNonDefaultServerRestart() {
    TableDescriptor ftd = getFTableDescriptor();
    ftd.setRecoveryDelay(2000);
    assertEquals(2000, ftd.getRecoveryDelay());

    FTable table = (FTable) createFTable(TABLE_NAME, ftd);
    assertEquals(2000, table.getTableDescriptor().getRecoveryDelay());

    checkRecoveryDelayOnAllServers(2000, PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        TABLE_NAME);
    restartServers();
    checkRecoveryDelayOnAllServers(2000, PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        TABLE_NAME);
    deleteTable(TABLE_NAME, ftd);
  }

  @Test
  public void testStartupRecoveryDelayMTableDefault() {
    TableDescriptor mtd = getMTableDescriptor();
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        mtd.getStartupRecoveryDelay());

    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        table.getTableDescriptor().getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testStartupRecoveryDelayFTableDefault() {
    TableDescriptor ftd = getFTableDescriptor();
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        ftd.getStartupRecoveryDelay());

    FTable table = (FTable) createFTable(TABLE_NAME, ftd);
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        table.getTableDescriptor().getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    deleteTable(TABLE_NAME, ftd);
  }

  @Test
  public void testStartupRecoveryDelayMTableNonDefault() {
    TableDescriptor mtd = getMTableDescriptor();
    mtd.setStartupRecoveryDelay(2004);
    assertEquals(2004, mtd.getStartupRecoveryDelay());

    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    assertEquals(2004, table.getTableDescriptor().getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, 2004,
        TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testStartupRecoveryDelayFTableNonDefault() {
    TableDescriptor ftd = getFTableDescriptor();
    ftd.setStartupRecoveryDelay(2003);
    assertEquals(2003, ftd.getStartupRecoveryDelay());

    FTable table = (FTable) createFTable(TABLE_NAME, ftd);
    assertEquals(2003, table.getTableDescriptor().getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, 2003,
        TABLE_NAME);
    deleteTable(TABLE_NAME, ftd);
  }


  @Test
  public void testStartupRecoveryDelayMTableDefaultServerRestart() {
    TableDescriptor mtd = getMTableDescriptor();
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        mtd.getStartupRecoveryDelay());

    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        table.getTableDescriptor().getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    restartServers();
    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);

    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testStartupRecoveryDelayFTableDefaultServerRestart() {
    TableDescriptor ftd = getFTableDescriptor();
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        ftd.getStartupRecoveryDelay());

    FTable table = (FTable) createFTable(TABLE_NAME, ftd);
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        table.getTableDescriptor().getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    restartServers();
    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    deleteTable(TABLE_NAME, ftd);
  }

  @Test
  public void testStartupRecoveryDelayMTableNonDefaultServerRestart() {
    TableDescriptor mtd = getMTableDescriptor();
    mtd.setStartupRecoveryDelay(2002);
    assertEquals(2002, mtd.getStartupRecoveryDelay());

    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    assertEquals(2002, table.getTableDescriptor().getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, 2002,
        TABLE_NAME);
    restartServers();
    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, 2002,
        TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testStartUpRecoveryDelayFTableNonDefaultServerRestart() {
    TableDescriptor ftd = getFTableDescriptor();
    ftd.setStartupRecoveryDelay(2001);
    assertEquals(2001, ftd.getStartupRecoveryDelay());

    FTable table = (FTable) createFTable(TABLE_NAME, ftd);
    assertEquals(2001, table.getTableDescriptor().getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, 2001,
        TABLE_NAME);
    restartServers();
    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, 2001,
        TABLE_NAME);
    deleteTable(TABLE_NAME, ftd);
  }

  @Test
  public void testBothRecoveryDelayMTableDefault() {
    TableDescriptor mtd = getMTableDescriptor();
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, mtd.getRecoveryDelay());
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        mtd.getStartupRecoveryDelay());

    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, mtd.getRecoveryDelay());
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        mtd.getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testBothRecoveryDelayFTableDefault() {
    TableDescriptor mtd = getFTableDescriptor();
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, mtd.getRecoveryDelay());
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        mtd.getStartupRecoveryDelay());

    FTable table = (FTable) createFTable(TABLE_NAME, mtd);
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, mtd.getRecoveryDelay());
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        mtd.getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testBothRecoveryDelayMTableDefaultServerRestart() {
    TableDescriptor mtd = getMTableDescriptor();
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, mtd.getRecoveryDelay());
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        mtd.getStartupRecoveryDelay());

    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, mtd.getRecoveryDelay());
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        mtd.getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    restartServers();
    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testBothRecoveryDelayFTableDefaultServerRestart() {
    TableDescriptor mtd = getFTableDescriptor();
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, mtd.getRecoveryDelay());
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        mtd.getStartupRecoveryDelay());

    FTable table = (FTable) createFTable(TABLE_NAME, mtd);
    assertEquals(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, mtd.getRecoveryDelay());
    assertEquals(PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT,
        mtd.getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    restartServers();
    checkRecoveryDelayOnAllServers(PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT,
        PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  ////
  @Test
  public void testBothRecoveryDelayMTableNonDefault() {
    TableDescriptor mtd = getMTableDescriptor();
    mtd.setStartupRecoveryDelay(120);
    mtd.setRecoveryDelay(121);
    assertEquals(121, mtd.getRecoveryDelay());
    assertEquals(120, mtd.getStartupRecoveryDelay());

    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    assertEquals(121, mtd.getRecoveryDelay());
    assertEquals(120, mtd.getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(121, 120, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testBothRecoveryDelayFTableNonDefault() {
    TableDescriptor mtd = getFTableDescriptor();
    mtd.setRecoveryDelay(111);
    mtd.setStartupRecoveryDelay(222);
    assertEquals(111, mtd.getRecoveryDelay());
    assertEquals(222, mtd.getStartupRecoveryDelay());

    FTable table = (FTable) createFTable(TABLE_NAME, mtd);
    assertEquals(111, mtd.getRecoveryDelay());
    assertEquals(222, mtd.getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(111, 222, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testBothRecoveryDelayMTableNonDefaultServerRestart() {
    TableDescriptor mtd = getMTableDescriptor();
    mtd.setRecoveryDelay(666);
    mtd.setStartupRecoveryDelay(999);
    assertEquals(666, mtd.getRecoveryDelay());
    assertEquals(999, mtd.getStartupRecoveryDelay());

    MTable table = (MTable) createMTable(TABLE_NAME, mtd);
    assertEquals(666, mtd.getRecoveryDelay());
    assertEquals(999, mtd.getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(666, 999, TABLE_NAME);
    restartServers();
    checkRecoveryDelayOnAllServers(666, 999, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }

  @Test
  public void testBothRecoveryDelayFTableNonDefaultServerRestart() {
    TableDescriptor mtd = getFTableDescriptor();
    mtd.setRecoveryDelay(333);
    mtd.setStartupRecoveryDelay(888);
    assertEquals(333, mtd.getRecoveryDelay());
    assertEquals(888, mtd.getStartupRecoveryDelay());

    FTable table = (FTable) createFTable(TABLE_NAME, mtd);
    assertEquals(333, mtd.getRecoveryDelay());
    assertEquals(888, mtd.getStartupRecoveryDelay());

    checkRecoveryDelayOnAllServers(333, 888, TABLE_NAME);
    restartServers();
    checkRecoveryDelayOnAllServers(333, 888, TABLE_NAME);
    deleteTable(TABLE_NAME, mtd);
  }
}
