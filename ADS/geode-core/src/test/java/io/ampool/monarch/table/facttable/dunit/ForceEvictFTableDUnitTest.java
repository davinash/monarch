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

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.functions.TestDUnitBase;
import io.ampool.monarch.table.internal.AdminImpl;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.store.StoreHandler;
import io.ampool.tierstore.wal.WALRecord;
import io.ampool.tierstore.wal.WALResultScanner;
import io.ampool.tierstore.wal.WriteAheadLog;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.geode.test.dunit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Category(FTableTest.class)
public class ForceEvictFTableDUnitTest extends MTableDUnitHelper {
  private static final int NUM_COLS = 10;
  List<VM> allServers = null;
  private static final TestDUnitBase testBase = new TestDUnitBase();
  private String tableName = null;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());
    createClientCache(client1);
    createClientCache();
    allServers = Arrays.asList(vm0, vm1, vm2);
  }

  @Override
  public void tearDown2() throws Exception {
    if (tableName != null) {
      MClientCacheFactory.getAnyInstance().getAdmin().deleteFTable(tableName);
      tableName = null;
    }
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

  private FTable createFtable(String tableName) {
    FTableDescriptor fd = new FTableDescriptor();
    fd.setBlockSize(2);

    fd.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    for (int i = 0; i < NUM_COLS; i++) {
      fd.addColumn("COL_" + i);
    }

    fd.setRedundantCopies(0);
    fd.setTotalNumOfSplits(1);

    FTable ftable = MClientCacheFactory.getAnyInstance().getAdmin().createFTable(tableName, fd);

    /*
     * Make sure tbale is created
     */
    checkTableOnServers(tableName);

    return ftable;
  }

  private void checkTableOnServers(String tableName) {
    for (VM vm : allServers) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          TableDescriptor td = MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
          assertNotNull(td);
          return null;
        }
      });
    }
  }

  private void verifyRecordCountOnClient(String tableName, int expCount) {
    FTable table = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    int actualCount = 0;
    while (itr.hasNext()) {
      Row row = (Row) itr.next();
      actualCount++;
    }
    assertEquals(expCount, actualCount);
  }

  /**
   * Create a Ftable populate 1000 records in it Verify the records in memory Flush the FTable and
   * then verify the records in WAL
   */
  @Test
  public void testforceFTableEviction() throws InterruptedException {
    this.tableName = "testTableFlush";
    int numOfRecords = 1000;
    int memoryCount = 0;
    int walCount = 0;

    Exception e = null;

    FTable table = createFtable(tableName);

    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    for (int j = 0; j < numOfRecords; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
    }
    // Verify that the records are populated
    verifyRecordCountOnClient(tableName, numOfRecords);

    walCount = getCountFromWalScan(tableName);

    // The WalCount shoule be 0 now as the flush has not been invoked.
    assertEquals(0, walCount);
    try {
      ((AdminImpl) MClientCacheFactory.getAnyInstance().getAdmin()).forceFTableEviction(tableName);
    } catch (Exception e1) {
      System.out.println(
          "FTableFlushToTierDUnitTest.testTableFlush Exception Caught ===> " + e.toString());
      e1.printStackTrace();
      e = e1;
    }
    assertNull(e);
    walCount = 0;
    // Now check the WALCount
    walCount = getCountFromWalScan(tableName);
    assertEquals(numOfRecords, walCount);

    // Now explicitly flush WAL
    flushWAL(tableName);
    walCount = getCountFromWalScan(tableName);
    assertEquals(0, walCount);

    verifyRecordCountOnClient(tableName, numOfRecords);
    assertEquals("EvictionCount should match number of evicted entries.", numOfRecords,
        getTotalEvictedCount(tableName));
  }

  private void asyncStartVm(final VM vm) {
    try {
      asyncStartServerOn(vm, DUnitLauncher.getLocatorString()).join();
    } catch (Exception e) {
      ////
    }
  }

  @Test
  public void testForceEvictWithRestart() throws InterruptedException {
    this.tableName = "testForceEvictWithRestart";
    int numOfRecords = 1000;

    FTableDescriptor fd = new FTableDescriptor();
    fd.setRedundantCopies(2).setTotalNumOfSplits(1)
        .setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER).setBlockSize(100);
    for (int i = 0; i < NUM_COLS; i++) {
      fd.addColumn("COL_" + i);
    }
    MClientCacheFactory.getAnyInstance().getAdmin().createFTable(tableName, fd);

    FTable table = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    for (int j = 0; j < numOfRecords; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      table.append(record);
      /* make sure that delta is written to disk-store in async mode */
      if (j % 50 == 0) {
        Thread.sleep(2_000);
      }
    }
    // Verify that the records are populated
    verifyRecordCountOnClient(tableName, numOfRecords);

    long totalSizeBeforeRestart = getTotalSize(tableName);
    System.out.println("totalSizeBeforeRestart = " + totalSizeBeforeRestart);

    allServers.forEach(this::stopServerOn);
    allServers.parallelStream().forEach(this::asyncStartVm);
    Thread.sleep(5_000);

    assertEquals("Incorrect total-size after restart.", totalSizeBeforeRestart,
        getTotalSize(tableName));

    try {
      MClientCacheFactory.getAnyInstance().getAdmin().forceFTableEviction(tableName);
    } catch (Exception e1) {
      fail("No expected expected.", e1);
    }
    int walCount = getCountFromWalScan(tableName);
    assertEquals(numOfRecords * 3, walCount);

    verifyRecordCountOnClient(tableName, numOfRecords);
  }

  private long getTotalEvictedCount(final String tableName) {
    long totalCount = 0;
    for (VM vm : new VM[] {vm0, vm1, vm2}) {
      totalCount += vm.invoke(() -> {
        final Region<Object, Object> region = MCacheFactory.getAnyInstance().getRegion(tableName);
        assertNotNull(region);
        long count = 0;
        for (BucketRegion br : ((PartitionedRegion) region).getDataStore()
            .getAllLocalBucketRegions()) {
          count += br.getEvictions();
        }
        return count;
      });
    }
    return totalCount;
  }

  private long getTotalSize(final String tableName) {
    long totalSize = 0;
    for (VM vm : new VM[] {vm0, vm1, vm2}) {
      totalSize += vm.invoke(() -> {
        final Region<Object, Object> region = MCacheFactory.getAnyInstance().getRegion(tableName);
        assertNotNull(region);
        long size = 0;
        for (BucketRegion br : ((PartitionedRegion) region).getDataStore()
            .getAllLocalBucketRegions()) {
          size += br.getBytesInMemory();
        }
        return size;
      });
    }
    return totalSize;
  }

  private void flushWAL(final String tableName) {
    final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
    for (int i = 0; i < vmList.size(); i++) {
      vmList.get(i).invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          for (int j = 0; j < 113; j++) {
            StoreHandler.getInstance().flushWriteAheadLog(tableName, j);
          }
          return null;
        }
      });
    }
  }

  public int getCountFromWalScan(String ftable) {
    return (getCountFromWal(vm0, ftable) + getCountFromWal(vm1, ftable)
        + getCountFromWal(vm2, ftable));
  }

  private static int getRegionCountOnServer(final VM vm, final String regionName)
      throws RMIException {
    return (int) vm.invoke(new SerializableCallable() {
      @Override

      public Object call() throws Exception {
        final PartitionedRegion pr =
            (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(regionName);
        assertNotNull(pr);
        return (getTotalEntryCount(pr));
      }
    });
  }

  private static int getTotalEntryCount(final PartitionedRegion pr) {
    return pr.getDataStore().getAllLocalBucketRegions().stream().mapToInt(BucketRegion::size).sum();
  }

  private static int getCountFromWal(final VM vm, final String regionName) throws RMIException {
    return (int) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        int scanCount = 0;
        for (int i = 0; i < 113; i++) {

          final WALResultScanner scanner = WriteAheadLog.getInstance().getScanner(regionName, i);
          while (true) {
            WALRecord a = scanner.next();
            if (a != null) {
              BlockValue blockValue = a.getBlockValue();
              scanCount += blockValue.getCurrentIndex();
            } else {
              break;
            }
          }
        }
        return (scanCount);
      }
    });
  }

}
