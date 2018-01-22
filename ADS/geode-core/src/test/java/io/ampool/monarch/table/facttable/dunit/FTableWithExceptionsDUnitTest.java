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

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MCacheLowMemoryException;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.types.TypeUtils;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(FTableTest.class)
public class FTableWithExceptionsDUnitTest extends MTableDUnitHelper {
  private static String TABLE_NAME = "FTableWithExceptionsDUnitTest";
  private static long NUM_OF_ROWS = 10_000L;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    super.tearDown2();
  }

  private void startServerWithEviction() throws IOException {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
    props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
    props.setProperty(DistributionConfig.LOCATORS_NAME, DUnitLauncher.getLocatorString());
    Cache c = null;
    try {
      c = CacheFactory.getAnyInstance();
      c.close();
    } catch (CacheClosedException cce) {
    }
    c = MCacheFactory.create(getSystem(props));
    c.getResourceManager().setEvictionHeapPercentage(80.0f);
    c.getResourceManager().setCriticalHeapPercentage(90.0f);
    CacheServer s = c.addCacheServer();
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    s.setPort(port);
    s.start();
  }

  private FTable ingestDataIntoFTable() throws InterruptedException {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    FTableDescriptor ftd = new FTableDescriptor();
    for (int colIdx = 0; colIdx < 10; colIdx++) {
      ftd.addColumn("Column-" + colIdx);
    }
    ftd.setRedundantCopies(3);
    ftd.setTotalNumOfSplits(1);
    FTable fTable = clientCache.getAdmin().createFTable(TABLE_NAME, ftd);

    final int randomInt = TypeUtils.getRandomInt((int) NUM_OF_ROWS - 1);
    System.out.println("Expecting LowMemoryException after recordCount=" + randomInt);

    long row = 0;
    try {
      int retry = 0;
      final VM[] vms = new VM[] {vm0, vm1, vm2};
      for (row = 0; row < NUM_OF_ROWS;) {
        Record record = new Record();
        for (int colIdx = 0; colIdx < 10; colIdx++) {
          record.add("Column-" + colIdx, TypeUtils.getRandomBytes(1000));
        }
        /* raise LME one by one and lastly on all servers */
        if (row >= randomInt) {
          if (retry == vms.length) {
            vms[0].invoke(this::raiseFakeNotification);
            vms[1].invoke(this::raiseFakeNotification);
            vms[2].invoke(this::raiseFakeNotification);
            retry++;
          } else {
            if (retry > 0) {
              vms[retry - 1].invoke(this::revokeFakeNotification);
            }
            vms[retry++].invoke(this::raiseFakeNotification);
          }
        }
        try {
          fTable.append(record);
          row++;
        } catch (MCacheLowMemoryException e) {
          if (retry > vms.length) {
            throw e;
          }
        }
      }
      fail("Expected LowMemoryException after recordCount=" + randomInt);
    } catch (MCacheLowMemoryException ignore) {
      System.out.println("Received Low Memory Exception for -> " + row + "; msg=" + ignore);
      NUM_OF_ROWS = row;
    }
    System.out.println("Rows Inserted Before MemoryException => " + row);
    return fTable;
  }

  private void doScanAndVerify(final FTable fTable, final Set<ServerLocation> servers) {
    for (final ServerLocation server : servers) {
      final Scan scan = new Scan();
      Map<Integer, Set<ServerLocation>> map = new HashMap<>();
      for (int i = 0; i < fTable.getTableDescriptor().getTotalNumOfSplits(); i++) {
        map.put(i, Collections.singleton(server));
      }
      scan.setBucketToServerMap(map);
      System.out.println("### Running scan on server= " + server);
      Scanner scanner = fTable.getScanner(scan);
      Iterator<Row> iterator = scanner.iterator();
      long count = 0L;
      while (iterator.hasNext()) {
        count++;
        iterator.next();
      }
      scanner.close();
      assertEquals("Incorrect scan count for server: " + server, NUM_OF_ROWS, count);
    }
  }

  /**
   * Raise fake notification so that LME is raised.
   */
  protected void raiseFakeNotification() {
    final GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();
    cache.getHeapEvictor().testAbortAfterLoopCount = 1;
    HeapMemoryMonitor.setTestDisableMemoryUpdates(true);
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "memoryEventTolerance", "0");

    HeapMemoryMonitor hmm = cache.getResourceManager().getHeapMonitor();
    hmm.setTestMaxMemoryBytes(100);

    hmm.updateStateAndSendEvent(90);
  }

  /**
   * Revoke the fake notification.
   */
  private void revokeFakeNotification() {
    final GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();
    cache.getHeapEvictor().testAbortAfterLoopCount = Integer.MAX_VALUE;
    HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "memoryEventTolerance", "100");

    HeapMemoryMonitor hmm = cache.getResourceManager().getHeapMonitor();
    hmm.setTestMaxMemoryBytes(1_000_000_000);

    hmm.updateStateAndSendEvent(90);
  }

  @After
  public void cleanUpMethod() {
    MClientCacheFactory.getAnyInstance().getAdmin().deleteFTable(TABLE_NAME);

    vm0.invoke(() -> MCacheFactory.getAnyInstance().close());
    vm1.invoke(() -> MCacheFactory.getAnyInstance().close());
    vm2.invoke(() -> MCacheFactory.getAnyInstance().close());
  }

  @Test
  public void testLowMemoryException() throws InterruptedException {
    IgnoredException.addIgnoredException("above heap critical threshold");
    IgnoredException.addIgnoredException("below heap critical threshold");
    IgnoredException.addIgnoredException("java.util.concurrent.RejectedExecutionException");

    vm0.invoke(this::startServerWithEviction);
    vm1.invoke(this::startServerWithEviction);
    vm2.invoke(this::startServerWithEviction);

    final FTable fTable = ingestDataIntoFTable();

    Map<Integer, Pair<ServerLocation, Long>> pMap = new HashMap<>();
    Map<Integer, Set<Pair<ServerLocation, Long>>> sMap = new HashMap<>();
    MTableUtils.getLocationAndCount(fTable, pMap, sMap);

    final Set<ServerLocation> servers =
        pMap.values().stream().map(Pair::getFirst).collect(Collectors.toSet());
    for (final Set<Pair<ServerLocation, Long>> entry : sMap.values()) {
      servers.addAll(entry.stream().map(Pair::getFirst).collect(Collectors.toSet()));
    }
    doScanAndVerify(fTable, servers);

    IgnoredException.removeAllExpectedExceptions();
  }
}
