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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MConfiguration;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.store.StoreHandler;
import io.ampool.store.StoreHierarchy;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.StressTest;


@Category(StressTest.class)
public class FTableLotOfRecordsDUnitTest extends MTableDUnitHelper {
  private static final int NUM_OF_COLUMNS = 10;
  private static final String COLUMN_NAME_PREFIX = "COL";
  private Host host = null;
  private VM vm0 = null;
  private static float EVICT_HEAP_PCT = 0.1f;
  private static int CRITICAL_EVICT_HEAP_PCT = 90;
  private static final String AMPOOL_LARGE_NUMER_OF_RECORDS = "AMPOOL_LARGE_NUMER_OF_RECORDS";

  @BeforeClass
  public static void initializeDistributedTestCase() {
    System.setProperty(AMPOOL_LARGE_NUMER_OF_RECORDS, AMPOOL_LARGE_NUMER_OF_RECORDS);
    JUnit4DistributedTestCase.initializeDistributedTestCase();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    System.out.println("FTableLotOfRecordsDUnitTest.postSetUp DUnitLauncher.getLocatorString() = "
        + DUnitLauncher.getLocatorString());
    startServerOn(vm0, DUnitLauncher.getLocatorString());
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    new ArrayList<>(Arrays.asList(vm0)).forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    }));
    super.tearDown2();
  }

  private FTable createTable(String tableName) {
    MConfiguration mconf = MConfiguration.create();
    String locatorString = DUnitLauncher.getLocatorString();
    int port = Integer.parseInt(
        locatorString.substring(locatorString.indexOf('[') + 1, locatorString.length() - 1));
    System.out.println("FTableLotOfRecordsDUnitTest.createTable substring " + port);

    MClientCache cache = new MClientCacheFactory().set("log-file", "/tmp/FTableClient.log")
        .addPoolLocator("127.0.0.1", port).create();

    FTableDescriptor fd = new FTableDescriptor();
    for (int i = 0; i < NUM_OF_COLUMNS; i++) {
      fd.addColumn(COLUMN_NAME_PREFIX + "_" + i);
    }
    fd.setRedundantCopies(0);
    fd.setTotalNumOfSplits(1);
    return cache.getAdmin().createFTable(tableName, fd);
  }

  private void appendRecords(String tableName, int numRecords, int batchSize) {
    FTable table = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    Record[] records = new Record[batchSize];
    int totalInserted = 0;
    for (int i = 0; i < numRecords / batchSize; i++) {
      for (int j = 0; j < batchSize; j++) {
        records[j] = new Record();
        for (int k = 0; k < NUM_OF_COLUMNS; k++) {
          records[j].add(COLUMN_NAME_PREFIX + "_" + k,
              Bytes.toBytes("COL_" + k + "_" + totalInserted));
        }
      }
      table.append(records);
      totalInserted += batchSize;
      if (totalInserted % 100000 == 0) {
        System.out.println("Total records inserted = " + totalInserted);
      }
    }
  }

  private void scanRecords(String tableName) {
    FTable table = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    final Scanner scanner = table.getScanner(new Scan());
    int count = 0;
    Row res = scanner.next();
    while (res != null) {
      count++;
      res = scanner.next();
    }
    System.out.println("FTableLotOfRecordsDUnitTest.scanRecords count = " + count);
    assertEquals(30000000, count);
  }

  private void disableORCStore() {
    vm0.invoke(new SerializableCallable() {
      public Object call() {
        /* Force creation of store instance to avoid adding of ORC store later. */
        System.out.println(StoreHandler.getInstance().toString());
        StoreHierarchy.getInstance().removeStore("ORC");
        return null;
      }
    });
  }

  private void setEvictionProperties() {
    vm0.invoke(new SerializableCallable() {
      public Object call() {
        ResourceManager resourceManager = MCacheFactory.getAnyInstance().getResourceManager();
        resourceManager.setEvictionHeapPercentage(EVICT_HEAP_PCT);
        resourceManager.setCriticalHeapPercentage(CRITICAL_EVICT_HEAP_PCT);
        return null;
      }
    });
  }

  @Test
  public void testLotOfRecords() {
    // disableORCStore();
    IgnoredException.addIgnoredException(
        "io.ampool.monarch.table.exceptions.StoreInternalException: Error while writing to WAL.");
    IgnoredException.addIgnoredException("above heap critical threshold");
    IgnoredException.addIgnoredException("below heap critical threshold");
    setEvictionProperties();
    String tableName = "testTable";
    try {
      createTable(tableName);
      appendRecords(tableName, 3000000, 1000);
      scanRecords(tableName);
    } catch (Throwable t) {
      System.out.println("Exception caught: " + t.getClass().toString());
      t.printStackTrace();
    }
  }
}
