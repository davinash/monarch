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

package io.ampool.monarch.table.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.internal.PostOpRegionObserver;

import org.apache.geode.cache.CacheClosedException;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * MTableCoprocessorFrameworkDUnitTest
 *
 * @since 0.2.0.0
 */

@Category(MonarchTest.class)
public class MTableCoprocessorCheckAndPutObserverDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();

  final int numOfEntries = 100;
  final int numOfCheckAndPut = 10;
  final int numOfFailCheckAndPut = 0;
  private String tableName = "EmployeeTable";
  private String observer = "io.ampool.monarch.table.coprocessor.SampleCheckAndPutObserver";

  public MTableCoprocessorCheckAndPutObserverDUnitTest() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();

    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());

    createClientCache(vm3);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(this.vm3);
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            MCacheFactory.getAnyInstance().close();
            return null;
          }
        }));
    super.tearDown2();
  }

  private void doPutOperationFromClient(VM vm, final int locatorPort, final boolean order) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          MTableDescriptor tableDescriptor = new MTableDescriptor();
          // Add Coprocessor
          tableDescriptor.addCoprocessor(observer);

          tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
              .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
          /*
           * if (order == false) { tableDescriptor.setTableType(MTableType.UNORDERED); }
           */
          tableDescriptor.setRedundantCopies(1);

          MClientCache clientCache = MClientCacheFactory.getAnyInstance();
          Admin admin = clientCache.getAdmin();
          MTable mtable = admin.createTable(tableName, tableDescriptor);
          assertEquals(mtable.getName(), tableName);

          for (int i = 0; i < numOfEntries; i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
            mtable.put(myput1);
          }

          for (int i = 0; i < numOfCheckAndPut; i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i * 10));
            boolean checkResult = mtable.checkAndPut(Bytes.toBytes(key1), Bytes.toBytes("ID"),
                Bytes.toBytes(i + 10), myput1);
            assertTrue(checkResult);
          }

          // reset values to older one
          for (int i = 0; i < numOfEntries; i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
            mtable.put(myput1);
          }

          for (int i = 0; i < numOfFailCheckAndPut; i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i * 10));
            boolean checkResult = mtable.checkAndPut(Bytes.toBytes(key1), Bytes.toBytes("ID"),
                Bytes.toBytes(i + 11), myput1);
            assertFalse(checkResult);
          }

        } catch (CacheClosedException cce) {
        }
        return null;
      }
    });

  }

  private Object verifyObserverEventsonOnServer(VM vm) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        // Region
        MCache c = MCacheFactory.getAnyInstance();
        Region r = c.getRegion(tableName);
        assertNotNull(r);
        CacheListener[] cls = ((PartitionedRegion) r).getCacheListeners();
        int preCheckAndPut = 0;
        int postCheckAndPut = 0;
        int failedPreCheckAndPut = 0;
        int failedPostCheckAndPut = 0;

        for (CacheListener cl : cls) {
          if (cl instanceof PostOpRegionObserver) {
            PostOpRegionObserver poro = (PostOpRegionObserver) cl;
            int obsNo = 1;
            for (MTableObserver obj : poro.getmObserverInstanceList(tableName)) {
              if (obj instanceof SampleCheckAndPutObserver) {
                System.out.println("Observer called " + obsNo++);
                SampleCheckAndPutObserver ro1 = (SampleCheckAndPutObserver) obj;
                preCheckAndPut = preCheckAndPut + ro1.getPreCheckAndPut();
                postCheckAndPut = postCheckAndPut + ro1.getPostCheckAndPut();
                failedPreCheckAndPut = failedPreCheckAndPut + ro1.getFailedPreCheckAndPut();
                failedPostCheckAndPut = failedPostCheckAndPut + ro1.getFailedPostCheckAndPut();
              }
            }
          }
        }
        return preCheckAndPut + ":" + postCheckAndPut + ":" + failedPreCheckAndPut + ":"
            + failedPostCheckAndPut;
      }
    });
  }

  private void doCreateTableAndBasicPutGet(final boolean order) throws Exception {
    doPutOperationFromClient(vm3, getLocatorPort(), order);
    verifyObserverEventsCount();
  }

  private void verifyObserverEventsCount() {
    int preCheckAndPut = 0;
    int postCheckAndPut = 0;
    int failedPreCheckAndPut = 0;
    int failedPostCheckAndPut = 0;

    String counts = (String) verifyObserverEventsonOnServer(vm0);
    preCheckAndPut = preCheckAndPut + Integer.parseInt(counts.split(":")[0]);
    postCheckAndPut = postCheckAndPut + Integer.parseInt(counts.split(":")[1]);
    failedPreCheckAndPut = failedPreCheckAndPut + Integer.parseInt(counts.split(":")[2]);
    failedPostCheckAndPut = failedPostCheckAndPut + Integer.parseInt(counts.split(":")[3]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm1);
    preCheckAndPut = preCheckAndPut + Integer.parseInt(counts.split(":")[0]);
    postCheckAndPut = postCheckAndPut + Integer.parseInt(counts.split(":")[1]);
    failedPreCheckAndPut = failedPreCheckAndPut + Integer.parseInt(counts.split(":")[2]);
    failedPostCheckAndPut = failedPostCheckAndPut + Integer.parseInt(counts.split(":")[3]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm2);
    preCheckAndPut = preCheckAndPut + Integer.parseInt(counts.split(":")[0]);
    postCheckAndPut = postCheckAndPut + Integer.parseInt(counts.split(":")[1]);
    failedPreCheckAndPut = failedPreCheckAndPut + Integer.parseInt(counts.split(":")[2]);
    failedPostCheckAndPut = failedPostCheckAndPut + Integer.parseInt(counts.split(":")[3]);
    System.out.println("Counts: " + counts);
    assertEquals(numOfCheckAndPut, preCheckAndPut);
    assertEquals(numOfCheckAndPut, postCheckAndPut);
    assertEquals(numOfFailCheckAndPut, failedPreCheckAndPut);
    // No post events are triggered after failing checkAndPut
    assertEquals(0, failedPostCheckAndPut);
  }

  private void deleteTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable mtable = clientCache.getTable(tableName);
    clientCache.getAdmin().deleteTable(tableName);
  }

  /**
   * Tests observer for put operation
   *
   * @throws Exception
   */
  @Test
  public void testDoObserverTest() throws Exception {
    doCreateTableAndBasicPutGet(true);
    deleteTable();

    // doCreateTableAndBasicPutGet(false);
    // deleteTable();
  }
  /*
   * @Test public void testObserverEvents() throws Exception { doCreateTableAndBasicPutGet(true);
   * verifyObserverEventsonOnServer(vm0); verifyObserverEventsonOnServer(vm1);
   * verifyObserverEventsonOnServer(vm2);
   * 
   * deleteTable(); }
   */
}
