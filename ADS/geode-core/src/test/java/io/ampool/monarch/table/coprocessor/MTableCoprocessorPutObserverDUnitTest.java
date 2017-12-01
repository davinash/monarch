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

import org.apache.geode.cache.*;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.internal.PostOpRegionObserver;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * MTableCoprocessorFrameworkDUnitTest
 *
 * @since 0.2.0.0
 */

@Category(MonarchTest.class)
public class MTableCoprocessorPutObserverDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();

  final int numOfEntries = 100;
  private String tableName = "EmployeeTable";

  public MTableCoprocessorPutObserverDUnitTest() {
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

  private void doPutOperationFromClient(VM vm, final int locatorPort, final boolean order,
      final int maxVersions, boolean batchPut) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          MTableDescriptor tableDescriptor = null;
          if (order == false) {
            tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
          } else {
            tableDescriptor = new MTableDescriptor();
          }
          // Add Coprocessor
          tableDescriptor.addCoprocessor("io.ampool.monarch.table.coprocessor.SamplePutObserver");

          tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
              .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

          tableDescriptor.setRedundantCopies(1);
          if (maxVersions > 1) {
            tableDescriptor.setMaxVersions(maxVersions);
          }

          MClientCache clientCache = MClientCacheFactory.getAnyInstance();
          Admin admin = clientCache.getAdmin();
          MTable mtable = admin.createTable(tableName, tableDescriptor);
          assertEquals(mtable.getName(), tableName);
          List<Put> puts = new ArrayList<>();
          for (int j = 1; j <= maxVersions; j++) {
            for (int i = 0; i < numOfEntries; i++) {
              String key1 = "RowKey" + i;
              Put myput1 = new Put(Bytes.toBytes(key1));
              myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
              myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + (10 * j)));
              myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + (10 * j)));
              myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + (10 * j)));
              if (!batchPut)
                mtable.put(myput1);
              else
                puts.add(myput1);
            }
          }
          if (batchPut) {
            mtable.put(puts);
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
        int prePut = 0;
        int postPut = 0;

        for (CacheListener cl : cls) {
          if (cl instanceof PostOpRegionObserver) {
            PostOpRegionObserver poro = (PostOpRegionObserver) cl;
            int obsNo = 1;
            for (MTableObserver obj : poro.getmObserverInstanceList(tableName)) {
              if (obj instanceof SamplePutObserver) {
                System.out.println("Observer called " + obsNo++);
                SamplePutObserver ro1 = (SamplePutObserver) obj;
                prePut = prePut + ro1.getTotalPrePutCount();
                postPut = postPut + ro1.getTotalPostPutCount();
                // assertEquals(numOfEntries,prePut);
                // assertEquals(numOfEntries,postPut);
              }
            }
          }
        }
        return prePut + ":" + postPut;
      }
    });
  }

  private void doCreateTableAndBasicPutGet(final boolean order, final int maxVersions,
      boolean batchPut) throws Exception {
    doPutOperationFromClient(vm3, getLocatorPort(), order, maxVersions, batchPut);
    verifyObserverEventsCount(maxVersions);
  }

  private void verifyObserverEventsCount(final int maxVersions) {
    int prePut = 0;
    int postPut = 0;
    String counts = (String) verifyObserverEventsonOnServer(vm0);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm1);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm2);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    assertEquals(numOfEntries * maxVersions, prePut);
    assertEquals(numOfEntries * maxVersions, postPut);
  }

  private void deleteTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable mtable = clientCache.getTable(tableName);
    clientCache.getAdmin().deleteTable(tableName);
  }

  /**
   * Tests observer for put operation on Ordered Table
   *
   * @throws Exception
   */
  @Test
  public void testDoObserverTest() throws Exception {
    doCreateTableAndBasicPutGet(true, 1, false);
    deleteTable();
  }

  /**
   * Tests observer for put operation on Unordered Table
   *
   * @throws Exception
   */
  @Test
  public void testDoObserverUnOrderedTest() throws Exception {
    doCreateTableAndBasicPutGet(false, 1, false);
    deleteTable();

  }

  /**
   * Tests observer for put operation on Ordered Table
   *
   * @throws Exception
   */
  @Test
  public void testDoObserverTestMultiVersioned() throws Exception {
    doCreateTableAndBasicPutGet(true, 3, false);
    deleteTable();
  }

  /**
   * Tests observer for put operation on Ordered Table
   *
   * @throws Exception
   */
  @Test
  public void testDoObserverTestMultiVersionedBatchPut() throws Exception {
    doCreateTableAndBasicPutGet(true, 3, true);
    deleteTable();
  }

}
