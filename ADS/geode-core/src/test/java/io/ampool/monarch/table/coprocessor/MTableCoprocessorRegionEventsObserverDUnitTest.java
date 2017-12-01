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

/**
 * MTableCoprocessorFrameworkDUnitTest
 *
 * @since 0.2.0.0
 */

@Category(MonarchTest.class)
public class MTableCoprocessorRegionEventsObserverDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();
  public static boolean beforeRegionDestroyCalled = false;

  final int numOfEntries = 10;
  private String tableName = "EmployeeTable";

  public MTableCoprocessorRegionEventsObserverDUnitTest() {
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
    super.tearDown2();
  }

  private void doPutOperationFromClient(VM vm, final int locatorPort, final boolean order) {
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
          tableDescriptor
              .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRegionObserver4");

          tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
              .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
          tableDescriptor.setRedundantCopies(1);

          MClientCache clientCache = MClientCacheFactory.getAnyInstance();
          Admin admin = clientCache.getAdmin();
          MTable mtable = admin.createTable(tableName, tableDescriptor);
          assertEquals(mtable.getName(), tableName);
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
        int postOpen = 0;
        int postClose = 0;
        int preClose = 0;

        for (CacheListener cl : cls) {
          if (cl instanceof PostOpRegionObserver) {
            PostOpRegionObserver poro = (PostOpRegionObserver) cl;
            int obsNo = 1;
            for (MTableObserver obj : poro.getmObserverInstanceList(tableName)) {
              if (obj instanceof SampleRegionObserver4) {
                System.out.println("Observer called " + obsNo++);
                SampleRegionObserver4 ro1 = (SampleRegionObserver4) obj;
                postOpen = postOpen + ro1.getTotalPostOpenCount();
                postClose = postClose + ro1.getTotalPostCloseCount();
                preClose = preClose + ro1.getTotalPreCloseCount();
                // assertEquals(numOfEntries,prePut);
                // assertEquals(numOfEntries,postPut);
              }
            }
          }
        }
        return postOpen + ":" + preClose + ":" + postClose;
      }
    });
  }

  private void doCreateTableAndBasicPutGet(final boolean order) throws Exception {
    doPutOperationFromClient(vm3, getLocatorPort(), order);
    verifyObserverEventsCount();
  }

  private void verifyObserverEventsCount() {
    int preClose = 0;
    int postClose = 0;
    int postOpen = 0;
    String counts = (String) verifyObserverEventsonOnServer(vm0);
    postOpen = postOpen + Integer.parseInt(counts.split(":")[0]);
    preClose = preClose + Integer.parseInt(counts.split(":")[1]);
    postClose = postClose + Integer.parseInt(counts.split(":")[2]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm1);
    postOpen = postOpen + Integer.parseInt(counts.split(":")[0]);
    preClose = preClose + Integer.parseInt(counts.split(":")[1]);
    postClose = postClose + Integer.parseInt(counts.split(":")[2]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm2);
    postOpen = postOpen + Integer.parseInt(counts.split(":")[0]);
    preClose = preClose + Integer.parseInt(counts.split(":")[1]);
    postClose = postClose + Integer.parseInt(counts.split(":")[2]);
    int noOfTables = 1;
    // assertEquals(noOfTables,preClose);
    // following event are called per geode server and currently no of geode servers are
    // 3
    // assertEquals(noOfTables*3,postClose);
    assertEquals(noOfTables * 3, postOpen);
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
  public void testDoObserverTestForRegionEvents() throws Exception {
    doCreateTableAndBasicPutGet(true);
    // NOTE : Not possible to test delete events as region observers are removed from as
    // region is destroyed
    deleteTable();
  }


  /**
   * Tests observer for put operation on UnOrdered Table
   *
   * @throws Exception
   */
  @Test
  public void testDoObserverTestForRegionEventsUnOrderedTable() throws Exception {
    doCreateTableAndBasicPutGet(false);
    // NOTE : Not possible to test delete events as region observers are removed from as
    // region is destroyed
    deleteTable();

  }

}
