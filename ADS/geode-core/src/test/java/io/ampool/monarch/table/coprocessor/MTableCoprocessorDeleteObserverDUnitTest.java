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
public class MTableCoprocessorDeleteObserverDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();

  final int numOfEntries = 100;
  private String tableName = "EmployeeTable";

  public MTableCoprocessorDeleteObserverDUnitTest() {
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

  private void doPutOperationFromClient(VM vm, final int locatorPort, final boolean order,
      int numVersions) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          MTableDescriptor tableDescriptor = new MTableDescriptor();
          // Add Coprocessor
          tableDescriptor
              .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleDeleteObserver");

          tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
              .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
          /*
           * if (order == false) { tableDescriptor.setTableType(MTableType.UNORDERED); }
           */
          tableDescriptor.setRedundantCopies(1);
          tableDescriptor.setMaxVersions(numVersions);

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

          for (int i = 0; i < numOfEntries; i++) {
            String key1 = "RowKey" + i;
            Delete mydel1 = new Delete(Bytes.toBytes(key1));
            mtable.delete(mydel1);
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
        int preDelete = 0;
        int postDelete = 0;

        for (CacheListener cl : cls) {
          if (cl instanceof PostOpRegionObserver) {
            PostOpRegionObserver poro = (PostOpRegionObserver) cl;
            int obsNo = 1;
            for (MTableObserver obj : poro.getmObserverInstanceList(tableName)) {
              if (obj instanceof SampleDeleteObserver) {
                SampleDeleteObserver ro1 = (SampleDeleteObserver) obj;
                preDelete = preDelete + ro1.getTotalPreDeleteCount();
                postDelete = postDelete + ro1.getTotalPostDeleteCount();
              }
            }
          }
        }
        return preDelete + ":" + postDelete;
      }
    });
  }

  private void doCreateTableAndBasicPutGet(final boolean order, int numVersion) throws Exception {
    doPutOperationFromClient(vm3, getLocatorPort(), order, numVersion);
    verifyObserverEventsCount();
  }

  private void verifyObserverEventsCount() {
    int preDel = 0;
    int postDel = 0;
    String counts = (String) verifyObserverEventsonOnServer(vm0);
    preDel = preDel + Integer.parseInt(counts.split(":")[0]);
    postDel = postDel + Integer.parseInt(counts.split(":")[1]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm1);
    preDel = preDel + Integer.parseInt(counts.split(":")[0]);
    postDel = postDel + Integer.parseInt(counts.split(":")[1]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm2);
    preDel = preDel + Integer.parseInt(counts.split(":")[0]);
    postDel = postDel + Integer.parseInt(counts.split(":")[1]);
    System.out.println("Counts: " + counts);
    assertEquals(numOfEntries, preDel);
    assertEquals(numOfEntries, postDel);
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
    doCreateTableAndBasicPutGet(true, 1);
    deleteTable();

    // doCreateTableAndBasicPutGet(false);
    // deleteTable();
  }

  /**
   * Tests observer for put operation
   *
   * @throws Exception
   */
  @Test
  public void testDoObserverMultiversionTest() throws Exception {
    doCreateTableAndBasicPutGet(true, 5);
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
