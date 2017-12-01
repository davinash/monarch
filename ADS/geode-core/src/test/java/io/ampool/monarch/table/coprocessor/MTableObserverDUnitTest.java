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
import static org.junit.Assert.assertNotNull;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
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
 * Test to verify that value is not altered before events are triggered in case of pre' events
 * 
 */

@Category(MonarchTest.class)
public class MTableObserverDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();
  public static boolean beforeRegionDestroyCalled = false;

  public static final int NUM_OF_ENTRIES = 10;
  private String tableName = "EmployeeTable";

  public MTableObserverDUnitTest() {
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

  private void createTable(VM vm, final int locatorPort, final boolean order) {
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
              .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRegionObserver5");

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
        int prePut = 0;
        int postPut = 0;

        for (CacheListener cl : cls) {
          if (cl instanceof PostOpRegionObserver) {
            PostOpRegionObserver poro = (PostOpRegionObserver) cl;
            int obsNo = 1;
            for (MTableObserver obj : poro.getmObserverInstanceList(tableName)) {
              if (obj instanceof SampleRegionObserver5) {
                System.out.println("Observer called " + obsNo++);
                SampleRegionObserver5 ro1 = (SampleRegionObserver5) obj;
                prePut = prePut + ro1.getTotalPrePutCount();
                postPut = postPut + ro1.getTotalPostPutCount();
                // assertEquals(NUM_OF_ENTRIES,prePut);
                // assertEquals(NUM_OF_ENTRIES,postPut);
              }
            }
          }
        }
        return prePut + ":" + postPut;
      }
    });
  }

  private void doCreateTableAndBasicPutGet(final boolean order) throws Exception {
    createTable(vm3, getLocatorPort(), order);
    putRecords(NUM_OF_ENTRIES, "");
    verifyObserverEventsCount();
    putRecords(NUM_OF_ENTRIES, "_NEW");

  }

  private void putRecords(int entries, String valueSuffix) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable mTable = clientCache.getMTable(tableName);

    for (int i = 0; i < entries; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("NAME" + i + valueSuffix));
      put.addColumn(Bytes.toBytes("ID"), Bytes.toBytes("ID" + i + valueSuffix));
      put.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes("AGE" + i + valueSuffix));
      put.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes("SALARY" + i + valueSuffix));
      mTable.put(put);
    }
  }

  private void verifyObserverEventsCount() {
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
    int noOfTables = 1;
    // assertEquals(noOfTables,preClose);
    // following event are called per geode server and currently no of geode servers are
    // 3
    // assertEquals(noOfTables*3,postClose);
    System.out
        .println("MTableObserverDUnitTest.verifyObserverEventsCount :: 180 prePuts " + prePut);
    System.out
        .println("MTableObserverDUnitTest.verifyObserverEventsCount :: 181 postPuts " + postPut);
    assertEquals(noOfTables * NUM_OF_ENTRIES, prePut);
    assertEquals(noOfTables * NUM_OF_ENTRIES, postPut);
  }

  private void deleteTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable mtable = clientCache.getTable(tableName);
    clientCache.getAdmin().deleteTable(tableName);
  }

  /**
   * Tests observer for put operation on Ordered Table
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
   */
  @Test
  public void testDoObserverTestForRegionEventsUnOrderedTable() throws Exception {
    doCreateTableAndBasicPutGet(false);
    // NOTE : Not possible to test delete events as region observers are removed from as
    // region is destroyed
    deleteTable();

  }

}
