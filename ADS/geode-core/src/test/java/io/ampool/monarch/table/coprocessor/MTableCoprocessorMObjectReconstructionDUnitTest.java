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
public class MTableCoprocessorMObjectReconstructionDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();
  public static final String ROW_KEY_PREFIX = "RowKey";

  public static final int numOfEntries = 10;
  private String tableName = "EmployeeTable";
  private String observer =
      "io.ampool.monarch.table.coprocessor.SampleObserverForObjectReconstruction";

  public MTableCoprocessorMObjectReconstructionDUnitTest() {
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
      boolean partialDelete, boolean isCheckAndDel, int versions) {
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
          tableDescriptor.setMaxVersions(versions);

          MClientCache clientCache = MClientCacheFactory.getAnyInstance();
          Admin admin = clientCache.getAdmin();
          MTable mtable = admin.createTable(tableName, tableDescriptor);
          assertEquals(mtable.getName(), tableName);
          if (!partialDelete) {
            for (int i = 0; i < numOfEntries; i++) {
              String key1 = ROW_KEY_PREFIX + i;
              Put myput1 = new Put(Bytes.toBytes(key1));
              myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
              myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
              myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
              myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
              mtable.put(myput1);
            }

            for (int i = 0; i < numOfEntries; i++) {
              String key1 = ROW_KEY_PREFIX + i;
              Delete mydel1 = new Delete(Bytes.toBytes(key1));
              if (isCheckAndDel) {
                mtable.checkAndDelete(Bytes.toBytes(key1), Bytes.toBytes("ID"),
                    Bytes.toBytes(i + 10), mydel1);
              } else
                mtable.delete(mydel1);
            }
            for (int i = 0; i < numOfEntries; i++) {
              String key1 = ROW_KEY_PREFIX + i;
              Put myput1 = new Put(Bytes.toBytes(key1));
              myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
              myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
              myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
              myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
              mtable.put(myput1);
            }
            // loop to check failure checkAndDelete events
            for (int i = 0; i < numOfEntries; i++) {
              String key1 = ROW_KEY_PREFIX + i;
              Delete mydel1 = new Delete(Bytes.toBytes(key1));
              if (isCheckAndDel) {
                mtable.checkAndDelete(Bytes.toBytes(key1), Bytes.toBytes("ID"),
                    Bytes.toBytes(i + 20), mydel1);
              }
            }

          } else {
            // System.out.println("Particular column delete....");
            for (int i = 0; i < numOfEntries; i++) {
              String key1 = ROW_KEY_PREFIX + i;
              Put myput1 = new Put(Bytes.toBytes(key1));
              myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
              myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
              myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
              myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
              mtable.put(myput1);
            }

            for (int i = 0; i < numOfEntries; i++) {
              String key1 = ROW_KEY_PREFIX + i;
              Delete mydel1 = new Delete(Bytes.toBytes(key1));
              mydel1.addColumn(Bytes.toBytes("NAME"));
              // mydel1.addColumn(Bytes.toBytes("ID"));
              if (isCheckAndDel) {
                mtable.checkAndDelete(Bytes.toBytes(key1), Bytes.toBytes("ID"),
                    Bytes.toBytes(i + 10), mydel1);
              } else
                mtable.delete(mydel1);
            }

            for (int i = 0; i < numOfEntries; i++) {
              String key1 = ROW_KEY_PREFIX + i;
              Put myput1 = new Put(Bytes.toBytes(key1));
              myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
              myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
              myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
              myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
              mtable.put(myput1);
            }
            // CHeckAndDel failure testing loop
            for (int i = 0; i < numOfEntries; i++) {
              String key1 = ROW_KEY_PREFIX + i;
              Delete mydel1 = new Delete(Bytes.toBytes(key1));
              mydel1.addColumn(Bytes.toBytes("NAME"));
              // mydel1.addColumn(Bytes.toBytes("ID"));
              if (isCheckAndDel) {
                mtable.checkAndDelete(Bytes.toBytes(key1), Bytes.toBytes("ID"),
                    Bytes.toBytes(i + 20), mydel1);
              }
            }
          }
        } catch (CacheClosedException cce) {
        }
        return null;
      }
    });

  }

  private void doPutOperationFromClient(VM vm, final int locatorPort, final boolean order,
      boolean isCheckAndPut, int versions) {
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
          tableDescriptor.setMaxVersions(versions);

          MClientCache clientCache = MClientCacheFactory.getAnyInstance();
          Admin admin = clientCache.getAdmin();
          MTable mtable = admin.createTable(tableName, tableDescriptor);
          assertEquals(mtable.getName(), tableName);
          for (int i = 0; i < numOfEntries; i++) {
            String key1 = ROW_KEY_PREFIX + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
            mtable.put(myput1);
          }

          if (isCheckAndPut) {
            // System.out.println("-------------------------------------------------------------------------------------");
            for (int i = 0; i < numOfEntries; i++) {
              String key1 = ROW_KEY_PREFIX + i;
              Put myput1 = new Put(Bytes.toBytes(key1));
              myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Deepak" + i));
              // myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 20));
              // myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 20));
              mtable.checkAndPut(Bytes.toBytes(key1), Bytes.toBytes("NAME"),
                  Bytes.toBytes("Nilkanth" + i), myput1);
            }
            for (int i = 0; i < numOfEntries; i++) {
              String key1 = ROW_KEY_PREFIX + i;
              Put myput1 = new Put(Bytes.toBytes(key1));
              myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Deepak" + i));
              // myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 20));
              // myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 20));
              mtable.checkAndPut(Bytes.toBytes(key1), Bytes.toBytes("NAME"),
                  Bytes.toBytes("Nilkanth" + i), myput1);
            }
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
        int preCheckAndDelete = 0;
        int postCheckAndDelete = 0;

        for (CacheListener cl : cls) {
          if (cl instanceof PostOpRegionObserver) {
            PostOpRegionObserver poro = (PostOpRegionObserver) cl;
            int obsNo = 1;
            for (MTableObserver obj : poro.getmObserverInstanceList(tableName)) {
              if (obj instanceof SampleObserverForObjectReconstruction) {
                SampleObserverForObjectReconstruction ro1 =
                    (SampleObserverForObjectReconstruction) obj;
                preDelete = preDelete + ro1.getTotalPreDeleteCount();
                postDelete = postDelete + ro1.getTotalPostDeleteCount();
                preCheckAndDelete = preCheckAndDelete + ro1.getTotalPreCheckAndDeleteCount();
                postCheckAndDelete = postCheckAndDelete + ro1.getTotalPostCheckAndDeleteCount();
              }
            }
          }
        }
        return preDelete + ":" + postDelete + ":" + preCheckAndDelete + ":" + postCheckAndDelete;
      }
    });
  }

  private Object verifyObserverEventsonOnServerPut(VM vm) {
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
        int preCheckAndPut = 0;
        int postCheckAndPut = 0;

        for (CacheListener cl : cls) {
          if (cl instanceof PostOpRegionObserver) {
            PostOpRegionObserver poro = (PostOpRegionObserver) cl;
            int obsNo = 1;
            for (MTableObserver obj : poro.getmObserverInstanceList(tableName)) {
              if (obj instanceof SampleObserverForObjectReconstruction) {
                SampleObserverForObjectReconstruction ro1 =
                    (SampleObserverForObjectReconstruction) obj;
                prePut = prePut + ro1.getTotalPrePutCount();
                postPut = postPut + ro1.getTotalPostPutCount();
                preCheckAndPut = preCheckAndPut + ro1.getTotalPreCheckAndPutCount();
                postCheckAndPut = postCheckAndPut + ro1.getTotalPostCheckAndPutCount();
              }
            }
          }
        }
        return prePut + ":" + postPut + ":" + preCheckAndPut + ":" + postCheckAndPut;
      }
    });
  }

  private void doCreateTableAndCompleteRowDelete(final boolean order, final boolean isCheckAndDel,
      int versions) throws Exception {
    doPutOperationFromClient(vm3, getLocatorPort(), order, false, isCheckAndDel, versions);
    if (isCheckAndDel)
      verifyObserverCheckAndDelEventsCount(false);
    else
      verifyObserverEventsCount();
    deleteTable();
  }

  private void doCreateTableAndPartialRowDelete(final boolean order, final boolean isCheckAndDel,
      int versions) throws Exception {
    doPutOperationFromClient(vm3, getLocatorPort(), order, true, isCheckAndDel, versions);
    if (isCheckAndDel)
      verifyObserverCheckAndDelEventsCount(true);
    else
      verifyObserverEventsCount();
    deleteTable();
  }

  private void doCreateTableAndPut(final boolean order, boolean isCheckAndPut, int versions)
      throws Exception {
    doPutOperationFromClient(vm3, getLocatorPort(), order, isCheckAndPut, versions);
    if (!isCheckAndPut)
      verifyObserverEventsCountForPut();
    else
      verifyObserverCheckAndPutEventsCount(false);
    deleteTable();
  }

  private void verifyObserverEventsCount() {
    int preDel = 0;
    int postDel = 0;
    String counts = (String) verifyObserverEventsonOnServer(vm0);
    preDel = preDel + Integer.parseInt(counts.split(":")[0]);
    postDel = postDel + Integer.parseInt(counts.split(":")[1]);
    // System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm1);
    preDel = preDel + Integer.parseInt(counts.split(":")[0]);
    postDel = postDel + Integer.parseInt(counts.split(":")[1]);
    // System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm2);
    preDel = preDel + Integer.parseInt(counts.split(":")[0]);
    postDel = postDel + Integer.parseInt(counts.split(":")[1]);
    // System.out.println("Counts: " + counts);
    assertEquals(numOfEntries, preDel);
    assertEquals(numOfEntries, postDel);
  }

  private void verifyObserverEventsCountForPut() {
    int preDel = 0;
    int postDel = 0;
    String counts = (String) verifyObserverEventsonOnServerPut(vm0);
    preDel = preDel + Integer.parseInt(counts.split(":")[0]);
    postDel = postDel + Integer.parseInt(counts.split(":")[1]);
    // System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServerPut(vm1);
    preDel = preDel + Integer.parseInt(counts.split(":")[0]);
    postDel = postDel + Integer.parseInt(counts.split(":")[1]);
    // System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServerPut(vm2);
    preDel = preDel + Integer.parseInt(counts.split(":")[0]);
    postDel = postDel + Integer.parseInt(counts.split(":")[1]);
    // System.out.println("Counts: " + counts);
    assertEquals(numOfEntries, preDel);
    assertEquals(numOfEntries, postDel);
  }

  private void verifyObserverCheckAndDelEventsCount(boolean isPartial) {
    int preCheckAndDel = 0;
    int postCheckAndDel = 0;
    String counts = (String) verifyObserverEventsonOnServer(vm0);
    preCheckAndDel = preCheckAndDel + Integer.parseInt(counts.split(":")[2]);
    postCheckAndDel = postCheckAndDel + Integer.parseInt(counts.split(":")[3]);
    // System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm1);
    preCheckAndDel = preCheckAndDel + Integer.parseInt(counts.split(":")[2]);
    postCheckAndDel = postCheckAndDel + Integer.parseInt(counts.split(":")[3]);
    // System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm2);
    preCheckAndDel = preCheckAndDel + Integer.parseInt(counts.split(":")[2]);
    postCheckAndDel = postCheckAndDel + Integer.parseInt(counts.split(":")[3]);
    // System.out.println("Counts: " + counts);
    // multiplied by 2 as Same set is repeated for succesfull checkAndDelete and un-successfull
    // checkAndDelete

    assertEquals(numOfEntries * 2, preCheckAndDel);
    // if a check and delete does not execute due to the check failing
    // we do not get a post delete event, so for partial success is
    // same as for full success.
    if (isPartial) {
      assertEquals(numOfEntries, postCheckAndDel);
    } else {
      assertEquals(numOfEntries, postCheckAndDel);
    }
  }

  private void verifyObserverCheckAndPutEventsCount(boolean isPartial) {
    int preCheckAndPut = 0;
    int postCheckAndPut = 0;
    String counts = (String) verifyObserverEventsonOnServerPut(vm0);
    preCheckAndPut = preCheckAndPut + Integer.parseInt(counts.split(":")[2]);
    postCheckAndPut = postCheckAndPut + Integer.parseInt(counts.split(":")[3]);
    // System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServerPut(vm1);
    preCheckAndPut = preCheckAndPut + Integer.parseInt(counts.split(":")[2]);
    postCheckAndPut = postCheckAndPut + Integer.parseInt(counts.split(":")[3]);
    // System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServerPut(vm2);
    preCheckAndPut = preCheckAndPut + Integer.parseInt(counts.split(":")[2]);
    postCheckAndPut = postCheckAndPut + Integer.parseInt(counts.split(":")[3]);
    // System.out.println("Counts: " + counts);
    // multiplied by 2 as Same set is repeated for succesfull checkAndPut and un-successfull
    // checkAndDelete

    assertEquals(numOfEntries * 2, preCheckAndPut);
    assertEquals(numOfEntries, postCheckAndPut);
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
  public void testObserverPartialDelete() throws Exception {
    doCreateTableAndPartialRowDelete(true, false, 1);
    doCreateTableAndPartialRowDelete(true, false, 3);
  }

  @Test
  public void testObserverCompleteRowDelete() throws Exception {
    doCreateTableAndCompleteRowDelete(true, false, 1);
    doCreateTableAndCompleteRowDelete(true, false, 3);
  }

  @Test
  public void testObserverPartialCheckAndDelete() throws Exception {
    doCreateTableAndPartialRowDelete(true, true, 1);
    doCreateTableAndPartialRowDelete(true, true, 3);
  }

  @Test
  public void testObserverCompleteRowCheckAndDeleteDelete() throws Exception {
    doCreateTableAndCompleteRowDelete(true, true, 1);
    doCreateTableAndCompleteRowDelete(true, true, 3);
  }

  @Test
  public void testObserverPut() throws Exception {
    doCreateTableAndPut(true, false, 1);
    doCreateTableAndPut(true, false, 3);
  }

  @Test
  public void testObserverCheckAndPut() throws Exception {
    doCreateTableAndPut(true, true, 1);
    doCreateTableAndPut(true, true, 3);
  }

  /*
   * public void testObserverEvents() throws Exception { doCreateTableAndBasicPutGet(true);
   * verifyObserverEventsonOnServer(vm0); verifyObserverEventsonOnServer(vm1);
   * verifyObserverEventsonOnServer(vm2);
   * 
   * deleteTable(); }
   */
}
