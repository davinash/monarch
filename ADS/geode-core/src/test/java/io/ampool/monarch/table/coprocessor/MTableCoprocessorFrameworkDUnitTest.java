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

import io.ampool.internal.AmpoolOpType;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.*;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.internal.MCoprocessorUtils;
import io.ampool.monarch.table.coprocessor.internal.PostOpRegionObserver;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.*;

/**
 * MTableCoprocessorFrameworkDUnitTest
 *
 * @since 0.2.0.0
 */

@Category(MonarchTest.class)
public class MTableCoprocessorFrameworkDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();

  final int numOfEntries = 10;
  final int ADMIN_ROW_COUNT = numOfEntries / 2;

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "Employee_Table_1_2";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 2;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";

  public MTableCoprocessorFrameworkDUnitTest() {
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
          // 1. create tableDescriptor and table.
          MTableDescriptor tableDescriptor = new MTableDescriptor();
          // Add Coprocessor
          tableDescriptor
              .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRegionObserver1");
          tableDescriptor
              .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRegionObserver2");
          tableDescriptor
              .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRegionObserver3");

          tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
              .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

          tableDescriptor.setRedundantCopies(1);

          MClientCache clientCache = MClientCacheFactory.getAnyInstance();
          Admin admin = clientCache.getAdmin();
          MTable mtable = admin.createTable(TABLE_NAME, tableDescriptor);
          assertEquals(mtable.getName(), TABLE_NAME);
          String key1 = null;
          // 2. put entries into table
          for (int i = 0; i < numOfEntries; i++) {
            key1 = "RowKey" + i;

            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }

          // 3. Get records from the table.
          for (int i = 0; i < numOfEntries; i++) {
            key1 = "RowKey" + i;
            Get get = new Get(Bytes.toBytes(key1));
            // myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
            // myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            // myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            // myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            Row result = mtable.get(get);

            assertNotNull(result);

            List<Cell> row = result.getCells();
            Iterator<MColumnDescriptor> iteratorColumnDescriptor =
                mtable.getTableDescriptor().getAllColumnDescriptors().iterator();

            for (Cell cell : row) {
              // byte[] expectedColumnName = iteratorColumnDescriptor.next().getColumnName();
              // byte[] expectedColumnValue = (byte[]) myput.getColumnValueMap().get(new
              // MTableKey(expectedColumnName));
              // if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
              // Assert.fail("Invalid Values for Column Name");
              // }
              // if (!Bytes.equals(expectedColumnValue, (byte[]) cell.getColumnValue())) {
              // System.out.println("expectedColumnValue => " +
              // Arrays.toString(expectedColumnValue));
              System.out.println(
                  "actuaColumnValue    =>  " + Arrays.toString((byte[]) cell.getColumnValue()));
              // Assert.fail("Invalid Values for Column Value");
              // }
            }

          }

          // }

          // 4. Delete rows from table.
          for (int i = 0; i < numOfEntries; i++) {
            String key2 = "RowKey" + i;

            Delete mydel = new Delete(Bytes.toBytes(key2));
            // myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
            // myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            // myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            // myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.delete(mydel);
          }

          /*
           * for (int i = 0; i < numOfEntries; i++) {
           * 
           * String key1 = "RowKey" + i; MPut myput = new MPut(Bytes.toBytes(key1));
           * myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
           * myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
           * myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
           * myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
           * 
           * MGet myget = new MGet(Bytes.toBytes("RowKey" + i)); MResult result = mtable.get(myget);
           * assertFalse(result.isEmpty());
           * 
           * List<MCell> row = result.getCells();
           * 
           * Iterator<MColumnDescriptor> iteratorColumnDescriptor =
           * mtable.getTableDescriptor().getAllColumnDescriptors().iterator();
           * 
           * for (MCell cell : row) { byte[] expectedColumnName =
           * iteratorColumnDescriptor.next().getColumnName(); byte[] expectedColumnValue = (byte[])
           * myput.getColumnValueMap().get(new MTableKey(expectedColumnName)); if
           * (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
           * Assert.fail("Invalid Values for Column Name"); } if (!Bytes.equals(expectedColumnValue,
           * (byte[]) cell.getColumnValue())) { System.out.println("expectedColumnValue =>  " +
           * Arrays.toString(expectedColumnValue)); System.out.println("actuaColumnValue    =>  " +
           * Arrays.toString((byte[]) cell.getColumnValue()));
           * Assert.fail("Invalid Values for Column Value"); } } }
           */
        } catch (CacheClosedException cce) {
        }
        return null;
      }
    });

  }

  private void doPutOperationFromClientForByPassWithBulkOps(VM vm, final int locatorPort,
      final boolean order) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          // 1. create tableDescriptor and table.
          MTableDescriptor tableDescriptor = new MTableDescriptor();
          // Add Coprocessors to table.
          tableDescriptor
              .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRegionObserver1");
          tableDescriptor
              .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRegionObserver2");
          tableDescriptor
              .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRegionObserver3");

          tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
              .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

          tableDescriptor.setRedundantCopies(1);

          MClientCache clientCache = MClientCacheFactory.getAnyInstance();
          Admin admin = clientCache.getAdmin();
          MTable mtable = admin.createTable(TABLE_NAME, tableDescriptor);
          assertEquals(mtable.getName(), TABLE_NAME);
          String key1 = null;


          // 2. bulk put entries into table
          List<Put> puts = new ArrayList<>();
          for (int i = 0; i < numOfEntries; i++) {
            key1 = "RowKey" + i;
            Put record = new Put(Bytes.toBytes(key1));
            record.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
            record.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            record.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            record.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
            puts.add(record);
          }
          mtable.put(puts);
          System.out
              .println("MTableCoprocessorFrameworkDUnitTest.call Bulk put successfully done!");

          // 3. bulk Get records from the table.
          List<Get> getList = new ArrayList<>();
          for (int i = 0; i < numOfEntries; i++) {
            key1 = "RowKey" + i;
            getList.add(new Get(Bytes.toBytes(key1)));
          }

          Row[] batchGetResult = mtable.get(getList);
          assertNotNull(batchGetResult);
          // verify result size
          assertEquals(numOfEntries, batchGetResult.length);
          System.out
              .println("MTableCoprocessorFrameworkDUnitTest.call Bulk Get successfully done!");

          /*
           * Uncomment when bi=ulk delete is supported. //4. Delete rows from table. List<MDelete>
           * deleteList = new ArrayList<>(); for (int i = 0; i < numOfEntries; i++) { key1 =
           * "RowKey" + i; deleteList.add(new MDelete(Bytes.toBytes(key1))); }
           * mtable.delete(deleteList); System.out.
           * println("MTableCoprocessorFrameworkDUnitTest.call Bulk delete successfully done!");
           */
        } catch (CacheClosedException cce) {
        }
        return null;
      }
    });
  }

  private void doPutOperationFromClientForByPass(VM vm, final int locatorPort,
      final boolean order) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          // 1. create tableDescriptor and table.
          MTableDescriptor tableDescriptor = new MTableDescriptor();
          // Add Coprocessor
          tableDescriptor
              .addCoprocessor("io.ampool.monarch.table.coprocessor.TableObserverExample");


          tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
              .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

          tableDescriptor.setRedundantCopies(1);

          MClientCache clientCache = MClientCacheFactory.getAnyInstance();
          Admin admin = clientCache.getAdmin();
          MTable mtable = admin.createTable(TABLE_NAME, tableDescriptor);
          assertEquals(mtable.getName(), TABLE_NAME);
          String key1 = null;
          // 2. put entries into table
          for (int i = 0; i < numOfEntries; i++) {
            key1 = "RowKey" + i;
            if (i == ADMIN_ROW_COUNT) {
              key1 = "admin";
            }
            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }

          // 3. Get records from the table.
          for (int i = 0; i < numOfEntries; i++) {
            key1 = "RowKey" + i;
            if (i == ADMIN_ROW_COUNT) {
              key1 = "admin";
            }
            Get get = new Get(Bytes.toBytes(key1));
            // myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
            // myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            // myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            // myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));


            Row result = mtable.get(get);

            assertNotNull(result);

            List<Cell> row = result.getCells();
            Iterator<MColumnDescriptor> iteratorColumnDescriptor =
                mtable.getTableDescriptor().getAllColumnDescriptors().iterator();

            for (Cell cell : row) {
              // byte[] expectedColumnName = iteratorColumnDescriptor.next().getColumnName();
              // byte[] expectedColumnValue = (byte[]) myput.getColumnValueMap().get(new
              // MTableKey(expectedColumnName));
              // if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
              // Assert.fail("Invalid Values for Column Name");
              // }
              // if (!Bytes.equals(expectedColumnValue, (byte[]) cell.getColumnValue())) {
              // System.out.println("expectedColumnValue => " +
              // Arrays.toString(expectedColumnValue));
              System.out.println(
                  "actuaColumnValue    =>  " + Arrays.toString((byte[]) cell.getColumnValue()));
              // Assert.fail("Invalid Values for Column Value");
              // }
            }

          }

          // }

          // 4. Delete rows from table.
          for (int i = 0; i < numOfEntries; i++) {
            String key2 = "RowKey" + i;
            if (i == ADMIN_ROW_COUNT) {
              key2 = "admin";
            }
            Delete mydel = new Delete(Bytes.toBytes(key2));
            // myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
            // myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            // myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            // myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.delete(mydel);
          }

          /*
           * for (int i = 0; i < numOfEntries; i++) {
           * 
           * String key1 = "RowKey" + i; MPut myput = new MPut(Bytes.toBytes(key1));
           * myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
           * myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
           * myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
           * myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
           * 
           * MGet myget = new MGet(Bytes.toBytes("RowKey" + i)); MResult result = mtable.get(myget);
           * assertFalse(result.isEmpty());
           * 
           * List<MCell> row = result.getCells();
           * 
           * Iterator<MColumnDescriptor> iteratorColumnDescriptor =
           * mtable.getTableDescriptor().getAllColumnDescriptors().iterator();
           * 
           * for (MCell cell : row) { byte[] expectedColumnName =
           * iteratorColumnDescriptor.next().getColumnName(); byte[] expectedColumnValue = (byte[])
           * myput.getColumnValueMap().get(new MTableKey(expectedColumnName)); if
           * (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
           * Assert.fail("Invalid Values for Column Name"); } if (!Bytes.equals(expectedColumnValue,
           * (byte[]) cell.getColumnValue())) { System.out.println("expectedColumnValue =>  " +
           * Arrays.toString(expectedColumnValue)); System.out.println("actuaColumnValue    =>  " +
           * Arrays.toString((byte[]) cell.getColumnValue()));
           * Assert.fail("Invalid Values for Column Value"); } } }
           */
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
        try {

          // Region
          MCache c = MCacheFactory.getAnyInstance();
          Region r = c.getRegion(TABLE_NAME);
          assertNotNull(r);
          CacheListener[] cls = ((PartitionedRegion) r).getCacheListeners();
          int prePut = 0;
          int preDelete = 0;
          int postPut = 0;
          int postDelete = 0;
          int preGet = 0;
          int postGet = 0;

          for (CacheListener cl : cls) {
            System.out.println("CCCCLLLL CacheLister attached = " + cl.getClass().getName());
            if (cl instanceof PostOpRegionObserver) {
              PostOpRegionObserver poro = (PostOpRegionObserver) cl;
              for (MTableObserver obj : poro.getmObserverInstanceList(TABLE_NAME)) {
                if (obj.toString()
                    .equals("io.ampool.monarch.table.coprocessor.SampleRegionObserver1")) {
                  SampleRegionObserver1 ro1 = (SampleRegionObserver1) obj;
                  System.out.println("SRO1 prePut: " + ro1.getTotalPrePutCount());
                  prePut = prePut + ro1.getTotalPrePutCount();
                  preDelete = preDelete + ro1.getTotalPreDeleteCount();
                  postPut = postPut + ro1.getTotalPostPutCount();
                  postDelete = postDelete + ro1.getTotalPostDeleteCount();
                  preGet = preGet + ro1.getTotalPreGetCount();
                  postGet = postGet + ro1.getTotalPostGetCount();

                } else if (obj.toString()
                    .equals("io.ampool.monarch.table.coprocessor.SampleRegionObserver2")) {
                  SampleRegionObserver2 ro2 = (SampleRegionObserver2) obj;
                  System.out.println("SRO2 prePut: " + ro2.getTotalPrePutCount());
                  prePut = prePut + ro2.getTotalPrePutCount();
                  preDelete = preDelete + ro2.getTotalPreDeleteCount();
                  postPut = postPut + ro2.getTotalPostPutCount();
                  postDelete = postDelete + ro2.getTotalPostDeleteCount();
                  preGet = preGet + ro2.getTotalPreGetCount();
                  postGet = postGet + ro2.getTotalPostGetCount();
                } else if (obj.toString()
                    .equals("io.ampool.monarch.table.coprocessor.SampleRegionObserver3")) {
                  SampleRegionObserver3 ro3 = (SampleRegionObserver3) obj;
                  System.out.println("SRO3 prePut: " + ro3.getTotalPrePutCount());
                  prePut = prePut + ro3.getTotalPrePutCount();
                  preDelete = preDelete + ro3.getTotalPreDeleteCount();
                  postPut = postPut + ro3.getTotalPostPutCount();
                  postDelete = postDelete + ro3.getTotalPostDeleteCount();
                  preGet = preGet + ro3.getTotalPreGetCount();
                  postGet = postGet + ro3.getTotalPostGetCount();
                } else if (obj.toString()
                    .equals("io.ampool.monarch.table.coprocessor.TableObserverExample")) {
                  TableObserverExample to = (TableObserverExample) obj;
                  System.out.println("TableObserverExample prePut: " + to.getTotalPrePutCount());
                  prePut = prePut + to.getTotalPrePutCount();
                  preDelete = preDelete + to.getTotalPreDeleteCount();
                  postPut = postPut + to.getTotalPostPutCount();
                  postDelete = postDelete + to.getTotalPostDeleteCount();
                  preGet = preGet + to.getTotalPreGetCount();
                  postGet = postGet + to.getTotalPostGetCount();

                } else {
                  System.out.println("XXXXXXX Invalid Observer CLASS found");
                }
              }

            }
          }
          return prePut + ":" + postPut + ":" + preDelete + ":" + postDelete + ":" + preGet + ":"
              + postGet;
        } catch (GemFireException ge) {
          ge.printStackTrace();
        }
        return null;
      }
    });

  }

  private void verifyObserverEventsCount() {
    int prePut = 0;
    int postPut = 0;
    int preDel = 0;
    int postDel = 0;
    int preGet = 0;
    int postGet = 0;
    String counts = (String) verifyObserverEventsonOnServer(vm0);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    preDel = preDel + Integer.parseInt(counts.split(":")[2]);
    postDel = postDel + Integer.parseInt(counts.split(":")[3]);
    preGet = preGet + Integer.parseInt(counts.split(":")[4]);
    postGet = postGet + Integer.parseInt(counts.split(":")[5]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm1);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    preDel = preDel + Integer.parseInt(counts.split(":")[2]);
    postDel = postDel + Integer.parseInt(counts.split(":")[3]);
    preGet = preGet + Integer.parseInt(counts.split(":")[4]);
    postGet = postGet + Integer.parseInt(counts.split(":")[5]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm2);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    preDel = preDel + Integer.parseInt(counts.split(":")[2]);
    postDel = postDel + Integer.parseInt(counts.split(":")[3]);
    preGet = preGet + Integer.parseInt(counts.split(":")[4]);
    postGet = postGet + Integer.parseInt(counts.split(":")[5]);
    System.out.println("Counts: " + counts);
    int numberOfObservers = 3;
    System.out
        .println("MMMM MTableCoprocessorFrameworkDUnitTest.verifyObserverEventsCount preGet = "
            + preGet + " PostGet = " + postGet);
    assertEquals(numOfEntries * numberOfObservers, prePut);
    assertEquals(numOfEntries * numberOfObservers, postPut);
    assertEquals(numOfEntries * numberOfObservers, preDel);
    assertEquals(numOfEntries * numberOfObservers, postDel);
    assertEquals(numOfEntries * numberOfObservers, preGet);
    assertEquals(numOfEntries * numberOfObservers, postGet);
  }

  private void verifyObserverEventsCountWithBypass() {
    int prePut = 0;
    int postPut = 0;
    int preDel = 0;
    int postDel = 0;
    int preGet = 0;
    int postGet = 0;
    String counts = (String) verifyObserverEventsonOnServer(vm0);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    preDel = preDel + Integer.parseInt(counts.split(":")[2]);
    postDel = postDel + Integer.parseInt(counts.split(":")[3]);
    preGet = preGet + Integer.parseInt(counts.split(":")[4]);
    postGet = postGet + Integer.parseInt(counts.split(":")[5]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm1);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    preDel = preDel + Integer.parseInt(counts.split(":")[2]);
    postDel = postDel + Integer.parseInt(counts.split(":")[3]);
    preGet = preGet + Integer.parseInt(counts.split(":")[4]);
    postGet = postGet + Integer.parseInt(counts.split(":")[5]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm2);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    preDel = preDel + Integer.parseInt(counts.split(":")[2]);
    postDel = postDel + Integer.parseInt(counts.split(":")[3]);
    preGet = preGet + Integer.parseInt(counts.split(":")[4]);
    postGet = postGet + Integer.parseInt(counts.split(":")[5]);
    System.out.println("Counts: " + counts);
    int numberOfObservers = 1;
    System.out
        .println("MMMM MTableCoprocessorFrameworkDUnitTest.verifyObserverEventsCount preGet = "
            + preGet + " PostGet = " + postGet);
    assertEquals(numOfEntries * numberOfObservers, prePut);
    assertEquals(numOfEntries * numberOfObservers, postPut);
    assertEquals(numOfEntries * numberOfObservers, preDel);
    assertEquals(numOfEntries * numberOfObservers, postDel);
    assertEquals(numOfEntries * numberOfObservers, preGet);
    assertEquals((numOfEntries * numberOfObservers) - numberOfObservers, postGet);
  }

  private void verifyObserverEventsCountWithBulkOps() {
    int prePut = 0;
    int postPut = 0;
    int preDel = 0;
    int postDel = 0;
    int preGet = 0;
    int postGet = 0;
    String counts = (String) verifyObserverEventsonOnServer(vm0);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    preDel = preDel + Integer.parseInt(counts.split(":")[2]);
    postDel = postDel + Integer.parseInt(counts.split(":")[3]);
    preGet = preGet + Integer.parseInt(counts.split(":")[4]);
    postGet = postGet + Integer.parseInt(counts.split(":")[5]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm1);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    preDel = preDel + Integer.parseInt(counts.split(":")[2]);
    postDel = postDel + Integer.parseInt(counts.split(":")[3]);
    preGet = preGet + Integer.parseInt(counts.split(":")[4]);
    postGet = postGet + Integer.parseInt(counts.split(":")[5]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm2);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    preDel = preDel + Integer.parseInt(counts.split(":")[2]);
    postDel = postDel + Integer.parseInt(counts.split(":")[3]);
    preGet = preGet + Integer.parseInt(counts.split(":")[4]);
    postGet = postGet + Integer.parseInt(counts.split(":")[5]);
    System.out.println("Counts: " + counts);
    int numberOfObservers = 3;
    System.out
        .println("MMMM MTableCoprocessorFrameworkDUnitTest.verifyObserverEventsCount preGet = "
            + preGet + " PostGet = " + postGet);
    assertEquals(numOfEntries * numberOfObservers, prePut);
    assertEquals(numOfEntries * numberOfObservers, postPut);
    // assertEquals(numOfEntries*numberOfObservers,preDel);
    // assertEquals(numOfEntries*numberOfObservers,postDel);
    // assertEquals(numOfEntries*numberOfObservers,preGet);
    // assertEquals(numOfEntries*numberOfObservers,postGet);
  }


  private void doCreateTableAndBasicPutGet(final boolean order) throws Exception {
    doPutOperationFromClient(vm3, getLocatorPort(), order);
  }

  private void doCreateTableAndBasicPutGetForByPass(final boolean order) throws Exception {
    doPutOperationFromClientForByPass(vm3, getLocatorPort(), order);
  }

  private void doCreateTableAndBasicPutWithBulkOps(final boolean order) throws Exception {
    doPutOperationFromClientForByPassWithBulkOps(vm3, getLocatorPort(), order);
  }

  private void verifyTrxCoprocessorExecution(Map<Integer, List<byte[]>> bucketWiseKeys,
      int noOfBuckets) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable mtable = clientCache.getTable(TABLE_NAME);
    // byte[] rowKey = bucketWiseKeys.get(0).get(0);

    // Do op as a part of transaction-3
    {
      MExecutionRequest request = new MExecutionRequest();
      request.setTransactionId("trx-3");
      Map<Integer, List<Object>> result = mtable.coprocessorService(
          "io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodD", null, null, request);
    }

    // Do op as a part of transaction-1
    {
      MExecutionRequest request = new MExecutionRequest();
      request.setTransactionId("trx-1");
      Map<Integer, List<Object>> result = mtable.coprocessorService(
          "io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodA", null, null, request);
    }

    // Do op as a part of transaction-2
    {
      MExecutionRequest request = new MExecutionRequest();
      request.setTransactionId("trx-2");
      Map<Integer, List<Object>> result = mtable.coprocessorService(
          "io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodB", null, null, request);
    }

    // Do op as a part of transaction-1
    {
      MExecutionRequest request = new MExecutionRequest();
      request.setTransactionId("trx-1");
      Map<Integer, List<Object>> result = mtable.coprocessorService(
          "io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodB", null, null, request);
    }

    // Do op as a part of transaction-3
    {
      MExecutionRequest request = new MExecutionRequest();
      request.setTransactionId("trx-3");
      Map<Integer, List<Object>> result = mtable.coprocessorService(
          "io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodC", null, null, request);
    }

    // Do op as a part of transaction-1
    {
      MExecutionRequest request = new MExecutionRequest();
      request.setMethodName("methodB");
      request.setTransactionId("trx-1");
      Map<Integer, List<Object>> result = mtable.coprocessorService(
          "io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodC", null, null, request);
    }

    // Do op as a part of transaction-2
    {
      MExecutionRequest request = new MExecutionRequest();
      request.setTransactionId("trx-2");
      Map<Integer, List<Object>> result = mtable.coprocessorService(
          "io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodD", null, null, request);
    }

    // Do op as a part of transaction-3
    {
      MExecutionRequest request = new MExecutionRequest();
      request.setTransactionId("trx-3");
      Map<Integer, List<Object>> result = mtable.coprocessorService(
          "io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodB", null, null, request);
    }

    // Do op as a part of transaction-1
    {
      MExecutionRequest request = new MExecutionRequest();
      request.setTransactionId("trx-1");
      Map<Integer, List<Object>> result = mtable.coprocessorService(
          "io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodD", null, null, request);
    }

    // Do op as a part of transaction-3
    {
      MExecutionRequest request = new MExecutionRequest();
      request.setTransactionId("trx-3");
      Map<Integer, List<Object>> result = mtable.coprocessorService(
          "io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodA", null, null, request);
    }

    // commit transaction-2
    {
      MExecutionRequest request = new MExecutionRequest();
      request.setTransactionId("trx-2");
      Map<Integer, List<Object>> bucketTotrxIdStateMap =
          mtable.coprocessorService("io.ampool.monarch.table.coprocessor.TrxRegionEndpoint",
              "commitTransaction", null, null, request);

      // validation
      {
        // Find total # of primary buckets
        Set<Integer> bucketIdSet = getPrimaryBuckets(mtable);
        assertEquals(bucketTotrxIdStateMap.size(), bucketIdSet.size());
      }
      List<String> expectedStateForTrx = new ArrayList<>();
      expectedStateForTrx.add("_methodB_0methodD_0");
      expectedStateForTrx.add("_methodB_1methodD_1");
      expectedStateForTrx.add("_methodB_2methodD_2");
      expectedStateForTrx.add("_methodB_3methodD_3");
      expectedStateForTrx.add("_methodB_4methodD_4");

      System.out.println(
          "MTableCoprocessorFrameworkDUnitTest.verifyTrxCoprocessorExecution Result.size = "
              + bucketTotrxIdStateMap.size());
      for (Map.Entry<Integer, List<Object>> entry : bucketTotrxIdStateMap.entrySet()) {
        // System.out.println("BucketID = " + entry.getKey());
        // System.out.println("MAP Type = "+ entry.getValue().get(0).getClass().getName());
        TrxTransactionState trxState = (TrxTransactionState) entry.getValue().get(0);
        assertTrue(expectedStateForTrx.contains(trxState.toString()));
        // System.out.println("TransactionId = "+ request.getTransactionId());
        // System.out.println("Transaction State = "+ trxState.toString());
        /*
         * for (Map.Entry<String, TrxTransactionState> trxState : trxToStateMap.entrySet()) {
         * System.out.println("XXXX Transaction = " + trxState.getKey());
         * System.out.println("XXXX State = " + trxState.getValue().toString()); }
         */

      }
    }

    // commit transaction-3
    {
      MExecutionRequest request = new MExecutionRequest();
      request.setTransactionId("trx-3");
      Map<Integer, List<Object>> bucketTotrxIdStateMap =
          mtable.coprocessorService("io.ampool.monarch.table.coprocessor.TrxRegionEndpoint",
              "commitTransaction", null, null, request);

      // validation
      List<String> expectedStateForTrx3 = new ArrayList<>();
      expectedStateForTrx3.add("_methodD_0methodC_0methodB_0methodA_0");
      expectedStateForTrx3.add("_methodD_1methodC_1methodB_1methodA_1");
      expectedStateForTrx3.add("_methodD_2methodC_2methodB_2methodA_2");
      expectedStateForTrx3.add("_methodD_3methodC_3methodB_3methodA_3");
      expectedStateForTrx3.add("_methodD_4methodC_4methodB_4methodA_4");

      {
        Set<Integer> bucketIdSet = getPrimaryBuckets(mtable);
        assertEquals(bucketTotrxIdStateMap.size(), bucketIdSet.size());
      }
      // System.out.println("MTableCoprocessorFrameworkDUnitTest.verifyTrxCoprocessorExecution
      // Result.size = "+ bucketTotrxIdStateMap.size());
      for (Map.Entry<Integer, List<Object>> entry : bucketTotrxIdStateMap.entrySet()) {
        // System.out.println("BucketID = " + entry.getKey());
        // System.out.println("MAP Type = "+ entry.getValue().get(0).getClass().getName());
        TrxTransactionState trxState = (TrxTransactionState) entry.getValue().get(0);
        assertTrue(expectedStateForTrx3.contains(trxState.toString()));
        // System.out.println("TransactionId = "+ request.getTransactionId());
        // System.out.println("Transaction State = "+ trxState.toString());
        /*
         * for (Map.Entry<String, TrxTransactionState> trxState : trxToStateMap.entrySet()) {
         * System.out.println("XXXX Transaction = " + trxState.getKey());
         * System.out.println("XXXX State = " + trxState.getValue().toString()); }
         */
      }
    }

    // commit transaction-1
    {
      MExecutionRequest request = new MExecutionRequest();
      request.setTransactionId("trx-1");
      Map<Integer, List<Object>> bucketTotrxIdStateMap =
          mtable.coprocessorService("io.ampool.monarch.table.coprocessor.TrxRegionEndpoint",
              "commitTransaction", null, null, request);

      // validation
      List<String> expectedStateForTrx1 = new ArrayList<>();
      expectedStateForTrx1.add("_methodA_0methodB_0methodC_0methodD_0");
      expectedStateForTrx1.add("_methodA_1methodB_1methodC_1methodD_1");
      expectedStateForTrx1.add("_methodA_2methodB_2methodC_2methodD_2");
      expectedStateForTrx1.add("_methodA_3methodB_3methodC_3methodD_3");
      expectedStateForTrx1.add("_methodA_4methodB_4methodC_4methodD_4");

      {
        Set<Integer> bucketIdSet = getPrimaryBuckets(mtable);
        assertEquals(bucketTotrxIdStateMap.size(), bucketIdSet.size());
      }
      // System.out.println("MTableCoprocessorFrameworkDUnitTest.verifyTrxCoprocessorExecution
      // Result.size = "+ bucketTotrxIdStateMap.size());
      for (Map.Entry<Integer, List<Object>> entry : bucketTotrxIdStateMap.entrySet()) {
        // System.out.println("BucketID = " + entry.getKey());
        // System.out.println("MAP Type = "+ entry.getValue().get(0).getClass().getName());
        TrxTransactionState trxState = (TrxTransactionState) entry.getValue().get(0);
        assertTrue(expectedStateForTrx1.contains(trxState.toString()));
        // System.out.println("TransactionId = "+ request.getTransactionId());
        // System.out.println("Transaction State = "+ trxState.toString());
        /*
         * for (Map.Entry<String, TrxTransactionState> trxState : trxToStateMap.entrySet()) {
         * System.out.println("XXXX Transaction = " + trxState.getKey());
         * System.out.println("XXXX State = " + trxState.getValue().toString()); }
         */
      }
    }
  }

  private Set<Integer> getPrimaryBuckets(MTable mTable) {
    // Find total # of primary buckets
    Map<Integer, ServerLocation> primaryBucketMap = new HashMap<>(113);
    MTableUtils.getLocationMap(mTable, null, primaryBucketMap, null, AmpoolOpType.ANY_OP);
    System.out.println(
        "XXX MTableCoprocessorFrameworkDUnitTest.verifyTrxCoprocessorExecution primaryBucketMap =  "
            + primaryBucketMap.size());
    return primaryBucketMap.keySet();
  }

  private void verifyCoprocessorExecution(Map<Integer, List<byte[]>> bucketWiseKeys) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable mtable = clientCache.getTable(TABLE_NAME);
    byte[] rowKey = bucketWiseKeys.get(0).get(0);

    Integer i1 = new Integer(1000);
    Integer i2 = new Integer(2000);
    Integer i3 = new Integer(3000);

    ArrayList<Integer> list = new ArrayList<Integer>();

    list.add(i1);
    list.add(i2);
    list.add(i3);

    int sum = 0;
    for (Integer i : list) {
      sum = sum + i.intValue();
    }
    int expectedAvgValue = sum / list.size();

    MExecutionRequest execution = new MExecutionRequest();
    execution.setArguments(list);
    // MResultCollector rc = new DefaultResultCollector();
    // execution.setResultCollector(new DefaultResultCollector(););
    System.out.println("Executing non-existing endpoint coprocessor..!");

    try {
      List<Object> error =
          mtable.coprocessorService("io.ampool.monarch.table.coprocessor.NotExistingCoprocessor",
              "computeAverage", rowKey, execution);
    } catch (IllegalArgumentException iae) {
      if (mtable.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
        System.out.println("Expected:: erorr for UnorderedTable " + iae.getMessage());
      } else {
        Assert.fail("Invalid exception caught " + iae.getMessage());
      }

    } catch (MCoprocessorException mcp) {
      if (mcp.getMessage().contains(
          "classname: io.ampool.monarch.table.coprocessor.NotExistingCoprocessornot found")) {
        System.out.println("Expected:: erorr " + mcp.getMessage());
      } else {
        Assert.fail("Invalid exception caught " + mcp.getMessage());
      }
    }
    System.out.println("Executing endpoint coprocessor..!");
    try {
      List<Object> result =
          mtable.coprocessorService("io.ampool.monarch.table.coprocessor.ComputeAverageFunction",
              "computeAverage", rowKey, execution);
      System.out.println("Verifying the Result, result.size() =  " + (int) result.size());
      for (Object item : result) {
        System.out.println("Verifying the Result, Average =  " + (int) item);
        assertEquals(expectedAvgValue, (int) item);
      }
    } catch (IllegalArgumentException iae) {
      Assert.fail("Invalid exception caught " + iae.getMessage());
    }
  }

  private void verifyPerBucketCoprocessorExecution(Map<Integer, List<byte[]>> bucketWiseKeys,
      int noOfBuckets) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable mtable = clientCache.getTable(TABLE_NAME);
    Random rand = new Random();

    for (int i = 0; i < noOfBuckets; i++) {
      int value = rand.nextInt(NUM_OF_ROWS);
      byte[] rowKey = bucketWiseKeys.get(i).get(value);

      try {
        MExecutionRequest request = new MExecutionRequest();
        request.setArguments(rowKey);

        List<Object> result =
            mtable.coprocessorService("io.ampool.monarch.table.coprocessor.ComputeAverageFunction",
                "getBIDToHostingServer", rowKey, request);
        // System.out.println("Verifying the Result, result.size() = " + (int) result.size());
        for (Object item : result) {
          assertTrue((boolean) item);
        }
      } catch (IllegalArgumentException iae) {
        Assert.fail("Invalid exception caught " + iae.getMessage());
      }
    }

    // Execute a co-processor on a not-existing or invalid key.
    {
      byte[] rowKey = Bytes.toBytes("invalidKey");

      try {
        MExecutionRequest request = new MExecutionRequest();
        request.setArguments(rowKey);

        List<Object> result =
            mtable.coprocessorService("io.ampool.monarch.table.coprocessor.ComputeAverageFunction",
                "getBIDToHostingServer", rowKey, request);
        System.out.println("Verifying the Result, result.size() =  " + (int) result.size());
        for (Object item : result) {
          assertFalse((boolean) item);
        }
      } catch (IllegalArgumentException iae) {
        Assert.fail("Invalid exception caught " + iae.getMessage());
      }
    }
  }

  protected void createTable(int noOfPartitions) {

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    // Add ENdpoint Coprocessor
    tableDescriptor.addCoprocessor("io.ampool.monarch.table.coprocessor.ComputeAverageFunction");

    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
    /*
     * if (order == false) { tableDescriptor.setTableType(MTableType.UNORDERED); }
     */
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setTotalNumOfSplits(noOfPartitions);
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(mtable.getName(), TABLE_NAME);
  }

  protected void createTable2(int noOfPartitions, MTableType tableType) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor(tableType);
    // tableDescriptor.addCoprocessor("io.ampool.monarch.table.coprocessor.ComputeAverageFunction");
    tableDescriptor.setTotalNumOfSplits(noOfPartitions);
    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  protected void createTable3(int noOfPartitions, MTableType tableType) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor(tableType);
    // tableDescriptor.addCoprocessor("io.ampool.monarch.table.coprocessor.TrxRegionEndpoint");
    tableDescriptor.setTotalNumOfSplits(noOfPartitions);
    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  protected void createTableFrom(VM vm, int noOfPartitions, MTableType tableType) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable2(noOfPartitions, tableType);
        return null;
      }
    });
  }

  protected void createTrxTableFrom(VM vm, int noOfPartitions) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable3(noOfPartitions, MTableType.ORDERED_VERSIONED);
        return null;
      }
    });
  }

  protected void createTrxTableFrom_Unordered(VM vm, int noOfPartitions) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable3(noOfPartitions, MTableType.UNORDERED);
        return null;
      }
    });
  }

  private void deleteTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable mtable = clientCache.getTable(TABLE_NAME);
    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  private Map<Integer, List<byte[]>> doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);

    // List<byte[]> allKeys =
    // getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(),
    // NUM_OF_ROWS_PER_BUCKET);
    Map<Integer, List<byte[]>> allKeys =
        getBucketToKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);
    int totalKeyCount = 0;
    for (Map.Entry<Integer, List<byte[]>> entry : allKeys.entrySet()) {
      totalKeyCount += entry.getValue().size();
    }
    System.out.println(
        "Generate keys buckets " + allKeys.keySet().size() + " key count  " + totalKeyCount);
    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), totalKeyCount);
    int totalPutKeys = 0;

    for (Map.Entry<Integer, List<byte[]>> entry : allKeys.entrySet()) {
      for (byte[] key : entry.getValue()) {
        Get get = new Get(key);
        Row result = table.get(get);
        assertTrue(result.isEmpty());
        Put record = new Put(key);
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex));
        }
        table.put(record);
        totalPutKeys++;
      }
    }

    assertEquals(table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS, totalKeyCount);
    return allKeys;
  }
  /*
   * TestCase - test mtable.coprocessorService() API 1. create table and add coprocessors using
   * tableDescriptor.addCoprocessor(String className) 2. execte the endpoint using
   * mtable.coprocessorService(className) and varify its result. 3. Verify the observer events.
   */

  @Test
  public void testObserverFramework() throws Exception {
    doCreateTableAndBasicPutGet(true);
    verifyObserverEventsCount();
    deleteTable();

    // doCreateTableAndBasicPutGet(false);
    // deleteTable();
  }


  @Test
  public void testTrxEndPointExecution() throws Exception {
    for (int i = 0; i < 50; i++) {
      System.out.println("ORDERED_TABLE Iteration = " + i);
      int noOfBuckets = 5;
      createTrxTableFrom(vm3, noOfBuckets);
      Map<Integer, List<byte[]>> bucketWiseKeys = doPuts();
      verifyTrxCoprocessorExecution(bucketWiseKeys, noOfBuckets);
      deleteTable();
    }

    // UNORDERED_TAABLE casee
    for (int i = 0; i < 50; i++) {
      System.out.println("UNORDERED_TAABLE Iteration = " + i);
      int noOfBuckets = 5;
      createTrxTableFrom_Unordered(vm3, noOfBuckets);
      Map<Integer, List<byte[]>> bucketWiseKeys = doPuts();
      verifyTrxCoprocessorExecution(bucketWiseKeys, noOfBuckets);
      deleteTable();
    }
  }


  /*
   * public void VerifyPerServerBuckets() { PartitionedRegion pr = (PartitionedRegion)
   * GemFireCacheImpl.getExisting().getRegion(TABLE_NAME); Set<BucketRegion>
   * allLocalPrimaryBucketRegions = pr.getDataStore().getAllLocalPrimaryBucketRegions(); for
   * (BucketRegion bucket : allLocalPrimaryBucketRegions) {
   * if(bucket.getBucketAdvisor().isPrimary()){
   * System.out.println("------ TEST.VerifyPerServerBuckets bID = "+ bucket.getId()); //total keys
   * //bucket.size() Set keys = bucket.getRegionMap().keySet(); for(Object key : keys){
   * System.out.println(key.toString()); }
   * System.out.println("-------------- END --------------------------------"); } } }
   */

  @Test
  public void testEndPointExecutionFramework() throws Exception {

    int noOfBuckets = 113;
    {
      createTableFrom(vm3, noOfBuckets, MTableType.ORDERED_VERSIONED);
      Map<Integer, List<byte[]>> bucketWiseKeys = doPuts();
      verifyCoprocessorExecution(bucketWiseKeys);
      deleteTable();
    }
    // UNORDERED_TABLE
    {
      createTableFrom(vm3, noOfBuckets, MTableType.UNORDERED);
      Map<Integer, List<byte[]>> bucketWiseKeys = doPuts();
      verifyCoprocessorExecution(bucketWiseKeys);
      deleteTable();
    }

    // UNORDERED_TABLE given a single key to do per bucket execution
    {
      createTableFrom(vm3, noOfBuckets, MTableType.UNORDERED);
      Map<Integer, List<byte[]>> bucketWiseKeys = doPuts();
      verifyPerBucketCoprocessorExecution(bucketWiseKeys, noOfBuckets);
      deleteTable();
    }
    // Debug code to verify per-bucket keys
    /*
     * vm0.invoke(new SerializableCallable() { public Object call() throws Exception {
     * VerifyPerServerBuckets(); return null; } });
     * 
     * vm1.invoke(new SerializableCallable() { public Object call() throws Exception {
     * VerifyPerServerBuckets(); return null; } });
     * 
     * vm2.invoke(new SerializableCallable() { public Object call() throws Exception {
     * VerifyPerServerBuckets(); return null; } });
     */
  }

  // preGet, postGet testcases with e.bypass=true/false,
  // usecase deny /skip result user when try to get with rowkey "Admin".
  @Test
  public void testObserversWithByPass() throws Exception {
    doCreateTableAndBasicPutGetForByPass(true);
    // doGetOp();
    verifyObserverEventsCountWithBypass();
    deleteTable();
  }

  @Test
  public void testObserversWithBulkOps() throws Exception {
    doCreateTableAndBasicPutWithBulkOps(true);
    verifyObserverEventsCountWithBulkOps();
    deleteTable();
  }
  /*
   * @Test public void testObserverEvents() throws Exception { doCreateTableAndBasicPutGet(true);
   * verifyObserverEventsonOnServer(vm0); verifyObserverEventsonOnServer(vm1);
   * verifyObserverEventsonOnServer(vm2);
   * 
   * deleteTable(); }
   */

  @Test
  public void testCoprocessorUtilsAPI() {

    // Testcase with invalid/non existing classname
    String notExistingClass = "io.ampool.monarch.table.coprocessor.NotExistingClass";
    Exception expectedException = null;
    try {
      MCoprocessorUtils.createInstance(notExistingClass);
    } catch (MCoprocessorException mcp) {
      expectedException = mcp;
    }
    assertTrue(expectedException instanceof MCoprocessorException);

    String validClassname = "io.ampool.monarch.table.coprocessor.TableObserverExample";
    expectedException = null;
    Object instance = null;
    try {
      instance = MCoprocessorUtils.createInstance(validClassname);
    } catch (MCoprocessorException mcp) {
      expectedException = mcp;
    }
    assertNull(expectedException);
    assertTrue(instance instanceof TableObserverExample);
  }
}
