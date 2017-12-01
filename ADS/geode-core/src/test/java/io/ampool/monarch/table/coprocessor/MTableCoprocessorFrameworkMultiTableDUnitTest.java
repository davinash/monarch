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

import org.apache.geode.GemFireException;
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
public class MTableCoprocessorFrameworkMultiTableDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();

  final int numOfEntries = 10;
  private String table1 = "Employee1";
  private String table2 = "Employee2";
  private String table3 = "Employee3";
  private String observer1 = "io.ampool.monarch.table.coprocessor.SampleRegionObserver1";
  private String observer2 = "io.ampool.monarch.table.coprocessor.SampleRegionObserver2";
  private String observer3 = "io.ampool.monarch.table.coprocessor.SampleRegionObserver3";

  public MTableCoprocessorFrameworkMultiTableDUnitTest() {
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
          createTable(table1, observer1);
          performOperation(table1);

          createTable(table2, observer2);
          performOperation(table2);

          createTable(table3, observer3);
          performOperation(table3);


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

  private void performOperation(String tableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable mtable = clientCache.getTable(tableName);
    for (int i = 0; i < numOfEntries; i++) {
      String key1 = "RowKey" + i;
      Put myput1 = new Put(Bytes.toBytes(key1));
      myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth" + i));
      myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
      myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
      myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
      mtable.put(myput1);
    }
    for (int i = 0; i < numOfEntries - 2; i++) {
      String key1 = "RowKey" + i;
      Delete mydel = new Delete(Bytes.toBytes(key1));
      mtable.delete(mydel);
    }
  }

  private void createTable(String tableName, String observer) {
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
  }

  private Object verifyObserverEventsonOnServer(VM vm, final String tableName) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          // Region
          MCache c = MCacheFactory.getAnyInstance();
          System.out.println("Table name in observer: " + tableName);
          Region r = c.getRegion(tableName);
          assertNotNull(r);
          CacheListener[] cls = ((PartitionedRegion) r).getCacheListeners();
          int prePut = 0;
          int preDelete = 0;
          int postPut = 0;
          int postDelete = 0;

          for (CacheListener cl : cls) {
            System.out.println("CCCCLLLL CacheLister attached = " + cl.getClass().getName());
            if (cl instanceof PostOpRegionObserver) {
              PostOpRegionObserver poro = (PostOpRegionObserver) cl;
              for (MTableObserver obj : poro.getmObserverInstanceList(tableName)) {
                if (obj.toString().equals(observer1)) {
                  SampleRegionObserver1 ro1 = (SampleRegionObserver1) obj;
                  System.out.println("SRO1 prePut: " + ro1.getTotalPrePutCount());
                  prePut = prePut + ro1.getTotalPrePutCount();
                  preDelete = preDelete + ro1.getTotalPreDeleteCount();
                  postPut = postPut + ro1.getTotalPostPutCount();
                  postDelete = postDelete + ro1.getTotalPostDeleteCount();
                } else if (obj.toString().equals(observer2)) {
                  SampleRegionObserver2 ro2 = (SampleRegionObserver2) obj;
                  System.out.println("SRO2 prePut: " + ro2.getTotalPrePutCount());
                  prePut = prePut + ro2.getTotalPrePutCount();
                  preDelete = preDelete + ro2.getTotalPreDeleteCount();
                  postPut = postPut + ro2.getTotalPostPutCount();
                  postDelete = postDelete + ro2.getTotalPostDeleteCount();
                } else if (obj.toString().equals(observer3)) {
                  SampleRegionObserver3 ro3 = (SampleRegionObserver3) obj;
                  System.out.println("SRO3 prePut: " + ro3.getTotalPrePutCount());
                  prePut = prePut + ro3.getTotalPrePutCount();
                  preDelete = preDelete + ro3.getTotalPreDeleteCount();
                  postPut = postPut + ro3.getTotalPostPutCount();
                  postDelete = postDelete + ro3.getTotalPostDeleteCount();
                } else {
                  System.out.println("XXXXXXX Invalid Observer CLASS found");
                }
              }

            }
          }
          return prePut + ":" + postPut + ":" + preDelete + ":" + postDelete;
        } catch (GemFireException ge) {
          ge.printStackTrace();
        }
        return null;
      }
    });
  }

  private void verifyObserverEventsCount(String tableName) {
    int prePut = 0;
    int postPut = 0;
    int preDel = 0;
    int postDel = 0;
    System.out.println("For table : " + tableName);
    String counts = (String) verifyObserverEventsonOnServer(vm0, tableName);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    preDel = preDel + Integer.parseInt(counts.split(":")[2]);
    postDel = postDel + Integer.parseInt(counts.split(":")[3]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm1, tableName);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    preDel = preDel + Integer.parseInt(counts.split(":")[2]);
    postDel = postDel + Integer.parseInt(counts.split(":")[3]);
    System.out.println("Counts: " + counts);
    counts = (String) verifyObserverEventsonOnServer(vm2, tableName);
    prePut = prePut + Integer.parseInt(counts.split(":")[0]);
    postPut = postPut + Integer.parseInt(counts.split(":")[1]);
    preDel = preDel + Integer.parseInt(counts.split(":")[2]);
    postDel = postDel + Integer.parseInt(counts.split(":")[3]);
    System.out.println("Counts: " + counts);
    int numberOfObservers = 1;
    assertEquals(numOfEntries * numberOfObservers, prePut);
    assertEquals(numOfEntries * numberOfObservers, postPut);
    assertEquals((numOfEntries - 2) * numberOfObservers, preDel);
    assertEquals((numOfEntries - 2) * numberOfObservers, postDel);
  }

  private void doCreateTableAndBasicPutGet(final boolean order) throws Exception {
    doPutOperationFromClient(vm3, getLocatorPort(), order);
    verifyObserverEventsCount(table1);
    verifyObserverEventsCount(table2);
    verifyObserverEventsCount(table3);

    // MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    // MTable mtable = clientCache.getTable("EmployeeTable");
    // Integer i1 = new Integer(1000);
    // Integer i2 = new Integer(2000);
    // Integer i3 = new Integer(3000);
    //
    // ArrayList<Integer> list = new ArrayList<Integer>();
    //
    // list.add(i1);
    // list.add(i2);
    // list.add(i3);
    //
    // int sum = 0;
    // for(Integer i : list){
    // sum = sum + i.intValue();
    // }
    // int expectedAvgValue = sum/list.size();
    //
    // MExecutionRequest execution = new MExecutionRequest();
    // execution.setArguments(list);
    // //MResultCollector rc = new DefaultResultCollector();
    // //execution.setResultCollector(new DefaultResultCollector(););
    // System.out.println("Executing endpoint coprocessor..!");
    // MResultCollector resultCollector =
    // mtable.coprocessorService("io.ampool.monarch.table.coprocessor.ComputeAverageFunction", null,
    // execution);
    // List<Object> result = (List<Object>) resultCollector.getResult();
    // System.out.println("Verifying the Result, result.size() = " + (int) result.size());
    // for (Object item : result) {
    // System.out.println("Verifying the Result, Average = " + (int)item);
    // Assert.assertEquals(expectedAvgValue, (int)item);
    // }

  }


  private void deleteTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    clientCache.getAdmin().deleteTable(table1);
    clientCache.getAdmin().deleteTable(table2);
    clientCache.getAdmin().deleteTable(table3);
  }
  /*
   * TestCase - test mtable.coprocessorService() API 1. create table and add coprocessors using
   * tableDescriptor.addCoprocessor(String className) 2. execte the endpoint using
   * mtable.coprocessorService(className) and varify its result. 3. Verify the observer events.
   */

  @Test
  public void testDoEndPointExecution() throws Exception {
    doCreateTableAndBasicPutGet(true);
    deleteTable();

    doCreateTableAndBasicPutGet(false);
    deleteTable();
  }

  /*
   * public void testObserverEvents() throws Exception { doCreateTableAndBasicPutGet(true);
   * verifyObserverEventsonOnServer(vm0); verifyObserverEventsonOnServer(vm1);
   * verifyObserverEventsonOnServer(vm2);
   * 
   * deleteTable(); }
   */
}
