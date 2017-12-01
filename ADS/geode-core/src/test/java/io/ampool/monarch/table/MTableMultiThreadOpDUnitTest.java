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
package io.ampool.monarch.table;

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.MTableKey;

import org.apache.geode.cache.CacheClosedException;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.logging.log4j.Logger;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MTableMultiThreadOpDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();
  VM server1, server2, server3;

  public MTableMultiThreadOpDUnitTest() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    server1 = vm0;
    server2 = vm1;
    server3 = vm2;
    startServerOn(this.server1, DUnitLauncher.getLocatorString());
    startServerOn(this.server2, DUnitLauncher.getLocatorString());
    startServerOn(this.server3, DUnitLauncher.getLocatorString());

    createClientCache(this.client1);
    createClientCache();

  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(this.client1);
    super.tearDown2();
  }

  private MTable createEmployeeTable(MClientCache cache, final boolean ordered) {

    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }

    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"))
        .addColumn(Bytes.toBytes("DEPT")).addColumn(Bytes.toBytes("DOJ"));
    tableDescriptor.setRedundantCopies(1);

    Admin admin = cache.getAdmin();
    return admin.createTable("EmployeeTable", tableDescriptor);

  }

  public void createTableFromClient1(VM vm, final int locatorPoaddrt, final boolean ordered) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        MTable table = createEmployeeTable(clientCache, ordered);
        return null;
      }
    });
  }


  private void verifySizeOfRegionOnServer(VM vm, final int expectedCount) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        try {
          MCache c = MCacheFactory.getAnyInstance();
          Region r = c.getRegion("EmployeeTable");

          assertEquals("Region Size MisMatch", expectedCount, r.size());

        } catch (CacheClosedException cce) {
        }
        return null;
      }
    });

  }

  class doPuts implements Runnable {
    private int numOfThreads;
    private List<byte[]> listOfKeys = null;
    private Random random = new Random(System.currentTimeMillis());

    public doPuts(int numOfThreads) {
      this.numOfThreads = numOfThreads;
      this.listOfKeys = new ArrayList<>(this.numOfThreads);

      for (int i = 0; i < this.numOfThreads; i++) {
        listOfKeys.add(Bytes.toBytes("KEY-" + i));
      }
    }

    private Put createPutRecord(byte[] rowKey, int position)
        throws IOException, ClassNotFoundException {
      Put putRecord = new Put(rowKey);

      MTable table = MClientCacheFactory.getAnyInstance().getTable("EmployeeTable");

      for (MColumnDescriptor columnDescriptor : table.getTableDescriptor()
          .getAllColumnDescriptors()) {
        byte[] columnName = columnDescriptor.getColumnName();
        putRecord.addColumn(columnName,
            Bytes.toBytes(Bytes.toString(columnName) + "Value-" + position));
      }
      return putRecord;
    }

    @Override
    public void run() {
      MTable table = MClientCacheFactory.getAnyInstance().getTable("EmployeeTable");
      int max = listOfKeys.size() - 1;

      for (int i = 0; i < 100; i++) {
        int index = random.nextInt((max - 0) + 1) + 0;
        byte[] rowKey = listOfKeys.get(index);
        Put put = null;
        try {
          put = createPutRecord(rowKey, i);
        } catch (IOException e) {
          e.printStackTrace();
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        }
        table.put(put);
      }
    }
  }

  class doGets implements Runnable {
    private int numOfThreads;
    private List<byte[]> listOfKeys = null;
    private Random random = new Random(System.currentTimeMillis());

    public doGets(int numOfThreads) {
      this.numOfThreads = numOfThreads;
      this.listOfKeys = new ArrayList<>(this.numOfThreads);

      for (int i = 0; i < this.numOfThreads; i++) {
        listOfKeys.add(Bytes.toBytes("KEY-" + i));
      }
    }

    @Override
    public void run() {
      MTable table = MClientCacheFactory.getAnyInstance().getTable("EmployeeTable");
      int max = listOfKeys.size() - 1;

      for (int i = 0; i < 100; i++) {
        int index = random.nextInt((max - 0) + 1) + 0;
        byte[] rowKey = listOfKeys.get(index);

        Get get = new Get(rowKey);
        Row result = table.get(get);
        // assertFalse(result.isEmpty());
      }
    }
  }

  private void performMultiThreadedOps(final int numOfThreads, final String threadName) {
    Thread[] putThreads = new Thread[numOfThreads];
    Thread[] getThreads = new Thread[numOfThreads];

    for (int i = 0; i < numOfThreads; i++) {
      putThreads[i] = new Thread(new doPuts(numOfThreads));
      putThreads[i].setName(threadName + "-PutThread" + i);

      getThreads[i] = new Thread(new doGets(numOfThreads));
      getThreads[i].setName(threadName + "-GetThread" + i);
    }

    for (int i = 0; i < numOfThreads; i++) {
      putThreads[i].start();
      getThreads[i].start();
    }

    for (int i = 0; i < numOfThreads; i++) {
      ThreadUtils.join(putThreads[i], 10 * 1000);
      ThreadUtils.join(getThreads[i], 10 * 1000);
    }
  }

  private void performMultiThreadedPutsOn(VM vm, final int numOfThreads, final String threadName) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        performMultiThreadedOps(numOfThreads, threadName);
        return null;
      }
    });
  }

  private void multiThreadedPutOp(final boolean ordered) {
    createTableFromClient1(this.client1, getLocatorPort(), ordered);

    int numOfThreads = 10;
    performMultiThreadedPutsOn(this.client1, numOfThreads, "ClientVM");

    performMultiThreadedOps(numOfThreads, "LocalHost");

    verifySizeOfRegionOnServer(this.server1, numOfThreads);
    verifySizeOfRegionOnServer(this.server2, numOfThreads);
    verifySizeOfRegionOnServer(this.server3, numOfThreads);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable("EmployeeTable");

  }

  @Test
  public void testMultiThreadedPutOp() {
    MTableKey key = new MTableKey(new byte[0]);
    key.setIsGetOp();

    System.out.println("MTableMultiThreadOpDUnitTest.testMultiThreadedPutOp ==> " + key.isGetOp());

    multiThreadedPutOp(true);
    multiThreadedPutOp(false);
  }

}
