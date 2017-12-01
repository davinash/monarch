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


import org.apache.geode.cache.CacheClosedException;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.*;


/**
 * MTableCoprocessorWithMTablePersistenceDUnitTest
 *
 * @since 0.2.0.0
 */

@Category(MonarchTest.class)
public class MTableCoprocessorWithMTablePersistenceDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();

  final int numOfEntries = 100;
  private String tableName = "EmployeeTable";
  private List<VM> allServerVms = null;

  private String coProcessorName1 = "io.ampool.monarch.table.coprocessor.SampleRegionObserver1";
  private String coProcessorName2 = "io.ampool.monarch.table.coprocessor.SampleRegionObserver2";
  private String coProcessorName3 = "io.ampool.monarch.table.coprocessor.SampleRegionObserver3";



  public MTableCoprocessorWithMTablePersistenceDUnitTest() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    allServerVms = new ArrayList<>(Arrays.asList(this.vm0, this.vm1, this.vm2));
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
      final MDiskWritePolicy policy) {
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
          // Add Coprocessors
          assertEquals(tableDescriptor.getCoprocessorList().size(), 0);
          tableDescriptor.addCoprocessor(coProcessorName1);
          tableDescriptor.addCoprocessor(coProcessorName2);
          tableDescriptor.addCoprocessor(coProcessorName3);
          tableDescriptor.enableDiskPersistence(policy);
          // tableDescriptor.setDiskStore("MTableJUnitDiskStore");
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

  private void verifyCoProcessorCount() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    System.out.println(
        "MTableCoprocessorWithMTablePersistenceDUnitTest.verifyCoProcessorCount " + "HERE 1");
    MTable mtable = clientCache.getTable(tableName);
    System.out.println("MTableCoprocessorWithMTablePersistenceDUnitTest.verifyCoProcessorCount"
        + mtable.getName());
    assertEquals(mtable.getTableDescriptor().getCoprocessorList().size(), 3);

  }

  private void restart() {
    closeMClientCache();
    stopAllCacheServers();

    startAllServerAsync(DUnitLauncher.getLocatorString());
    createClientCache();

  }

  private void stopAllCacheServers() {
    allServerVms.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        Iterator iter = MCacheFactory.getAnyInstance().getCacheServers().iterator();
        if (iter.hasNext()) {
          CacheServer server = (CacheServer) iter.next();
          server.stop();
        }
        cache.close();
        return null;
      }
    }));
  }

  private void startAllServerAsync(final String locatorString) {
    AsyncInvocation vm0Task = vm0.invokeAsync(new SerializableCallable() {
      @Override
      public Object call() throws Exception {

        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system_after_restart.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locatorString);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");

        MCache cache = null;
        try {
          cache = MCacheFactory.getAnyInstance();
          cache.close();
        } catch (CacheClosedException cce) {
        }
        cache = MCacheFactory.create(getSystem(props));
        CacheServer s = cache.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        System.out.println("MTableDiskPersistDUnitTest.call.VM0.PORT = " + port);
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        return null;
      }
    });


    AsyncInvocation vm1Task = vm1.invokeAsync(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system_after_restart.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locatorString);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
        MCache cache = null;
        try {
          cache = MCacheFactory.getAnyInstance();
          cache.close();
        } catch (CacheClosedException cce) {
        }
        cache = MCacheFactory.create(getSystem(props));
        CacheServer s = cache.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        System.out.println("MTableDiskPersistDUnitTest.call.VM1.PORT = " + port);
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        return null;
      }
    });

    AsyncInvocation vm2Task = vm2.invokeAsync(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system_after_restart.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locatorString);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
        MCache cache = null;
        try {
          cache = MCacheFactory.getAnyInstance();
          cache.close();
        } catch (CacheClosedException cce) {
        }
        cache = MCacheFactory.create(getSystem(props));
        CacheServer s = cache.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        System.out.println("MTableDiskPersistDUnitTest.call.VM2.PORT = " + port);
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        return null;
      }
    });


    try {
      vm0Task.join();
      vm1Task.join();
      vm2Task.join();

    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }



  private void doCreateTable(final boolean order, final MDiskWritePolicy policy) throws Exception {
    doPutOperationFromClient(vm3, getLocatorPort(), order, policy);

    verifyCoProcessorCount();

  }


  private void deleteTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    clientCache.getAdmin().deleteTable(tableName);
  }

  /**
   * Tests CoProcessor getCoprocessorList operation on Ordered Table with async disk persistence
   *
   * @throws Exception
   */
  @Test
  public void testCoProcessorOrderedTableAsyncDiskPersistence() throws Exception {
    doCreateTable(true, MDiskWritePolicy.ASYNCHRONOUS);
    deleteTable();

  }


  /**
   * Tests CoProcessor getCoprocessorList operation on Ordered Table with sync disk persistence
   *
   * @throws Exception
   */
  @Test
  public void testCoProcessorOrderedTableSyncDiskPersistence() throws Exception {
    doCreateTable(true, MDiskWritePolicy.SYNCHRONOUS);
    deleteTable();

  }


  /**
   * Tests CoProcessor getCoprocessorList operation on UnOrdered Table with async disk persistence
   *
   * @throws Exception
   */
  @Test
  public void testCoProcessorUnOrderedTableAsyncDiskPersistence() throws Exception {
    doCreateTable(false, MDiskWritePolicy.ASYNCHRONOUS);
    deleteTable();
  }


  /**
   * Tests CoProcessor getCoprocessorList operation on UnOrdered Table with sync disk persistence
   *
   * @throws Exception
   */
  @Test
  public void testCoProcessorUnOrderedTableSyncDiskPersistence() throws Exception {
    doCreateTable(false, MDiskWritePolicy.SYNCHRONOUS);
    deleteTable();
  }


  /**
   * Tests CoProcessor getCoprocessorList operation on UnOrdered Table with sync disk persistence
   *
   * @throws Exception
   */
  @Test
  public void testCoProcessorPostServerRestart() throws Exception {
    doCreateTable(false, MDiskWritePolicy.SYNCHRONOUS);
    stopAllCacheServers();

    startAllServerAsync(DUnitLauncher.getLocatorString());
    verifyCoProcessorCount();
    deleteTable();
  }
}
