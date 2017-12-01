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
import org.apache.geode.internal.logging.LogService;
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
import java.util.ArrayList;
import java.util.Arrays;

// import dunit.Host;
// import dunit.SerializableCallable;
// import dunit.VM;
// import dunit.standalone.DUnitLauncher;

/**
 * MTableCoprocessorCountDUnitTest
 *
 * @since 0.2.0.0
 */

@Category(MonarchTest.class)
public class MTableCoprocessorCountDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();

  final int numOfEntries = 100;
  private String tableName = "EmployeeTable";

  private String coProcessorName1 = "io.ampool.monarch.table.coprocessor.SampleRegionObserver1";
  private String coProcessorName2 = "io.ampool.monarch.table.coprocessor.SampleRegionObserver2";
  private String coProcessorName3 = "io.ampool.monarch.table.coprocessor.SampleRegionObserver3";

  public MTableCoprocessorCountDUnitTest() {
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
    MTable mtable = clientCache.getTable(tableName);
    assertEquals(mtable.getTableDescriptor().getCoprocessorList().size(), 3);

  }


  private void doCreateTable(final boolean order) throws Exception {
    doPutOperationFromClient(vm3, getLocatorPort(), order);
    verifyCoProcessorCount();

  }


  private void deleteTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    clientCache.getAdmin().deleteTable(tableName);
  }

  /**
   * Tests CoProcessor getCoprocessorList operation on Ordered Table
   *
   * @throws Exception
   */
  @Test
  public void testCoProcessorCountandName() throws Exception {
    doCreateTable(true);
    deleteTable();

  }

  /**
   * Tests CoProcessor getCoprocessorList operation on UnOrdered Table
   *
   * @throws Exception
   */
  @Test
  public void testCoProcessorCountandNameonUnOrderedTable() throws Exception {
    doCreateTable(false);
    deleteTable();
  }

}
