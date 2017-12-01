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

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.logging.log4j.Logger;
import org.junit.experimental.categories.Category;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static junit.framework.TestCase.assertNotNull;

@Category(MonarchTest.class)
public class MTableUserTableDUnitTest extends MTableDUnitHelper {
  public MTableUserTableDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "ALLOPERATION";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 100;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";


  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);

    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);
    super.tearDown2();
  }

  public void createTable(boolean userType) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setUserTable(userType);


    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertNotNull(table);
    assertEquals(table.getName(), TABLE_NAME);
  }

  private void createTableOn(VM vm, boolean tableType) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(tableType);
        return null;
      }
    });
  }

  private void verifyBucketRegionProperties(boolean tableType) {
    MCache cache = MCacheFactory.getAnyInstance();
    assertNotNull(cache);
    Region r = cache.getRegion(TABLE_NAME);
    System.out.println("ATTRIBUTES  " + r.getAttributes());
    System.out.println(" 11111111" + r);
    assertNotNull(r);
    if (tableType) {
      assertEquals(DataPolicy.PARTITION, r.getAttributes().getDataPolicy());
    } else {
      assertEquals(DataPolicy.PERSISTENT_REPLICATE, r.getAttributes().getDataPolicy());
    }

  }

  private void verifyBucketRegionPropertiesOn(VM vm, boolean tableType) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {

        verifyBucketRegionProperties(tableType);
        return null;
      }
    });
  }

  private void updateTableDescriptorFromServerCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTableDescriptor td =
            MCacheFactory.getAnyInstance().getTable(TABLE_NAME).getTableDescriptor();
        assertNotNull(td);
        td.addColumn("NEWCOL");
        return null;
      }
    });
  }

  private void verifyTableDescriptorFromAllServers() {
    List<VM> vms = new ArrayList();
    vms.add(vm0);
    vms.add(vm1);

    for (VM vm : vms) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          MTableDescriptor td =
              MCacheFactory.getAnyInstance().getTable(TABLE_NAME).getTableDescriptor();
          assertNotNull(td);
          assertEquals(2, td.getAllColumnDescriptors().size());
          td = MCacheFactory.getAnyInstance().getTable(TABLE_NAME + "_SERVER").getTableDescriptor();
          assertNotNull(td);
          assertEquals(2, td.getAllColumnDescriptors().size());
          return null;
        }
      });
    }
  }

  private void createTableOnServerAndUpdateTD(VM vm, MTableDescriptor td, String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MCacheFactory.getAnyInstance().getAdmin().createTable(tableName, td);
        table.getTableDescriptor().addColumn("TEST2");
        return null;
      }
    });
  }

  @Test
  public void testPutFromOneClientAndGetFromAnother() {
    // Create a table with data policy PARTITION
    createTableOn(this.client1, true);

    verifyBucketRegionPropertiesOn(this.vm0, true);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);

    // Create a table with data policy REPLICATE
    createTableOn(this.client1, false);

    verifyBucketRegionPropertiesOn(this.vm1, false);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }


  @Test
  public void testTableDescriptorUpdate() {
    MTableDescriptor td = new MTableDescriptor();
    td.addColumn("TEST");
    MClientCacheFactory.getAnyInstance().getAdmin().createTable(TABLE_NAME, td);
    createTableOnServerAndUpdateTD(vm1, td, TABLE_NAME + "_SERVER");
    updateTableDescriptorFromServerCache(vm0);
    verifyTableDescriptorFromAllServers();
  }

}
