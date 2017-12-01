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

import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;

import java.util.*;

@Category(MonarchTest.class)
public class MTableListTablesDUnitTest extends MTableDUnitHelper {

  private static final String TABLE_NAME = "MyMTable";
  protected final int NUM_OF_COLUMNS = 2;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  public static final int REDUDANT_COPIES = 1;

  public MTableListTablesDUnitTest() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();

    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
    createClientCache();

  }

  protected void createTable(String tableName) {
    MCache cache = MCacheFactory.getAnyInstance();
    assertNotNull(cache);
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(REDUDANT_COPIES);
    MTable table = cache.getAdmin().createTable(tableName, tableDescriptor);
    assertEquals(table.getName(), tableName);
    assertNotNull(table);
  }

  protected void createTableOn(VM vm, final String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(tableName);
        return null;
      }
    });
  }

  protected void verifyListTables(int numTables) {
    MCache cache = MCacheFactory.getAnyInstance();
    assertNotNull(cache);
    Admin admin = cache.getAdmin();
    Map<String, MTableDescriptor> mTableDescriptors = admin.listMTables();
    assertTrue(mTableDescriptors.size() == numTables);
    for (MTableDescriptor mTableDescriptor : mTableDescriptors.values()) {
      assertTrue(mTableDescriptor.getColumnDescriptorsMap().size() - 1 == NUM_OF_COLUMNS);
      assertTrue(mTableDescriptor.getRedundantCopies() == REDUDANT_COPIES);
    }

    Set<String> strings = mTableDescriptors.keySet();
    List<String> list = new ArrayList<>(strings);
    for (int i = 1; i <= strings.size(); i++) {
      assertTrue(list.contains(TABLE_NAME + i));
    }

    String[] tableNames = admin.listTableNames();
    assertTrue(tableNames.length == numTables);
    List<String> tableNamesList = Arrays.asList(tableNames);
    for (int i = 1; i <= numTables; i++) {
      assertTrue(tableNamesList.contains(TABLE_NAME + i));
    }
  }

  private void verifyListTablesClient(int numTables) {
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    Admin admin = cache.getAdmin();
    Map<String, MTableDescriptor> mTableDescriptors = admin.listMTables();
    assertTrue(mTableDescriptors.size() == numTables);
    for (MTableDescriptor mTableDescriptor : mTableDescriptors.values()) {
      assertTrue(mTableDescriptor.getColumnDescriptorsMap().size() - 1 == NUM_OF_COLUMNS);
      assertTrue(mTableDescriptor.getRedundantCopies() == REDUDANT_COPIES);
    }

    Set<String> strings = mTableDescriptors.keySet();
    List<String> list = new ArrayList<>(strings);
    for (int i = 1; i <= strings.size(); i++) {
      assertTrue(list.contains(TABLE_NAME + i));
    }

    String[] tableNames = admin.listTableNames();
    assertTrue(tableNames.length == numTables);
    List<String> tableNamesList = Arrays.asList(tableNames);
    for (int i = 1; i <= numTables; i++) {
      assertTrue(tableNamesList.contains(TABLE_NAME + i));
    }
  }

  protected void verifyListTablesOn(VM vm, final int numTables) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyListTables(numTables);
        return null;
      }
    });
  }

  protected void verifyListTablesOnClient(VM vm, final int numTables) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyListTablesClient(numTables);
        return null;
      }
    });
  }

  @Test
  public void testListTables() {
    int numTables = 5;

    createTable(TABLE_NAME + "1");
    createTableOn(vm0, TABLE_NAME + "2");
    createTableOn(vm1, TABLE_NAME + "3");
    createTableOn(vm2, TABLE_NAME + "4");
    createTableOn(client1, TABLE_NAME + "5");

    verifyListTables(numTables);
    verifyListTablesOn(vm0, numTables);
    verifyListTablesOn(vm1, numTables);
    verifyListTablesOn(vm2, numTables);
    verifyListTablesOnClient(client1, numTables);
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);

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
}
