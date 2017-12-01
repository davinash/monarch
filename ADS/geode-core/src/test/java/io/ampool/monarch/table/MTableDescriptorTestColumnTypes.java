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

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.DataTypeFactory;
import org.apache.logging.log4j.Logger;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.List;

@Category(MonarchTest.class)
public class MTableDescriptorTestColumnTypes extends MTableDUnitHelper {
  public MTableDescriptorTestColumnTypes() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();
  Host host = Host.getHost(0);
  private VM vm0 = host.getVM(0);
  private VM vm1 = host.getVM(1);

  private VM client1 = host.getVM(3);

  protected final int NUM_OF_COLUMNS = 4;
  protected final String TABLE_NAME = "ALLOPERATION";
  protected final String COLUMN_NAME_PREFIX = "COLUMN";


  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();
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

  public MTableColumnType getType(final String typeStr) {
    return new MTableColumnType(DataTypeFactory.getTypeFromString(typeStr));
  }

  public void createTable(boolean userType) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor = tableDescriptor.addColumn(COLUMN_NAME_PREFIX + 0,
        getType(BasicTypes.CHARS.name() + "(100)"));
    tableDescriptor = tableDescriptor.addColumn(COLUMN_NAME_PREFIX + 1,
        getType(BasicTypes.VARCHAR.name() + "(500)"));
    tableDescriptor = tableDescriptor.addColumn(COLUMN_NAME_PREFIX + 2,
        getType(BasicTypes.BIG_DECIMAL.name() + "(10,10)"));
    tableDescriptor =
        tableDescriptor.addColumn(COLUMN_NAME_PREFIX + 3, getType(BasicTypes.INT.name()));
    tableDescriptor = tableDescriptor.addColumn(COLUMN_NAME_PREFIX + 4, getType("array<INT>"));
    tableDescriptor =
        tableDescriptor.addColumn(COLUMN_NAME_PREFIX + 5, getType("array<BIG_DECIMAL(10,10)>"));

    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setUserTable(userType);


    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertNotNull(table);
    assertEquals(table.getName(), TABLE_NAME);

    table = clientCache.getTable(TABLE_NAME);
    assertNotNull(table);

    MTableDescriptor tableDescriptor1 = table.getTableDescriptor();
    assertNotNull(tableDescriptor1);
    List<MColumnDescriptor> columnDescriptorsList = tableDescriptor1.getAllColumnDescriptors();
    assertColumnType(columnDescriptorsList.get(0), BasicTypes.CHARS.name() + "(100)");
    assertColumnType(columnDescriptorsList.get(1), BasicTypes.VARCHAR.name() + "(500)");
    assertColumnType(columnDescriptorsList.get(2), BasicTypes.BIG_DECIMAL.name() + "(10,10)");
    assertColumnType(columnDescriptorsList.get(3), BasicTypes.INT.toString());
    assertColumnType(columnDescriptorsList.get(4), "array<INT>");
    assertColumnType(columnDescriptorsList.get(5), "array<BIG_DECIMAL(10,10)>");
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

  private void assertColumnType(final MColumnDescriptor cd, final String typeStr) {
    assertEquals(typeStr, cd.getColumnType().toString());
  }

  private void verifyMTableColumnTypes(boolean tableType) {
    MCache cache = MCacheFactory.getAnyInstance();
    assertNotNull(cache);
    MTable table = cache.getTable(TABLE_NAME);
    assertNotNull(table);
    MTableDescriptor tableDescriptor = table.getTableDescriptor();
    assertNotNull(tableDescriptor);
    List<MColumnDescriptor> columnDescriptorsList = tableDescriptor.getAllColumnDescriptors();

    assertColumnType(columnDescriptorsList.get(0), BasicTypes.CHARS + "(100)");
    assertColumnType(columnDescriptorsList.get(1), BasicTypes.VARCHAR + "(500)");
    assertColumnType(columnDescriptorsList.get(2), BasicTypes.BIG_DECIMAL + "(10,10)");
    assertColumnType(columnDescriptorsList.get(3), BasicTypes.INT.name());
    assertColumnType(columnDescriptorsList.get(4), "array<INT>");
    assertColumnType(columnDescriptorsList.get(5), "array<BIG_DECIMAL(10,10)>");
  }

  private void verifyMTableColumnTypesOn(VM vm, boolean tableType) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {

        verifyMTableColumnTypes(tableType);
        return null;
      }
    });
  }


  public void testMTableColumnTypes() {
    createTableOn(this.client1, true);

    verifyMTableColumnTypesOn(this.vm0, true);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }
}
