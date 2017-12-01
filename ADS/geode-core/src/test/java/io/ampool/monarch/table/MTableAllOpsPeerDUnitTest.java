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

import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Category(MonarchTest.class)
public class MTableAllOpsPeerDUnitTest extends MTableDUnitHelper {
  public MTableAllOpsPeerDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "MTableAllOpsPeerDUnitTest";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 100;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected final int LATEST_TIMESTAMP = 300;
  protected final int MAX_VERSIONS = 5;
  protected final int TABLE_MAX_VERSIONS = 7;


  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    allVMList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2, vm3));
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    startServerOn(this.vm3, DUnitLauncher.getLocatorString());

    createClientCache();
  }

  private List<VM> allVMList = null;

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    allVMList.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    }));

    super.tearDown2();
  }

  private void createTableOn() {
    MCache memberCache = MCacheFactory.getAnyInstance();
    assertNotNull(memberCache);

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    Admin admin = memberCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertNotNull(table);
    assertEquals(table.getName(), TABLE_NAME);


  }

  private void createTableOnMember(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTableOn();
        return null;
      }

    });
  }

  private void doGetTableFromAllVMs(boolean tableExists) {
    invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache mCache = MCacheFactory.getAnyInstance();
        MTable table = null;

        if (tableExists) {
          table = mCache.getTable(TABLE_NAME);
          assertNotNull(table);
        } else {
          table = mCache.getTable(TABLE_NAME);
          assertNull(table);
        }
        return null;
      }
    });
  }

  private void doVerifyMetaRegionEntriesAllVMs() {
    invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        Region r = cache.getRegion(MTableUtils.AMPL_META_REGION_NAME);
        assertNotNull(r);
        Object o = r.get(TABLE_NAME);
        assertNotNull(o);
        MTableDescriptor tableDescriptor = (MTableDescriptor) o;
        assertNotNull(tableDescriptor);
        return null;
      }
    });
  }

  private void doDeleteTableFrom(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        cache.getAdmin().deleteTable(TABLE_NAME);
        return null;
      }
    });
  }

  private void doPuts(final int startRow, final int endRow) {
    MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = startRow; rowIndex <= endRow; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }
  }

  private void doPutsFrom(VM vm, final int startRow, final int endRow) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPuts(startRow, endRow);
        return null;
      }
    });
  }

  private void doVerifyGetsFrom(VM vm, final int startRow, final int endRow) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNotNull(table);
        for (int rowIndex = startRow; rowIndex <= endRow; rowIndex++) {
          Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
          Row result = table.get(get);
          assertEquals(NUM_OF_COLUMNS + 1, result.size());
          assertFalse(result.isEmpty());

          /* Verify the row Id features */
          if (!Bytes.equals(result.getRowId(), Bytes.toBytes(KEY_PREFIX + rowIndex))) {
            System.out.println(
                "expectedColumnName => " + Arrays.toString(Bytes.toBytes(KEY_PREFIX + rowIndex)));
            System.out.println("actualColumnName   => " + Arrays.toString(result.getRowId()));
            Assert.fail("Invalid Row Id");
          }

          int columnIndex = 0;
          List<Cell> row = result.getCells();
          for (int i = 0; i < row.size() - 1; i++) {
            Assert.assertNotEquals(NUM_OF_COLUMNS, columnIndex);
            byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
            byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

            if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
              System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
              System.out
                  .println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
              Assert.fail("Invalid Values for Column Name");
            }
            if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
              System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
              System.out.println(
                  "actualValue    => " + Arrays.toString((byte[]) row.get(i).getColumnValue()));
              Assert.fail("Invalid Values for Column Value");
            }
            columnIndex++;
          }
        }
        return null;
      }
    });

  }

  private void doVerifyGetsAllVMs(final int startRow, final int endRow) {
    doVerifyGetsFrom(this.vm0, startRow, endRow);
    doVerifyGetsFrom(this.vm1, startRow, endRow);
    doVerifyGetsFrom(this.vm2, startRow, endRow);
    doVerifyGetsFrom(this.vm3, startRow, endRow);
  }

  private void doDeleteRowsFrom(VM vm, final int startRow, final int endRow) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNotNull(table);
        for (int rowIndex = startRow; rowIndex <= endRow; rowIndex++) {
          Delete delete = new Delete(Bytes.toBytes(KEY_PREFIX + rowIndex));
          table.delete(delete);
        }
        return null;
      }
    });

  }


  /* Test Case */
  /*
   * 1. Create MTable on one of the Memebers in the distributed System 2. Check regions created on
   * all the members 3. Try doing getTable on all the members.
   */
  @Test
  public void testTableCreateGetAndDeleteAllVMs() {
    createTableOnMember(this.vm0);
    verifyInternalRegionAllVMs(TABLE_NAME, true);
    doGetTableFromAllVMs(true);
    doVerifyMetaRegionEntriesAllVMs();
    doDeleteTableFrom(this.vm3);
    verifyInternalRegionAllVMs(TABLE_NAME, false);
    doGetTableFromAllVMs(false);
  }


  @Test
  public void testPutsGetsAllVms() {
    createTableOnMember(this.vm0);
    doPutsFrom(this.vm0, 0, 25);
    doPutsFrom(this.vm1, 26, 50);
    doPutsFrom(this.vm2, 51, 75);
    doPutsFrom(this.vm3, 76, 99);
    verifyRegionSizeAllVMs(TABLE_NAME, 100);
    doVerifyGetsFrom(this.vm0, 76, 99);
    doVerifyGetsFrom(this.vm3, 0, 25);
    doVerifyGetsFrom(this.vm1, 51, 75);
    doVerifyGetsFrom(this.vm2, 26, 50);
    doVerifyGetsAllVMs(0, 99);

    doDeleteRowsFrom(this.vm0, 0, 25);
    doVerifyGetsAllVMs(26, 99);

  }

}
