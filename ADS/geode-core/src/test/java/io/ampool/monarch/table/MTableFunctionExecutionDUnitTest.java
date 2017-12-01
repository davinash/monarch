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

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MTableExistsException;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Category(MonarchTest.class)
public class MTableFunctionExecutionDUnitTest extends MTableDUnitHelper {
  public MTableFunctionExecutionDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "MTableAllOpsClientServerDUnitTest";
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
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
    createClientCache();

  }

  private void closeMCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        cache.close();
        return null;
      }
    });
  }

  private void closeMCacheAll() {
    closeMCache(this.vm0);
    closeMCache(this.vm1);
    closeMCache(this.vm2);
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);
    closeMCacheAll();
    super.tearDown2();
  }

  private class CreateTableUsingFE extends FunctionAdapter implements Declarable {

    @Override
    public void init(Properties props) {

    }

    @Override
    public void execute(FunctionContext context) {
      MCache mCache = MCacheFactory.getAnyInstance();
      MTableDescriptor tableDescriptor = new MTableDescriptor();
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.setRedundantCopies(1);
      Admin admin = mCache.getAdmin();
      MTable table = null;
      try {
        table = admin.createTable(TABLE_NAME, tableDescriptor);
      } catch (MTableExistsException e) {
        table = mCache.getTable(TABLE_NAME);
      }
      assertEquals(table.getName(), TABLE_NAME);
      assertNotNull(table);
      context.getResultSender().lastResult(null);
    }

    @Override
    public String getId() {
      return this.getClass().getName();

    }
  }

  private void registerFunctionOn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        FunctionService.registerFunction(new CreateTableUsingFE());
        FunctionService.registerFunction(new MTablePutOPsFE());
        return null;
      }
    });
  }


  private void runFunctionFromClient(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CreateTableUsingFE fte = new CreateTableUsingFE();
        FunctionService.registerFunction(fte);
        MClientCache cache = MClientCacheFactory.getAnyInstance();
        Execution members = FunctionService.onServers(((MonarchCacheImpl) cache).getDefaultPool());

        members.execute(fte.getId());

        /* Try Accesing the same Table on Client */
        MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNotNull(table);

        return null;
      }
    });
  }

  private void verifyTableAccessFromVM(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNotNull(table);
        return null;
      }
    });
  }

  @Test
  public void testFunctionExecutionCreateTable() {
    IgnoredException.addIgnoredException(MTableExistsException.class.getName());
    registerFunctionOn(this.vm0);
    registerFunctionOn(this.vm1);
    registerFunctionOn(this.vm2);

    runFunctionFromClient(this.client1);
    verifyTableAccessFromVM(this.vm1);
    verifyTableAccessFromVM(this.vm2);
    IgnoredException.removeAllExpectedExceptions();
  }


  private void doPutsUsingFE() {
    MTablePutOPsFE tablePutOPsFE = new MTablePutOPsFE();
    FunctionService.registerFunction(tablePutOPsFE);
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    Execution members = FunctionService.onServers(((MonarchCacheImpl) cache).getDefaultPool());

    members.execute(tablePutOPsFE.getId());

  }


  private class MTablePutOPsFE extends FunctionAdapter implements Declarable {

    @Override
    public void init(Properties props) {

    }

    @Override
    public void execute(FunctionContext context) {
      MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
      for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
        Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex));
        }
        table.put(record);
      }
      context.getResultSender().lastResult(null);
    }

    @Override
    public String getId() {
      return this.getClass().getName();

    }
  }


  private void doGets() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int k = 0; k < row.size() - 1; k++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out
              .println("actualColumnName   => " + Arrays.toString(row.get(k).getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) row.get(k).getColumnValue())) {
          System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
          System.out.println(
              "actualValue    => " + Arrays.toString((byte[]) row.get(k).getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex++;
      }
    }
  }

  public void doVerifyGetsOnAllVMs(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
          Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
          Row result = table.get(get);
          assertEquals(NUM_OF_COLUMNS + 1, result.size());
          assertFalse(result.isEmpty());
          int columnIndex = 0;
          List<Cell> row = result.getCells();
          for (int k = 0; k < row.size() - 1; k++) {
            Assert.assertNotEquals(10, columnIndex);
            byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
            byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

            if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
              System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
              System.out
                  .println("actualColumnName   => " + Arrays.toString(row.get(k).getColumnName()));
              Assert.fail("Invalid Values for Column Name");
            }
            if (!Bytes.equals(exptectedValue, (byte[]) row.get(k).getColumnValue())) {
              System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
              System.out.println(
                  "actualValue    => " + Arrays.toString((byte[]) row.get(k).getColumnValue()));
              Assert.fail("Invalid Values for Column Value");
            }
            columnIndex++;
          }
        }

        return null;
      }
    });
  }

  @Test
  public void testMTableOpsThroughFE() {
    IgnoredException.addIgnoredException(MTableExistsException.class.getName());
    registerFunctionOn(this.vm0);
    registerFunctionOn(this.vm1);
    registerFunctionOn(this.vm2);

    runFunctionFromClient(this.client1);
    verifyTableAccessFromVM(this.vm1);
    verifyTableAccessFromVM(this.vm2);

    /* Let us put some data from local client */
    doPutsUsingFE();
    doVerifyGetsOnAllVMs(this.vm0);
    doVerifyGetsOnAllVMs(this.vm1);
    doVerifyGetsOnAllVMs(this.vm2);

    doGets();
    IgnoredException.removeAllExpectedExceptions();
  }

}
