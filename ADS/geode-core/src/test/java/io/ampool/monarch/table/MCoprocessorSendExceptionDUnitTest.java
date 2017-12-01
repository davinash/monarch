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
import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.coprocessor.MCoprocessorContext;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MCoprocessorSendExceptionDUnitTest extends MTableDUnitHelper {
  public MCoprocessorSendExceptionDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  private List<VM> allVMList = null;

  public final static int NUM_OF_COLUMNS = 10;
  public final String TABLE_NAME = "MCoprocessorSendExceptionDUnitTest";
  public final String KEY_PREFIX = "KEY";
  public final static String VALUE_PREFIX = "VALUE";
  public final static int NUM_OF_ROWS = 10;
  public final static String COLUMN_NAME_PREFIX = "COLUMN";
  public final int LATEST_TIMESTAMP = 300;
  public final int MAX_VERSIONS = 5;
  public final int TABLE_MAX_VERSIONS = 7;


  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();

    allVMList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());

    createClientCache(this.client1);
    createClientCache();
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


  protected void createTable() {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  public void createTable(final boolean ordered, final String coProcessClass) {
    MCache cache = MCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    Admin admin = cache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  protected void createTableOn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable();
        return null;
      }
    });
  }

  private void createTableOn(VM vm, final boolean ordered, final String coProcessClass) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(ordered, coProcessClass);
        return null;
      }
    });
  }


  private Map<Integer, List<byte[]>> doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);

    List<byte[]> allKeys = new ArrayList<byte[]>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> {
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    return keysForAllBuckets;
  }


  public static class MCoprocessorSendException extends MCoprocessor {
    public MCoprocessorSendException() {}

    public Object checkException(MCoprocessorContext context) {
      Exception exception = null;
      try {
        Object o = null;
        o.toString();
      } catch (NullPointerException npe) {
        throw npe;
      }
      return null;
    }

    @Override
    public String getId() {
      return this.id;
    }
  }


  @Test
  public void testMCoprocessorSendException() {
    createTable(true,
        "io.ampool.monarch.table.MCoprocessorSendExceptionDUnitTest$MCoprocessorSendException");
    Map<Integer, List<byte[]>> uniformKeys = doPuts();
    // Get the table from
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    MExecutionRequest execution = new MExecutionRequest();
    execution.setArguments(uniformKeys);

    List<Object> result = table.coprocessorService(
        "io.ampool.monarch.table.MCoprocessorSendExceptionDUnitTest$MCoprocessorSendException",
        "checkException", uniformKeys.get(0).get(0), execution);

    System.out.println("MCoprocessorSendExceptionDUnitTest.testMCoprocessorSendException OUT = "
        + result.get(0).getClass());
    assertTrue(result.get(0) instanceof NullPointerException);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);

  }
}
