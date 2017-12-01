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

import org.apache.geode.cache.execute.*;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
import org.apache.geode.internal.cache.execute.ServerRegionFunctionExecutor;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MTableBucketFunctionContextDUnitTest extends MTableDUnitHelper {
  public MTableBucketFunctionContextDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  private final int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "MTableBucketMetaDataDUnitTest";
  private final String KEY_PREFIX = "KEY";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 10;
  private final String COLUMN_NAME_PREFIX = "COLUMN";
  private final int LATEST_TIMESTAMP = 300;
  private final int MAX_VERSIONS = 5;
  private final int TABLE_MAX_VERSIONS = 7;

  private int port0 = -1;
  private int port1 = -1;
  private int port2 = -1;


  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    this.port0 = (int) startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    this.port1 = (int) startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    this.port2 = (int) startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);

    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    });
    System.out.println("MTableBucketMetaDataDUnitTest.tearDown2::vm0 stopped");

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    });
    System.out.println("MTableBucketMetaDataDUnitTest.tearDown2::vm1 stopped");

    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    });
    System.out.println("MTableBucketMetaDataDUnitTest.tearDown2::vm2 stopped");
    super.tearDown2();
  }

  private void createTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }


  private void createTableOn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable();
        return null;
      }
    });
  }

  private List<byte[]> doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);

    List<byte[]> allKeys = new ArrayList<byte[]>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> {
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    System.out.println("MTableBucketMetaDataDUnitTest.doPuts:KEYS =>  " + allKeys.size());

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    return allKeys;
  }

  private Object doPutFrom(VM vm) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return doPuts();
      }
    });
  }


  private class FuntionServerLocation extends FunctionAdapter implements InternalEntity {

    @Override
    public void execute(FunctionContext context) {
      RegionFunctionContextImpl rfci = (RegionFunctionContextImpl) context;
      /* assertEquals(rfci.getRegionDataOrder(), RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED); */
      context.getResultSender().lastResult(null);
    }

    @Override
    public String getId() {
      return this.getClass().getName();
    }
  }

  @Test
  public void testFunctionExecutionContext() {
    Function fsl = new FuntionServerLocation();
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2)).forEach((VM) -> {
      VM.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          FunctionService.registerFunction(fsl);
          return null;
        }
      });
    });
    createTable();
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    FunctionService.registerFunction(fsl);

    Execution members = FunctionService.onMTable(table).withTableSplitId(112);
    assertTrue(members instanceof ServerRegionFunctionExecutor);
    ServerRegionFunctionExecutor executor = (ServerRegionFunctionExecutor) members;
    assertNotNull(executor.getTable());
    // assertFalse( executor.getScanOrCoProcessorContext());
    assertEquals(112, executor.getTargetBucketId());

    /*
     * members = FunctionService.onMTable(table).withTableSplitId(110).forCoProcessor();
     * assertTrue(members instanceof ServerRegionFunctionExecutor); executor =
     * (ServerRegionFunctionExecutor)members; assertNotNull(executor.getTable()); assertTrue(
     * executor.getScanOrCoProcessorContext()); assertEquals(110,executor.getTargetBucketId());
     * 
     * members = FunctionService.onMTable(table).withTableSplitId(99).forScanner();
     * assertTrue(members instanceof ServerRegionFunctionExecutor); executor =
     * (ServerRegionFunctionExecutor)members; assertNotNull(executor.getTable()); assertTrue(
     * executor.getScanOrCoProcessorContext()); assertEquals(99,executor.getTargetBucketId());
     * 
     * 
     * members = FunctionService.onMTable(table).withTableSplitId(5).forScanner().forCoProcessor();
     * assertTrue(members instanceof ServerRegionFunctionExecutor); executor =
     * (ServerRegionFunctionExecutor)members; assertNotNull(executor.getTable()); assertTrue(
     * executor.getScanOrCoProcessorContext()); assertEquals(5,executor.getTargetBucketId());
     * 
     * 
     * members = FunctionService.onMTable(table).withTableSplitId(15).forCoProcessor().forScanner();
     * assertTrue(members instanceof ServerRegionFunctionExecutor); executor =
     * (ServerRegionFunctionExecutor)members; assertNotNull(executor.getTable()); assertTrue(
     * executor.getScanOrCoProcessorContext()); assertEquals(15,executor.getTargetBucketId());
     */

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);

  }

}
