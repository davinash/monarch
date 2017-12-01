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
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.coprocessor.MCoprocessorContext;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MWrongTableRegionException;
import org.apache.logging.log4j.Logger;
import static org.junit.Assert.*;
import java.util.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class MTableRegionDunitTest extends MTableDUnitHelper {
  public MTableRegionDunitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  private final static int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "MTableRegionDunitTest";
  private final String KEY_PREFIX = "KEY";
  private final String VALUE_PREFIX = "VALUE";
  private final static int NUM_OF_ROWS = 10;
  private final static String COLUMN_NAME_PREFIX = "COLUMN";
  private final int LATEST_TIMESTAMP = 300;
  private final int MAX_VERSIONS = 5;
  private final int TABLE_MAX_VERSIONS = 7;
  private static final String COPROCESSOR_CLASS_NAME =
      "io.ampool.monarch.table.MTableRegionDunitTest$MTableRegionOpsFn";


  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
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

  public void createTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
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

  public static class MTableRegionOpsFn extends MCoprocessor {

    public Object doRegionOps(MCoprocessorContext context) {
      MTableRegion tableRegion = context.getMTableRegion();
      assertNotNull(tableRegion);
      MExecutionRequest request = context.getRequest();
      Map<String, Object> arguments = (Map<String, Object>) request.getArguments();

      byte[] keyOutOfRange = (byte[]) arguments.get("KEY_OUT_OF_RANGE");
      assertNotNull(keyOutOfRange);

      List<byte[]> keys = (List<byte[]>) arguments.get("ACTUAL_KEYS");
      assertNotNull(keys);

      byte[] validKey = (byte[]) arguments.get("KEY_IN_RANGE");

      assertEquals(NUM_OF_ROWS, tableRegion.size());

      verifyPutOperation(keyOutOfRange, tableRegion, validKey);
      System.out.println("MTableRegionOpsFn.run.verifyPutOperation");
      verifyGetOperation(keyOutOfRange, tableRegion, validKey);
      System.out.println("MTableRegionOpsFn.run.verifyGetOperation");
      verifyDeleteOperation(keyOutOfRange, tableRegion, validKey);
      System.out.println("MTableRegionOpsFn.run.verifyDeleteOperation");

      assertEquals(NUM_OF_ROWS - 1, tableRegion.size());
      System.out.println("MTableRegionOpsFn.run.size");

      return null;
    }

    private void verifyDeleteOperation(byte[] keyOutOfRange, MTableRegion tableRegion,
        byte[] validKey) {

      Delete record = new Delete(keyOutOfRange);
      Exception exception = null;
      try {
        tableRegion.delete(record);
      } catch (MWrongTableRegionException mtre) {
        exception = mtre;
      }
      assertTrue(exception instanceof MWrongTableRegionException);

      record = new Delete(validKey);
      exception = null;
      try {
        tableRegion.delete(record);
      } catch (MWrongTableRegionException mtre) {
        mtre.printStackTrace();
        exception = mtre;
      }
      assertNull(exception);
    }

    private void verifyGetOperation(byte[] keyOutOfRange, MTableRegion tableRegion,
        byte[] validKey) {

      Get record = new Get(keyOutOfRange);
      Exception exception = null;
      try {
        tableRegion.get(record);
      } catch (MWrongTableRegionException mtre) {
        exception = mtre;
      }
      assertTrue(exception instanceof MWrongTableRegionException);

      record = new Get(validKey);
      exception = null;
      try {
        tableRegion.get(record);
      } catch (MWrongTableRegionException mtre) {
        mtre.printStackTrace();
        exception = mtre;
      }
      assertNull(exception);
    }


    private void verifyPutOperation(byte[] keyOutOfRange, MTableRegion tableRegion,
        byte[] validKey) {
      // try doing put with out of range key
      Put record = new Put(keyOutOfRange);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes("OUT_OF_RANGE" + columnIndex));
      }
      Exception exception = null;
      try {
        tableRegion.put(record);
      } catch (MWrongTableRegionException mtre) {
        exception = mtre;
      }
      assertTrue(exception instanceof MWrongTableRegionException);

      record = new Put(validKey);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes("UPDATED_VALUE" + columnIndex));
      }
      exception = null;
      try {
        tableRegion.put(record);
      } catch (MWrongTableRegionException mtre) {
        mtre.printStackTrace();
        exception = mtre;
      }
      assertNull(exception);
    }
  }

  @Test
  public void testMTableRegionOps() {
    createTableOn(this.client1);
    Map<Integer, List<byte[]>> uniformKeys = doPuts();

    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    MExecutionRequest execution = new MExecutionRequest();
    // List<Object> arguments = new ArrayList<Object>();
    Map<String, Object> arguments = new HashMap<>();

    for (int i = 0; i < table.getTableDescriptor().getTotalNumOfSplits(); i++) {
      List<byte[]> bucketKeys = uniformKeys.get(i);
      arguments.put("KEY_IN_RANGE", uniformKeys.get(i).get(0));
      if (i == table.getTableDescriptor().getTotalNumOfSplits() - 1) {
        arguments.put("KEY_OUT_OF_RANGE", uniformKeys.get(0).get(0));
      } else {
        arguments.put("KEY_OUT_OF_RANGE", uniformKeys.get(i + 1).get(0));
      }

      bucketKeys.sort(Bytes.BYTES_COMPARATOR);
      arguments.put("ACTUAL_KEYS", bucketKeys);

      execution.setArguments(arguments);

      List<Object> resultCollector = table.coprocessorService(COPROCESSOR_CLASS_NAME, "doRegionOps",
          uniformKeys.get(i).get(0), execution);
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }
}
