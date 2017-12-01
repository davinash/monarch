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

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;

import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.IllegalColumnNameException;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import io.ampool.monarch.table.exceptions.TableInvalidConfiguration;
import io.ampool.monarch.table.exceptions.RowKeyOutOfRangeException;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.types.BasicTypes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MTableAllGetPutDunit extends MTableDUnitConfigFramework {
  public MTableAllGetPutDunit() {
    super();
  }

  @Test
  public void testSimpleGetPut() {
    String[] keys = {"005", "006", "007", "008", "009"};
    Long timestamp = 123456798l;

    runAllConfigs(new FrameworkRunnable() {
      @Override
      public void run() {
        MTable table = getTable();
        assertTrue(table.isEmpty());
        doPut(table, keys, timestamp);
        assertFalse(table.isEmpty());
        verifyGet(table, keys, timestamp);
        doVerifyGetFromClient(client1, keys, timestamp);
      }

      @Override
      public void runAfterRestart() {
        MTable table = getTable();
        assertNotNull(table);

        if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
          assertFalse(table.isEmpty());
          verifyGet(table, keys, timestamp);
          doVerifyGetFromClient(client1, keys, timestamp);
        } else {
          assertTrue(table.isEmpty());
        }
      }
    });
  }

  private void doPut(MTable table, String[] keys, Long timestamp) {
    // Simple Put
    Put put = new Put(keys[0]);
    put.addColumn(COLUMNNAME_PREFIX + 1, Bytes.toBytes(VALUE_PREFIX + 1));
    table.put(put);

    // Put with timestamp
    put = new Put(keys[1]);
    put.setTimeStamp(timestamp);
    put.addColumn(COLUMNNAME_PREFIX + 1, Bytes.toBytes(VALUE_PREFIX + 0));
    try {
      table.put(put);
    } catch (Throwable t) {
      // TableInvalidConfiguration("Setting timestamp is allowed only for ORDERED_VERSIONED type
      // table");
      assertTrue(t.getMessage(), t instanceof TableInvalidConfiguration);
      assertEquals("Setting timestamp is allowed only for ORDERED_VERSIONED type table",
          t.getMessage());
    }

    // Updating speific column value
    put = new Put(keys[1]);
    put.setTimeStamp(timestamp);
    put.addColumn(COLUMNNAME_PREFIX + 1, Bytes.toBytes(VALUE_PREFIX + 1));
    try {
      table.put(put);
    } catch (Throwable t) {
      // TableInvalidConfiguration("Setting timestamp is allowed only for ORDERED_VERSIONED type
      // table");
      assertTrue(t.getMessage(), t instanceof TableInvalidConfiguration);
      assertEquals("Setting timestamp is allowed only for ORDERED_VERSIONED type table",
          t.getMessage());
    }

    // Update specific version adding column value
    put = new Put(keys[1]);
    put.setTimeStamp(timestamp);
    put.addColumn(COLUMNNAME_PREFIX + 2, Bytes.toBytes(VALUE_PREFIX + 2));
    try {
      table.put(put);
    } catch (Throwable t) {
      // TableInvalidConfiguration("Setting timestamp is allowed only for ORDERED_VERSIONED type
      // table");
      assertTrue(t.getMessage(), t instanceof TableInvalidConfiguration);
      assertEquals("Setting timestamp is allowed only for ORDERED_VERSIONED type table",
          t.getMessage());
    }

    // put with empty byte[]
    try {
      put = new Put(new byte[] {});
    } catch (Throwable t) {
      // IllegalStateException("Row Key cannot be 0 size");
      assertTrue(t.getMessage(), t instanceof IllegalStateException);
      assertEquals("Row Key cannot be 0 size", t.getMessage());
    }

    // put with null
    try {
      put = new Put((byte[]) null);
    } catch (Throwable t) {
      // IllegalStateException("Row Key cannot be null");
      assertTrue(t.getMessage(), t instanceof IllegalStateException);
      assertEquals("Row Key cannot be null", t.getMessage());
    }

    put = new Put("800");
    try {
      table.put(put);
    } catch (Throwable t) {
      // RowKeyOutOfRangeException "Row Key [] out of defined Range"
      assertTrue(t.getMessage(), t instanceof IllegalArgumentException);
      assertEquals("At-least one Column value should be added!", t.getMessage());
    }

    // null put to table
    try {
      table.put((Put) null);
    } catch (Throwable t) {
      // IllegalArgumentException("Invalid Put. operation with null values is not allowed!");
      assertTrue(t.getMessage(), t instanceof IllegalArgumentException);
      assertEquals("Invalid Put. operation with null values is not allowed!", t.getMessage());
    }

    int maxVersions = table.getTableDescriptor().getMaxVersions();
    if (maxVersions > 1) {
      // Put Max versions
      for (int i = 0; i < maxVersions; i++) {
        put = new Put(keys[2]);
        put.setTimeStamp(i + 1);
        put.addColumn(COLUMNNAME_PREFIX + 1, Bytes.toBytes(VALUE_PREFIX + i));
        table.put(put);
      }

      // Put Max + Max versions
      for (int i = 0; i < maxVersions + maxVersions; i++) {
        put = new Put(keys[3]);
        long timeStampForkey3 = i + 1;
        put.setTimeStamp(timeStampForkey3);
        put.addColumn(COLUMNNAME_PREFIX + 1, Bytes.toBytes(VALUE_PREFIX + i));
        table.put(put);
      }
    }

    // Updating a value
    put = new Put(keys[4]);
    put.addColumn(COLUMNNAME_PREFIX + 1, Bytes.toBytes(VALUE_PREFIX + 0));
    table.put(put);
    put = new Put(keys[4]);
    put.addColumn(COLUMNNAME_PREFIX + 1, Bytes.toBytes(VALUE_PREFIX + 1));
    put.addColumn(COLUMNNAME_PREFIX + 2, Bytes.toBytes(VALUE_PREFIX + 2));
    table.put(put);
  }

  private void doVerifyGetFromClient(VM vm, String[] keys, Long timestamp) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyGet(getTableFromClientCache(), keys, timestamp);
        return null;
      }
    });
  }

  private void verifyGet(MTable table, String[] keys, Long timestamp) {
    // Simple Get
    Row result = table.get(new Get(keys[0]));
    assertTrue("Result is empty", !result.isEmpty());
    assertEquals(keys[0], Bytes.toString(result.getRowId()));
    List<Cell> cells = result.getCells();
    assertNotNull(cells);
    assertTrue(cells.size() > 0);
    Cell cell = cells.get(1);
    assertNotNull(cell);
    assertEquals(new String(VALUE_PREFIX + 1), Bytes.toString((byte[]) cell.getColumnValue()));

    // Get after update
    result = table.get(new Get(keys[4]));
    assertTrue("Result is empty", !result.isEmpty());
    assertEquals(keys[4], Bytes.toString(result.getRowId()));
    assertNotNull(result.getRowTimeStamp());
    cells = result.getCells();
    assertNotNull(cells);
    assertTrue(cells.size() > 0);
    cell = cells.get(1);
    assertNotNull(cell);
    assertEquals(new String(VALUE_PREFIX + 1), Bytes.toString((byte[]) cell.getColumnValue()));

    // Get with timestamp
    Get get = new Get(keys[1]);
    get.setTimeStamp(timestamp);
    try {
      result = table.get(get);
      assertTrue("Result is empty", !result.isEmpty());
      assertEquals(keys[1], Bytes.toString(result.getRowId()));
      assertEquals(timestamp, result.getRowTimeStamp());
      cells = result.getCells();
      assertNotNull(cells);
      assertTrue(cells.size() > 1);
      cell = cells.get(1);
      assertNotNull(cell);
      assertEquals(new String(VALUE_PREFIX + 1), Bytes.toString((byte[]) cell.getColumnValue()));
      cell = cells.get(2);
      assertNotNull(cell);
      assertEquals(new String(VALUE_PREFIX + 2), Bytes.toString((byte[]) cell.getColumnValue()));
    } catch (Throwable t) {
      // TableInvalidConfiguration("Setting timestamp is allowed only for ORDERED_VERSIONED type
      // table")
      assertTrue(t.getMessage(), t instanceof TableInvalidConfiguration);
      assertEquals("Setting timestamp is allowed only for ORDERED_VERSIONED type table",
          t.getMessage());
    }

    // Get with specific column
    get = new Get(keys[4]);
    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 1));
    result = table.get(get);
    assertTrue("Result is empty", !result.isEmpty());
    assertEquals(keys[4], Bytes.toString(result.getRowId()));
    assertNotNull(timestamp);
    cells = result.getCells();
    assertNotNull(cells);
    assertTrue(cells.size() > 0);
    cell = cells.get(0);
    assertNotNull(cell);
    assertEquals(new String(VALUE_PREFIX + 1), Bytes.toString((byte[]) cell.getColumnValue()));

    // Get with timestamp and specific column
    get = new Get(keys[1]);
    get.setTimeStamp(timestamp);
    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 1));
    try {
      result = table.get(get);
      assertTrue("Result is empty", !result.isEmpty());
      assertEquals(keys[1], Bytes.toString(result.getRowId()));
      assertEquals(timestamp, result.getRowTimeStamp());
      cells = result.getCells();
      assertNotNull(cells);
      assertTrue(cells.size() > 0);
      cell = cells.get(0);
      assertNotNull(cell);
      assertEquals(new String(VALUE_PREFIX + 1), Bytes.toString((byte[]) cell.getColumnValue()));
    } catch (Throwable t) {
      // TableInvalidConfiguration("Setting timestamp is allowed only for ORDERED_VERSIONED type
      // table")
      assertTrue(t.getMessage(), t instanceof TableInvalidConfiguration);
      assertEquals("Setting timestamp is allowed only for ORDERED_VERSIONED type table",
          t.getMessage());
    }

    // Get with specific column names
    get = new Get(keys[4]);
    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 1));
    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
    result = table.get(get);
    assertTrue("Result is empty", !result.isEmpty());
    assertEquals(keys[4], Bytes.toString(result.getRowId()));
    assertNotNull(timestamp);
    cells = result.getCells();
    assertNotNull(cells);
    assertTrue(cells.size() > 1);
    cell = cells.get(0);
    assertNotNull(cell);
    assertEquals(new String(VALUE_PREFIX + 1), Bytes.toString((byte[]) cell.getColumnValue()));
    cell = cells.get(1);
    assertNotNull(cell);
    assertEquals(new String(VALUE_PREFIX + 2), Bytes.toString((byte[]) cell.getColumnValue()));

    // Get with timestamp and specific column names
    get = new Get(keys[1]);
    get.setTimeStamp(timestamp);
    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 1));
    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
    try {
      result = table.get(get);
      assertTrue("Result is empty", !result.isEmpty());
      assertEquals(keys[1], Bytes.toString(result.getRowId()));
      assertEquals(timestamp, result.getRowTimeStamp());
      cells = result.getCells();
      assertNotNull(cells);
      assertTrue(cells.size() > 1);
      cell = cells.get(0);
      assertNotNull(cell);
      assertEquals(new String(VALUE_PREFIX + 1), Bytes.toString((byte[]) cell.getColumnValue()));
      cell = cells.get(1);
      assertNotNull(cell);
      assertEquals(new String(VALUE_PREFIX + 2), Bytes.toString((byte[]) cell.getColumnValue()));
    } catch (Throwable t) {
      // TableInvalidConfiguration("Setting timestamp is allowed only for ORDERED_VERSIONED type
      // table")
      assertTrue(t.getMessage(), t instanceof TableInvalidConfiguration);
      assertEquals("Setting timestamp is allowed only for ORDERED_VERSIONED type table",
          t.getMessage());
    }

    // Get with invalid rowkey - null
    try {
      get = new Get((byte[]) null);
    } catch (Throwable t) {
      // IllegalStateException("Row Key null or empty");
      assertTrue(t.getMessage(), t instanceof IllegalStateException);
      assertEquals("Row Key null or empty", t.getMessage());
    }

    // Get with invalid rowkey - empty
    try {
      get = new Get(new byte[] {});
    } catch (Throwable t) {
      // IllegalStateException("Row Key null or empty");
      assertTrue(t.getMessage(), t instanceof IllegalStateException);
      assertEquals("Row Key null or empty", t.getMessage());
    }

    // Get with non existent rowkey
    get = new Get("001");
    result = table.get(get);
    assertNotNull(result);
    assertTrue(result.isEmpty());

    // Get with non existent rowkey which is out of keyspace
    get = new Get("800");
    try {
      result = table.get(get);
    } catch (Throwable t) {
      // RowKeyOutOfRangeException "Row Key [] out of defined Range"
      assertTrue(t.getMessage(), t instanceof RowKeyOutOfRangeException);
      assertEquals("Row Key [56, 48, 48] out of defined Range", t.getMessage());
    }

    // Get with invalid timestamp
    try {
      get = new Get(keys[1]);
      get.setTimeStamp(-1);
      result = table.get(get);
      assertTrue(result.isEmpty());
    } catch (Throwable t) {
      if (t instanceof IllegalArgumentException) {
        // IllegalArgumentException("Timestamp cannot be negative");
        assertTrue(t.getMessage(), t instanceof IllegalArgumentException);
        assertEquals("Timestamp cannot be negative", t.getMessage());
      } else {
        // TableInvalidConfiguration("Setting timestamp is allowed only for ORDERED_VERSIONED type
        // table")
        assertTrue(t.getMessage(), t instanceof TableInvalidConfiguration);
        assertEquals("Setting timestamp is allowed only for ORDERED_VERSIONED type table",
            t.getMessage());
      }
    }

    // Get with invalid columname
    try {
      get = new Get(keys[1]);
      get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX));
      result = table.get(get);
      assertTrue(result.isEmpty());
    } catch (Throwable t) {
      // IllegalColumnNameException("Column is not defined in schema")
      assertTrue(t.getMessage(), t instanceof IllegalColumnNameException);
      assertEquals("Column is not defined in schema", t.getMessage());
    }

    // Get with invalid columname and timestamp
    try {
      get = new Get(keys[1]);
      get.setTimeStamp(-1);
      get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX));
      result = table.get(get);
      assertTrue(result.isEmpty());
    } catch (Throwable t) {
      if (t instanceof IllegalArgumentException) {
        // IllegalArgumentException("Timestamp cannot be negative");
        assertTrue(t.getMessage(), t instanceof IllegalArgumentException);
        assertEquals("Timestamp cannot be negative", t.getMessage());
      } else {
        // TableInvalidConfiguration("Setting timestamp is allowed only for ORDERED_VERSIONED type
        // table")
        assertTrue(t.getMessage(), t instanceof TableInvalidConfiguration);
        assertEquals("Setting timestamp is allowed only for ORDERED_VERSIONED type table",
            t.getMessage());
      }
    }

    // Getting all versions
    int maxVersions = table.getTableDescriptor().getMaxVersions();
    if (maxVersions > 1) {
      // Put Max versions
      for (int i = 0; i < maxVersions; i++) {
        get = new Get(keys[2]);
        get.setTimeStamp(i + 1);
        get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 1));
        result = table.get(get);
        assertTrue("Result is empty", !result.isEmpty());
        assertEquals(keys[2], Bytes.toString(result.getRowId()));
        assertEquals((Long) Integer.toUnsignedLong(i + 1), result.getRowTimeStamp());
        cells = result.getCells();
        assertNotNull(cells);
        assertTrue(cells.size() > 0);
        cell = cells.get(0);
        assertNotNull(cell);
        assertEquals(new String(VALUE_PREFIX + i), Bytes.toString((byte[]) cell.getColumnValue()));

        get = new Get(keys[3]);
        long timeStampForkey3 = i + 1 + maxVersions;
        get.setTimeStamp(timeStampForkey3);
        get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 1));
        result = table.get(get);
        assertTrue("Result is empty", !result.isEmpty());
        assertEquals(keys[3], Bytes.toString(result.getRowId()));
        assertEquals((Long) timeStampForkey3, result.getRowTimeStamp());
        cells = result.getCells();
        assertNotNull(cells);
        assertTrue(cells.size() > 0);
        cell = cells.get(0);
        assertNotNull(cell);
        assertEquals(new String(VALUE_PREFIX + (i + 5)),
            Bytes.toString((byte[]) cell.getColumnValue()));
      }
    }
  }

  @Test
  public void testGetAll() {

    String[] keys = {"005", "006", "007", "008", "009"};
    Long timestamp = 123456798l;

    FrameworkRunnable frameworkRunnable = new FrameworkRunnable() {
      @Override
      public void run() {
        MTable table = getTable();
        doPutForBatchGet(table, keys, timestamp);
        verifyGetAll(table, keys, timestamp);
        verifyGetAllFromClient(client1, keys, timestamp);
      }

      @Override
      public void runAfterRestart() {
        MTable table = getTable();
        if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
          verifyGetAll(table, keys, timestamp);
          verifyGetAllFromClient(client1, keys, timestamp);
        }
      }
    };

    // runConfig(3,frameworkRunnable,true,false);

    runAllConfigs(frameworkRunnable);

  }

  private void doPutForBatchGet(MTable table, String[] keys, Long timestamp) {
    List<Put> putList = new ArrayList<>();
    Put put = null;
    for (int j = 0; j < keys.length; j++) {
      put = new Put(keys[j]);
      if (j % 2 == 0 && table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
        for (int i = 0; i < NUM_COLUMNS; i++) {
          put.addColumn(COLUMNNAME_PREFIX + i, Bytes.toBytes(VALUE_PREFIX + timestamp + i));
        }
        // Not setting for j =1 and 3
        put.setTimeStamp(timestamp);
      } else {
        for (int i = 0; i < NUM_COLUMNS; i++) {
          put.addColumn(COLUMNNAME_PREFIX + i, Bytes.toBytes(VALUE_PREFIX + i));
        }
      }
      table.put(put);
      putList.add(put);
    }

    // Invalid PutAll having same rowkey twice
    // TODO In 2.0 we have removed this check of adding same key twice should fail
    // so disabling this code block
    // List<MPut> invalidPutList = new ArrayList<>(putList);
    // // Adding key[0] twice
    // invalidPutList.add(new MPut(keys[0]));
    //// try {
    //// table.put(invalidPutList);
    //// } catch (Throwable t){
    //// System.out.println("MTableAllGetPutDunit.doPutForBatchGet :: " + "I should come here");
    //// t.printStackTrace();
    //// //IllegalArgumentException "Cannot add put with same rowkey twice in batch put"
    //// assertTrue(t instanceof IllegalArgumentException);
    //// assertEquals("Cannot add put with same rowkey twice in batch put", t.getMessage());
    //// }

    // Valid PutAll
    table.put(putList);

    if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
      // Update key[0]
      put = new Put(keys[0]);
      for (int i = 0; i < NUM_COLUMNS; i++) {
        put.addColumn(COLUMNNAME_PREFIX + i, Bytes.toBytes(VALUE_PREFIX + i));
      }
      put.setTimeStamp(timestamp);
      table.put(put);

      // Update key[2]
      put = new Put(keys[2]);
      for (int i = 0; i < NUM_COLUMNS; i++) {
        put.addColumn(COLUMNNAME_PREFIX + i, Bytes.toBytes(VALUE_PREFIX + i));
      }
      put.setTimeStamp(timestamp + 2);
      table.put(put);

      // Update key[4]
      put = new Put(keys[4]);
      for (int i = 0; i < NUM_COLUMNS; i++) {
        put.addColumn(COLUMNNAME_PREFIX + i, Bytes.toBytes(VALUE_PREFIX + i));
      }
      put.setTimeStamp(timestamp + 4);
      table.put(put);
    }
  }

  private void verifyGetAll(MTable table, String[] keys, Long timestamp) {
    // 0 : timestamp
    // 2 : timestamp + 2
    // 4 : timestamp + 4

    List<Get> getList = new ArrayList<>();
    Get get = null;
    Row result = null;
    Row[] results = null;
    List<Cell> cells = null;

    // Simple GetAll

    for (int i = 0; i < keys.length; i++) {
      String key = keys[i];
      get = new Get(key);
      getList.add(get);
    }

    // Invalid GetAll having same rowkey twice
    List<Get> invalidGetList = new ArrayList<>(getList);

    // Adding key[0] twice
    invalidGetList.add(new Get(keys[0]));
    try {
      table.get(invalidGetList);
    } catch (Throwable t) {
      // IllegalArgumentException "Cannot add get with same rowkey twice in batch get"
      assertTrue(t instanceof IllegalArgumentException);
      assertEquals("Cannot add get with same rowkey twice in batch get", t.getMessage());
    }

    // Valid simple GetAll
    results = table.get(getList);
    assertNotNull(results);
    assertTrue(results.length > 0);
    for (int i = 0; i < results.length; i++) {
      if (i % 2 == 0 && table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
        System.out.println("MTableAllGetPutDunit.verifyGetAll :: " + "iter: " + i);
        cells = assertResultAndReturnCells(results[i], keys[i], (Long) (timestamp + i));
      } else {
        cells = assertResultAndReturnCells(results[i], keys[i]);
      }
      cells = results[i].getCells();
      assertAllCellsValues(cells);
    }

    // GetAll with null input
    try {
      table.get((ArrayList) null);
    } catch (Throwable t) {
      // IllegalArgumentException Invalid Gets. Operation with 0 size keys is not allowed
      assertTrue(t instanceof IllegalArgumentException);
      assertEquals("Invalid Get. operation with null values is not allowed", t.getMessage());
    }

    // GetAll with empty list
    try {
      table.get(new ArrayList<Get>() {});
    } catch (Throwable t) {
      // IllegalArgumentException Invalid Gets. Operation with 0 size keys is not allowed
      assertTrue(t instanceof IllegalArgumentException);
      assertEquals("Invalid Gets. Operation with 0 size keys is not allowed", t.getMessage());
    }

    // GetAll with timestamp
    getList = new ArrayList<>();
    for (int i = 0; i < keys.length; i++) {
      String key = keys[i];
      get = new Get(key);
      if (i % 2 == 0 && table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
        get.setTimeStamp(timestamp + i);
      }
      getList.add(get);
    }
    try {
      results = table.get(getList);
      assertNotNull(results);
      assertTrue(results.length > 0);
      for (int i = 0; i < results.length; i++) {
        if (i % 2 == 0
            && table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
          cells = assertResultAndReturnCells(results[i], keys[i], (Long) (timestamp + i));
        } else {
          cells = assertResultAndReturnCells(results[i], keys[i]);
        }
        assertAllCellsValues(cells);
      }
    } catch (Throwable t) {
      // TableInvalidConfiguration("Setting timestamp is allowed only for ORDERED_VERSIONED type
      // table")
      assertTrue(t.getMessage(), t instanceof TableInvalidConfiguration);
      assertEquals("Setting timestamp is allowed only for ORDERED_VERSIONED type table",
          t.getMessage());
    }

    // GetAll with columnnames
    getList = new ArrayList<>();
    for (int i = 0; i < keys.length; i++) {
      String key = keys[i];
      get = new Get(key);
      get.addColumn(COLUMNNAME_PREFIX + 1);
      getList.add(get);
    }
    results = table.get(getList);
    assertNotNull(results);
    assertTrue(results.length > 0);
    for (int i = 0; i < results.length; i++) {
      if (i % 2 == 0 && table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
        cells = assertResultAndReturnCells(results[i], keys[i], (Long) (timestamp + i));
      } else {
        cells = assertResultAndReturnCells(results[i], keys[i]);
      }
      assertNotNull(cells);
      assertEquals(1, cells.size());
      Cell cell = cells.get(0);
      assertCellValues(cell, 1);
    }


    // GetAll with both timestamp and columnnames
    getList = new ArrayList<>();
    for (int i = 0; i < keys.length; i++) {
      String key = keys[i];
      get = new Get(key);
      get.addColumn(COLUMNNAME_PREFIX + 1);
      if (i % 2 == 0 && table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
        get.setTimeStamp(timestamp + i);
      }
      getList.add(get);
    }
    try {
      results = table.get(getList);
      assertNotNull(results);
      assertTrue(results.length > 0);
      for (int i = 0; i < results.length; i++) {
        if (i % 2 == 0
            && table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
          cells = assertResultAndReturnCells(results[i], keys[i], (Long) (timestamp + i));
        } else {
          cells = assertResultAndReturnCells(results[i], keys[i]);
        }
        assertNotNull(cells);
        assertEquals(1, cells.size());
        Cell cell = cells.get(0);
        assertCellValues(cell, 1);
      }
    } catch (Throwable t) {
      // TableInvalidConfiguration("Setting timestamp is allowed only for ORDERED_VERSIONED type
      // table")
      assertTrue(t.getMessage(), t instanceof TableInvalidConfiguration);
      assertEquals("Setting timestamp is allowed only for ORDERED_VERSIONED type table",
          t.getMessage());
    }

    // // GetAll with same key twice
    // getList = new ArrayList<>();
    // for (int i = 0; i < keys.length + 1; i++) {
    // // key[0] is added twice
    // String key = keys[i % keys.length];
    // get = new MGet(key);
    // getList.add(get);
    // }
    // results = table.get(getList);
    // assertNotNull(results);
    // assertTrue(results.length > 0);
    // for (int i = 0; i < results.length; i++) {
    // if (i % 2 == 0 && table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED)
    // {
    // cells = assertResultAndReturnCells(results[i], keys[i% keys.length], (Long) (timestamp + i));
    // } else {
    // cells = assertResultAndReturnCells(results[i], keys[i% keys.length]);
    // }
    // assertAllCellsValues(cells);
    // }

    //
    // // GetAll with same key twice with different columns
    // getList = new ArrayList<>();
    // get = new MGet(keys[0]);
    // get.addColumn(COLUMNNAME_PREFIX + 0);
    // get.addColumn(COLUMNNAME_PREFIX + 1);
    // getList.add(get);
    //
    // get = new MGet(keys[1]);
    // getList.add(get);
    //
    // get = new MGet(keys[0]);
    // get.addColumn(COLUMNNAME_PREFIX + 2);
    // get.addColumn(COLUMNNAME_PREFIX + 3);
    // getList.add(get);
    //
    // results = table.get(getList);
    // assertNotNull(results);
    // assertTrue(results.length > 0);
    // result = results[0];
    // cells = assertResultAndReturnCells(result, keys[0]);
    // assertCellValues(cells.get(0), 0);
    // assertCellValues(cells.get(1), 1);
    //
    // result = results[1];
    // cells = assertResultAndReturnCells(result, keys[1]);
    // assertAllCellsValues(cells);
    //
    // result = results[2];
    // cells = assertResultAndReturnCells(result, keys[0]);
    // assertCellValues(cells.get(0), 2);
    // assertCellValues(cells.get(1), 3);
    //
    //
    // // GetAll with same key twice with different timestamp
    // if(table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
    // getList = new ArrayList<>();
    // get = new MGet(keys[2]);
    // get.setTimeStamp(timestamp);
    // getList.add(get);
    //
    // get = new MGet(keys[3]);
    // getList.add(get);
    //
    // get = new MGet(keys[2]);
    // get.setTimeStamp(timestamp + 2);
    // getList.add(get);
    //
    // results = table.get(getList);
    // assertNotNull(results);
    // assertTrue(results.length > 0);
    // result = results[0];
    // cells = assertResultAndReturnCells(result, keys[2]);
    // assertNotNull(cells);
    // assertEquals(NUM_COLUMNS, cells.size());
    // for (int j = 0; j < cells.size(); j++) {
    // MCell cell = cells.get(j);
    // assertNotNull(cell);
    // assertEquals(new ByteArrayKey(cell.getColumnName()),
    // new ByteArrayKey(Bytes.toBytes(COLUMNNAME_PREFIX + j)));
    // assertEquals(new ByteArrayKey((byte[]) cell.getColumnValue()),
    // new ByteArrayKey(Bytes.toBytes(VALUE_PREFIX + timestamp + j)));
    // }
    //
    // result = results[1];
    // cells = assertResultAndReturnCells(result, keys[3]);
    // assertAllCellsValues(cells);
    //
    // result = results[2];
    // cells = assertResultAndReturnCells(result, keys[2]);
    // assertAllCellsValues(cells);
    // }

    // GetAll with invalid rowkeys
    getList = new ArrayList<>();
    get = new Get("015");
    getList.add(get);

    get = new Get("016");
    getList.add(get);

    get = new Get("013");
    getList.add(get);

    results = table.get(getList);
    assertNotNull(results);
    assertTrue(results.length > 0);
    for (Row row : results) {
      assertTrue(row.isEmpty());
    }

    // GetAll with valid and invalid rowkeys
    getList = new ArrayList<>();
    get = new Get(keys[2]);
    getList.add(get);

    get = new Get("012");
    getList.add(get);

    get = new Get(keys[3]);
    getList.add(get);
    results = table.get(getList);
    assertNotNull(results);
    assertTrue(results.length > 0);
    assertAllCellsValues(assertResultAndReturnCells(results[0], keys[2]));
    assertTrue(results[1].isEmpty());
    assertAllCellsValues(assertResultAndReturnCells(results[2], keys[3]));

    // GetAll with null value
    getList = new ArrayList<>();
    getList.add(null);
    try {
      table.get(getList);
    } catch (Throwable t) {
      // IllegalArgumentException Invalid Get operation with null value found in list of gets
      assertTrue(t.getMessage(), t instanceof IllegalArgumentException);
      assertEquals("Invalid Get operation with null value found in list of gets", t.getMessage());
    }
  }

  private void assertAllCellsValues(List<Cell> cells) {
    assertNotNull(cells);
    assertEquals(NUM_COLUMNS + 1 /* GEN-1696 */, cells.size());
    for (int j = 0; j < cells.size() - 1 /* GEN- 1696 */; j++) {
      Cell cell = cells.get(j);
      assertCellValues(cell, j);
    }
  }

  private List<Cell> assertResultAndReturnCells(Row result, String key) {
    return assertResultAndReturnCells(result, key, null);
  }

  private List<Cell> assertResultAndReturnCells(Row result, String key, Long timestamp) {
    assertNotNull(result);
    assertFalse(result.isEmpty());
    assertEquals(new ByteArrayKey(Bytes.toBytes(key)), new ByteArrayKey(result.getRowId()));
    assertNotNull(result.getRowTimeStamp());
    if (timestamp != null) {
      assertEquals(timestamp, result.getRowTimeStamp());
    }
    List<Cell> cells = result.getCells();
    assertNotNull(cells);
    assertTrue(cells.size() > 0);
    return cells;
  }

  private void assertCellValues(Cell cell, int columnNo) {
    assertNotNull(cell);
    assertEquals(new ByteArrayKey(cell.getColumnName()),
        new ByteArrayKey(Bytes.toBytes(COLUMNNAME_PREFIX + columnNo)));
    assertEquals(new ByteArrayKey(Bytes.toBytes(VALUE_PREFIX + columnNo)),
        new ByteArrayKey((byte[]) cell.getColumnValue()));
  }

  private void verifyGetAllFromClient(VM vm, String[] keys, Long timestamp) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = getTableFromClientCache();
        assertNotNull(table);
        verifyGetAll(table, keys, timestamp);
        return null;
      }
    });
  }

  @Test
  public void testGetPutWithEmptyKey() {
    java.util.function.BiConsumer<MTable, String> verifyGet = (table, key) -> {
      Exception e1 = null;

      System.out.println("MTableAllGetPutDunit.run: MGet key is:" + key);
      for (int j = 0; j <= 1; j++) {
        Row result = null;
        try {
          Get get = null;
          if (j == 0) {
            get = new Get(key);
          } else {
            if (key != null) {
              get = new Get(Bytes.toBytes(key));
            } else {
              get = new Get((byte[]) null);
            }
          }
          result = table.get(new Get(key));
          assertTrue(!result.isEmpty());
        } catch (Exception e2) {
          e1 = e2;
        }
        if (key == null || key.length() == 0) {
          System.out.println("Exception caught: " + e1.getClass().toString());
          assertTrue(e1 != null);
          assertTrue(e1 instanceof IllegalStateException);
        } else {
          assertTrue(e1 == null);
          assertEquals(key, Bytes.toString(result.getRowId()));
          List<Cell> cells = result.getCells();
          assertNotNull(cells);
          assertTrue(cells.size() > 0);
          for (int k = 0; k < NUM_COLUMNS; k++) {
            Cell cell = cells.get(k);
            assertNotNull(cell);
            assertEquals(new String(VALUE_PREFIX + k),
                Bytes.toString((byte[]) cell.getColumnValue()));
          }
        }
      }
    };

    final String[] keys = {"", "006", null, "008", "", null};

    runAllConfigs(new FrameworkRunnable() {

      @Override
      public void run() {
        MTable table = getTable();
        for (int i = 0; i < keys.length; i++) {
          final String key = keys[i];
          System.out.println(
              "MTableAllGetPutDunit.run :: ===========================================================");
          System.out.println("MTableAllGetPutDunit.run :: Running for key #" + i + " : "
              + (key == null ? "null" : key));
          System.out.println(
              "MTableAllGetPutDunit.run :: ===========================================================");

          Exception e = null;

          for (int j = 0; j <= 1; j++) {
            try {
              Put put = null;
              if (j == 0) {
                put = new Put(key);
              } else {
                if (key != null) {
                  put = new Put(Bytes.toBytes(key));
                } else {
                  put = new Put((byte[]) null);
                }
              }
              for (int k = 0; k < NUM_COLUMNS; k++) {
                put.addColumn(COLUMNNAME_PREFIX + k, Bytes.toBytes(VALUE_PREFIX + k));
              }
              System.out.println("MTableAllGetPutDunit.run: key in MPut is:" + key);
              table.put(put);
            } catch (Exception e1) {
              e = e1;
            }
            if (key == null || key.length() == 0) {
              System.out.println("Exception caught: " + e.getClass().toString());
              assertTrue(e != null);
              assertTrue(e instanceof IllegalStateException);
            } else {
              assertTrue(e == null);
            }
            verifyGet.accept(table, key);
          }
        }
      }

      @Override
      public void runAfterRestart() {
        MTable table = getTable();
        for (int i = 0; i < keys.length; i++) {
          final String key = keys[i];

          System.out.println(
              "MTableAllGetPutDunit.runAfterRestart :: ===========================================================");
          System.out.println("MTableAllGetPutDunit.runAfterRestart :: Running for key #" + i + " : "
              + (key == null ? "null" : key));
          System.out.println(
              "MTableAllGetPutDunit.runAfterRestart :: ===========================================================");

          if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
            verifyGet.accept(table, key);
          }
        }
      }
    });
  }

  @Test
  public void testGetAllPutAll() {

    /* Empty puts: Verify that all columns are null */
    java.util.function.BiConsumer<MTable, String[]> verifyGet1 = (table, keys) -> {
      Exception e = null;
      Get get = null;
      for (String key : keys) {
        try {
          get = new Get(key);
        } catch (Exception e1) {
          e = e1;
        }

        if (key == null || key.length() == 0) {
          assertTrue(e != null);
          assertTrue(e instanceof IllegalStateException);
        } else {
          assertTrue(e == null);
          Row result = table.get(get);
          List<Cell> cells = result.getCells();
          assertTrue(cells.size() == 0);
          // for (Cell mcell : cells) {
          for (int i = 0; i < cells.size() - 1; i++) {
            assertTrue(cells.get(i).getColumnValue() == null);

          }
        }
      }

      // verify get all
      List<Get> gets = new ArrayList<>();
      for (String key : keys) {
        e = null;
        try {
          get = new Get(key);
        } catch (Exception e1) {
          e = e1;
        }

        if (key == null || key.length() == 0) {
          assertTrue(e != null);
          assertTrue(e instanceof IllegalStateException);
        } else {
          assertTrue(e == null);
          gets.add(get);
        }
      }

      Row[] results = table.get(gets);
      for (Row result : results) {
        List<Cell> cells = result.getCells();
        assertTrue(cells.size() == 0);
        // for (Cell mcell : cells) {
        for (int i = 0; i < cells.size() - 1; i++) {
          assertTrue(cells.get(i).getColumnValue() == null);
        }
      }
    };

    /* Valid puts: Verify that all columns have valid data */
    java.util.function.BiConsumer<MTable, String[]> verifyGet2 = (table, keys) -> {
      Exception e = null;
      Get get = null;
      for (String key : keys) {
        try {
          get = new Get(key);
          // TODO: add setRowkey and use here.
        } catch (Exception e1) {
          e = e1;
        }

        if (key == null || key.length() == 0) {
          assertTrue(e != null);
          assertTrue(e instanceof IllegalStateException);
        } else {
          assertTrue(e == null);
          Row result = table.get(get);
          // getCurrentTableType
          List<Cell> cells = result.getCells();
          assertTrue(cells.size() > 0);
          for (int j = 0; j < cells.size() - 1 /* GEN-1696 */; j++) {
            assertTrue((VALUE_PREFIX + j)
                .compareTo(Bytes.toString((byte[]) cells.get(j).getColumnValue())) == 0);
          }
        }
      }

      // verify get all
      List<Get> gets = new ArrayList<>();
      for (String key : keys) {
        e = null;
        try {
          get = new Get(key);
        } catch (Exception e1) {
          e = e1;
        }

        if (key == null || key.length() == 0) {
          assertTrue(e != null);
          assertTrue(e instanceof IllegalStateException);
        } else {
          assertTrue(e == null);
          gets.add(get);
        }
      }

      Row[] results = table.get(gets);
      for (Row result : results) {
        List<Cell> cells = result.getCells();
        assertTrue(cells.size() > 0);
        for (int j = 0; j < cells.size() - 1 /* GEN- 1696 */; j++) {
          assertTrue((VALUE_PREFIX + j)
              .compareTo(Bytes.toString((byte[]) cells.get(j).getColumnValue())) == 0);
        }
      }
    };

    final String[] keys = {"006", "008", "", null};

    runAllConfigs(new FrameworkRunnable() {

      @Override
      public void run() {
        MTable table = getTable();
        Exception e = null;
        List<Put> putList = new ArrayList<Put>();
        // null put
        e = null;
        putList.clear();
        putList.add(null);
        putList.add(null);
        try {
          table.put(putList);
        } catch (Exception e1) {
          e = e1;
        }

        assertTrue(e != null);
        assertTrue(e instanceof IllegalArgumentException);

        // no puts in list
        putList.clear();
        e = null;
        try {
          table.put(putList);
        } catch (Exception e1) {
          e = e1;
        }

        assertTrue(e != null);
        assertTrue(e instanceof IllegalArgumentException);

        putList.clear();

        for (int i = 0; i < keys.length; i++) {
          final String key = keys[i];

          // Valid puts
          e = null;
          try {
            Put put = new Put(key);
            for (int j = 0; j < NUM_COLUMNS; j++) {
              put.addColumn(COLUMNNAME_PREFIX + j, Bytes.toBytes(VALUE_PREFIX + j));
            }
            System.out.println("MTableAllGetPutDunit.run: key in MPut is:" + key);
            putList.add(put);

          } catch (Exception e1) {
            e = e1;
          }
          if (key == null || key.length() == 0) {
            System.out.println("Exception caught: " + e.getClass().toString());
            assertTrue(e != null);
            assertTrue(e instanceof IllegalStateException);
          } else {
            assertTrue(e == null);
          }

        }
        e = null;
        try {
          table.put(putList);
        } catch (Exception e1) {
          e = e1;
        }

        // The put should not fail.
        assertTrue(e == null);
        verifyGet2.accept(table, keys);

        Iterator<Row> iterator = table.getScanner(new Scan()).iterator();
        while (iterator.hasNext()) {
          Row next = iterator.next();
          table.delete(new Delete(next.getRowId()));
        }

        putList.clear();

        for (int i = 0; i < keys.length; i++) {
          final String key = keys[i];
          // Empty put

          e = null;
          try {
            Put put1 = new Put(key);
            // No columns in Put(Empty Put)
            System.out.println("MTableAllGetPutDunit.run: key in MPut is:" + key);
            putList.add(put1);

          } catch (Exception e1) {
            e = e1;
          }
          if (key == null || key.length() == 0) {
            System.out.println("Exception caught: " + e.getClass().toString());
            assertTrue(e != null);
            assertTrue(e instanceof IllegalStateException);
          } else {
            assertTrue(e == null);
          }
        }
        e = null;
        try {
          table.put(putList);
        } catch (Exception e1) {
          e = e1;
        }

        // The put should not fail.
        assertTrue(e instanceof IllegalArgumentException);
        verifyGet1.accept(table, keys);

        putList.clear();

        for (int i = 0; i < keys.length; i++) {
          final String key = keys[i];

          // Valid puts
          e = null;
          try {
            Put put = new Put(key);
            for (int j = 0; j < NUM_COLUMNS; j++) {
              put.addColumn(COLUMNNAME_PREFIX + j, Bytes.toBytes(VALUE_PREFIX + j));
            }
            System.out.println("MTableAllGetPutDunit.run: key in MPut is:" + key);
            putList.add(put);

          } catch (Exception e1) {
            e = e1;
          }
          if (key == null || key.length() == 0) {
            System.out.println("Exception caught: " + e.getClass().toString());
            assertTrue(e != null);
            assertTrue(e instanceof IllegalStateException);
          } else {
            assertTrue(e == null);
          }

        }
        e = null;
        try {
          table.put(putList);
        } catch (Exception e1) {
          e = e1;
        }

        // The put should not fail.
        assertTrue(e == null);
        verifyGet2.accept(table, keys);

      }

      @Override
      public void runAfterRestart() {
        MTable table = getTable();
        if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
          verifyGet2.accept(table, keys);
        }
      }
    });
  }

  @Test
  public void testPutAllWithUpdateMerge() {

    java.util.function.BiConsumer<MTable, String> verifyGet = (table, key) -> {
      Get get = new Get(key);
      if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
        get.setTimeStamp(1);
      }
      Row result = table.get(get);
      assertTrue(result != null);
      // put with timestamp 1 will never reach the server
      if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
        // put with timestamp 1 will never reach the server : see JIRA: GEN-869
        assertTrue(result.isEmpty());
      } else {
        assertTrue(!result.isEmpty());
      }

      if (table.getTableDescriptor().getTableType() != MTableType.ORDERED_VERSIONED) {
        assertTrue(Bytes.toString(result.getRowId()).compareTo(key) == 0);

        List<Cell> cells = result.getCells();
        if (table.getTableDescriptor().getMaxVersions() > 1) {
          assertTrue(cells.size() == NUM_COLUMNS);
          // Should get half columns
          for (int i = 0; i < NUM_COLUMNS; i++) {
            Cell cell = cells.get(i);
            Object value = cell.getColumnValue();
            if (i % 2 != 0) {
              assertTrue((VALUE_PREFIX + i).compareTo(Bytes.toString((byte[]) value)) == 0);
            } else {
              assertTrue(value == null);
            }
          }
        }
      }

      get = new Get(key);
      if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
        get.setTimeStamp(2);
      }
      result = table.get(get);
      assertTrue(result != null);
      assertTrue(!result.isEmpty());
      List<Cell> cells = result.getCells();
      assertTrue(cells.size() == NUM_COLUMNS + 1 /* GEN- 1696 */);
      assertTrue(Bytes.toString(result.getRowId()).compareTo(key) == 0);

      // Should get all the columns in version 2
      for (int i = 0; i < NUM_COLUMNS - 1 /* GEN- 1696 */; i++) {
        Cell cell = cells.get(i);
        Object value = cell.getColumnValue();
        if (value == null) {
          System.out.println(Bytes.toString(cell.getColumnName()) + ":" + null);
        } else {
          System.out.println(Bytes.toString(cell.getColumnName()) + ":"
              + Bytes.toString((byte[]) cell.getColumnValue()));
        }
        if (i % 2 != 0) {
          assertTrue((VALUE_PREFIX + i).compareTo(Bytes.toString((byte[]) value)) == 0);
        } else {
          assertTrue(value == null);
        }
        //
        // if (value == null) {
        // System.out.println(Bytes.toString(cell.getColumnName()) + ":" + null );
        // } else {
        // System.out.println(Bytes.toString(cell.getColumnName()) + ":" + Bytes.toString((byte
        // [])cell.getColumnValue()) );
        // }
        // assertTrue((VALUE_PREFIX + i).compareTo(Bytes.toString((byte [])value)) == 0);
      }
    };

    final String key = "001";
    runAllConfigs(new FrameworkRunnable() {
      @Override
      public void run() {
        MTable table = getTable();
        List<Put> puts = new ArrayList<>();

        Put put = new Put(key);
        if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
          put.setTimeStamp(2);
        }
        // Set rest of the columns
        for (int i = 1; i < NUM_COLUMNS; i += 2) {
          put.addColumn(COLUMNNAME_PREFIX + i, Bytes.toBytes(VALUE_PREFIX + i));
        }
        puts.add(put);

        table.put(puts);

        verifyGet.accept(table, key);

      }

      @Override
      public void runAfterRestart() {
        MTable table = getTable();
        if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
          verifyGet.accept(table, key);
        }
      }
    });
  }

  @Test
  public void testPutAllWithUpdateMultiVersion() {

    java.util.function.BiConsumer<MTable, Pair<String[], Integer>> verifyPuts =
        (table, context) -> {
          String keys[] = context.getFirst();

          String testValuePrefix = "";


          int tableVersions = table.getTableDescriptor().getMaxVersions();
          Boolean isOrderedTable =
              table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED ? true
                  : false;
          for (int i = context.getSecond(); i >= 0
              && table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED
              && tableVersions > 0; i--, tableVersions--) {

            switch (i) {
              case 0:
                testValuePrefix = "";
                break;
              case 1:
                testValuePrefix = "U_";
                break;
              case 2:
                testValuePrefix = null;
            }

            for (String key : keys) {
              Get get = new Get(key);
              if (isOrderedTable) {
                get.setTimeStamp(i);
              }
              System.out.println("Fetching : i = " + i + " key:" + key + " with TS:" + i);
              Row result = table.get(get);
              assertTrue(Bytes.toString(result.getRowId()).compareTo(key) == 0);
              List<Cell> mcells = result.getCells();
              assertTrue(mcells.size() == NUM_COLUMNS + 1 /* GEN-1696 */);

              for (int j = 0; j < NUM_COLUMNS - 1 /* GEN-1696 */; j++) {

                Cell cell = mcells.get(j);
                Object o = cell.getColumnValue();
                if (o == null) {
                  System.out.println(Bytes.toString(cell.getColumnName()) + "=null");
                } else {
                  System.out.println(
                      Bytes.toString(cell.getColumnName()) + "=" + Bytes.toString((byte[]) o));
                }
                if (j == i && j < keys.length) {
                  if (o != null)
                    assertTrue(Bytes.toString((byte[]) o)
                        .compareTo(testValuePrefix + VALUE_PREFIX + j) == 0);
                }
              }
            }
          }
        };

    final String[] keys = {"001", "002", "009"};
    runAllConfigs(new FrameworkRunnable() {
      @Override
      public void run() {
        MTable table = getTable();
        Boolean isOrderedTable =
            table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED ? true
                : false;
        String testValuePrefix = "";
        for (int i = 0; i <= 2; i++) {
          switch (i) {
            case 0:
              testValuePrefix = "";
              break;
            case 1:
              testValuePrefix = "U_";
              break;
            case 2:
              testValuePrefix = null;
              break;
            default:
              assertTrue(false);
          }

          List<Put> putList = new ArrayList<>();
          for (String key : keys) {
            Put put = new Put(key);
            System.out.println("i = " + i + " Putting key:" + key + "with TS=" + i);
            if (isOrderedTable) {
              put.setTimeStamp(i);
            }
            if (testValuePrefix != null) {
              System.out.println("Adding column:" + COLUMNNAME_PREFIX + i + ":" + testValuePrefix
                  + VALUE_PREFIX + i);
              put.addColumn(COLUMNNAME_PREFIX + i,
                  Bytes.toBytes(testValuePrefix + VALUE_PREFIX + i));
            } else {
              System.out.println("Adding column:" + COLUMNNAME_PREFIX + i + ": null");
              put.addColumn(COLUMNNAME_PREFIX + i, null);
            }
            putList.add(put);
          }
          table.put(putList);
          verifyPuts.accept(table, new Pair<>(keys, i));
        }
      }

      @Override
      public void runAfterRestart() {
        MTable table = getTable();
        if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
          verifyPuts.accept(table, new Pair<>(keys, 2));
        }
      }
    });
  }

  @Test
  public void testPutAllInvalidInput() {
    java.util.function.BiConsumer<MTable, String[]> verifyGetTS = (table, keys) -> {
      for (int i = 0; i < keys.length; i++) {
        Get get = new Get(keys[i]);
        Exception e = null;
        if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
          try {
            get.setTimeStamp(i - 1);
          } catch (Exception e1) {
            e = e1;
          }
          if (i - 1 < 0) {
            assertNotNull(e);
            assertTrue(e instanceof IllegalArgumentException);
          } else {
            assertNull(e);
          }
        }
        Row result = table.get(get);
        // System.out.println("getting Key: " + keys[i]);
        if (i - 1 < 0) {
          assertTrue(result.getRowTimeStamp() != i - 1);
        } else {
          assertTrue(!result.isEmpty());
          if (i - 1 > 0
              && table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
            assertTrue(result.getRowTimeStamp() == i - 1);
          } else {
            assertTrue(result.getRowTimeStamp() > 0);
          }
          List<Cell> cells = result.getCells();
          for (int j = 0; j < NUM_COLUMNS; j++) {
            Cell cell = cells.get(j);
            byte[] value = (byte[]) cell.getColumnValue();
            assertTrue(Bytes.toString(value).compareTo(VALUE_PREFIX + j) == 0);
          }
        }
      }
    };

    java.util.function.BiConsumer<MTable, String[]> verifyKeysMissing = (table, keys) -> {
      List<Get> gets = new ArrayList<>();
      for (int i = 0; i < keys.length; i++) {
        Get get = new Get(keys[i]);
        Exception e = null;
        Row result = table.get(get);
        // System.out.println("getting Key: " + keys[i]);
        assertTrue(result.isEmpty());
        gets.add(get);
      }

      Row[] results = table.get(gets);
      for (Row result : results) {
        assertNotNull(result);
        assertTrue(result.isEmpty());
      }
    };

    runAllConfigs(new FrameworkRunnable() {
      @Override
      public void run() {
        MTable table = getTable();
        String[] keys = {"001", "002", "003"};

        List<Put> puts = new ArrayList<>();
        // Invalid column name
        Exception e = null;
        for (int i = 0; i < keys.length; i++) {
          Put put = new Put(keys[i]);
          System.out.println("Key: " + keys[i]);
          for (int j = 0; j < NUM_COLUMNS - 1 /* GEN-1696 */; j++) {
            // add invalid col in even put
            if (i % 2 == 0) {
              System.out.println("Adding column: " + "INVAL" + COLUMNNAME_PREFIX + j);
              put.addColumn("INVAL" + COLUMNNAME_PREFIX + j, Bytes.toBytes(VALUE_PREFIX + j));
            } else {
              System.out.println("Adding column: " + COLUMNNAME_PREFIX + j);
              put.addColumn(COLUMNNAME_PREFIX + j, Bytes.toBytes(VALUE_PREFIX + j));
            }
          }
          puts.add(put);
        }
        e = null;
        try {
          table.put(puts);
        } catch (Exception e1) {
          e = e1;
        }

        assertNotNull(e);

        // verify that there are no rows in the table
        verifyKeysMissing.accept(table, keys);

        table = truncateTable(TABLENAME);
        puts.clear();


        // -ve time stamp
        for (int i = 0; i < keys.length; i++) {
          // System.out.println("Putting key: " + keys[i]);
          Put put = new Put(keys[i]);
          e = null;
          if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
            try {
              put.setTimeStamp(i - 1);
            } catch (Exception e1) {
              e = e1;
            }
            if (i - 1 < 0) {
              assertNotNull(e);
              assertTrue(e instanceof IllegalArgumentException);
            } else {
              assertNull(e);
            }
          }

          for (int j = 0; j < NUM_COLUMNS; j++) {
            put.addColumn(COLUMNNAME_PREFIX + j, Bytes.toBytes(VALUE_PREFIX + j));
          }

          puts.add(put);
        }
        table.put(puts);
        verifyGetTS.accept(table, keys);
      }

      @Override
      public void runAfterRestart() {
        MTable table = getTable();
        String[] keys = {"001", "002", "003"};
        if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
          verifyGetTS.accept(table, keys);
        }
      }
    });
  }

  @Test
  public void testSimplePutAll() {
    java.util.function.BiConsumer<String, String[]> verifyGet =
        (Serializable & BiConsumer<String, String[]>) (tableIn, keys) -> {
          MTable table = getTableFromClientCache();
          for (String key : keys) {
            Row result = table.get(new Get(key));
            assertNotNull(result);
            assertTrue(!result.isEmpty());
            List<Cell> cells = result.getCells();
            for (int i = 0; i < NUM_COLUMNS; i++) {
              Cell cell = cells.get(i);
              assertNotNull(cell);
              byte[] value = (byte[]) cell.getColumnValue();
              byte[] name = cell.getColumnName();
              assertTrue(Bytes.toString(name).compareTo(COLUMNNAME_PREFIX + i) == 0);
              assertTrue(Bytes.toString(value).compareTo(VALUE_PREFIX + i) == 0);
            }
          }
        };


    java.util.function.BiConsumer<String, String[]> verifyGetFromClient1 =
        (Serializable & BiConsumer<String, String[]>) (table, keys) -> {
          client1.invoke(new SerializableCallable() {
            @Override
            public Object call() {
              verifyGet.accept(null, keys);
              return null;
            }
          });
        };


    runAllConfigs(new FrameworkRunnable() {
      final String[] keys = {"001", "005", "009"};

      @Override
      public void run() {
        MTable table = getTable();
        List<Put> puts = new ArrayList<>();
        for (String key : keys) {
          Put put = new Put(key);
          for (int j = 0; j < NUM_COLUMNS; j++) {
            put.addColumn(COLUMNNAME_PREFIX + j, Bytes.toBytes(VALUE_PREFIX + j));
          }
          puts.add(put);
        }
        table.put(puts);
        verifyGet.accept(TABLENAME, keys);
        verifyGetFromClient1.accept(TABLENAME, keys);
      }

      @Override
      public void runAfterRestart() {
        MTable table = getTable();
        if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
          verifyGet.accept(TABLENAME, keys);
        }
      }
    });
  }

  private Object getValueForType(String type, long seed, String key) {
    long newSeed = seed + Integer.parseInt(key);
    Random rndGen = new Random(newSeed);

    int rand = rndGen.nextInt(128);
    Object retVal = null;
    if (type.compareTo("BOOLEAN") == 0) {
      if (rand % 2 == 0) {
        retVal = false;
      } else {
        retVal = true;
      }
    }
    if (type.compareTo("BYTE") == 0) {
      retVal = (byte) rand;
    }
    if (type.compareTo("SHORT") == 0) {
      retVal = (short) rand;
    }
    if (type.compareTo("INT") == 0) {
      retVal = rand;
    }
    if (type.compareTo("LONG") == 0) {
      retVal = (long) rand;
    }
    if (type.compareTo("DOUBLE") == 0) {
      retVal = (double) rand;
    }
    if (type.compareTo("FLOAT") == 0) {
      retVal = (float) rand;
    }
    if (type.compareTo("BIG_DECIMAL") == 0) {
      retVal = new BigDecimal(Integer.toString(rand));
    }
    if (type.compareTo("STRING") == 0 || type.compareTo("CHARS") == 0
        || type.compareTo("VARCHAR") == 0) {
      retVal = Integer.toString(rand);
    }
    if (type.compareTo("DATE") == 0) {
      retVal = new java.sql.Date(rand);
    }
    if (type.compareTo("TIMESTAMP") == 0) {
      retVal = new java.sql.Timestamp(rand);
    }
    if (type.compareTo("BINARY") == 0) {
      retVal = Bytes.toBytes(rand);
    }
    assertNotNull(retVal);
    return retVal;
  }

  @Test
  public void testPutallWithTypes() {
    final String[] keys = {"001", "003", "007", "009"};
    final long testSeed = System.nanoTime();

    java.util.function.BiConsumer<String, Long> verifyGets =
        (Serializable & BiConsumer) (tableIn, seed) -> {
          List<Get> gets = new ArrayList<>();
          MTable table = getTableFromClientCache();
          for (String key : keys) {
            Get get = new Get(key);
            gets.add(get);
            Row result = table.get(get);
            verifyResult(result, key, (long) seed);
          }
          Row[] rows = table.get(gets);
          for (int i = 0; i < rows.length; i++) {
            verifyResult(rows[i], keys[i], (long) seed);
          }
        };


    java.util.function.BiConsumer<String, Long> verifyGetsFromClient1 = (tableIn, seed) -> {
      client1.invoke(new SerializableCallable() {
        @Override
        public Object call() {
          verifyGets.accept(tableIn, seed);
          return null;
        }
      });
    };

    runAllConfigs(new FrameworkRunnable() {
      @Override
      public void run() {
        MTable table = getTable();
        List<Put> puts = new ArrayList<>();
        for (String key : keys) {
          Put put = new Put(key);
          for (int i = 0; i < types.length; i++) {
            put.addColumn(COLUMNNAME_PREFIX + i, getValueForType(types[i], testSeed, key));
          }
          puts.add(put);
        }
        table.put(puts);
        verifyGets.accept(TABLENAME, testSeed);
        verifyGetsFromClient1.accept(TABLENAME, testSeed);
      }

      @Override
      public void runAfterRestart() {
        MTable table = getTable();
        if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
          verifyGets.accept(TABLENAME, testSeed);
          verifyGetsFromClient1.accept(TABLENAME, testSeed);
        }
      }
    }, true, true);
  }

  @Test
  public void testWithNullAndEmptyValues() {
    final String[] keys = {"001", "002"};
    java.util.function.BiConsumer<String, Boolean> verifyGets =
        (Serializable & BiConsumer<String, Boolean>) (tableName, withTypes) -> {
          // with type is not passed

          MTable table = getTableFromClientCache();
          withTypes = table.getTableDescriptor().getAllColumnDescriptors().get(0)
              .getColumnType() == BasicTypes.BOOLEAN;
          for (String key : keys) {
            Row result = table.get(new Get(key));
            assertNotNull(result);
            assertTrue(!result.isEmpty());
            List<Cell> cells = result.getCells();
            assertTrue(cells.size() == NUM_COLUMNS + 1 /* GEN-1696 */);
            assertTrue(Bytes.toString(result.getRowId()).compareTo(key) == 0);
            for (int i = 0; i < NUM_COLUMNS - 1 /* GEN-1696 */; i++) {
              Object value = cells.get(i).getColumnValue();
              String colName = Bytes.toString(cells.get(i).getColumnName());
              if (i % 2 == 0 || withTypes) {
                assertNull(value);
              } else {
                assertEquals(0, ((byte[]) value).length);
              }
            }
          }
        };

    java.util.function.BiConsumer<String, Boolean> verifyGetsFromClient1 =
        (tableName, withTypes) -> {
          verifyGets.accept(tableName, withTypes);
        };

    for (int i = 0; i <= 1; i++) {
      System.out.println("Calling runAllConfigs for types = " + (i == 0 ? false : true));
      runAllConfigs(new FrameworkRunnable() {
        @Override
        public void run() {
          MTable table = getTable();
          List<Put> puts = new ArrayList<Put>();
          for (String key : keys) {
            Put put = new Put(key);
            if (table.getTableDescriptor().getAllColumnDescriptors().get(0)
                .getColumnType() == BasicTypes.BINARY) {
              // No types
              for (int j = 0; j < NUM_COLUMNS; j++) {
                if (j % 2 == 0) {
                  put.addColumn(COLUMNNAME_PREFIX + j, null);
                } else {
                  put.addColumn(COLUMNNAME_PREFIX + j, Bytes.toBytes(""));
                }
              }
            } else {
              // with types
              for (int j = 0; j < NUM_COLUMNS; j++) {
                if (j % 2 == 0) {
                  put.addColumn(COLUMNNAME_PREFIX + j, null);
                } else {
                  put.addColumn(COLUMNNAME_PREFIX + j, null);
                }
              }
            }
            puts.add(put);
          }
          table.put(puts);
          verifyGets.accept(TABLENAME, null);
          verifyGetsFromClient1.accept(TABLENAME, null);
        }

        @Override
        public void runAfterRestart() {
          MTable table = getTable();
          if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
            verifyGets.accept(TABLENAME, null);
            verifyGetsFromClient1.accept(TABLENAME, null);
          }
        }
      }, true, i == 0 ? false : true);
    }
  }

  private void verifyResult(Row result, String key, Long seed) {
    assertNotNull(result);
    assertTrue(!result.isEmpty());
    assertEquals(new ByteArrayKey(Bytes.toBytes(key)), new ByteArrayKey(result.getRowId()));
    assertTrue(result.getRowTimeStamp() > 0);
    List<Cell> cells = result.getCells();
    for (int i = 0; i < cells.size() - 1 /* GEN-1696 */; i++) {
      Cell cell = cells.get(i);
      assertTrue(Bytes.toString(cell.getColumnName()).compareTo(COLUMNNAME_PREFIX + i) == 0);
      Object value = cell.getColumnValue();
      // System.out.println("Incoming:" + value.toString());
      // System.out.println("Expected:" + getValueForType(cell.getColumnType().toString(), seed,
      // key).toString());
      assertTrue(types[i].compareTo((cell.getColumnType()).toString()) == 0);
      if (value instanceof byte[]) {
        assertTrue(Bytes.compareTo((byte[]) value,
            (byte[]) getValueForType(cell.getColumnType().toString(), seed, key)) == 0);
      } else {
        assertTrue(value.toString().compareTo(
            getValueForType(cell.getColumnType().toString(), seed, key).toString()) == 0);
      }
    }
  }


  @Test
  public void testGetPutWithCoprocessorWithoutTypes() {
    String UPDATE_COPROCESSOR_CLASS = "io.ampool.monarch.table.MTableUpdateCoprocessor";
    String[] keys = MTableUpdateCoprocessor.keys;

    runAllConfigs(new FrameworkRunnable() {
      private void insertRows(MTable table) {
        for (int keyIndex = 0; keyIndex < keys.length; keyIndex++) {
          Put myput1 = new Put(Bytes.toBytes(keys[keyIndex]));
          for (int i = 0; i < MTableDUnitConfigFramework.NUM_COLUMNS; i++) {
            myput1.addColumn(MTableDUnitConfigFramework.COLUMNNAME_PREFIX + i,
                Bytes.toBytes(MTableDUnitConfigFramework.VALUE_PREFIX + i));
          }
          table.put(myput1);
        }
      }

      @Override
      public void run() {
        MTable table = getTable();
        // Execute for both Ordered and UnOrdered Table.
        {
          Exception e = null;
          try {
            insertRows(table);
            verifyGets("");
            MExecutionRequest request = new MExecutionRequest();
            request.setArguments("COPROCESSOR1");
            System.out.println("=============Updating the rows first time");
            table.coprocessorService(UPDATE_COPROCESSOR_CLASS, "updateRowsUsingPut", null, null,
                request);
            table.coprocessorService(UPDATE_COPROCESSOR_CLASS, "verifyGets", null, null, request);
          } catch (MCoprocessorException cce) {
            e = cce;
            System.out.println("Exception caught in coprocessor execution");
            cce.printStackTrace();
          }
          assertNull(e);
          verifyGets("COPROCESSOR1");

          try {
            insertRows(table);
            verifyGets("");
            MExecutionRequest request = new MExecutionRequest();
            request.setArguments("COPROCESSOR2");
            System.out.println("=============Updating the rows second time");
            table.coprocessorService(UPDATE_COPROCESSOR_CLASS, "updateRowsUsingPutAll", null, null,
                request);
            table.coprocessorService(UPDATE_COPROCESSOR_CLASS, "verifyGets", null, null, request);
          } catch (MCoprocessorException cce) {
            e = cce;
            System.out.println("Exception caught in coprocessor execution");
            cce.printStackTrace();
          }
          assertNull(e);
          verifyGets("COPROCESSOR2");

          try {
            insertRows(table);
            verifyGets("");
            MExecutionRequest request = new MExecutionRequest();
            request.setArguments("COPROCESSOR3");
            System.out.println("=============Updating the rows third time");
            table.coprocessorService(UPDATE_COPROCESSOR_CLASS, "updateRowsUsingPutOnMTableRegion",
                null, null, request);
            // table.coprocessorService(UPDATE_COPROCESSOR_CLASS, "verifyGets", null, null,
            // request);
          } catch (MCoprocessorException cce) {
            e = cce;
            System.out.println("Exception caught in coprocessor execution");
            cce.printStackTrace();
          }
          assertNull(e);
          System.out.println("Calling verify get after updating third time");
          verifyGets("COPROCESSOR3");
        }
      }

      public boolean verifyGets(String updatedStr) {
        MTable table = getTable();

        MTableUpdateCoprocessor.verifyGetsCommon(table, updatedStr, "CLIENT");
        return true;
      }
    });
  }

  @Test
  public void testSetGetRowKey() {
    final String[] keys = {"001", "", null, "000", "099", "100"};
    runAllConfigs(new FrameworkRunnable() {
      private void verifyGets() {
        MTable table = getTable();
        Exception e = null;
        Get get = null;
        for (int j = 0; j < 2; j++) {
          for (int i = 0; i < keys.length; i++) {
            e = null;
            if (get == null) {
              get = new Get("test");
            }
            try {
              if (j == 0) {
                get.setRowKey(keys[i]);
              } else {
                get.setRowKey(Bytes.toBytes(keys[i]));
              }
            } catch (Exception e1) {
              e = e1;
            }
            if (keys[i] == null || keys[i].length() == 0) {
              assertNotNull(e);
            } else {
              Row result = table.get(get);
              assertNotNull(result);
              assertFalse(result.isEmpty());
              assertTrue(Bytes.toString(result.getRowId()).compareTo(keys[i]) == 0);
              List<Cell> cells = result.getCells();
              assertEquals(NUM_COLUMNS + 1 /* GEN-1696 */, cells.size());
              for (int colIndex = 0; colIndex < NUM_COLUMNS - 1 /* GEN-1696 */; colIndex++) {
                Cell cell = cells.get(colIndex);
                byte[] value = (byte[]) cell.getColumnValue();
                assertTrue(Bytes.toString(value).compareTo(VALUE_PREFIX + colIndex) == 0);
              }
            }
          }
        }
      }

      @Override
      public void run() {
        MTable table = getTable();
        Put put = null;
        Exception e = null;
        for (int j = 0; j < 2; j++) {
          for (int i = 0; i < keys.length; i++) {
            e = null;
            if (put == null) {
              put = new Put("test");
            }
            try {
              if (j == 0) {
                put.setRowKey(keys[i]);
              } else {
                put.setRowKey(Bytes.toBytes(keys[i]));
              }
            } catch (Exception e1) {
              e = e1;
            }
            if (keys[i] == null || keys[i].length() == 0) {
              assertNotNull(e);
            } else {
              put.clear();
              for (int colIndex = 0; colIndex < NUM_COLUMNS; colIndex++) {
                put.addColumn(COLUMNNAME_PREFIX + colIndex, Bytes.toBytes(VALUE_PREFIX + colIndex));
              }
              table.put(put);
            }
          }
          verifyGets();
        }
      }
    });
  }
}
