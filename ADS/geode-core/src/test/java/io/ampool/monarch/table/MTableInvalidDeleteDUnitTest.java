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
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.RowKeyDoesNotExistException;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MTableInvalidDeleteDUnitTest extends MTableDUnitHelper {

  protected static final Logger logger = LogService.getLogger();

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "MTableAllOpsDUnitTest";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 10;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected int TIMEOUT = 6;

  public MTableInvalidDeleteDUnitTest() {
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

  protected MTable createTable() {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }

    tableDescriptor
        .setExpirationAttributes(new MExpirationAttributes(TIMEOUT, MExpirationAction.DESTROY));
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
    return table;
  }

  @Test
  public void testInvalidDelete() throws InterruptedException {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    MTable table = createTable();
    // Since there is not data in table scan should return null
    doScanNull();

    String rowKey = "random-key";
    Get get = new Get(rowKey);
    Row row = table.get(get);
    assertTrue(row.isEmpty());
    boolean success = false;

    // Deleting non-existent row completely
    Delete delete = new Delete(rowKey);
    try {
      table.delete(delete);
    } catch (RowKeyDoesNotExistException e) {
      // Expecting RowKeyDoesNotExistException
      success = true;
    }
    assertTrue(success);
    success = false;

    try {
      table.checkAndDelete(Bytes.toBytes(rowKey), Bytes.toBytes(COLUMN_NAME_PREFIX + 1),
          Bytes.toBytes("random-value"), delete);
    } catch (RowKeyDoesNotExistException e) {
      // Expecting RowKeyDoesNotExistException
      success = true;
    }
    assertTrue(success);
    success = false;

    doScanNull();

    // Partially deleting a non-existent record
    delete = new Delete(rowKey);
    delete.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 1));
    try {
      table.delete(delete);
    } catch (RowKeyDoesNotExistException e) {
      // Expecting RowKeyDoesNotExistException
      success = true;
    }
    assertTrue(success);
    success = false;

    doScanNull();

    try {
      table.checkAndDelete(Bytes.toBytes(rowKey), Bytes.toBytes(COLUMN_NAME_PREFIX + 2),
          Bytes.toBytes("random-value"), delete);
    } catch (RowKeyDoesNotExistException e) {
      // Expecting RowKeyDoesNotExistException
      success = true;
    }
    assertTrue(success);
    success = false;

    doScanNull();

    try {
      // complete put
      table.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes(COLUMN_NAME_PREFIX + 1),
          Bytes.toBytes("random-value"), new Put(rowKey));
    } catch (RowKeyDoesNotExistException e) {
      // Expecting RowKeyDoesNotExistException
      success = true;
    }
    assertTrue(success);
    success = false;

    doScanNull();

    // partial put
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_NAME_PREFIX + 2, Bytes.toBytes("random-value"));
    try {
      table.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes(COLUMN_NAME_PREFIX + 1),
          Bytes.toBytes("random-value"), put);
    } catch (RowKeyDoesNotExistException e) {
      // Expecting RowKeyDoesNotExistException
      success = true;
    }
    assertTrue(success);
    doScanNull();

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  private void doScanNull() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    final Scanner scanner = table.getScanner(new Scan());
    assertNull(scanner.next());
    scanner.close();
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
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
  }
}
