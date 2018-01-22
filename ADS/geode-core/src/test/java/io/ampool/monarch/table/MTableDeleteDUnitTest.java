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

import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.functions.TestDUnitBase;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@Category(MonarchTest.class)
@RunWith(Parameterized.class)
public class MTableDeleteDUnitTest {

  protected static final Logger logger = LogService.getLogger();
  protected final int NUM_OF_COLUMNS = 3;
  protected final String TABLE_NAME = "ALLOPERATION";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 10;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected final int LATEST_TIMESTAMP = 300;
  protected final int MAX_VERSIONS = 5;
  protected final int TABLE_MAX_VERSIONS = 7;

  private void doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }
  }

  public void createTable(final boolean ordered) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(2);
    tableDescriptor.setMaxVersions(5);

    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertNotNull(table);
    assertEquals(table.getName(), TABLE_NAME);
  }

  private void doGets() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertTableData(table, Collections.emptySet());
  }

  private static final TestDUnitBase testBase = new TestDUnitBase();

  private static MClientCache clientCache;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{true, 1}, //// Ordered_Versioned with max-versions 1
            {true, 5}, //// Ordered_Versioned with max-versions 5
            {false, 1}, //// UnOrdered with max-versions 1
    });
  };

  @Parameterized.Parameter(value = 0)
  public boolean isOrderedTable;
  @Parameterized.Parameter(value = 1)
  public int maxVersions;
  @Rule
  public TestName testName = new TestName();

  public MTableDeleteDUnitTest() {}

  private MTable getTable() {
    return clientCache.getTable(TABLE_NAME);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    testBase.preSetUp();
    testBase.postSetUp();
    clientCache = testBase.getClientCache();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    testBase.preTearDownCacheTestCase();
  }

  private List<Long> timestamps = Collections.emptyList();

  /**
   * before and after test methods.. executed before and after each test.. added these since @Before
   * and @After do not seem to execute as expected.
   */
  @Before
  public void setUpTest() {
    System.out.printf("%s.%s :: TableType=%s; MaxVersions= %s\n", this.getClass().getSimpleName(),
            testName.getMethodName(),
            isOrderedTable ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED, maxVersions);

    createTable(isOrderedTable);
    if (this.maxVersions > 1) {
      timestamps = addAndAssertVersions(getTable(), 5);
    } else {
      doPuts();
      doGets();
    }
  }

  @After
  public void cleanUpTest() {
    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  /**
   * Assert the data from the table. The specified set contains the cells (row-column) in the table,
   * if any, where the column-value is deleted.
   *
   * @param table the table
   * @param deletedCells the deleted cell positions
   */
  private void assertTableData(final MTable table, Set<String> deletedCells) {
    assertTableData(table, deletedCells, -1);
  }

  private void assertTableData(final MTable table, Set<String> deletedCells, final long version) {
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      String valueSfx = "";
      if (version > 0) {
        get.setTimeStamp(version);
        valueSfx = "_" + version;
      }
      Row result = table.get(get);
      if (deletedCells.contains(rowIndex + "")) {
        assertTrue(result.isEmpty());
      } else {
        assertFalse(result.isEmpty());
        assertEquals(NUM_OF_COLUMNS + 1, result.size());
      }

      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (Cell cell : row) {
        if (MTableUtils.KEY_COLUMN_NAME.equalsIgnoreCase(Bytes.toString(cell.getColumnName()))) {
          continue;
        }
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        if (deletedCells.contains(rowIndex + "-" + columnIndex)) {
          assertNull(cell.getColumnValue());
        } else {
          byte[] expectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex + valueSfx);
          assertArrayEquals("Invalid ColumnName.", expectedColumnName, cell.getColumnName());
          assertArrayEquals(
                  "Invalid ColumnValue: expected= " + VALUE_PREFIX + columnIndex + valueSfx,
                  expectedValue, (byte[]) cell.getColumnValue());
        }
        columnIndex++;
      }
    }
  }

  /** Tests for Ordered_Versioned table with column having multiple versions **/
  /**
   * Add versions for all columns and then assert that the data for the specific versions. It
   * returns a list of versions (timestamps) so as to assert on the required.
   *
   * @param table the table
   * @param maxVersions the number of version to add for each column
   * @return the list of timestamps added for each column
   */
  private List<Long> addAndAssertVersions(final MTable table, final int maxVersions) {
    List<Long> timestamps = new ArrayList<>(maxVersions);
    for (int versionNumber = 0; versionNumber < maxVersions; versionNumber++) {
      long ts = System.currentTimeMillis();
      timestamps.add(ts);
      for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
        Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
        record.setTimeStamp(ts);
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                  Bytes.toBytes(VALUE_PREFIX + columnIndex + "_" + ts));
        }
        table.put(record);
      }
    }

    for (int versionNumber = 0; versionNumber < maxVersions; versionNumber++) {
      assertTableData(table, Collections.emptySet(), timestamps.get(versionNumber));
    }
    return timestamps;
  }

  private void assertTableDataNoChange(MTable table) {
    if (timestamps.isEmpty()) {
      assertTableData(table, Collections.emptySet());
    } else {
      assertTableData(table, Collections.emptySet(), timestamps.get(0));
      assertTableData(table, Collections.emptySet(), timestamps.get(1));
      assertTableData(table, Collections.emptySet(), timestamps.get(2));
      assertTableData(table, Collections.emptySet(), timestamps.get(3));
    }
  }

  /******** Actual tests for checkAndDelete.. *********/

  @Test
  public void testDeleteSingleColumn() throws IOException {
    MTable table = getTable();
    Delete delete = new Delete(Bytes.toBytes(KEY_PREFIX + 0));
    if (!timestamps.isEmpty()) {
      delete.setTimestamp(timestamps.get(0));
    }
    delete.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 1));

    table.delete(delete);

    Set<String> deletedCells = new HashSet<String>(1) {
      {
        add("0-1");
      }
    };
    if (timestamps.isEmpty()) {
      assertTableData(table, deletedCells);
    } else {
      assertTableData(table, deletedCells, timestamps.get(0));
      assertTableData(table, Collections.emptySet(), timestamps.get(1));
      assertTableData(table, Collections.emptySet(), timestamps.get(2));
      assertTableData(table, Collections.emptySet(), timestamps.get(3));
    }
  }

  @Test
  public void testDeleteSingleColumnMatchAndDelete() {
    MTable table = getTable();
    Delete delete = new Delete(Bytes.toBytes(KEY_PREFIX + 0));
    if (!timestamps.isEmpty()) {
      delete.setTimestamp(timestamps.get(1));
    }
    delete.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0));

    try {
      table.delete(delete);
    } catch (Exception e) {
      fail("No exception expected.");
    }

    Set<String> deletedCells = new HashSet<String>(1) {
      {
        add("0-0");
      }
    };
    if (timestamps.isEmpty()) {
      assertTableData(table, deletedCells);
    } else {
      assertTableData(table, deletedCells, timestamps.get(0));
      assertTableData(table, deletedCells, timestamps.get(1));
      assertTableData(table, Collections.emptySet(), timestamps.get(2));
      assertTableData(table, Collections.emptySet(), timestamps.get(3));
    }
  }

  @Test
  public void testDeleteTwoColumns() {
    MTable table = getTable();
    Delete delete = new Delete(Bytes.toBytes(KEY_PREFIX + 0));
    if (!timestamps.isEmpty()) {
      delete.setTimestamp(timestamps.get(3));
    }
    delete.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 1));
    delete.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 2));

    try {
      table.delete(delete);
    } catch (Exception e) {
      fail("No exception expected.");
    }

    Set<String> deletedCells = new HashSet<String>(2) {
      {
        add("0-1");
        add("0-2");
      }
    };

    if (timestamps.isEmpty()) {
      assertTableData(table, deletedCells);
    } else {
      assertTableData(table, deletedCells, timestamps.get(0));
      assertTableData(table, deletedCells, timestamps.get(1));
      assertTableData(table, deletedCells, timestamps.get(2));
      assertTableData(table, deletedCells, timestamps.get(3));
    }
  }

  @Test
  public void testDeleteTwoColumns_1() {
    MTable table = getTable();
    Delete delete = new Delete(Bytes.toBytes(KEY_PREFIX + 0));
    if (!timestamps.isEmpty()) {
      delete.setTimestamp(timestamps.get(0));
    }
    delete.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 1));
    delete.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 2));

    try {
      table.delete(delete);
    } catch (Exception e) {
      fail("No excpetion expected.");
    }

    // assertTrue(checkResult);
    Set<String> deletedCells = new HashSet<String>(2) {
      {
        add("0-1");
        add("0-2");
      }
    };
    if (timestamps.isEmpty()) {
      assertTableData(table, deletedCells);
    } else {
      assertTableData(table, deletedCells, timestamps.get(0));
      assertTableData(table, Collections.emptySet(), timestamps.get(1));
      assertTableData(table, Collections.emptySet(), timestamps.get(2));
      assertTableData(table, Collections.emptySet(), timestamps.get(3));
    }
  }

  @Test
  public void testDeleteCompleteRow() {
    MTable table = getTable();
    Delete delete = new Delete(Bytes.toBytes(KEY_PREFIX + 0));
    if (!timestamps.isEmpty()) {
      delete.setTimestamp(timestamps.get(2));
    }

    try {
      table.delete(delete);
    } catch (Exception e) {
      fail("No exception expected.");
    }

    Set<String> deletedCells = new HashSet<String>(1) {
      {
        add("0");
      }
    };
    if (timestamps.isEmpty()) {
      assertTableData(table, deletedCells);
    } else {
      assertTableData(table, deletedCells, timestamps.get(0));
      assertTableData(table, deletedCells, timestamps.get(1));
      assertTableData(table, deletedCells, timestamps.get(2));
      assertTableData(table, Collections.emptySet(), timestamps.get(3));
    }
  }
}
