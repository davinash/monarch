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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.exceptions.IllegalColumnNameException;
import io.ampool.monarch.table.exceptions.MException;
import io.ampool.monarch.table.exceptions.RowKeyDoesNotExistException;
import io.ampool.monarch.table.functions.TestDUnitBase;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runner.notification.Failure;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category(MonarchTest.class)
public class MTableCheckAndPutDUnitTest {
  protected static final Logger logger = LogService.getLogger();

  @Test
  public void testCheckAndPutSuite() {
    final Result result = JUnitCore.runClasses(MTableCheckAndPutInternal.class);
    for (final Failure failure : result.getFailures()) {
      System.out.println("Test failed: " + failure.toString());
    }
    assertTrue(result.wasSuccessful());
  }

  @RunWith(Parameterized.class)
  public static final class MTableCheckAndPutInternal {
    protected final int NUM_OF_COLUMNS = 10;
    protected final String TABLE_NAME = "ALLOPERATION";
    protected final String KEY_PREFIX = "KEY";
    protected final String VALUE_PREFIX = "VALUE";
    protected final int NUM_OF_ROWS = 10;
    protected final String COLUMN_NAME_PREFIX = "COLUMN";
    protected final int LATEST_TIMESTAMP = 300;
    protected final int MAX_VERSIONS = 5;
    protected final int TABLE_MAX_VERSIONS = 7;

    private void doPuts() {
      MTable table = clientCache.getTable(TABLE_NAME);
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
      MTableDescriptor tableDescriptor = null;
      if (ordered == false) {
        tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
      } else {
        tableDescriptor = new MTableDescriptor();
      }
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.setRedundantCopies(1);
      tableDescriptor.setMaxVersions(5);
      if (doPersist) {
        tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
      }
      Admin admin = clientCache.getAdmin();
      MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
      assertEquals(table.getName(), TABLE_NAME);
    }

    private void doGets() {
      MTable table = clientCache.getTable(TABLE_NAME);
      assertTableData(table, Collections.emptyMap());
    }

    private static final TestDUnitBase testBase = new TestDUnitBase();

    private static MClientCache clientCache;

    @Parameters(name = "{index}: IsOrderedTable={0}, MaxVersions={1}, DiskPersist={2}")
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] {{true, 1, false}, //// Ordered_Versioned with
                                                             //// max-versions 1
          {true, 5, false}, //// Ordered_Versioned with max-versions 5
          {false, 1, false}, //// UnOrdered with max-versions 1
          //// Disk persistence enabled..
          {true, 1, true}, //// Ordered_Versioned with max-versions 1
          {true, 5, true}, //// Ordered_Versioned with max-versions 5
          {false, 1, true}, //// UnOrdered with max-versions 1
      });
    }

    @Parameter(value = 0)
    public boolean isOrderedTable;
    @Parameter(value = 1)
    public int maxVersions;
    @Parameter(value = 2)
    public boolean doPersist;
    @Rule
    public TestName testName = new TestName();

    public MTableCheckAndPutInternal() {}

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
     * before and after test methods.. executed before and after each test.. added these
     * since @Before and @After do not seem to execute as expected.
     */
    @Before
    public void setUpTest() {
      System.out.printf("%s.%s :: TableType=%s; MaxVersions= %s; DiskPersist= %s\n",
          this.getClass().getSimpleName(), testName.getMethodName(),
          isOrderedTable ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED, maxVersions,
          doPersist);

      createTable(isOrderedTable);
      doPuts();
      doGets();
      if (this.maxVersions > 1) {
        timestamps = addAndAssertVersions(getTable(), 4);
      }
      if (doPersist) {
        testBase.restart();
      }
    }

    @After
    public void cleanUpTest() {
      clientCache.getAdmin().deleteTable(TABLE_NAME);
    }

    /**
     * Assert the data from the table. The specified map contains the modified values, in the table,
     * if any, and uses these for assertions.
     *
     * @param table the table
     * @param modifiedValueMap the modified values for the specific row-column
     */
    private void assertTableData(final MTable table, Map<String, String> modifiedValueMap) {
      assertTableData(table, modifiedValueMap, -1);
    }

    private void assertTableData(final MTable table, Map<String, String> modifiedValueMap,
        final long version) {
      for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
        Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
        String valueSfx = "";
        if (version > 0) {
          get.setTimeStamp(version);
          valueSfx = "_" + version;
        }
        Row result = table.get(get);
        assertFalse(result.isEmpty());
        assertEquals(NUM_OF_COLUMNS + 1, result.size());

        int columnIndex = 0;
        List<Cell> row = result.getCells();
        for (int k = 0; k < row.size() - 1; k++) {
          Assert.assertNotEquals(10, columnIndex);
          byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
          String expectedValue = modifiedValueMap.get(rowIndex + "-" + columnIndex);
          if (expectedValue == null) {
            expectedValue = VALUE_PREFIX + columnIndex + valueSfx;
          }
          assertArrayEquals("Invalid ColumnName.", expectedColumnName, row.get(k).getColumnName());
          assertArrayEquals("Invalid ColumnValue: expected= " + expectedValue,
              Bytes.toBytes(expectedValue), (byte[]) row.get(k).getColumnValue());
          columnIndex++;
        }
      }
    }

    /**
     * Assert that the original data is not changed at all..
     *
     * @param table the ampool table
     */
    private void assertTableDataNoChange(final MTable table) {
      if (timestamps.isEmpty()) {
        assertTableData(table, Collections.emptyMap());
      } else {
        /** not sure how assert with version base version.. **/
        assertTableData(table, Collections.emptyMap(), timestamps.get(0));
        assertTableData(table, Collections.emptyMap(), timestamps.get(1));
        assertTableData(table, Collections.emptyMap(), timestamps.get(2));
        assertTableData(table, Collections.emptyMap(), timestamps.get(3));
      }
    }

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
        long ts = System.nanoTime();
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
        assertTableData(table, Collections.emptyMap(), timestamps.get(versionNumber));
      }
      return timestamps;
    }

    private MTable getTable() {
      return clientCache.getTable(TABLE_NAME);
    }

    /**
     * Actual tests for checkAndPut..
     *
     * All the tests are executed with following inputs: - ordered table with single version -
     * ordered table with 5 versions - unordered table (with single version)
     */
    /**
     * Test for modifying value of a single column. Verify that the column value gets modified after
     * checkAndPut -- same column is used for check and modification.
     */
    @Test
    public void testPutSingleColumn() {
      MTable table = getTable();
      String valueSfx = "";
      Put put = new Put(KEY_PREFIX + 0);
      if (!timestamps.isEmpty()) {
        put.setTimeStamp(timestamps.get(2));
        valueSfx = ("_" + timestamps.get(timestamps.size() - 1));
      }
      put.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0), Bytes.toBytes(VALUE_PREFIX + 100));

      /** currently the check happens for the value that was put first (sorted by timestamp-asc) **/
      boolean checkResult = false;
      try {
        checkResult = table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 0),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0), Bytes.toBytes(VALUE_PREFIX + 0 + valueSfx), put);
      } catch (Exception e) {
        fail("NO exception expected.");
      }

      assertTrue(checkResult);
      final Map<String, String> modifiedValueMap = new HashMap<String, String>(1) {
        {
          put("0-0", VALUE_PREFIX + 100);
        }
      };

      if (!timestamps.isEmpty()) {
        assertTableData(table, modifiedValueMap, timestamps.get(2));
        assertTableData(table, Collections.emptyMap(), timestamps.get(0));
        assertTableData(table, Collections.emptyMap(), timestamps.get(1));
        assertTableData(table, Collections.emptyMap(), timestamps.get(3));
      } else {
        assertTableData(table, modifiedValueMap);
      }
    }

    /**
     * Assert on modification of two (multiple) column values. Verify that the column value gets
     * modified after checkAndPut -- different columns are used to perform check and modification.
     */
    @Test
    public void testPutTwoColumns() {
      MTable table = getTable();

      Put put = new Put(KEY_PREFIX + 0);
      String valueSfx = "";
      if (!timestamps.isEmpty()) {
        put.setTimeStamp(timestamps.get(3));
        valueSfx = "_" + timestamps.get(timestamps.size() - 1);
      }
      put.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 1), Bytes.toBytes(VALUE_PREFIX + 111));
      put.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 2), Bytes.toBytes(VALUE_PREFIX + 222));

      boolean checkResult = false;
      try {
        checkResult = table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 0),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0), Bytes.toBytes(VALUE_PREFIX + 0 + valueSfx), put);
      } catch (Exception e) {
        fail("No exception expected.");
      }

      assertTrue(checkResult);
      final Map<String, String> modifiedValueMap = new HashMap<String, String>(2) {
        {
          put("0-1", VALUE_PREFIX + 111);
          put("0-2", VALUE_PREFIX + 222);
        }
      };

      if (timestamps.isEmpty()) {
        assertTableData(table, modifiedValueMap);
      } else {
        assertTableData(table, modifiedValueMap, timestamps.get(3));
        assertTableData(table, Collections.emptyMap(), timestamps.get(0));
        assertTableData(table, Collections.emptyMap(), timestamps.get(1));
        assertTableData(table, Collections.emptyMap(), timestamps.get(2));
      }
    }

    /**
     * Assert on modification of two (multiple) column values with check on old-version. Verify that
     * column-value of two columns is modified as expected after checkAndPut.
     */
    @Test
    public void testPutTwoColumnsCheckOnOldVersion() {
      MTable table = getTable();

      Put put = new Put(KEY_PREFIX + 0);
      String valueSfx = "";
      if (!timestamps.isEmpty()) {
        put.setTimeStamp(timestamps.get(3));
      }
      put.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 1), Bytes.toBytes(VALUE_PREFIX + 111));
      put.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 2), Bytes.toBytes(VALUE_PREFIX + 222));

      boolean checkResult = false;
      try {
        checkResult = table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 0),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0), Bytes.toBytes(VALUE_PREFIX + 0 + valueSfx), put);
      } catch (Exception e) {
        fail("No exception expected.");
      }

      final Map<String, String> modifiedValueMap = new HashMap<String, String>(2) {
        {
          put("0-1", VALUE_PREFIX + 111);
          put("0-2", VALUE_PREFIX + 222);
        }
      };

      if (timestamps.isEmpty()) {
        assertTrue(checkResult);
        assertTableData(table, modifiedValueMap);
      } else {
        assertFalse(checkResult);
        assertTableData(table, Collections.emptyMap(), timestamps.get(0));
        assertTableData(table, Collections.emptyMap(), timestamps.get(1));
        assertTableData(table, Collections.emptyMap(), timestamps.get(2));
        assertTableData(table, Collections.emptyMap(), timestamps.get(3));
      }
    }

    /**
     * Assert on modification of all column values. Verify that all column values are modified as
     * expected in a single checkAndPut.
     */
    @Test
    public void testPutAllColumns() {
      MTable table = getTable();

      String valueSfx = "";
      Put put = new Put(KEY_PREFIX + 0);
      if (!timestamps.isEmpty()) {
        put.setTimeStamp(timestamps.get(2));
        valueSfx = ("_" + timestamps.get(timestamps.size() - 1));
      }
      final Map<String, String> modifiedValueMap = new HashMap<>(NUM_OF_COLUMNS);
      for (int i = 0; i < NUM_OF_COLUMNS; i++) {
        put.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + i),
            Bytes.toBytes(VALUE_PREFIX + (1000 + i)));
        modifiedValueMap.put("0-" + i, VALUE_PREFIX + (1000 + i));
      }

      boolean checkResult = false;
      try {
        checkResult = table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 0),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0), Bytes.toBytes(VALUE_PREFIX + 0 + valueSfx), put);
      } catch (Exception e) {
        fail("No exception expected.");
      }

      assertTrue(checkResult);

      if (timestamps.isEmpty()) {
        assertTableData(table, modifiedValueMap);
      } else {
        assertTableData(table, modifiedValueMap, timestamps.get(2));
        assertTableData(table, Collections.emptyMap(), timestamps.get(0));
        assertTableData(table, Collections.emptyMap(), timestamps.get(1));
        assertTableData(table, Collections.emptyMap(), timestamps.get(3));
      }
    }

    /**
     * Assert put with no columns has no effect.
     */
    @Test
    public void testPutNoColumns() {
      MTable table = getTable();

      String valueSfx = "";
      Put put = new Put(KEY_PREFIX + 0);
      if (!timestamps.isEmpty()) {
        put.setTimeStamp(timestamps.get(0));
        valueSfx = ("_" + timestamps.get(timestamps.size() - 1));
      }

      boolean checkResult = false;
      try {
        checkResult = table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 0),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 0), Bytes.toBytes(VALUE_PREFIX + 0 + valueSfx), put);
      } catch (Exception e) {
        fail("Expected no exception.");
      }

      assertTrue(checkResult);
      assertTableDataNoChange(table);
    }

    /**
     * Negative test expecting exception when the row provided is not same as row-key in put.
     */
    @Test
    public void testInvalidRowKey() {
      MTable table = getTable();

      Put put = new Put(KEY_PREFIX + 0);
      if (!timestamps.isEmpty()) {
        put.setTimeStamp(timestamps.get(1));
      }

      /* Check if the row key and Put key is the same */
      try {
        table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 1000), Bytes.toBytes(COLUMN_NAME_PREFIX + 0),
            Bytes.toBytes(VALUE_PREFIX + 0), put);
        fail("Expected IllegalStateException.");
      } catch (IllegalStateException ise) {
        //// expected this exception..
      } catch (Exception ex) {
        fail("Expected IllegalStateException.");
      }

      assertTableDataNoChange(table);
    }

    /**
     * Assert that checkAndPut returns false when different column-value is provided for the check,
     * with single column specified in MPut.
     */
    @Test
    public void testPutCheckNoMatch() {
      MTable table = getTable();

      Put put = new Put(KEY_PREFIX + 1);
      if (!timestamps.isEmpty()) {
        put.setTimeStamp(timestamps.get(2));
      }
      put.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 1), Bytes.toBytes(VALUE_PREFIX + 500));

      boolean checkResult = false;
      try {
        checkResult = table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 1),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 1), Bytes.toBytes(VALUE_PREFIX + 1000), put);
      } catch (Exception e) {
        fail("Expected no exception.");
      }

      assertFalse(checkResult);
      assertTableDataNoChange(table);
    }

    /**
     * Assert that checkAndPut returns false when different column-value is provided for the check,
     * with all columns specified in MPut.
     */
    @Test
    public void testPutCheckNoMatchCompleteRowUpdate() {
      MTable table = getTable();

      Put put = new Put(KEY_PREFIX + 1);
      if (!timestamps.isEmpty()) {
        put.setTimeStamp(timestamps.get(3));
      }

      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        put.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex * 100));
      }

      boolean checkResult = true;
      try {
        checkResult = table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 1),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 1), Bytes.toBytes(VALUE_PREFIX + 1000), put);
      } catch (Exception e) {
        fail("Expected no exception.");
      }

      assertFalse(checkResult);
      assertTableDataNoChange(table);
    }

    /**
     * Assert that checkAndPut with non-existent row-key raises the correct exception i.e.
     * MCacheInternalErrorException.
     */
    @Test
    public void testPutInvalidRowKeyCompleteRow() {
      MTable table = getTable();
      Put put = new Put(Bytes.toBytes(KEY_PREFIX + 1000));
      if (!timestamps.isEmpty()) {
        put.setTimeStamp(timestamps.get(3));
      }
      try {
        table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 1000), Bytes.toBytes(COLUMN_NAME_PREFIX + 0),
            Bytes.toBytes(VALUE_PREFIX + 0), put);
        fail("Expected MCacheInternalErrorException.");
      } catch (MException ex) {
        //// expected this exception...
        assertTrue(ex instanceof RowKeyDoesNotExistException);
      } catch (Exception ex) {
        fail("Expected MCacheInternalErrorException.");
      }
      assertTableDataNoChange(table);
    }

    /**
     * Assert that when invalid column is provided for check it raises IllegalColumnNameException.
     */
    @Test
    public void testPutCheckInvalidColumn() {
      MTable table = getTable();
      Put put = new Put(Bytes.toBytes(KEY_PREFIX + 0));
      if (!timestamps.isEmpty()) {
        put.setTimeStamp(timestamps.get(3));
      }
      try {
        table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 0), Bytes.toBytes(COLUMN_NAME_PREFIX + 1000),
            Bytes.toBytes(VALUE_PREFIX + 0), put);
        fail("Expected IllegalColumnNameException.");
      } catch (IllegalColumnNameException ex) {
        //// expected this exception...
      } catch (Exception ex) {
        fail("Expected IllegalColumnNameException.");
      }
      assertTableDataNoChange(table);
    }

    /**
     * Assert that checkAndPut raises IllegalColumnNameException when invalid column-name is
     * provided in the respective put.
     */
    @Test
    public void testPutInvalidColumn() {
      MTable table = getTable();
      Put put = new Put(Bytes.toBytes(KEY_PREFIX + 0));
      if (!timestamps.isEmpty()) {
        put.setTimeStamp(timestamps.get(3));
      }
      put.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 1000), Bytes.toBytes(VALUE_PREFIX + 500));
      try {
        table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 0), Bytes.toBytes(COLUMN_NAME_PREFIX + 0),
            Bytes.toBytes(VALUE_PREFIX + 0), put);
        fail("Expected IllegalColumnNameException.");
      } catch (IllegalColumnNameException ex) {
        //// expected this exception...
      } catch (Exception ex) {
        fail("Expected IllegalColumnNameException.");
      }
      assertTableDataNoChange(table);
    }

    /**
     * checkAndPut test-cases with possible null values user-specified | existing-value | API return
     * ------------------------------------------------ NULL | NULL | true NULL | value-2 | false
     * Value-1 | NULL | false Value-1 | Value-2 | false
     */
    @Test
    public void testCheckAndPutWithNull() {
      MTable table = getTable();

      Put newRecord = new Put(KEY_PREFIX + 10);
      newRecord.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 4), Bytes.toBytes(VALUE_PREFIX + 104));
      newRecord.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 6), Bytes.toBytes(VALUE_PREFIX + 106));
      newRecord.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 8), Bytes.toBytes(VALUE_PREFIX + 108));


      table.put(newRecord);

      // test-1: Both user-specified and existing value are null, checkAndPut succeeds in this case.
      Put put1 = new Put(KEY_PREFIX + 10);
      put1.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 1),
          Bytes.toBytes(VALUE_PREFIX + "-updated"));
      boolean checkResult = false;
      try {
        checkResult = table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 10),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 1), null, put1);
      } catch (Exception e) {
        fail("No exception expected.");
      }
      System.out.println("TEST Both Null checkResult = " + checkResult);
      assertTrue(checkResult);
      // verify using Get
      Get myGet = new Get(Bytes.toBytes(KEY_PREFIX + 10));
      myGet.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 1));
      Row myResult = table.get(myGet);

      String expectedValue = VALUE_PREFIX + "-updated";
      List<Cell> myRow = myResult.getCells();
      for (Cell cell : myRow) {
        // System.out.println("ColumnName => " + Bytes.toString(cell.getColumnName()) + " AND
        // ColumnValue => " + Bytes.toString((byte[]) cell.getColumnValue()));
        assertArrayEquals("Invalid ColumnValue: expected= " + expectedValue,
            Bytes.toBytes(expectedValue), (byte[]) cell.getColumnValue());
      }

      // test-2: user-specified value is null and existing value is not null, checkAndPut returns
      // false.
      Put put2 = new Put(KEY_PREFIX + 10);
      put2.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 4),
          Bytes.toBytes(VALUE_PREFIX + "-updated"));
      checkResult = true;
      try {
        checkResult = table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 10),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 4), null, put2);
      } catch (Exception e) {
        fail("No exception expected.");
      }
      System.out.println("TEST User-Null, Existing-NotNull checkResult = " + checkResult);
      assertFalse(checkResult);

      // test-3: user-specified notNull value and existing value is null, checkAndPut returns false.
      Put put3 = new Put(KEY_PREFIX + 10);
      put3.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 2),
          Bytes.toBytes(VALUE_PREFIX + "-updated"));
      checkResult = true;
      String oldValue = "oldValue";
      try {
        checkResult = table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 10),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 2), Bytes.toBytes(oldValue), put3);
      } catch (Exception e) {
        fail("No exception expected.");
      }
      // System.out.println("TEST User-NotNull, Existing-Null checkResult = " + checkResult);
      assertFalse(checkResult);

      // test-4: user-specified value1 and existing value is value2, checkAndPut returns false.
      Put put4 = new Put(KEY_PREFIX + 10);
      put4.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 6),
          Bytes.toBytes(VALUE_PREFIX + "-updated"));
      checkResult = true;
      try {
        checkResult = table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 10),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 6), Bytes.toBytes(oldValue), put4);
      } catch (Exception e) {
        fail("No exception expected.");
      }
      // System.out.println("TEST User specified and existing value does not match checkResult = " +
      // checkResult);
      assertFalse(checkResult);
    }

    /**
     * For, Non-exisitng key, checkAndPut test-cases with possible values (null/not-null)
     * user-specified | server cache change ------------------------------------------------ NULL |
     * New row-key is added with columns specified in put (last param of API) Not-NULL |
     * RowKeyDoesNotExistException: Row Id does not exists
     */
    @Test
    public void testCheckAndPutWithNotExistingKey() {
      MTable table = getTable();

      // test-1: Non exisitng case with user specifying value as null.
      // checkAndPut succeeds in this case and new record (non-existing) gets added.
      Put newRecord = new Put(KEY_PREFIX + 99);
      newRecord.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 4), Bytes.toBytes(VALUE_PREFIX + 994));
      newRecord.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 6), Bytes.toBytes(VALUE_PREFIX + 996));
      newRecord.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 8), Bytes.toBytes(VALUE_PREFIX + 998));

      List<byte[]> expectedRecord =
          Arrays.asList(null, null, null, null, Bytes.toBytes(VALUE_PREFIX + 994), null,
              Bytes.toBytes(VALUE_PREFIX + 996), null, Bytes.toBytes(VALUE_PREFIX + 998), null);
      boolean checkResult = false;
      try {
        checkResult = table.checkAndPut(Bytes.toBytes(KEY_PREFIX + 99),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 1), null, newRecord);
      } catch (Exception e) {
        // fail("No exception expected.");
      }
      System.out.println("TEST Both Null checkResult = " + checkResult);
      assertFalse(checkResult);
      // verify using Get
      Get myGet = new Get(Bytes.toBytes(KEY_PREFIX + 99));
      Row myResult = table.get(myGet);
      List<Cell> myRow = myResult.getCells();
      int cellIndex = 0;
      for (int k = 0; k < myRow.size() - 1; k++) {
        // System.out.println("ColumnName => " + Bytes.toString(cell.getColumnName()) + " AND
        // ColumnValue => " + Bytes.toString((byte[]) cell.getColumnValue()));
        assertArrayEquals("Invalid ColumnValue: expected= " + expectedRecord.get(cellIndex),
            expectedRecord.get(cellIndex), (byte[]) myRow.get(k).getColumnValue());
        cellIndex++;
      }

      // test-2: Non exisitng key with user specifying not-null value.
      // user gets RowKeyDoesNotExistException.
      {
        Put notExistingRecord = new Put(KEY_PREFIX + "-error");
        notExistingRecord.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 4),
            Bytes.toBytes(VALUE_PREFIX + "-error"));

        String oldValue = "column-value-error";
        Exception expectedException = null;
        try {
          table.checkAndPut(Bytes.toBytes(KEY_PREFIX + "-error"),
              Bytes.toBytes(COLUMN_NAME_PREFIX + 1), Bytes.toBytes(oldValue), notExistingRecord);
        } catch (RowKeyDoesNotExistException ex) {
          expectedException = ex;
        }
        assertTrue(expectedException instanceof RowKeyDoesNotExistException);
      }
    }
  }
}
