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

package io.ampool.monarch.hive;

import io.ampool.client.AmpoolClient;
import io.ampool.monarch.RMIException;
import io.ampool.monarch.TestBase;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.types.BasicTypes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

/**
 * Created on: 2015-12-03
 * Since version: 0.2.0
 */
public class MonarchRecordWriterTest extends TestBase {

  private static final String regionName = "region_writer_test";
  private static final String resourceFile = "data/sample1.txt";
  public static final String COLUMNS = "c1,c2,c3,c4";
  public static final List<String> COLUMN_LIST = Arrays.asList(COLUMNS.split(","));
  public static final Map<String, String>
      TYPE_HIVE_TO_MTABLE_MAP =
      new HashMap<String, String>(4) {{
        put("c1", BasicTypes.LONG.name());
        put("c2", BasicTypes.STRING.name());
        put("c3", BasicTypes.INT.name());
        put("c4", BasicTypes.STRING.name());
      }};

  /**
   * Write the provided data (lines) to Geode, via RecordWriter, and assert on the data
   * wrote/put.
   * The records/rows are put into Geode by block-size. So, the number of records/rows,
   * equal to block-size, are stored against single key in Geode.
   * @param lines the list of lines to write; each line corresponds to a row from table
   * @param expectedMap the expected number of records/rows per block wrote to Geode
   * @param taskId the task identifier, MR-job id for example, separated by _
   * @param blockSize the block-size -- number of records stored for per key
   */
  protected void assertOnRecordWriter(final List<String> lines, Map<String, Integer> expectedMap,
                                      final String taskId, int blockSize)
      throws RMIException, IOException {
    Configuration
        conf =
        TestHelper.getConfiguration(regionName, testBase.getLocatorPort(), blockSize, taskId,
            "unordered");
    conf.set("columns", COLUMNS);

    String keyPrefix = TestHelper.writeUsingRecordWriter_Array(lines, conf);
    assertOnDataWritten(expectedMap, conf, keyPrefix);
  }

  /**
   * Assert on the data put in Monarch is same as expected.
   * @param expectedMap the expected number of records/rows per block wrote/put
   * @param conf the configuration required to connect as client
   * @param keyPrefix the prefix of the keys used to put data
   */
  private void assertOnDataWritten(Map<String, Integer> expectedMap, Configuration conf,
                                   String keyPrefix) {
    /** assert the required by using local client **/
    String conf_columns = conf.get("columns");
    String[] columns = conf_columns == null ? null : conf_columns.split(",");
    final int expectedColumns = columns == null ? COLUMN_LIST.size() + 1 : columns.length;
    MTable table = MonarchUtils.getTableInstance(conf, columns);
    int numRows = expectedMap.values().stream().mapToInt(Integer::valueOf).sum();
    Row result;
    for (int i = 0; i < numRows; i++) {
      result = table.get(new Get(keyPrefix + i));
      assertNotNull(result);
      assertEquals(result.getCells().size(), expectedColumns + 1);
      for (Cell mCell : result.getCells()) {
        assertNotNull(mCell.getColumnValue());
      }
    }
    /** assert on the total-count **/
    conf.set(MonarchUtils.REGION, regionName + MonarchUtils.META_TABLE_SFX);
    //table = MonarchUtils.getTableInstance(conf);
    table = MonarchUtils.getMetaTableInstance(table);
    result = table.get(new Get(keyPrefix + MonarchUtils.KEY_BLOCKS_SFX));
    assertEquals(result.getCells().get(0).getColumnValue(), String.valueOf(numRows));
  }

  @BeforeMethod
  public void setUpBeforeMethod(final Method method) throws Exception {
    final Map<String, String> map = new HashMap<>();
    map.put(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());
    map.put(MonarchUtils.REGION, regionName);
    if (!method.getName().contains("MultipleVersions")) {
      MonarchUtils.createConnectionAndTable(regionName, map, false, null, TYPE_HIVE_TO_MTABLE_MAP);
    }
  }

  @AfterMethod
  public void cleanUpAfterMethod() throws RMIException, Exception {
    final Map<String, String> map = new HashMap<>();
    map.put(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());
    map.put(MonarchUtils.REGION, regionName);
    MonarchUtils.destroyTable(regionName, map, false, true);

    Configuration conf = new Configuration();
    conf.set("monarch.locator.port", testBase.getLocatorPort());
  }

  /**
   * Simple test for record-writer where number of rows are written to Geode, based on
   * the specified block-size.
   * The sample file contains 30 lines/rows. Here with block-size 10 total four blocks
   * are stored each with max 10 records/rows.
   */
  @Test
  public void testWrite_SingleWriter() throws Exception {
//    System.out.println("MonarchRecordWriterTest.testWrite_SingleWriter");
    final List<String> lines = TestHelper.getResourceAsString(resourceFile);
    final Map<String, Integer> expectedMap = new HashMap<String, Integer>(4) {{
      put("0", 10);
      put("1", 10);
      put("2", 10);
      put("3", 1);
    }};
    final int blockSize = 10;

    assertOnRecordWriter(lines, expectedMap, "", blockSize);

    Configuration conf = new Configuration();
    conf.set("monarch.locator.port", testBase.getLocatorPort());
  }

  /**
   * Test for single writer with block-size greater than total number of records.
   */
  @Test
  public void testWrite_SingleWriterLargeBlockSize() throws Exception {
//    System.out.println("MonarchRecordWriterTest.testWrite_SingleWriterLargeBlockSize");
    final List<String> lines = TestHelper.getResourceAsString(resourceFile);
    final Map<String, Integer> expectedMap = new HashMap<String, Integer>(4) {{
      put("0", 31);
    }};
    final int blockSize = 100;

    assertOnRecordWriter(lines, expectedMap, "", blockSize);

    Configuration conf = new Configuration();
    conf.set("monarch.locator.port", testBase.getLocatorPort());
  }

  /**
   * Test for multiple writers running in separate threads, writing data simultaneously.
   */
  @Test
  public void testWrite_MultipleWriters() throws RMIException, IOException, InterruptedException {
//    System.out.println("MonarchRecordWriterTest.testWrite_MultipleWriters");
    final List<String> lines = TestHelper.getResourceAsString(resourceFile);
    final Map<String, Integer> expectedMap = new HashMap<String, Integer>(4) {{
      put("0", 10);
      put("1", 10);
      put("2", 10);
      put("3", 1);
    }};
    final int blockSize = 10;

    ExecutorService es = Executors.newFixedThreadPool(3);
    for (int i = 0; i < 3; i++) {
      final String tid = "000_" + i;
      es.submit(() -> {
        try {
          assertOnRecordWriter(lines, expectedMap, tid, blockSize);
        } catch (Exception e) {
          fail("No exception expected: " + e.getMessage());
        }
      });
    }
    es.shutdown();
    es.awaitTermination(5, TimeUnit.SECONDS);
    if (!es.isTerminated()) {
      es.shutdownNow();
    }
    Configuration conf = new Configuration();
    conf.set("monarch.locator.port", testBase.getLocatorPort());
  }

  @DataProvider
  public static Object[][] getKeyData() {
    return new Object[][]{
        {"non.existent.key", "application_id_00001345_0001", "application_id_00001345_0001-"},
        {MonarchRecordWriter.MAPREDUCE_TASK_ID, "", ""},
        {MonarchRecordWriter.MAPREDUCE_TASK_ID, "application00001340011", ""},
        {MonarchRecordWriter.MAPREDUCE_TASK_ID, "application_id_00001345_0011", "0011-"},
        {MonarchRecordWriter.MAPREDUCE_TASK_PARTITION, "0", "0-"},
        {MonarchRecordWriter.MAPREDUCE_TASK_PARTITION, "application_id_00001345_0011",
            "application_id_00001345_0011-"},
        {null, "application_id_00001345_0001", "application_id_00001345_0001-"},
    };
  }

  /**
   * Test all possible combinations of key prefix.
   * Also, test the usage of configurable variable name.. so that first key
   * and then value for the key is retrieved from configuration.
   * @param keyInConf the key to be added to the configuration
   * @param valueInConf the value for above key to be added in the configuration
   * @param expectedKeyPrefix expected key-prefix to be used by monarch-record-writer
   */
  @Test(dataProvider = "getKeyData")
  public void testKeyPrefix(final String keyInConf, final String valueInConf,
                            String expectedKeyPrefix) {
//    System.out.println("MonarchRecordWriterTest.testKeyPrefix :: " + keyInConf + "=" + valueInConf);
    final Configuration
        conf =
        TestHelper.getConfiguration(regionName, testBase.getLocatorPort(), 10, "", "unordered");
    /** add two configuration vars.. one meta-var that has actual name and the other has value
     *   just to make sure that configurable variable work as expected
     */
    if (keyInConf == null) {
      conf.set(MonarchRecordWriter.MONARCH_UNIQUE_VAR,
          MonarchRecordWriter.MAPREDUCE_TASK_PARTITION);
      conf.set(MonarchRecordWriter.MAPREDUCE_TASK_PARTITION, valueInConf);
    } else if (keyInConf.startsWith("non.")) {
      conf.set(MonarchRecordWriter.MONARCH_UNIQUE_VAR, keyInConf);
      conf.set(keyInConf, valueInConf);
    } else {
      conf.set(keyInConf, valueInConf);
    }
    MonarchRecordWriter mrw = new MonarchRecordWriter(conf);
    if ("".equals(expectedKeyPrefix)) {
      expectedKeyPrefix = System.identityHashCode(mrw) + "-";
    }

    assertEquals(mrw.getKeyPrefix(), expectedKeyPrefix);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testWrite_Exception() throws IOException {
//    System.out.println("MonarchRecordWriterTest.testWriteException");
    Text dummyData = new Text("dummyData");
    MonarchRecordWriter
        mrw =
        new MonarchRecordWriter(TestHelper
            .getConfiguration("my_region", testBase.getLocatorPort(), 10, "", "unordered"));
    mrw.write(dummyData);

    Configuration conf = new Configuration();
    conf.set("monarch.locator.port", testBase.getLocatorPort());
  }

  /**
   * Test to assert that separate write in a region does overwrite data with same key-prefix.
   * Same data is put in a region/table via separate executions and assert corresponding data.
   */
  @Test
  public void testMultipleInsertWithSamePrefix() throws Exception {
//    System.out.println("MonarchRecordWriterTest.testMultipleInsertWithSamePrefix");
    final List<String> lines = TestHelper.getResourceAsString(resourceFile);
    final int blockSize = 10;
    final Map<String, Integer> expectedMap = new HashMap<String, Integer>(4) {{
      put("0", 20);
      put("1", 20);
      put("2", 20);
      put("3", 2);
    }};
    Configuration
        conf =
        TestHelper
            .getConfiguration(regionName, testBase.getLocatorPort(), blockSize, "0_0", "unordered");
    conf.set("columns", COLUMNS);

    TestHelper.writeUsingRecordWriter_Array(lines, conf);
    String keyPrefix = TestHelper.writeUsingRecordWriter_Array(lines, conf);

    assertOnDataWritten(expectedMap, conf, keyPrefix);
  }

  /**
   * Test to assert that separate write in a region does overwrite data with different key-prefix.
   */
  @Test
  public void testMultipleInsertWithDifferentPrefix() throws Exception {
//    System.out.println("MonarchRecordWriterTest.testMultipleInsertWithDifferentPrefix");
    final List<String> lines = TestHelper.getResourceAsString(resourceFile);
    final int blockSize = 10;
    final Map<String, Integer> expectedMap = new HashMap<String, Integer>(4) {{
      put("0", 10);
      put("1", 10);
      put("2", 10);
      put("3", 1);
    }};
    Configuration
        conf =
        TestHelper
            .getConfiguration(regionName, testBase.getLocatorPort(), blockSize, "", "unordered");
    conf.set("columns", COLUMNS);

    String keyPrefix = TestHelper.writeUsingRecordWriter_Array(lines, conf);
    assertOnDataWritten(expectedMap, conf, keyPrefix);

    conf.set(MonarchUtils.REGION, regionName);
    keyPrefix = TestHelper.writeUsingRecordWriter_Array(lines, conf);
    assertOnDataWritten(expectedMap, conf, keyPrefix);
  }

  @DataProvider
  public static Object[][] dataMultipleVersions() {
    return new Object[][]{
        {"ordered", 31, 31, "0", "10", "1"},
        {"ordered", 2, 2, "1", "10", "1"},
        {"ordered", 19, 19, "2", "10", "1"},
        {"ordered", 31, 31, "0", "10", "10"},
        {"ordered", 2, 20, "1", "10", "10"},
        {"ordered", 19, 29, "2", "10", "10"},
        {"ordered", 2, 20, "1", "99", "10"},
        {"ordered", 19, 29, "2", "99", "10"},
        {"ordered", 2, 20, "1", "10", "99"},
        {"ordered", 19, 29, "2", "10", "99"},
        {"ordered", 2, 31, "1", "99", "99"},
        {"ordered", 19, 31, "2", "99", "99"},
        {"unordered", 31, 31, "0", "10", "1"},
        {"unordered", 2, 2, "1", "10", "1"},
        {"unordered", 19, 19, "2", "10", "1"},
        {"unordered", 31, 31, "0", "10", "10"},
        {"unordered", 2, 20, "1", "10", "10"},
        {"unordered", 19, 29, "2", "10", "10"},
        {"unordered", 2, 20, "1", "99", "10"},
        {"unordered", 19, 29, "2", "99", "10"},
        {"unordered", 2, 20, "1", "10", "99"},
        {"unordered", 19, 29, "2", "10", "99"},
        {"unordered", 2, 31, "1", "99", "99"},
        {"unordered", 19, 31, "2", "99", "99"},
    };
  }

  /**
   * Test to assert that writing versions with ordered table works as expected.
   * @param expectedRowCount expected row count as stored with multiple versions
   * @param explodedRowCount actual row count where multiple versions are exploded as separate rows
   * @param keyColumn the column idex of key-column
   * @param writeMaxVersions the max-versions to be used for table creation
   * @param scanMaxVersions the max-versions to be used for querying during scan
   * @throws Exception in case of an error
   */
  @Test(dataProvider = "dataMultipleVersions")
  public void testMultipleVersions(final String tabletype, final int expectedRowCount,
                                   final int explodedRowCount,
                                   final String keyColumn, final String writeMaxVersions,
                                   final String scanMaxVersions) throws Exception {
    final Map<String, String> map = new HashMap<>();
    map.put(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());
    map.put(MonarchUtils.REGION, regionName);
    map.put(MonarchUtils.MONARCH_TABLE_TYPE, tabletype);
    map.put(MonarchUtils.WRITE_KEY_COLUMN, String.valueOf(keyColumn));
    map.put(MonarchUtils.MAX_VERSIONS, writeMaxVersions);
    MonarchUtils.createConnectionAndTable(regionName, map, false, null, TYPE_HIVE_TO_MTABLE_MAP);

    final List<String> lines = TestHelper.getResourceAsString(resourceFile);
    Configuration conf = TestHelper.getConfiguration(regionName, testBase.getLocatorPort(), 1000,
        "abc_001", MonarchUtils.TABLE_TYPE_ORDERED);
    conf.set("columns", COLUMNS);
    conf.set(MonarchUtils.WRITE_KEY_COLUMN, String.valueOf(keyColumn));
    conf.set(MonarchUtils.MONARCH_BATCH_SIZE, "1");

    TestHelper.writeUsingRecordWriter_Array(lines, conf);
    assertOnScanCount(regionName, scanMaxVersions, expectedRowCount, explodedRowCount);
  }

  /**
   * Assert that the correct row counts, actual and exploded, are returned from scan.
   * @param tableName the table -name
   * @param maxVersions the max-versions to be used for querying during scan
   * @param expectedRowCount expected row count as stored with multiple versions
   * @param explodedRowCount actual row count where multiple versions are exploded as separate rows
   */
  private void assertOnScanCount(final String tableName, final String maxVersions,
                                 final int expectedRowCount, final int explodedRowCount) {
    AmpoolClient ac = new AmpoolClient("localhost", Integer.parseInt(testBase.getLocatorPort()));
    Scan scan = new Scan().setMaxVersions(Integer.parseInt(maxVersions), false);
    final Scanner scanner = ac.getMTable(tableName).getScanner(scan);
    int rowCount = 0, explodedCount = 0;
    for (final Row row : scanner) {
      rowCount++;
      explodedCount += (row.getAllVersions().size());
    }
    assertEquals(rowCount, expectedRowCount);
    assertEquals(explodedCount, explodedRowCount);
  }
}