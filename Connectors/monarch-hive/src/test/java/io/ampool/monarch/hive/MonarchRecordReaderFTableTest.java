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

import io.ampool.monarch.TestBase;
import io.ampool.monarch.RMIException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.types.CompareOp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import static org.testng.Assert.assertEquals;


public class MonarchRecordReaderFTableTest extends TestBase {

  private static final String regionName = "region_reader_ftable_test";
  private static final String resourceFile = "data/sample1.txt";
  public static final String COLUMNS = "c1,c2,c3,c4";
  public static final List<String> COLUMN_LIST = Arrays.asList(COLUMNS.split(","));
  public static final Map<String, String> TYPE_HIVE_TO_MTABLE_MAP = new LinkedHashMap<String, String>(5) {{
    put("c1", "bigint");
    put("c2", "string");
    put("c3", "int");
    put("c4", "string");
  }};
  private long readLineCount;

  @BeforeTest
  public void setUpBeforeMethod() throws Exception {
    readLineCount = populateDataFromFile(resourceFile);
  }

  @AfterTest
  public void setUpAfterMethod() throws RMIException, Exception {
    final Map<String, String> map = new HashMap<>();
    map.put(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());
    map.put(MonarchUtils.REGION, regionName);
    MonarchUtils.destroyFTable(regionName, map, false, true);
  }

  /**
   * Provide the configuration with required region name.
   *
   * @return the configuration object
   */
  private Configuration getConfiguration(String colsToGet) {
    Configuration conf = new Configuration();
    conf.set(MonarchUtils.REGION, regionName);
    conf.set("columns", COLUMNS);
    conf.set(MonarchUtils.MONARCH_TABLE_TYPE, MonarchUtils.DEFAULT_TABLE_TYPE);
    return conf;
  }

  /**
   * Provide the configuration for each test.
   *
   * @return
   */
  @DataProvider
  private Object[][] getConf() {
    return new Object[][]{
      {getConfiguration(COLUMNS)}
    };
  }

  /**
   * Get input splits for the specified split-size.
   *
   * @param regionName the region name
   * @param splitSize  the split-size
   * @return an array of splits to be read
   */
  private InputSplit[] getSplits(final String regionName, final int splitSize) throws IOException {
    JobConf jobConf = new JobConf();
    jobConf.set(MonarchUtils.REGION, regionName);
    jobConf.set("mapred.input.dir", "/home/mgalande");
    jobConf.set(MonarchUtils.SPLIT_SIZE_KEY, String.valueOf(splitSize));
    jobConf.set(MonarchUtils.MONARCH_TABLE_TYPE, MonarchUtils.DEFAULT_TABLE_TYPE);
    return MonarchSplit.getSplits(jobConf, 1);
  }

  /**
   * Make sure data is written using MonarchRecordWriter before reader tests.
   *
   * @throws IOException
   * @throws RMIException
   */
  public int populateDataFromFile(final String resourceFile) throws Exception {
    /** create region and region_meta.. **/
    final Map<String, String> map = new HashMap<>();
    map.put(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());
    map.put(MonarchUtils.REGION, regionName);
    MonarchUtils.createConnectionAndFTable(regionName, map, false, null, TYPE_HIVE_TO_MTABLE_MAP);

    final List<String> lines = TestHelper.getResourceAsString(resourceFile);
    Configuration conf = TestHelper.getConfiguration(regionName, testBase.getLocatorPort(), 100, "", MonarchUtils.DEFAULT_TABLE_TYPE);
    conf.set("columns", COLUMNS);
    TestHelper.writeUsingRecordWriter_Array(lines, conf);
    return lines.size();
  }

  /**
   * Read from Geode, using MonarchRecordReader, all the records from the provided split.
   * The split contains the range of records to be read by the record reader. It
   * returns the total number of records read by this method.
   *
   * @param conf       the reader configuration -- must have the region name
   * @param split      the input-split containing the records to be read
   * @param predicates the predicates to filter out unwanted results
   * @return the total number of records read
   */
  private long readUsingRecordReader(final Configuration conf, final InputSplit split,
                                     final Filter... predicates) {
    MonarchRecordReader mrr = new MonarchRecordReader(conf);
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    for (int i=0; i<predicates.length; i++) {
      filterList.addFilter(predicates[i]);
    }
    mrr.pushDownfilters = filterList;
    long size = 0;
    try {
      mrr.initialize(split, conf);
      Writable key = mrr.createKey();
      Writable value = mrr.createValue();
      while (mrr.next(key, value)) {
        ++size;
      }
      mrr.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return size;
  }

  /**
   * Test using sequential reader.
   *
   * @throws Exception
   */
  @Test(dataProvider = "getConf")
  public void testReader_SequentialReaders(final Configuration conf) throws Exception {
//    System.out.println("MonarchRecordReaderTest.testReader_SequentialReaders");

    long totalRecords = 0;
    for (InputSplit is : getSplits(regionName, 50)) {
      totalRecords += readUsingRecordReader(conf, is);
    }

    assertEquals(totalRecords, readLineCount);
  }

  /**
   * Test using parallel readers.
   *
   * @throws Exception
   */
  @Test(dataProvider = "getConf")
  public void testReader_ParallelReaders(final Configuration conf) throws Exception {
//    System.out.println("MonarchRecordReaderTest.testReader_ParallelReaders");

    ExecutorService es = Executors.newFixedThreadPool(3);
    long totalRecords = 0;
    List<FutureTask> fts = new ArrayList<>(5);
    for (final InputSplit is : getSplits(regionName, 10)) {
      FutureTask ft = new FutureTask<>(() -> readUsingRecordReader(conf, is));
      fts.add(ft);
      es.submit(ft);
    }
    es.shutdown();

    for (final FutureTask ft : fts) {
      totalRecords += (Long) ft.get();
    }

    assertEquals(totalRecords, readLineCount);
  }

  /**
   * Test using sequential reader.
   *
   * @throws Exception
   */
  @Test(dataProvider = "getConf")
  public void testReaderWithSmallerBatchSize(final Configuration conf) throws Exception {
//    System.out.println("MonarchRecordReaderTest.testReaderWithSmallerBatchSize");
    conf.set(MonarchUtils.MONARCH_BATCH_SIZE, "2");

    long totalRecords = 0;
    for (InputSplit is : getSplits(regionName, 10)) {
      totalRecords += readUsingRecordReader(conf, is);
    }

    assertEquals(totalRecords, readLineCount);
  }

  @DataProvider
  public static Object[][] getPredicates() {
    return new Object[][]{
      {19, new Filter[]{
        new SingleColumnValueFilter(COLUMN_LIST.get(2), CompareOp.GREATER_OR_EQUAL, 0),
        new SingleColumnValueFilter(COLUMN_LIST.get(2), CompareOp.NOT_EQUAL, 4096)}},
      {0, new Filter[]{
        new SingleColumnValueFilter(COLUMN_LIST.get(2), CompareOp.EQUAL, 0),
        new SingleColumnValueFilter(COLUMN_LIST.get(2), CompareOp.NOT_EQUAL, 4096)}},
      {1, new Filter[]{
        new SingleColumnValueFilter(COLUMN_LIST.get(0), CompareOp.EQUAL, 12978724L),
        new SingleColumnValueFilter(COLUMN_LIST.get(3), CompareOp.REGEX, ".*")}},
      {6, new Filter[]{
        new SingleColumnValueFilter(COLUMN_LIST.get(0), CompareOp.GREATER, 0L),
        new SingleColumnValueFilter(COLUMN_LIST.get(3), CompareOp.REGEX, ".*spark.*")}},
      {0, new Filter[]{
        new SingleColumnValueFilter(COLUMN_LIST.get(0), CompareOp.LESS, 0L)}},
    };
  }

  /**
   * Test reader with predicates..
   */
  @Test(dataProvider = "getPredicates")
  public void testReaderWithPredicates(final int expectedCount, final Filter[] phs) throws IOException{
//    System.out.println("MonarchRecordReaderTest.testReaderWithPredicates");
    long totalRecords = 0;
    for (InputSplit is : getSplits(regionName, 50)) {
      totalRecords += readUsingRecordReader(getConfiguration(COLUMNS), is, phs);
    }

    assertEquals(totalRecords, expectedCount);
  }

  /**
   * Read using record reader and assert that the columns not requested have 0 length.
   * <p>
   * @param conf       the reader configuration -- must have the region name
   * @param split      the input-split containing the records to be read
   * @param predicates the predicates to filter out unwanted results
   * @param readColIds the column ids to retrieve
   * @return total number of records read
   */
  private long readAndAssertOnEmptyCols(final Configuration conf, final InputSplit split,
                                        final String readColIds, final Filter[] predicates) throws IOException{
    MonarchRecordReader mrr = new MonarchRecordReader(conf);
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    if (predicates != null) {
      for (int i = 0; i < predicates.length; i++) {
        filterList.addFilter(predicates[i]);
      }
      mrr.pushDownfilters = filterList;
    }
//    mrr.readColIds = readColIds;

    /*List<Integer> readColIdList = readColIds == null ? Collections.emptyList() :
      Arrays.stream(readColIds.split(",")).mapToInt(Integer::valueOf)
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);*/
    List<Integer> readColIdList = ColumnProjectionUtils.getReadColumnIDs(conf);
    long size = 0;
    try {
      mrr.initialize(split, conf);
      Writable key = mrr.createKey();
      Writable value = mrr.createValue();
      while (mrr.next(key, value)) {
        BytesRefArrayWritable braw = (BytesRefArrayWritable) value;
        /** assert that skipped (not read) columns have 0 length **/
        for (int i = 0; i < braw.size(); i++) {
          if (!readColIdList.isEmpty() && !readColIdList.contains(i)) {
            assertEquals(0, braw.get(i).getLength());
          }
        }
        ++size;
      }
      mrr.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return size;
  }

  @DataProvider
  public static Object[][] getPredicatesCols() {
    return new Object[][]{
      {31, "0,3", "c1,c4", null},
      {19, "0,1", "c1,c2",new Filter[]{
        new SingleColumnValueFilter(COLUMN_LIST.get(2), CompareOp.GREATER_OR_EQUAL, 0),
        new SingleColumnValueFilter(COLUMN_LIST.get(2), CompareOp.NOT_EQUAL, 4096)}},
      {19, "0,2", "c1,c3", new Filter[]{
        new SingleColumnValueFilter(COLUMN_LIST.get(2), CompareOp.GREATER_OR_EQUAL, 0),
        new SingleColumnValueFilter(COLUMN_LIST.get(2), CompareOp.NOT_EQUAL, 4096)}},
      {0, "2", "c3", new Filter[]{
        new SingleColumnValueFilter(COLUMN_LIST.get(2), CompareOp.EQUAL, 0),
        new SingleColumnValueFilter(COLUMN_LIST.get(2), CompareOp.NOT_EQUAL, 4096)}},
      {1, null, null, new Filter[]{
        new SingleColumnValueFilter(COLUMN_LIST.get(0), CompareOp.EQUAL, 12978724L),
        new SingleColumnValueFilter(COLUMN_LIST.get(3), CompareOp.REGEX, ".*")}},
      {6, "3", "c4", new Filter[]{
        new SingleColumnValueFilter(COLUMN_LIST.get(0), CompareOp.GREATER, 0L),
        new SingleColumnValueFilter(COLUMN_LIST.get(3), CompareOp.REGEX, ".*spark.*")}},
      {31, null, null, new Filter[]{
        new SingleColumnValueFilter(COLUMN_LIST.get(0), CompareOp.GREATER, 0L)}},
    };
  }

  /**
   * Assert that only requested columns are fetched and empty cells are returned for rest.
   *
   * @param expectedCount expected number of rows
   * @param colsToGet comma separated list of column ids to retrieve
   * @param phs the predicate holder to be tested before retrieving the rows
   */
  @Test(dataProvider = "getPredicatesCols")
  public void testReaderWithSelectedCols(final int expectedCount, final String colsToGet, final String colIDs,
                                         final Filter[] phs) throws IOException{
//    System.out.println("MonarchRecordReaderTest.testReaderWithSelectedCols");
    long totalRecords = 0;
    for (InputSplit is : getSplits(regionName, 50)) {
      totalRecords += readAndAssertOnEmptyCols(getConfiguration(colIDs), is, colsToGet, phs);
    }

    assertEquals(totalRecords, expectedCount);
  }
}
