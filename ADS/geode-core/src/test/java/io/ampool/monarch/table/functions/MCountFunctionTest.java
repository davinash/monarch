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

package io.ampool.monarch.table.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;

import io.ampool.monarch.table.Scanner;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.MCountFunction;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.ProxyMTableRegion;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.MPredicateHolder;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.interfaces.TypePredicateOp;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class MCountFunctionTest {

  public static TestDUnitBase testBase = new TestDUnitBase();

  @BeforeClass
  public static void setUp() throws Exception {
    testBase.preSetUp();
    testBase.postSetUp();
    putDataInTable(regionNameUnOrdered, false, null);
    putDataInTable(regionNameOrdered, true,
        new Pair<>("row__0".getBytes(), "row__9999999999".getBytes()));
    putDataInFTable(fTableRegion);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    MClientCache clientCache = testBase.getClientCache();
    clientCache.getAdmin().deleteTable(regionNameUnOrdered);
    clientCache.getAdmin().deleteTable(regionNameOrdered);
    clientCache.getAdmin().deleteFTable(fTableRegion);
    testBase.preTearDownCacheTestCase();
  }

  /** data.. **/
  private static final String[] SAMPLE_DATA =
      new String[] {"12976997,drwxrwxr-x,4096,target", "12978335,-rw-rw-r--,30,target/.plxarc",
          "12976998,drwxrwxr-x,4096,target/maven-shared-archive-resources",
          "12977001,drwxrwxr-x,4096,target/maven-shared-archive-resources/META-INF",
          "12978333,-rw-rw-r--,443,target/maven-shared-archive-resources/META-INF/DEPENDENCIES",
          "12977003,-rw-rw-r--,156,target/maven-shared-archive-resources/META-INF/NOTICE",
          "12978332,-rw-rw-r--,11358,target/maven-shared-archive-resources/META-INF/LICENSE",
          "12978691,drwxrwxr-x,4096,target/tmp", "12978693,drwxrwxr-x,4096,target/tmp/conf",
          "12978708,-rw-rw-r--,1582,target/tmp/conf/hiveserver2-site.xml",
          "12978727,drwxrwxr-x,4096,target/tmp/conf/spark",
          "12978728,-rw-rw-r--,3109,target/tmp/conf/spark/log4j2.xml",
          "12978729,drwxrwxr-x,4096,target/tmp/conf/spark/standalone",
          "12978730,-rw-rw-r--,7801,target/tmp/conf/spark/standalone/hive-site.xml",
          "12978731,drwxrwxr-x,4096,target/tmp/conf/spark/yarn-client",
          "12978732,-rw-rw-r--,8109,target/tmp/conf/spark/yarn-client/hive-site.xml",
          "12978696,-rw-rw-r--,6706,target/tmp/conf/hive-site-old.xml",
          "12978733,drwxrwxr-x,4096,target/tmp/conf/tez",
          "12978735,-rw-rw-r--,190,target/tmp/conf/tez/tez-site.xml",
          "12978734,-rw-rw-r--,8581,target/tmp/conf/tez/hive-site.xml",
          "12978723,drwxrwxr-x,4096,target/tmp/conf/llap",
          "12978726,-rw-rw-r--,190,target/tmp/conf/llap/tez-site.xml",
          "12978725,-rw-rw-r--,1643,target/tmp/conf/llap/llap-daemon-site.xml",
          "12978724,-rw-rw-r--,8398,target/tmp/conf/llap/hive-site.xml",
          "12978695,-rw-rw-r--,7177,target/tmp/conf/hive-log4j2.xml",
          "12978698,-rw-rw-r--,1586,target/tmp/conf/hivemetastore-site.xml",
          "12978694,-rw-rw-r--,442,target/tmp/conf/fair-scheduler-test.xml",
          "12978697,-rw-rw-r--,8407,target/tmp/conf/hive-site.xml",
          "12978692,drwxrwxr-x,4096,target/warehouse", "12978686,drwxrwxr-x,4096,target/antrun",
          "12978688,-rw-rw-r--,506,target/antrun/build-main.xml"};

  private static final DataType[] dataTypes =
      new DataType[] {BasicTypes.LONG, BasicTypes.STRING, BasicTypes.INT, BasicTypes.STRING};
  private static final Function[] converter = new Function[] {e -> Long.valueOf(e.toString()),
      Object::toString, e -> Integer.valueOf(e.toString()), Object::toString};
  private static final String regionNameUnOrdered = "region_get_count_unordered";
  private static final String regionNameOrdered = "region_get_count_ordered";
  private static final String fTableRegion = "region_get_count_ftable";
  private static final String[] columnNames = new String[] {"c0", "c1", "c2", "c3"};
  private static final Map<String, String> columnMap = new LinkedHashMap<String, String>(4) {
    {
      put("c0", "LONG");
      put("c1", "STRING");
      put("c2", "INT");
      put("c3", "STRING");
    }
  };

  @SuppressWarnings("unchecked")
  public static void putDataInFTable(final String tableName) throws IOException, RMIException {
    MClientCache clientCache = testBase.getClientCache();
    final FTableDescriptor ftd = new FTableDescriptor();
    columnMap.entrySet().stream()
        .forEach(e -> ftd.addColumn(e.getKey(), new MTableColumnType(e.getValue())));
    FTable fTable = clientCache.getAdmin().createFTable(tableName, ftd);
    assertEquals(fTable.getName(), tableName);
    assertNotNull(fTable);

    Record[] records = new Record[SAMPLE_DATA.length];
    for (int i = 0; i < SAMPLE_DATA.length; i++) {
      records[i] = new Record();
      int colIndex = 0;
      for (String col : SAMPLE_DATA[i].split(",")) {
        records[i].add(columnNames[colIndex], converter[colIndex++].apply(col));
      }
    }
    fTable.append(records);
  }

  /** helpers for putting data appropriately **/
  /**
   * Put the specified data in region as 2-D byte array (byte[][]).
   *
   * @throws IOException
   * @throws RMIException
   */
  @SuppressWarnings("unchecked")
  public static void putDataInTable(final String tableName, final boolean isOrdered,
      final Pair<byte[], byte[]> startStopKeyPair) throws IOException, RMIException {

    MClientCache clientCache = testBase.getClientCache();

    final MTableDescriptor td =
        isOrdered ? new MTableDescriptor() : new MTableDescriptor(MTableType.UNORDERED);
    if (isOrdered && startStopKeyPair != null) {
      td.setStartStopRangeKey(startStopKeyPair.getFirst(), startStopKeyPair.getSecond());
    }
    td.setRedundantCopies(1);
    td.setTotalNumOfSplits(50);
    columnMap.entrySet().stream()
        .forEach(e -> td.addColumn(e.getKey(), new MTableColumnType(e.getValue())));
    MTable table = clientCache.getAdmin().createTable(tableName, td);
    assertEquals(table.getName(), tableName);
    assertNotNull(table);

    List<Put> putList = new ArrayList<>(SAMPLE_DATA.length);
    for (int rowId = 0; rowId < SAMPLE_DATA.length; rowId++) {
      putList.add(new Put("row__" + rowId));
      int i = 0;
      for (String col : SAMPLE_DATA[rowId].split(",")) {
        putList.get(rowId).addColumn(columnNames[i], converter[i++].apply(col));
      }
    }
    table.put(putList);
  }

  private long getCountFromScanner(final MTable mTable, final Filter[] predicates) {
    Scan scan = new Scan();
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    if (predicates != null) {
      for (int i = 0; i < predicates.length; i++) {
        filterList.addFilter(predicates[i]);
      }
    }
    scan.setFilter(filterList);
    Scanner scanner = mTable.getScanner(scan);
    long count = 0;
    for (final Row ignored : scanner) {
      count++;
    }
    return count;
  }

  private long getFTableCountFromScanner(final FTable mTable, final Filter[] predicates) {

    Scan scan = new Scan();
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    if (predicates != null) {
      for (int i = 0; i < predicates.length; i++) {
        filterList.addFilter(predicates[i]);
      }
    }
    scan.setFilter(filterList);
    Scanner scanner = mTable.getScanner(scan);

    long count = 0;
    for (final Row ignored : scanner) {
      count++;
    }
    return count;
  }

  private void assertTotalCount(final String regionName, final int numberOfSplits,
      final Filter[] predicates, final int expectedCount) {
    MTable mTable = testBase.getClientCache().getTable(regionName);
    Map<Integer, Set<ServerLocation>> map = new HashMap<>();
    List<MTableUtils.MSplit> splits = MTableUtils.getSplits(regionName, numberOfSplits, 113, map);

    long totalCount = 0;
    for (MTableUtils.MSplit split : splits) {
      totalCount += MTableUtils.getTotalCount(mTable, split.getBuckets(), predicates, true);
    }
    assertEquals(expectedCount, getCountFromScanner(mTable, predicates));
    assertEquals(expectedCount, totalCount);
  }

  private void assertFTableTotalCount(final String regionName, final int numberOfSplits,
      final Filter[] predicates, final int expectedCount) {
    FTable mTable = testBase.getClientCache().getFTable(regionName);
    Map<Integer, Set<ServerLocation>> map = new HashMap<>();
    List<MTableUtils.MSplit> splits = MTableUtils.getSplits(regionName, numberOfSplits, 113, map);

    long totalCount = 0;
    for (MTableUtils.MSplit split : splits) {
      totalCount += MTableUtils.getTotalCount(mTable, split.getBuckets(), predicates, true);
    }
    assertEquals(expectedCount, getFTableCountFromScanner(mTable, predicates));
    assertEquals(expectedCount, totalCount);
  }

  private Filter[] newPredicates(final String columnId, final TypePredicateOp op,
      final Object value) {
    return new Filter[] {new SingleColumnValueFilter(columnId, (CompareOp) op, value)};
  }

  /** Tests for count with various combinations.. **/
  @Test
  public void testGetCount_NoBucketIds_UnOrdered() throws IOException, RMIException {
    MTable mTable = testBase.getClientCache().getTable(regionNameUnOrdered);
    long totalCount = MTableUtils.getTotalCount(mTable, null, null, true);

    assertEquals(SAMPLE_DATA.length, getCountFromScanner(mTable, null));
    assertEquals(SAMPLE_DATA.length, totalCount);
  }

  @Test
  public void testGetCount_SplitBucketIds_UnOrdered() {
    assertTotalCount(regionNameUnOrdered, 3, null, SAMPLE_DATA.length);
  }

  @Test
  public void testGetCount_NoBucketIds_Ordered() throws IOException, RMIException {
    MTable mTable = testBase.getClientCache().getTable(regionNameOrdered);
    long totalCount = MTableUtils.getTotalCount(mTable, null, null, true);

    assertEquals(SAMPLE_DATA.length, getCountFromScanner(mTable, null));
    assertEquals(SAMPLE_DATA.length, totalCount);
  }

  @Test
  public void testGetCount_fTable() throws IOException, RMIException {
    FTable table = testBase.getClientCache().getFTable(fTableRegion);
    long totalCount = MTableUtils.getTotalCount(table, null, null, true);

    System.out.println("NNNNN MCountFunctionTest.testGetCount_fTable totalCount = " + totalCount);
    long scannerCount = getFTableCountFromScanner(table, null);
    System.out
        .println("NNNNN MCountFunctionTest.testGetCount_fTable scannerCount = " + scannerCount);
    assertEquals(SAMPLE_DATA.length, getFTableCountFromScanner(table, null));
    assertEquals(SAMPLE_DATA.length, totalCount);
  }

  @Test
  public void testGetCount_WithPredicates_FTable() {
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[2], CompareOp.GREATER_OR_EQUAL, 0), 31);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[2], CompareOp.LESS_OR_EQUAL, 0), 0);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[2], CompareOp.GREATER, Integer.MAX_VALUE), 0);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[2], CompareOp.LESS, Integer.MAX_VALUE), 31);
    assertFTableTotalCount(fTableRegion, 3, newPredicates(columnNames[2], CompareOp.LESS, 4096),
        11);
    assertFTableTotalCount(fTableRegion, 3, newPredicates(columnNames[2], CompareOp.EQUAL, 4096),
        12);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[2], CompareOp.NOT_EQUAL, 4096), 19);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[2], CompareOp.LESS_OR_EQUAL, 4096), 23);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[0], CompareOp.GREATER_OR_EQUAL, 0L), 31);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[0], CompareOp.LESS_OR_EQUAL, 0L), 0);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[0], CompareOp.GREATER, Long.MAX_VALUE), 0);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[0], CompareOp.LESS, Long.MAX_VALUE), 31);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[0], CompareOp.EQUAL, 12978724L), 1);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[0], CompareOp.NOT_EQUAL, 12978724L), 30);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[0], CompareOp.EQUAL, 12978724123L), 0);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[0], CompareOp.NOT_EQUAL, 12978724123L), 31);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[3], CompareOp.EQUAL, "target/warehouse"), 1);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[3], CompareOp.NOT_EQUAL, "target/warehouse"), 30);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[1], CompareOp.EQUAL, "-rw-rw-r--"), 19);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[1], CompareOp.NOT_EQUAL, "-rw-rw-r--"), 12);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[1], CompareOp.EQUAL, "drwxrwxr-x"), 12);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[1], CompareOp.REGEX, "d.wxr[xw]{2}[r]-x"), 12);
    assertFTableTotalCount(fTableRegion, 3,
        newPredicates(columnNames[3], CompareOp.REGEX, ".*spark.*"), 6);
  }

  @Test
  public void testGetCount_SplitBucketIds_Ordered() {
    assertTotalCount(regionNameOrdered, 3, null, SAMPLE_DATA.length);
  }

  @Test
  public void testGetCount_WithPredicates_UnOrdered() {
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[2], CompareOp.GREATER_OR_EQUAL, 0), 31);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[2], CompareOp.LESS_OR_EQUAL, 0), 0);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[2], CompareOp.GREATER, Integer.MAX_VALUE), 0);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[2], CompareOp.LESS, Integer.MAX_VALUE), 31);
    assertTotalCount(regionNameUnOrdered, 3, newPredicates(columnNames[2], CompareOp.LESS, 4096),
        11);
    assertTotalCount(regionNameUnOrdered, 3, newPredicates(columnNames[2], CompareOp.EQUAL, 4096),
        12);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[2], CompareOp.NOT_EQUAL, 4096), 19);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[2], CompareOp.LESS_OR_EQUAL, 4096), 23);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[0], CompareOp.GREATER_OR_EQUAL, 0L), 31);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[0], CompareOp.LESS_OR_EQUAL, 0L), 0);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[0], CompareOp.GREATER, Long.MAX_VALUE), 0);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[0], CompareOp.LESS, Long.MAX_VALUE), 31);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[0], CompareOp.EQUAL, 12978724L), 1);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[0], CompareOp.NOT_EQUAL, 12978724L), 30);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[0], CompareOp.EQUAL, 12978724123L), 0);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[0], CompareOp.NOT_EQUAL, 12978724123L), 31);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[3], CompareOp.EQUAL, "target/warehouse"), 1);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[3], CompareOp.NOT_EQUAL, "target/warehouse"), 30);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[1], CompareOp.EQUAL, "-rw-rw-r--"), 19);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[1], CompareOp.NOT_EQUAL, "-rw-rw-r--"), 12);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[1], CompareOp.EQUAL, "drwxrwxr-x"), 12);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[1], CompareOp.REGEX, "d.wxr[xw]{2}[r]-x"), 12);
    assertTotalCount(regionNameUnOrdered, 3,
        newPredicates(columnNames[3], CompareOp.REGEX, ".*spark.*"), 6);
  }

  @Test
  public void testGetCount_WithPredicates_Ordered() {
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[2], CompareOp.GREATER_OR_EQUAL, 0), 31);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[2], CompareOp.LESS_OR_EQUAL, 0), 0);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[2], CompareOp.GREATER, Integer.MAX_VALUE), 0);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[2], CompareOp.LESS, Integer.MAX_VALUE), 31);
    assertTotalCount(regionNameOrdered, 3, newPredicates(columnNames[2], CompareOp.LESS, 4096), 11);
    assertTotalCount(regionNameOrdered, 3, newPredicates(columnNames[2], CompareOp.EQUAL, 4096),
        12);
    assertTotalCount(regionNameOrdered, 3, newPredicates(columnNames[2], CompareOp.NOT_EQUAL, 4096),
        19);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[2], CompareOp.LESS_OR_EQUAL, 4096), 23);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[0], CompareOp.GREATER_OR_EQUAL, 0L), 31);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[0], CompareOp.LESS_OR_EQUAL, 0L), 0);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[0], CompareOp.GREATER, Long.MAX_VALUE), 0);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[0], CompareOp.LESS, Long.MAX_VALUE), 31);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[0], CompareOp.EQUAL, 12978724L), 1);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[0], CompareOp.NOT_EQUAL, 12978724L), 30);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[0], CompareOp.EQUAL, 12978724123L), 0);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[0], CompareOp.NOT_EQUAL, 12978724123L), 31);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[3], CompareOp.EQUAL, "target/warehouse"), 1);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[3], CompareOp.NOT_EQUAL, "target/warehouse"), 30);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[1], CompareOp.EQUAL, "-rw-rw-r--"), 19);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[1], CompareOp.NOT_EQUAL, "-rw-rw-r--"), 12);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[1], CompareOp.EQUAL, "drwxrwxr-x"), 12);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[1], CompareOp.REGEX, "d.wxr[xw]{2}[r]-x"), 12);
    assertTotalCount(regionNameOrdered, 3,
        newPredicates(columnNames[3], CompareOp.REGEX, ".*spark.*"), 6);
  }

  /** some basic tests.. **/
  @Test
  public void testGetId() throws Exception {
    assertEquals(MCountFunction.class.getName(), new MCountFunction().getId());
  }

  @Test
  public void testArgs_0() {
    Set<Integer> set = new HashSet<Integer>(1) {
      {
        add(1);
      }
    };
    Filter[] predicates = newPredicates(columnNames[2], CompareOp.GREATER_OR_EQUAL, 0);
    MCountFunction.Args args = new MCountFunction.Args("abc", Collections.emptySet(), true);
    args.setBucketIds(set);
    // args.setPredicates(predicates);
    args.setFilter(predicates);

    Set resultSet = null;
    MPredicateHolder[] resultPredicates = null;
    try {
      Field field = args.getClass().getDeclaredField("bucketIds");
      field.setAccessible(true);
      resultSet = (Set) field.get(args);
      field = args.getClass().getDeclaredField("predicates");
      field.setAccessible(true);
      resultPredicates = (MPredicateHolder[]) field.get(args);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      e.printStackTrace();
    }
    assertEquals(set, resultSet);
    // assertArrayEquals(predicates, resultPredicates);
  }

  @Test(expected = FunctionException.class)
  public void testArgs_1() {
    MTable mTable = testBase.getClientCache().getTable(regionNameUnOrdered);
    FunctionService.onServers(((ProxyMTableRegion) mTable).getTableRegion().getRegionService())
        .withArgs(null).execute(MCountFunction.COUNT_FUNCTION).getResult();
  }

  @Test(expected = FunctionException.class)
  public void testArgs_2() {
    MTable mTable = testBase.getClientCache().getTable(regionNameUnOrdered);
    MCountFunction.Args args = new MCountFunction.Args(null, null, true);
    FunctionService.onServers(((ProxyMTableRegion) mTable).getTableRegion().getRegionService())
        .withArgs(args).execute(MCountFunction.COUNT_FUNCTION).getResult();
  }
}
