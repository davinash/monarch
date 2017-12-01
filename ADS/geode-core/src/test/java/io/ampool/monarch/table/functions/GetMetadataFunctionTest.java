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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableColumnType;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.ProxyMTableRegion;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.interfaces.DataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.MGetMetadataFunction;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class GetMetadataFunctionTest {
  public static TestDUnitBase testBase = new TestDUnitBase();

  @BeforeClass
  public static void setUp() throws Exception {
    testBase.preSetUp();
    testBase.postSetUp();
    putDataInTable(regionNameUnOrdered, false, null);
    putDataInTable(regionNameOrdered, true,
        new Pair<>("row__0".getBytes(), "row__9999999999".getBytes()));
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    MClientCache clientCache = testBase.getClientCache();
    clientCache.getAdmin().deleteMTable(regionNameUnOrdered);
    clientCache.getAdmin().deleteMTable(regionNameOrdered);
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
  private static final String regionNameImmutable = "region_get_count_immutable";
  private static final String regionNameOrdered = "region_get_count_ordered";
  private static final String[] columnNames = new String[] {"c0", "c1", "c2", "c3"};
  private static final Map<String, String> columnMap = new LinkedHashMap<String, String>(4) {
    {
      put("c0", "LONG");
      put("c1", "STRING");
      put("c2", "INT");
      put("c3", "STRING");
    }
  };

  /** helpers for putting data appropriately **/
  /**
   * Put the specified data in region as 2-D byte array (byte[][]).
   *
   * @throws IOException
   * @throws org.apache.geode.test.dunit.RMIException
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

    totalDataLen = 0;
    List<Put> putList = new ArrayList<>(SAMPLE_DATA.length);
    for (int rowId = 0; rowId < SAMPLE_DATA.length; rowId++) {
      putList.add(new Put("row__" + rowId));
      int i = 0;
      for (String col : SAMPLE_DATA[rowId].split(",")) {
        final Object val = converter[i].apply(col);
        putList.get(rowId).addColumn(columnNames[i], val);
        totalDataLen += (dataTypes[i].serialize(val).length);
        i++;
      }
    }
    table.put(putList);
  }

  private static int totalDataLen = 0;
  private static int totalDataLenF = 0;

  /**
   * Test basic functionality of GetMetadataFunction with un-ordered table.
   *
   * @throws Exception
   */
  @Test
  public void testExecute_UnOrdered() throws Exception {
    MTable mTable = testBase.getClientCache().getTable(regionNameUnOrdered);
    Map<Integer, Pair<ServerLocation, Long>> primaryMap = new HashMap<>(10);
    Map<Integer, Set<Pair<ServerLocation, Long>>> secondaryMap = new HashMap<>(10);
    MTableUtils.getLocationAndCount(mTable, primaryMap, secondaryMap);

    long totalCount = MTableUtils.getTotalCount(mTable, null, null, true);
    long primaryCount = primaryMap.values().stream().mapToLong(Pair::getSecond).sum();
    long secondaryCount = secondaryMap.values().stream()
        .mapToLong(x -> x.stream().mapToLong(Pair::getSecond).sum()).sum();

    assertEquals(totalCount, primaryCount);
    // Since we have redundant copies set to 1, we will have double the totalcount
    assertEquals(totalCount, secondaryCount);
  }

  @Test
  public void testTotalDataSize() throws Exception {
    MTable mTable = testBase.getClientCache().getTable(regionNameUnOrdered);
    Map<Integer, Pair<ServerLocation, Long>> primaryMap = new HashMap<>(10);
    Map<Integer, Set<Pair<ServerLocation, Long>>> secondaryMap = new HashMap<>(10);
    MTableUtils.getLocationAndSize(mTable, primaryMap, secondaryMap);

    long primarySize = primaryMap.values().stream().mapToLong(Pair::getSecond).sum();
    long secondarySize = secondaryMap.values().stream()
        .mapToLong(x -> x.stream().mapToLong(Pair::getSecond).sum()).sum();

    long actualSize = 0;
    int count = 0;
    for (final Row row : mTable.getScanner(new Scan())) {
      count++;
      for (final Cell cell : row.getCells()) {
        if (!MTableUtils.KEY_COLUMN_NAME
            .equals(BasicTypes.STRING.deserialize(cell.getColumnName()))) {
          actualSize += (cell.getValueLength());
        }
      }
    }
    assertEquals(SAMPLE_DATA.length, count);
    assertEquals(totalDataLen, actualSize);
    assertEquals(totalDataLen, primarySize);
    assertEquals(totalDataLen, secondarySize);
  }

  @SuppressWarnings("unchecked")
  public static void putDataInTableF(final String tableName, FTableDescriptor.BlockFormat format)
      throws IOException, RMIException {
    MClientCache clientCache = testBase.getClientCache();

    final FTableDescriptor td = new FTableDescriptor();
    td.setRedundantCopies(1);
    td.setTotalNumOfSplits(10);
    td.setBlockSize(2);
    td.setBlockFormat(format);
    columnMap.entrySet().stream()
        .forEach(e -> td.addColumn(e.getKey(), new MTableColumnType(e.getValue())));
    FTable table = clientCache.getAdmin().createFTable(tableName, td);
    assertEquals(table.getName(), tableName);
    assertNotNull(table);

    totalDataLenF = 0;
    Record[] records = new Record[SAMPLE_DATA.length];
    for (int rowId = 0; rowId < SAMPLE_DATA.length; rowId++) {
      records[rowId] = new Record();
      int i = 0;
      int d = 0;
      for (String col : SAMPLE_DATA[rowId].split(",")) {
        final byte[] val = dataTypes[i].serialize(converter[i].apply(col));
        records[rowId].add(columnNames[i], val);
        d += val.length;
        i++;
      }
      table.append(records[rowId]);
      totalDataLenF += (d + BasicTypes.LONG.lengthOfByteArray());
    }
  }

  public static Object[] getBlockFormat() {
    return FTableDescriptor.BlockFormat.values();
  }

  @Test
  @Parameters(method = "getBlockFormat")
  public void testTotalDataSizeF(final FTableDescriptor.BlockFormat format) throws Exception {
    putDataInTableF(regionNameImmutable, format);

    FTable table = testBase.getClientCache().getFTable(regionNameImmutable);
    Map<Integer, Pair<ServerLocation, Long>> primaryMap = new HashMap<>(10);
    Map<Integer, Set<Pair<ServerLocation, Long>>> secondaryMap = new HashMap<>(10);
    MTableUtils.getLocationAndSize(table, primaryMap, secondaryMap);

    long primarySize = primaryMap.values().stream().mapToLong(Pair::getSecond).sum();
    long secondarySize = secondaryMap.values().stream()
        .mapToLong(x -> x.stream().mapToLong(Pair::getSecond).sum()).sum();

    long actualSize = 0;
    int count = 0;
    for (final Row row : table.getScanner(new Scan())) {
      count++;
      for (final Cell cell : row.getCells()) {
        actualSize += (cell.getValueLength());
      }
    }
    testBase.getClientCache().getAdmin().deleteFTable(regionNameImmutable);

    assertEquals(SAMPLE_DATA.length, count);
    assertEquals(totalDataLenF, actualSize);
    if (format == FTableDescriptor.BlockFormat.AMP_BYTES) {
      assertEquals(totalDataLenF, primarySize);
      assertEquals(totalDataLenF, secondarySize);
    } else {
      assertEquals("Incorrect primary/secondary size.", primarySize, secondarySize);
    }
  }

  /**
   * Test basic functionality of GetMetadataFunction with ordered table.
   *
   * @throws Exception
   */
  @Test
  public void testExecute_Ordered() throws Exception {
    MTable mTable = testBase.getClientCache().getTable(regionNameUnOrdered);
    Map<Integer, Pair<ServerLocation, Long>> primaryMap = new HashMap<>(10);
    Map<Integer, Set<Pair<ServerLocation, Long>>> secondaryMap = new HashMap<>(10);
    MTableUtils.getLocationAndCount(mTable, primaryMap, secondaryMap);

    long totalCount = MTableUtils.getTotalCount(mTable, null, null, true);
    long primaryCount = primaryMap.values().stream().mapToLong(Pair::getSecond).sum();
    long secondaryCount = secondaryMap.values().stream()
        .mapToLong(x -> x.stream().mapToLong(Pair::getSecond).sum()).sum();

    assertEquals(totalCount, primaryCount);
    // Since we have redundant copies set to 1, we will have double the totalcount
    assertEquals(totalCount, secondaryCount);
  }

  private MTable createDummyTable(final String tableName) {
    final MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    td.setRedundantCopies(1);
    columnMap.entrySet().stream()
        .forEach(e -> td.addColumn(e.getKey(), new MTableColumnType(e.getValue())));
    MTable table = testBase.getClientCache().getAdmin().createTable(tableName, td);
    assertEquals(table.getName(), tableName);
    assertNotNull(table);
    return table;
  }

  /**
   * Try to get metadata for deleted table.
   *
   * @throws Exception
   */
  @Test
  public void testExecuteDeletedRegion() throws Exception {
    final String tableName = "dummy_table";
    final MTable mTable = createDummyTable(tableName);
    testBase.getClientCache().getAdmin().deleteTable(tableName);
    Map<Integer, Pair<ServerLocation, Long>> primaryMap = new HashMap<>(10);
    Map<Integer, Set<Pair<ServerLocation, Long>>> secondaryMap = new HashMap<>(10);
    MTableUtils.getLocationAndCount(mTable, primaryMap, secondaryMap);
    assertEquals(0, primaryMap.size());
    assertEquals(0, secondaryMap.size());

    long totalCount = MTableUtils.getTotalCount(mTable, null, null, true);
    assertEquals(0, totalCount);
  }

  /**
   * Try to get metadata with `null` arguments and assert IllegalArgumentException.
   *
   * @throws Exception
   */
  @Test
  public void testExecuteWithNullArguments() throws Exception {
    final String tableName = "dummy_table";
    final MTable mTable = createDummyTable(tableName);
    try {
      FunctionService.onServers(((ProxyMTableRegion) mTable).getTableRegion().getRegionService())
          .execute(MGetMetadataFunction.GET_METADATA_FUNCTION).getResult();
      fail("Expected IllegalArgumentException.");
    } catch (FunctionException fe) {
      assertTrue(fe.getCause() instanceof IllegalArgumentException);
    } finally {
      testBase.getClientCache().getAdmin().deleteTable(tableName);
    }
  }

  /**
   * Try to get metadata with `null` region-name and assert IllegalArgumentException.
   * 
   * @throws Exception
   */
  @Test
  public void testExecuteWithNullRegionName() throws Exception {
    final String tableName = "dummy_table";
    MGetMetadataFunction.Args args = new MGetMetadataFunction.Args(null);
    final MTable mTable = createDummyTable(tableName);
    try {
      FunctionService.onServers(((ProxyMTableRegion) mTable).getTableRegion().getRegionService())
          .withArgs(args).execute(MGetMetadataFunction.GET_METADATA_FUNCTION).getResult();
      fail("Expected IllegalArgumentException.");
    } catch (FunctionException fe) {
      assertTrue(fe.getCause() instanceof IllegalArgumentException);
    } finally {
      testBase.getClientCache().getAdmin().deleteTable(tableName);
    }
  }

  @Test
  public void testGetId() throws Exception {
    assertEquals(MGetMetadataFunction.class.getName(), new MGetMetadataFunction().getId());
  }
}
