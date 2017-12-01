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

package io.ampool.monarch.table.facttable.junit;

import static io.codearte.catchexception.shade.mockito.Matchers.anyInt;
import static io.codearte.catchexception.shade.mockito.Matchers.anyString;
import static io.codearte.catchexception.shade.mockito.Mockito.doReturn;
import static io.codearte.catchexception.shade.mockito.Mockito.mock;
import static io.codearte.catchexception.shade.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.FilterList.Operator;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.filter.internal.BlockKeyFilter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.ftable.internal.FTableScanner;
import io.ampool.monarch.table.region.FTableByteUtils;
import io.ampool.monarch.table.region.ScanContext;
import io.ampool.monarch.table.region.ScanEntryHandler;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.store.DefaultConstructorMissingException;
import io.ampool.store.StoreHandler;
import io.ampool.store.StoreRecord;
import io.ampool.store.StoreUtils;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.TierStoreWriter;
import io.ampool.tierstore.wal.WriteAheadLog;
import io.ampool.utils.ReflectionUtils;
import io.codearte.catchexception.shade.mockito.Mockito;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.internal.FileUtil;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionMap;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


@Category(FTableTest.class)
@RunWith(JUnitParamsRunner.class)
public class FTableScannerTest {

  @Test
  public void handleSpecialColumnFilters() throws Exception {
    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, 1000l);
    Filter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, 2000l);
    FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2);

    Filter filter3 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.GREATER, 5000l);
    Filter filter4 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, 6000l);

    FilterList list1 = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter3, filter4);

    FilterList list2 = new FilterList(FilterList.Operator.MUST_PASS_ONE, list, list1);

    Scan scan = new Scan();
    scan.setFilter(list2);

    FTableScanner scanner = new FTableScanner();
    final FilterList newFilter = scanner.handleSpecialColumnFilters(scan.getFilter());


    // verify now the return list

    assertTrue(newFilter.getOperator() == Operator.MUST_PASS_ONE);
    final List<Filter> filters = newFilter.getFilters();
    assertEquals(filters.size(), 2);
    for (int i = 0; i < filters.size(); i++) {
      final Filter filter = filters.get(i);
      assertTrue(filter instanceof FilterList);
      final FilterList filterList = (FilterList) filter;
      assertTrue(filterList.getOperator() == Operator.MUST_PASS_ALL);
      final List<Filter> filterss = filterList.getFilters();
      assertEquals(filterss.size(), 2);
      if (i == 0) {
        // first filter
        for (int j = 0; j < filterss.size(); j++) {
          final Filter singleFilter = filterss.get(j);
          assertTrue(singleFilter instanceof BlockKeyFilter);
          final BlockKeyFilter sFilter = (BlockKeyFilter) singleFilter;
          if (j == 0) {
            assertTrue(sFilter.getOperator() == CompareOp.GREATER);
            assertTrue(Bytes.toLong(((byte[]) (sFilter.getValue()))) == 1000l);
          } else {
            assertTrue(sFilter.getOperator() == CompareOp.LESS);
            assertTrue(Bytes.toLong(((byte[]) (sFilter.getValue()))) == 2000l);
          }
        }
      } else {
        // second filter
        for (int j = 0; j < filterss.size(); j++) {
          final Filter singleFilter = filterss.get(j);
          assertTrue(singleFilter instanceof BlockKeyFilter);
          final BlockKeyFilter sFilter = (BlockKeyFilter) singleFilter;
          if (j == 0) {
            assertTrue(sFilter.getOperator() == CompareOp.GREATER);
            assertTrue(Bytes.toLong(((byte[]) (sFilter.getValue()))) == 5000l);
          } else {
            assertTrue(sFilter.getOperator() == CompareOp.LESS);
            assertTrue(Bytes.toLong(((byte[]) (sFilter.getValue()))) == 6000l);
          }
        }
      }

    }
  }

  /* helper methods to mock the required objects and ingest the data in various tiers */

  /**
   * Mock the RegionMap that is used for in-memory scan.
   *
   * @return the mocked region-map object
   */
  private static RegionMap mockRegionMap() {
    when(REGION.getName()).thenReturn(TABLE_NAME);
    PartitionedRegionDataStore ds = mock(PartitionedRegionDataStore.class);
    BucketRegion br = mock(BucketRegion.class);
    when(REGION.getDataStore()).thenReturn(ds);
    when(ds.getLocalBucketById(anyInt())).thenReturn(br);
    BucketAdvisor ba = mock(BucketAdvisor.class);
    when(br.getBucketAdvisor()).thenReturn(ba);
    when(ba.isHosting()).thenReturn(true);
    RegionMap rm = mock(RegionMap.class);
    when(br.getRegionMap()).thenReturn(rm);
    return rm;
  }

  private static ConverterDescriptor getConverter() {
    final String converterClass = "io.ampool.tierstore.internal.ORCConverterDescriptor";
    try {
      Class<?> clazz = Class.forName(converterClass);
      return (ConverterDescriptor) clazz.getConstructor(TableDescriptor.class).newInstance(td);
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException
        | InvocationTargetException | InstantiationException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Initialize the StoreHandler and one local disk tier store.
   *
   * @return the store handler
   * @throws IOException if I/O exception had occurred
   */
  private static StoreHandler initTierStore() throws IOException {
    StoreHandler.getInstance();
    ReflectionUtils.setStaticFieldValue(WriteAheadLog.class, "initialized", false);
    WriteAheadLog.getInstance().init(new File(WAL_DIR).toPath());
    StoreHandler sh = Mockito.spy(StoreHandler.getInstance());
    ConverterDescriptor cd = getConverter();
    ConcurrentHashMap<String, ConverterDescriptor> cdMap = new ConcurrentHashMap<>();
    cdMap.put(TABLE_NAME, cd);

    // when(sh.getTierStores()).thenReturn(new HashMap<>());
    TierStore lts;
    final String ORC_READER_CLASS = "io.ampool.tierstore.readers.orc.TierStoreORCReader";
    final String ORC_WRITER_CLASS = "io.ampool.tierstore.writers.orc.TierStoreORCWriter";
    TierStoreReader reader;
    TierStoreWriter writer;
    try {
      final Constructor<?> c = StoreUtils
          .getTierStoreConstructor(Class.forName("io.ampool.tierstore.stores.LocalTierStore"));
      lts = (TierStore) c.newInstance("local-disk-tier");
      reader = (TierStoreReader) Class.forName(ORC_READER_CLASS).newInstance();
      writer = (TierStoreWriter) Class.forName(ORC_WRITER_CLASS).newInstance();
      ReflectionUtils.setStaticFieldValue(c.getDeclaringClass(), "converterDescriptors", cdMap);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException
        | DefaultConstructorMissingException | InvocationTargetException e) {
      e.printStackTrace();
      throw new IOException("Error.." + e.getMessage());
    }

    Properties props = new Properties();
    props.setProperty("base.dir.path", TIER_1_DIR);
    ReflectionUtils.setFieldValue(lts, "reader", reader);
    ReflectionUtils.setFieldValue(lts, "writer", writer);
    ReflectionUtils.setFieldValue(lts, "storeProperties", props);
    sh.registerStore(TIER_1_NAME, lts);
    doReturn(OPTS).when(sh).getStoreHierarchy(anyString());
    ReflectionUtils.setStaticFieldValue(StoreHandler.class, "storeHandler", sh);
    return sh;
  }

  /**
   * Setup the tier-1 (i.e. local-disk-tier) with the specified number of records.
   *
   * @param totalCount the number of records to ingest
   * @throws IOException if I/O exception had occurred
   */
  private static void setupTier1(final int totalCount) throws IOException {
    BlockKey k2 = new BlockKey(1, 1, 0);
    StoreRecord[] srs = new StoreRecord[5];
    for (int i = 0; i < totalCount; i++) {
      StoreRecord record = new StoreRecord(td.getNumOfColumns());
      record.addValue(C1_OFFSET[2] + i);
      record.addValue(C2_PFX[2] + i);
      record.addValue(Double.valueOf(i + "." + i + i));
      record.addValue(1L);
      srs[i] = record;
    }
    STORE_HANDLER.writeToStore(TABLE_NAME, BUCKET_ID, srs);
  }

  /**
   * Setup the WAL with the specified number of records.
   *
   * @param totalCount the number of records to ingest
   * @throws IOException if I/O exception had occurred
   */
  private static void setupWal(final int totalCount) throws IOException {
    BlockKey k1 = new BlockKey(2, 2, 0);
    BlockValue v1 = new BlockValue(10);
    for (int i = 0; i < totalCount; i++) {
      Record record = new Record();
      record.add("c1", C1_OFFSET[1] + i);
      record.add("c2", C2_PFX[1] + i);
      record.add("c3", Double.valueOf(i + "." + i + i));
      record.add(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, 2L);
      v1.checkAndAddRecord(FTableByteUtils.fromRecord(td, record));
    }
    STORE_HANDLER.append(TABLE_NAME, BUCKET_ID, k1, v1);
  }

  /**
   * Setup the in-memory tier with the specified number of records.
   *
   * @param totalCount the number of records to ingest
   */
  @SuppressWarnings("unchecked")
  private static void setupMemory(final int totalCount) {
    BlockKey k0 = new BlockKey(3, 3, 0);
    BlockValue v0 = new BlockValue(10);
    for (int i = 0; i < totalCount; i++) {
      Record record = new Record();
      record.add("c1", C1_OFFSET[0] + i);
      record.add("c2", C2_PFX[0] + i);
      record.add("c3", Double.valueOf(i + "." + i + i));
      v0.checkAndAddRecord(FTableByteUtils.fromRecord(td, record));
    }
    RowTupleConcurrentSkipListMap map = new RowTupleConcurrentSkipListMap();
    RegionEntry re = mock(RegionEntry.class);
    when(re._getValue()).thenReturn(v0);
    map.put(k0, re);
    when(REGION_MAP.getInternalMap()).thenReturn(map);
  }

  private static final String[] COLUMNS =
      new String[] {"c1", "c2", "c3", FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME};
  private static final DataType[] TYPES = new DataType[] {BasicTypes.INT, BasicTypes.STRING,
      BasicTypes.DOUBLE, FTableDescriptor.INSERTION_TIMESTAMP_COL_TYPE};
  private static final int[] C1_OFFSET = {0, 10, 100};
  private static final String[] C2_PFX = {"String_", "W_String_", "T_String_"};

  private static final FTableDescriptor td = new FTableDescriptor();

  private static final String TIER_1_NAME = "local-disk-tier";
  private static final String TIER_1_DIR = "LDT";
  private static final String WAL_DIR = "WAL";

  private static final String TABLE_NAME = "dummy";
  private static final int BUCKET_ID = 0;
  private static PartitionedRegion REGION;
  private static RegionMap REGION_MAP;
  private static StoreHandler STORE_HANDLER;

  private static LinkedHashMap<String, TierStoreConfiguration> OPTS =
      new LinkedHashMap<String, TierStoreConfiguration>() {
        {
          put(TIER_1_NAME, new TierStoreConfiguration());
        }
      };

  /* use the methods for data ingestion in respective tiers */
  private static Method M_MEM;
  private static Method M_WAL;
  private static Method M_TIER1;

  static {
    td.setSchema(new Schema(COLUMNS, TYPES));

    try {
      M_MEM = FTableScannerTest.class.getDeclaredMethod("setupMemory", int.class);
      M_WAL = FTableScannerTest.class.getDeclaredMethod("setupWal", int.class);
      M_TIER1 = FTableScannerTest.class.getDeclaredMethod("setupTier1", int.class);
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }
  }

  @BeforeClass
  public static void setUpClass() throws IOException, NoSuchMethodException {
    REGION = mock(PartitionedRegion.class);
    REGION_MAP = mockRegionMap();
    STORE_HANDLER = initTierStore();
  }

  @Before
  public void setUpMethod() {
    new File(TIER_1_DIR).mkdirs();
  }

  @After
  public void cleanUpMethod() throws IOException {
    if (REGION_MAP.getInternalMap() != null) {
      ((Map) REGION_MAP.getInternalMap()).clear();
    }
    /* need to cleanup the cached WAL writers but store is mocked */
    WriteAheadLog.getInstance().deleteBucket(TABLE_NAME, BUCKET_ID);
    FileUtil.delete(new File(TIER_1_DIR));
  }

  /**
   * Get the total number of records retrieved via scanner.
   *
   * @param scanner the scanner
   * @param sc the scan context
   * @return the total number of records retrieved by scan
   */
  private int getScanCount(final FTableScanner scanner, ScanContext sc)
      throws IOException, InterruptedException {
    Map.Entry entry;
    int count = 0;
    while (scanner.hasNext()) {
      entry = (Map.Entry) scanner.next();
      final ScanEntryHandler.Status s = sc.getHandler().handle((Row) entry.getValue(), sc);
      if (s == ScanEntryHandler.Status.SEND) {
        count++;
      }
    }
    return count;
  }

  /**
   * Get the total number of records retrieved via scanner. Also, assert on the the respective
   * column values.
   *
   * @param scan the scan object
   * @param scanner the scanner
   * @param perTierCount the number of records per tier
   * @param tierIdxArr an array of respective tier index for prefix/offsets
   * @return the total number of records retrieved by scan
   */
  private int assertAndGetCount(final Scan scan, final FTableScanner scanner,
      final int perTierCount, final int[] tierIdxArr) {
    Map.Entry entry;
    Row row;
    int count = 0;
    int i;
    int tierIdx = 0;
    while (scanner.hasNext()) {
      entry = (Map.Entry) scanner.next();
      if (entry == null) {
        break;
      }
      row = (Row) entry.getValue();
      i = count % perTierCount;

      /* assert on respective column values */
      assertEquals("Incorrect value for: c1", C1_OFFSET[tierIdxArr[tierIdx]] + i, row.getValue(0));
      assertEquals("Incorrect value for: c2", C2_PFX[tierIdxArr[tierIdx]] + i, row.getValue(1));
      assertEquals("Incorrect value for: c3", Double.valueOf(i + "." + i + i), row.getValue(2));
      count++;
      if ((i + 1) == perTierCount) {
        tierIdx++;
      }
    }
    return count;
  }

  /**
   * The data provider for test: testScanCount It provides an array of the respective methods to
   * ingest/mimic the data in respective tiers and then execute a full table scan to assert the
   * count appropriately.
   *
   * @return the required inputs
   * @throws NoSuchMethodException if the provided method does not exist
   */
  public Object[] dataScanCount() throws NoSuchMethodException {
    return new Object[][] {{new Method[] {M_MEM, M_WAL, M_TIER1}, 5, 15, new int[] {0, 1, 2}},
        {new Method[] {M_MEM, M_TIER1}, 5, 10, new int[] {0, 2}},
        {new Method[] {M_MEM, M_WAL}, 5, 10, new int[] {0, 1}},
        {new Method[] {M_WAL, M_TIER1}, 5, 10, new int[] {1, 2}},
        {new Method[] {M_MEM}, 5, 5, new int[] {0}}, {new Method[] {M_WAL}, 5, 5, new int[] {1}},
        {new Method[] {M_TIER1}, 5, 5, new int[] {2}}, {new Method[] {}, 5, 0, new int[] {}},};
  }

  @Test
  @Parameters(method = "dataScanCount")
  public void testScanCount(final Method[] dms, final int perTierCount, final int totalCount,
      final int[] tierIdx)
      throws IOException, InvocationTargetException, IllegalAccessException, InterruptedException {

    for (final Method dm : dms) {
      dm.invoke(this, perTierCount);
    }

    Scan scan = new Scan();
    scan.setBucketId(BUCKET_ID);
    scan.setMessageChunkSize(1);

    final ScanContext sc = new ScanContext(null, REGION, scan, td, null, null);
    int count = assertAndGetCount(scan,
        new FTableScanner(sc, (RowTupleConcurrentSkipListMap) REGION_MAP.getInternalMap()),
        perTierCount, tierIdx);
    System.out.println("FTableScannerTest.testScanCount::: totalCount= " + count);
    assertEquals("Incorrect number of records.", totalCount, count);
  }

  private void setupAll(final int totalCount)
      throws InvocationTargetException, IllegalAccessException {
    M_MEM.invoke(this, totalCount);
    M_WAL.invoke(this, totalCount);
    M_TIER1.invoke(this, totalCount);
  }

  /**
   * Data provider for test: testScanCountWithFilters
   *
   * @return the required inputs
   */
  private Object[] dataScanCountWithFilters() {
    return new Object[][] {{null, 5, 15},
        /* SingleColumnValueFilter -- multiple cases */
        {new SingleColumnValueFilter("c1", CompareOp.EQUAL, null), 5, 0},
        {new SingleColumnValueFilter("c1", CompareOp.NOT_EQUAL, null), 5, 15},
        {new SingleColumnValueFilter("c1", CompareOp.NOT_EQUAL, 10L), 5, 14},
        {new SingleColumnValueFilter("c2", CompareOp.REGEX, "Str.*"), 5, 5},
        {new SingleColumnValueFilter("c2", CompareOp.REGEX, ".*Str.*"), 5, 15},
        {new SingleColumnValueFilter("c2", CompareOp.REGEX, ".*[13]$"), 5, 6},
        {new SingleColumnValueFilter("c3", CompareOp.LESS_OR_EQUAL, 3.3), 5, 9},
        {new SingleColumnValueFilter("c3", CompareOp.GREATER, 4.4), 5, 3},
        /* SingleColumnValueFilter with AND */
        {new FilterList(FilterList.Operator.MUST_PASS_ALL)
            .addFilter(new SingleColumnValueFilter("c1", CompareOp.NOT_EQUAL, 0L))
            .addFilter(new SingleColumnValueFilter("c2", CompareOp.REGEX, ".*[13]$")), 5, 6}, ////
        {new FilterList(FilterList.Operator.MUST_PASS_ALL)
            .addFilter(new SingleColumnValueFilter("c1", CompareOp.EQUAL, 10L))
            .addFilter(new SingleColumnValueFilter("c2", CompareOp.REGEX, ".*[23]$")), 5, 0}, ////
        /* SingleColumnValueFilter with AND */
        {new FilterList(FilterList.Operator.MUST_PASS_ONE)
            .addFilter(new SingleColumnValueFilter("c1", CompareOp.EQUAL, 3))
            .addFilter(new SingleColumnValueFilter("c2", CompareOp.REGEX, ".*[13]$")), 5, 6}, ////

    };
  }

  /**
   * Run the test with filters applied on the row using the respective ScanContext, like a
   * real-world case.
   *
   * @param filter the filter to be applied during the scan
   * @param tCount number of records to be ingested per tier
   * @param expectedCount the expected scan count
   * @throws InvocationTargetException if data could not be ingested properly
   * @throws IllegalAccessException if data could not be ingested properly
   * @throws InterruptedException if data could not be ingested properly
   * @throws IOException if data could not be ingested properly
   */
  @Test
  @Parameters(method = "dataScanCountWithFilters")
  public void testScanCountWithFilters(final Filter filter, final int tCount,
      final int expectedCount)
      throws InvocationTargetException, IllegalAccessException, InterruptedException, IOException {
    setupAll(tCount);

    Scan scan = new Scan();
    scan.setBucketId(BUCKET_ID);
    scan.setFilter(filter);
    scan.setMessageChunkSize(1_000);

    final ScanContext sc = new ScanContext(null, REGION, scan, td, null, null);
    int count = getScanCount(
        new FTableScanner(sc, (RowTupleConcurrentSkipListMap) REGION_MAP.getInternalMap()), sc);
    System.out.println("FTableScannerTest.testScanCountWithFilters:: totalCount= " + count);
    assertEquals("Incorrect count from scan.", expectedCount, count);
  }
}
