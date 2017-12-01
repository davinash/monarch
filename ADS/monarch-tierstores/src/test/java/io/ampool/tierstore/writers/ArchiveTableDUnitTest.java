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

package io.ampool.tierstore.writers;

import io.ampool.api.ClusterException;
import io.ampool.api.HDFSQuasiService;
import io.ampool.api.HDFSQuasiServiceException;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.ArchiveConfiguration;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.filter.internal.TimestampFilter;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.InternalTable;
import io.ampool.monarch.table.internal.TableType;
import io.ampool.monarch.types.CompareOp;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.hadoop.hive.ql.io.orc.FTableOrcStruct;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(MonarchTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class ArchiveTableDUnitTest extends MTableDUnitHelper {
  public static HDFSQuasiService hdfsQuasiService = null;
  public static final int numberOfDatanodes = 3;
  public static String hdfs_work_dir = "/tmp/HDFS";
  private static String hadoopConfPath;
  private static final String baseDir = "ORCDir1";

  @BeforeClass
  public static void beforeClass() throws HDFSQuasiServiceException {
    // start HDFS service
    hdfsQuasiService = new HDFSQuasiService();
    hdfsQuasiService.create_new(hdfs_work_dir, numberOfDatanodes, null,
        HDFSQuasiService.getDeafultHDFSConfPath(hdfs_work_dir), false, false, null, null, null);

    hadoopConfPath = HDFSQuasiService.getDeafultHDFSConfPath(hdfs_work_dir);
  }

  @AfterClass
  public static void afterClass() throws ClusterException {
    HDFSQuasiService.destroy(hdfs_work_dir);
  }

  public ArchiveTableDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();
  protected static final int NUM_OF_COLUMNS = 10;
  protected static final String VALUE_PREFIX = "VALUE";
  protected static final int NUM_OF_ROWS = 10;
  protected static final String COLUMN_NAME_PREFIX = "COLUMN";
  protected static final int LATEST_TIMESTAMP = 300;
  protected static final int MAX_VERSIONS = 5;
  protected static final int TABLE_MAX_VERSIONS = 7;



  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][] {
        // { descriptor, filter, expected entries}
        {getTableDescriptor(TableType.ORDERED_VERSIONED), null, 0},
        {getTableDescriptor(TableType.ORDERED_VERSIONED), new TimestampFilter(CompareOp.EQUAL, 0l),
            NUM_OF_ROWS},
        {getTableDescriptor(TableType.ORDERED_VERSIONED),
            new TimestampFilter(CompareOp.GREATER, 0l), 0},
        {getTableDescriptor(TableType.ORDERED_VERSIONED),
            new TimestampFilter(CompareOp.GREATER_OR_EQUAL, 0l), 0},
        {getTableDescriptor(TableType.ORDERED_VERSIONED), new TimestampFilter(CompareOp.LESS, 0l),
            NUM_OF_ROWS},
        {getTableDescriptor(TableType.ORDERED_VERSIONED),
            new TimestampFilter(CompareOp.LESS_OR_EQUAL, 0l), NUM_OF_ROWS},

        // with max versions as 5
        {getTableDescriptor(TableType.ORDERED_VERSIONED, 5), null, 0},
        {getTableDescriptor(TableType.ORDERED_VERSIONED, 5),
            new TimestampFilter(CompareOp.EQUAL, 0l), NUM_OF_ROWS},
        {getTableDescriptor(TableType.ORDERED_VERSIONED, 5),
            new TimestampFilter(CompareOp.GREATER, 0l), 0},
        {getTableDescriptor(TableType.ORDERED_VERSIONED, 5),
            new TimestampFilter(CompareOp.GREATER_OR_EQUAL, 0l), 0},
        {getTableDescriptor(TableType.ORDERED_VERSIONED, 5),
            new TimestampFilter(CompareOp.LESS, 0l), NUM_OF_ROWS},
        {getTableDescriptor(TableType.ORDERED_VERSIONED, 5),
            new TimestampFilter(CompareOp.LESS_OR_EQUAL, 0l), NUM_OF_ROWS},

        // unordered table
        {getTableDescriptor(TableType.UNORDERED), null, 0},
        {getTableDescriptor(TableType.UNORDERED), new TimestampFilter(CompareOp.EQUAL, 0l),
            NUM_OF_ROWS},
        {getTableDescriptor(TableType.UNORDERED), new TimestampFilter(CompareOp.GREATER, 0l), 0},
        {getTableDescriptor(TableType.UNORDERED),
            new TimestampFilter(CompareOp.GREATER_OR_EQUAL, 0l), 0},
        {getTableDescriptor(TableType.UNORDERED), new TimestampFilter(CompareOp.LESS, 0l),
            NUM_OF_ROWS},
        {getTableDescriptor(TableType.UNORDERED), new TimestampFilter(CompareOp.LESS_OR_EQUAL, 0l),
            NUM_OF_ROWS},

        // Ftable
        {getTableDescriptor(TableType.IMMUTABLE), null, 0},
        {getTableDescriptor(TableType.IMMUTABLE),
            new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
                CompareOp.EQUAL, 0),
            NUM_OF_ROWS},
        {getTableDescriptor(TableType.IMMUTABLE),
            new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
                CompareOp.LESS_OR_EQUAL, 0),
            NUM_OF_ROWS},
        {getTableDescriptor(TableType.IMMUTABLE),
            new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
                CompareOp.LESS, 0),
            NUM_OF_ROWS},
        {getTableDescriptor(TableType.IMMUTABLE),
            new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
                CompareOp.GREATER, 0),
            0},
        {getTableDescriptor(TableType.IMMUTABLE),
            new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
                CompareOp.GREATER_OR_EQUAL, 0),
            0},
        {getTableDescriptor(TableType.IMMUTABLE),
            new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
                CompareOp.NOT_EQUAL, 0),
            0},
        // with max versions as 5
        {getTableDescriptor(TableType.UNORDERED, 5), null, 0},
        {getTableDescriptor(TableType.UNORDERED, 5), new TimestampFilter(CompareOp.EQUAL, 0l),
            NUM_OF_ROWS},
        {getTableDescriptor(TableType.UNORDERED, 5), new TimestampFilter(CompareOp.GREATER, 0l), 0},
        {getTableDescriptor(TableType.UNORDERED, 5),
            new TimestampFilter(CompareOp.GREATER_OR_EQUAL, 0l), 0},
        {getTableDescriptor(TableType.UNORDERED, 5), new TimestampFilter(CompareOp.LESS, 0l),
            NUM_OF_ROWS},
        {getTableDescriptor(TableType.UNORDERED, 5),
            new TimestampFilter(CompareOp.LESS_OR_EQUAL, 0l), NUM_OF_ROWS},


    });
  }

  @Parameterized.Parameter(value = 0)
  public TableDescriptor tableDescriptor;
  @Parameterized.Parameter(value = 1)
  public Filter filter;
  @Parameterized.Parameter(value = 2)
  public int expected;


  private static TableDescriptor getTableDescriptor(final TableType tableType) {
    return getTableDescriptor(tableType, 1, 10, 2);
  }

  private static TableDescriptor getTableDescriptor(final TableType tableType,
      final int maxVersions) {
    return getTableDescriptor(tableType, maxVersions, 10, 2);
  }

  private static TableDescriptor getTableDescriptor(final TableType tableType,
      final int maxVersions, final int numPartitions) {
    return getTableDescriptor(tableType, maxVersions, numPartitions, 2);
  }

  private static TableDescriptor getTableDescriptor(final TableType tableType,
      final int maxVersions, final int numPartitions, final int redundantCopies) {

    TableDescriptor tableDescriptor = null;
    if (tableType != TableType.IMMUTABLE) {
      MTableType mTableType = (tableType == TableType.ORDERED_VERSIONED
          ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
      tableDescriptor = new MTableDescriptor(mTableType);
      // set version for ordered table
      if (tableType == TableType.ORDERED_VERSIONED) {
        ((MTableDescriptor) tableDescriptor).setMaxVersions(maxVersions);
      }
    } else {
      tableDescriptor = new FTableDescriptor();
    }

    tableDescriptor.setTotalNumOfSplits(numPartitions);
    tableDescriptor.setRedundantCopies(redundantCopies);

    // add columns
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    return tableDescriptor;
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache();
  }

  private void closeMCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        cache.close();
        return null;
      }
    });
  }

  private void closeMCacheAll() {
    closeMCache(this.vm0);
    closeMCache(this.vm1);
    closeMCache(this.vm2);
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMCacheAll();
    super.tearDown2();
  }

  protected InternalTable createTable(final String tableName) {
    Admin admin = MClientCacheFactory.getAnyInstance().getAdmin();
    InternalTable table = null;
    if (tableDescriptor instanceof MTableDescriptor) {
      table = (InternalTable) admin.createMTable(tableName, (MTableDescriptor) tableDescriptor);
    } else {
      table = (InternalTable) admin.createFTable(tableName, (FTableDescriptor) tableDescriptor);
    }
    assertEquals(tableName, tableName);
    assertNotNull(table);

    return table;
  }

  private void doAppends(String tableName) {
    FTable fTable = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    for (int i = 0; i < NUM_OF_ROWS; i++) {
      Record record = new Record();
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.add(COLUMN_NAME_PREFIX + columnIndex, Bytes.toBytes(VALUE_PREFIX + columnIndex + i));
      }
      fTable.append(record);
    }
  }

  private void doPuts(Set<String> keys, String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);

    int totalPutKeys = 0;

    for (String key : keys) {
      Get get = new Get(key);
      Row result = table.get(get);
      assertTrue(result.isEmpty());
      Put record = new Put(key);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
      totalPutKeys++;
    }
  }

  private void doGets(Set<String> keys, String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    assertNotNull(table);

    keys.forEach((X) -> {
      Get get = new Get(X);
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS, result.size() - 1);
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out
              .println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
          System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
          System.out.println(
              "actualValue    => " + Arrays.toString((byte[]) row.get(i).getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex++;
      }

    });
  }


  @Test
  public void testArchiveTable() throws Exception {
    final String tableName = getTestMethodName().replace('[', '_').replace(']', '_');
    MClientCache mClientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = mClientCache.getAdmin();
    InternalTable table = createTable(tableName);
    Set<String> keys = new TreeSet<>();
    Random rd = new Random();
    while (keys.size() != NUM_OF_ROWS) {
      keys.add(String.valueOf(rd.nextInt(Integer.MAX_VALUE)));
    }

    if (tableDescriptor instanceof MTableDescriptor) {
      doPuts(keys, tableName);
      doGets(keys, tableName);
    } else {
      doAppends(tableName);
    }

    Iterator clientKeysIterator = keys.iterator();
    // Full scan without start & stop row
    Scanner resultScanner = table.getScanner(new Scan());
    Iterator<Row> resultIterator = resultScanner.iterator();

    int resultCount = 0;
    List<Row> scanResults = new ArrayList<>();
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      if (tableDescriptor instanceof MTableDescriptor
          && ((MTableDescriptor) tableDescriptor).getTableType() == MTableType.ORDERED_VERSIONED) {
        assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      }
      if (result != null) {
        // System.out.println("result no "+ resultCount +" result key"+ result);
        resultCount++;
        scanResults.add(result);
      }
    }
    assertEquals(NUM_OF_ROWS, resultCount);

    // create the archive configuration
    ArchiveConfiguration archiveConfiguration = new ArchiveConfiguration();
    Properties properties = new Properties();
    properties.setProperty("hadoop.site.xml.path", hadoopConfPath);
    properties.setProperty(ArchiveConfiguration.BASE_DIR_PATH, baseDir);
    archiveConfiguration.setFsProperties(properties);
    admin.archiveTable(tableName, filter, archiveConfiguration);

    // Full scan without start & stop row
    resultScanner = table.getScanner(new Scan());
    resultIterator = resultScanner.iterator();

    resultCount = 0;
    List<Row> scanResultsAfterArchive = new ArrayList<>();
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      if (result != null) {
        // System.out.println("result no "+ resultCount +" result key"+ result);
        resultCount++;
        scanResultsAfterArchive.add(result);
      }
    }
    assertEquals(expected, resultCount);
    verifyFilesOnExternalStore(tableName, archiveConfiguration.getBatchSize());

    verifyORCRecords(tableName, scanResults);
    if (tableDescriptor instanceof MTableDescriptor) {
      admin.deleteMTable(tableName);
    } else {
      admin.deleteFTable(tableName);
    }
    mClientCache.close();
  }

  private void verifyFilesOnExternalStore(final String tablename, final int batchSize) {
    int filesCount = hdfsQuasiService.getFilesCount(baseDir, tablename);
    int expectedCount = 0;
    if (expected == 0) {
      if (NUM_OF_ROWS < batchSize) {
        expectedCount = 1;
      } else {
        expectedCount = NUM_OF_ROWS / batchSize;
      }
    }
    assertEquals(expectedCount, filesCount);
  }

  private void verifyORCRecords(final String tablename, List<Row> rows) throws IOException {
    int expectedrecordsInStore = 0;
    List<OrcStruct> orcRecords = hdfsQuasiService.getORCRecords(baseDir, tablename);
    if (expected == 0) {
      expectedrecordsInStore = NUM_OF_ROWS;
    }
    assertEquals(expectedrecordsInStore, orcRecords.size());
    if (expectedrecordsInStore != 0) {
      assertEquals(rows.size(), orcRecords.size());
      for (int i = 0; i < rows.size(); i++) {
        assertTrue(isEqual(rows.get(i), orcRecords.get(i)));
      }
    }
  }

  private boolean isEqual(Row row, OrcStruct orcStruct) {
    List<Cell> cells = row.getCells();
    FTableOrcStruct fTableOrcStruct = new FTableOrcStruct(orcStruct);
    int fieldsCount = orcStruct.getNumFields();
    assertEquals(cells.size(), fieldsCount);
    for (int i = 0; i < fieldsCount; i++) {
      if (fTableOrcStruct.getFieldValue(i) instanceof BytesWritable) {
        byte[] fieldValue = ((BytesWritable) fTableOrcStruct.getFieldValue(i)).copyBytes();
        byte[] columnValue = (byte[]) cells.get(i).getColumnValue();
        if (Bytes.compareTo(fieldValue, columnValue) != 0) {
          return false;
        }
      } else if (fTableOrcStruct.getFieldValue(i) instanceof LongWritable) {
        long fieldValue = ((LongWritable) fTableOrcStruct.getFieldValue(i)).get();
        long columnValue = (long) cells.get(i).getColumnValue();
        if (fieldValue != columnValue) {
          return false;
        }
      } else {
        return false;
      }
    }
    return true;
  }
}
