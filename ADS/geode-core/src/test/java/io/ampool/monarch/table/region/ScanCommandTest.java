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
package io.ampool.monarch.table.region;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.KeyOnlyFilter;
import io.ampool.monarch.table.filter.RowFilter;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.filter.ValueFilter;
import io.ampool.monarch.table.filter.internal.TimestampFilter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.ThinRow;
import io.ampool.monarch.table.internal.ThinRowShared;
import io.ampool.monarch.table.results.FormatAwareRow;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.interfaces.DataType;
import io.codearte.catchexception.shade.mockito.Mockito;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.cache.Region;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class ScanCommandTest {
  /* sample data and respective byte-arrays */
  final Object[][] data = new Object[][] {{null, "String_1", 11}, {2L, "String_2", 22},
      {3L, "String_3", 33}, {4L, "String_4", 44}, {5L, "String_5", 55}};
  private static final byte[][] KEYS = new byte[][] {new byte[] {0}, new byte[] {1}, new byte[] {2},
      new byte[] {3}, new byte[] {4},};
  private static final byte[][] VALUES = new byte[][] {
      new byte[] {2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 6, 0, 0, 0, 11, 83, 116, 114, 105, 110, 103,
          95, 49, 0, 0, 0, 17},
      new byte[] {2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 7, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 22, 83,
          116, 114, 105, 110, 103, 95, 50, 0, 0, 0, 25},
      new byte[] {2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 3, 7, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 33, 83,
          116, 114, 105, 110, 103, 95, 51, 0, 0, 0, 25},
      new byte[] {2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 4, 7, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 44, 83,
          116, 114, 105, 110, 103, 95, 52, 0, 0, 0, 25},
      new byte[] {2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 5, 7, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 55, 83,
          116, 114, 105, 110, 103, 95, 53, 0, 0, 0, 25},};

  /* the scan context and reused Row object */
  private static final Region REGION = Mockito.mock(Region.class);

  /**
   * Data provider for: testExecuteSimpleFilter.
   *
   * @return an array of test cases
   */
  private Object[] dataExecuteSimpleFilter() {
    return new Object[][] {{null, 5}, // no filter should return all rows
        {new RowFilter(CompareOp.EQUAL, new byte[] {4}), 1}, // Row filter
        {new RowFilter(CompareOp.NOT_EQUAL, new byte[] {4}), 4}, // Row filter
        {new RowFilter(CompareOp.EQUAL, new byte[] {10}), 0}, // Row filter
        {new TimestampFilter(CompareOp.EQUAL, 2L), 1}, // Timestamp filter
        {new TimestampFilter(CompareOp.EQUAL, 200L), 0}, // Timestamp filter
        {new TimestampFilter(CompareOp.GREATER, 0L), 5}, // Timestamp filter
        {new TimestampFilter(CompareOp.NOT_EQUAL, 2L), 4}, // Timestamp filter
        /* SingleColumnValueFilter -- multiple cases */
        {new SingleColumnValueFilter("c1", CompareOp.EQUAL, null), 1},
        {new SingleColumnValueFilter("c1", CompareOp.NOT_EQUAL, null), 4},
        {new SingleColumnValueFilter("c1", CompareOp.NOT_EQUAL, 10L), 5},
        {new SingleColumnValueFilter("c2", CompareOp.REGEX, "Str.*"), 5},
        {new SingleColumnValueFilter("c2", CompareOp.REGEX, ".*[13]$"), 2},
        {new SingleColumnValueFilter("c3", CompareOp.LESS_OR_EQUAL, 33), 3},
        {new SingleColumnValueFilter("c3", CompareOp.GREATER, 44), 1},
        {new SingleColumnValueFilter(MTableUtils.KEY_COLUMN_NAME, CompareOp.EQUAL, null), 0},
        {new SingleColumnValueFilter(MTableUtils.KEY_COLUMN_NAME, CompareOp.NOT_EQUAL, null), 5},
        {new SingleColumnValueFilter(MTableUtils.KEY_COLUMN_NAME, CompareOp.EQUAL, new byte[] {0}),
            1},
        {new SingleColumnValueFilter(MTableUtils.KEY_COLUMN_NAME, CompareOp.EQUAL, new byte[] {11}),
            0},
        /* SingleColumnValueFilter with AND */
        {new FilterList(FilterList.Operator.MUST_PASS_ALL)
            .addFilter(new SingleColumnValueFilter("c1", CompareOp.NOT_EQUAL, null))
            .addFilter(new SingleColumnValueFilter("c2", CompareOp.REGEX, ".*[13]$")), 1}, ////
        {new FilterList(FilterList.Operator.MUST_PASS_ALL)
            .addFilter(new SingleColumnValueFilter("c1", CompareOp.EQUAL, null))
            .addFilter(new SingleColumnValueFilter("c2", CompareOp.REGEX, ".*[23]$")), 0}, ////
        /* SingleColumnValueFilter with AND */
        {new FilterList(FilterList.Operator.MUST_PASS_ONE)
            .addFilter(new SingleColumnValueFilter("c1", CompareOp.EQUAL, 3))
            .addFilter(new SingleColumnValueFilter("c2", CompareOp.REGEX, ".*[13]$")), 2}, ////
    };
  }

  @Test
  @Parameters(method = "dataExecuteSimpleFilter")
  public void testExecuteSimpleFilter(final Filter filter, final int expectedRowCount)
      throws Exception {
    final Schema schema = new Schema(new String[] {"c1", "c2", "c3", MTableUtils.KEY_COLUMN_NAME},
        new DataType[] {BasicTypes.LONG, BasicTypes.STRING, BasicTypes.INT, BasicTypes.BINARY});
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    td.setSchema(schema);

    final List<Cell> cells = ScanUtils.createCells(new Scan(), td.getAllColumnDescriptors());
    ThinRow row = new ThinRow(null, null, new ThinRowShared(cells, td, Collections.emptyList()),
        ThinRow.RowFormat.M_FULL_ROW);

    int matchedCount = 0;
    for (int i = 0; i < KEYS.length; i++) {
      row.reset(KEYS[i], VALUES[i]);
      matchedCount += ScanUtils.executeSimpleFilter(row, filter, td) ? 1 : 0;
    }
    assertEquals("Incorrect number of rows matching filters.", expectedRowCount, matchedCount);
  }

  private static final byte[][] VALUES_FTABLE = new byte[][] {
      new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 83, 116, 114, 105, 110, 103, 95, 49, 0, 0, 0,
          12},
      new byte[] {0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 22, 83, 116, 114, 105, 110, 103, 95, 50, 0, 0, 0,
          12},
      new byte[] {0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 33, 83, 116, 114, 105, 110, 103, 95, 51, 0, 0, 0,
          12},
      new byte[] {0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 44, 83, 116, 114, 105, 110, 103, 95, 52, 0, 0, 0,
          12},
      new byte[] {0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 55, 83, 116, 114, 105, 110, 103, 95, 53, 0, 0, 0,
          12},};

  private Object[] dataExecuteSimpleFilter_FTable() {
    return new Object[][] {{null, 5}, // no filter should return all rows
        /* SingleColumnValueFilter -- multiple cases */
        {new SingleColumnValueFilter("c1", CompareOp.EQUAL, 0), 1},
        {new SingleColumnValueFilter("c1", CompareOp.NOT_EQUAL, 0), 4},
        {new SingleColumnValueFilter("c1", CompareOp.NOT_EQUAL, 10L), 5},
        {new SingleColumnValueFilter("c2", CompareOp.REGEX, "Str.*"), 5},
        {new SingleColumnValueFilter("c2", CompareOp.REGEX, ".*[13]$"), 2},
        {new SingleColumnValueFilter("c3", CompareOp.LESS_OR_EQUAL, 33), 3},
        {new SingleColumnValueFilter("c3", CompareOp.GREATER, 44), 1},
        /* SingleColumnValueFilter with AND */
        {new FilterList(FilterList.Operator.MUST_PASS_ALL)
            .addFilter(new SingleColumnValueFilter("c1", CompareOp.NOT_EQUAL, 0))
            .addFilter(new SingleColumnValueFilter("c2", CompareOp.REGEX, ".*[13]$")), 1}, ////
        {new FilterList(FilterList.Operator.MUST_PASS_ALL)
            .addFilter(new SingleColumnValueFilter("c1", CompareOp.EQUAL, null))
            .addFilter(new SingleColumnValueFilter("c2", CompareOp.REGEX, ".*[23]$")), 0}, ////
        /* SingleColumnValueFilter with AND */
        {new FilterList(FilterList.Operator.MUST_PASS_ONE)
            .addFilter(new SingleColumnValueFilter("c1", CompareOp.EQUAL, 3))
            .addFilter(new SingleColumnValueFilter("c2", CompareOp.REGEX, ".*[13]$")), 2}, ////
    };
  }

  @Test
  @Parameters(method = "dataExecuteSimpleFilter_FTable")
  public void testExecuteSimpleFilter_FTable(final Filter filter, final int expectedRowCount)
      throws Exception {
    FTableDescriptor td = new FTableDescriptor();
    td.setSchema(new Schema(new String[] {"c1", "c2", "c3"},
        new DataType[] {BasicTypes.LONG, BasicTypes.STRING, BasicTypes.INT}));

    final ThinRow row = (ThinRow) ScanUtils.getRowProducer(new Scan(), td).apply(null, null);
    int matchedCount = 0;
    for (int i = 0; i < KEYS.length; i++) {
      row.reset(KEYS[i], VALUES_FTABLE[i]);
      matchedCount += ScanUtils.executeSimpleFilter(row, filter, td) ? 1 : 0;
    }
    assertEquals("Incorrect number of rows matching filters.", expectedRowCount, matchedCount);
  }

  @Test
  public void testFilter_MTable() throws InterruptedException, IOException {
    final int count = 20;
    int matchedCount = 0;
    MTableDescriptor td = new MTableDescriptor();
    td.setSchema(new Schema(new String[] {"c_1", "c_2", "c_3", "c_4", "c_5", "c_6", "c_7", "c_8"}));
    final ThinRow row = (ThinRow) ScanUtils.getRowProducer(new Scan(), td).apply(null, null);

    final byte[] value =
        {1, 1, 1, 0, 0, 0, 13, -26, -118, -63, -120, -31, -1, 37, 113, -52, -86, 50, 106, -67, 41,
            -63, -70, 15, 53, 120, -65, -72, 85, 76, 25, -42, 101, -115, 60, 105, 0, 0, 0, 35, 0, 0,
            0, 31, 0, 0, 0, 28, 0, 0, 0, 27, 0, 0, 0, 26, 0, 0, 0, 19, 0, 0, 0, 18, 0, 0, 0, 13};
    final Filter filter =
        new SingleColumnValueFilter("c_1", CompareOp.EQUAL, new byte[] {37, 113, -52, -86, 50});
    long l1 = System.nanoTime();
    for (int i = 0; i < count; i++) {
      row.reset(null, value);
      matchedCount += ScanUtils.executeSimpleFilter(row, filter, td) ? 1 : 0;
    }
    System.out.println("# Time= " + (System.nanoTime() - l1) / 1_000_000_000.0);
    System.out.println("matchedCount = " + matchedCount);
    assertEquals("Incorrect number of records matching filter.", count, matchedCount);
  }

  @Test
  public void testFilter_FTable() throws InterruptedException, IOException {
    final int count = 20;
    int matchedCount = 0;
    // System.in.read();
    FTableDescriptor td = new FTableDescriptor();
    td.setSchema(new Schema(new String[] {"c_1", "c_2", "c_3", "c_4", "c_5", "c_6", "c_7", "c_8"}));
    final ThinRow row = (ThinRow) ScanUtils.getRowProducer(new Scan(), td).apply(null, null);

    final byte[] value = {58, 127, 90, -103, 35, 67, -40, 7, -71, 121, 91, -104, -67, 17, 70, 65,
        40, -29, -46, -55, 79, 31, 34, -101, 66, 123, 43, -89, 0, 0, 0, 23, 0, 0, 0, 22, 0, 0, 0,
        22, 0, 0, 0, 18, 0, 0, 0, 18, 0, 0, 0, 17, 0, 0, 0, 13, 0, 0, 0, 8};
    final Filter filter =
        new SingleColumnValueFilter("c_1", CompareOp.EQUAL, new byte[] {-71, 121, 91, -104, -67});
    long l1 = System.nanoTime();
    for (int i = 0; i < count; i++) {
      row.reset(null, value);
      matchedCount += ScanUtils.executeSimpleFilter(row, filter, td) ? 1 : 0;
    }
    System.out.println("# Time= " + (System.nanoTime() - l1) / 1_000_000_000.0);
    System.out.println("matchedCount = " + matchedCount);
    assertEquals("Incorrect number of records matching filter.", count, matchedCount);
  }

  /**
   * Data provider for: testScanContextHandler
   *
   * @return an array of test cases
   */
  private Object[] dataScanContextHandler() {
    final Scan bScan = new Scan();
    final Scan mvScan = new Scan().setMaxVersions(2, true);
    final Scan mvScan1 = new Scan().setMaxVersions(2, false);
    final Scan scScan = new Scan();
    scScan.setColumns(Arrays.asList(0, 1));

    final TableDescriptor ftd = new FTableDescriptor();
    final TableDescriptor mtd = new MTableDescriptor();
    final TableDescriptor mvTd = new MTableDescriptor().setMaxVersions(2);
    final Schema schema = new Schema(new String[] {"c1", "c2", "c3"});
    ftd.setSchema(schema);
    mtd.setSchema(schema);
    mvTd.setSchema(schema);

    final Filter cf = new SingleColumnValueFilter("c1", CompareOp.EQUAL, null);
    final Filter tf = new TimestampFilter(CompareOp.EQUAL, 10L);
    final Filter rf = new RowFilter(CompareOp.EQUAL, "dummy");
    final Filter af = new FilterList(FilterList.Operator.MUST_PASS_ALL).addFilter(cf).addFilter(tf);
    final Filter of = new FilterList(FilterList.Operator.MUST_PASS_ONE).addFilter(cf).addFilter(tf);
    final Filter of1 = new FilterList(FilterList.Operator.MUST_PASS_ONE).addFilter(cf);

    return new Object[][] { ////
        {ftd, bScan, ScanEntryHandler.NoFilterHandler.SELF}, ////
        {ftd, scScan, ScanEntryHandler.NoFilterHandler.SELF}, ////
        {ftd, new Scan().setFilter(of), ScanEntryHandler.SimpleFilterHandler.SELF}, ////
        {mtd, bScan, ScanEntryHandler.NoFilterHandler.SELF}, ////
        {mtd, mvScan, ScanEntryHandler.MultiVersionHandler.SELF}, ////
        {mtd, scScan, ScanEntryHandler.NoFilterHandler.SELF}, ////
        {mvTd, bScan, ScanEntryHandler.MultiVersionHandler.SELF}, ////
        {mvTd, mvScan1, ScanEntryHandler.MultiVersionHandler.SELF}, ////
        {mvTd, new Scan().setFilter(cf), ScanEntryHandler.MultiVersionHandler.SELF}, ////
        {mtd, new Scan().setFilter(cf), ScanEntryHandler.SimpleFilterHandler.SELF}, ////
        {mtd, new Scan().setFilter(rf), ScanEntryHandler.SimpleFilterHandler.SELF}, ////
        {mtd, new Scan().setFilter(af), ScanEntryHandler.SimpleFilterHandler.SELF}, ////
        {mtd, new Scan().setFilter(of), ScanEntryHandler.SimpleFilterHandler.SELF}, ////
        {mtd, new Scan().setFilter(of1), ScanEntryHandler.SimpleFilterHandler.SELF}, ////
    };
  }

  @Test
  @Parameters(method = "dataScanContextHandler")
  public void testScanContextHandler(final TableDescriptor td, final Scan scan,
      final ScanEntryHandler handler) throws InterruptedException {
    ScanContext sc = new ScanContext(null, REGION, scan, td, null, null);
    assertEquals("Incorrect entry handler.", handler, sc.getEntryHandler());
  }

  /**
   * Data provider for: testRowProducer
   *
   * @return an array of test cases
   */
  private Object[] dataRowProducer() {
    final Scan bScan = new Scan();
    final Scan mvScan = new Scan().setMaxVersions(2, true);
    final Scan mvScan1 = new Scan().setMaxVersions(2, false);
    final Scan scScan = new Scan();
    scScan.setColumns(Arrays.asList(0, 1));

    Schema schema = new Schema(new String[] {"c_1", "c_2", "c_3"});
    final TableDescriptor ftd = new FTableDescriptor();
    final TableDescriptor mtd = new MTableDescriptor();
    final TableDescriptor mvTd = new MTableDescriptor().setMaxVersions(2);
    ftd.setSchema(schema);
    mtd.setSchema(schema);
    mvTd.setSchema(schema);

    final Filter cf = new SingleColumnValueFilter("c1", CompareOp.EQUAL, null);
    final Filter tf = new TimestampFilter(CompareOp.EQUAL, 10L);
    final Filter vf = new ValueFilter(CompareOp.EQUAL, "dummy");
    final Filter of = new FilterList(FilterList.Operator.MUST_PASS_ONE).addFilter(cf).addFilter(tf);
    final Filter kf = new KeyOnlyFilter();

    return new Object[][] { ////
        {ftd, bScan, ThinRow.RowFormat.F_FULL_ROW}, ////
        {ftd, scScan, ThinRow.RowFormat.F_PARTIAL_ROW}, ////
        {ftd, new Scan().setFilter(cf), ThinRow.RowFormat.F_FULL_ROW}, ////
        {ftd, new Scan().setFilter(of), ThinRow.RowFormat.F_FULL_ROW}, ////
        {mtd, bScan, ThinRow.RowFormat.M_FULL_ROW}, ////
        {mtd, mvScan, null}, ////
        {mtd, scScan, ThinRow.RowFormat.M_PARTIAL_ROW}, ////
        {mvTd, bScan, null}, ////
        {mvTd, mvScan1, null}, ////
        {mtd, new Scan().setFilter(cf), ThinRow.RowFormat.M_FULL_ROW}, ////
        {mtd, new Scan().setFilter(vf), ThinRow.RowFormat.MTABLE_WITHOUT_NULL_CELLS}, ////
        {mvTd, new Scan().setFilter(vf), ThinRow.RowFormat.MTABLE_WITHOUT_NULL_CELLS}, ////
        {mtd, new Scan().setFilter(kf), null}, ////
    };
  }

  @Test
  @Parameters(method = "dataRowProducer")
  public void testRowProducer(final TableDescriptor td, final Scan scan,
      final ThinRow.RowFormat expectedFormat) {
    Row row = ScanUtils.getRowProducer(scan, td).apply(null, null);
    if (expectedFormat == null) {
      assertTrue("Incorrect row received; expected FormatAwareRow.", row instanceof FormatAwareRow);
    } else if (row instanceof ThinRow) {
      assertEquals("Incorrect row format.", expectedFormat, ((ThinRow) row).getRowFormat());
    } else {
      fail("Incorrect row format received: " + row.getClass());
    }
  }
}
