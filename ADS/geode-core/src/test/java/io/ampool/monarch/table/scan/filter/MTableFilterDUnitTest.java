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
package io.ampool.monarch.table.scan.filter;

import org.apache.geode.internal.Assert;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.exceptions.MFilterException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterBase;
import io.ampool.monarch.table.internal.CellRef;
import io.ampool.monarch.table.internal.RowImpl;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.List;

@Category(MonarchTest.class)
public class MTableFilterDUnitTest extends MTableFilterTestBase {

  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();
  }

  public MTableFilterDUnitTest() {
    super();
  }

  @Test
  public void testFilterAllRemaining() {
    int numRows = 10;
    MTable mt = createTableAndPutRows("testFilterAllRemaining", numRows);

    Scan scan = new Scan();
    Filter filter = new FilterBase() {
      boolean isFirstRow = true;

      @Override
      public boolean filterAllRemaining() {
        if (isFirstRow) {
          isFirstRow = false;
          return false;
        }
        return true;
      }

      @Override
      public boolean hasFilterRow() {
        return true;
      }

      @Override
      public boolean hasFilterCell() {
        return false;
      }
    };

    scan.setFilter(filter);
    Scanner scanner = null;
    try {
      scanner = mt.getScanner(scan);
      assertNotNull(scanner);
      int count = 0;
      for (Row result : scanner) {
        count++;
      }
      assertEquals(1, count);

      scan.setReturnKeysFlag(false);
      scanner = mt.getScanner(scan);
      assertNotNull(scanner);
      count = 0;
      for (Row result : scanner) {
        count++;
      }
      assertEquals(1, count);

    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  @Test
  public void testFilterRowKeyCalled() {
    int numRows = 10;
    MTable mt = createTableAndPutRows("testFilterRowKeyCalled", numRows);

    Scan scan = new Scan();
    Filter filter = new FilterBase() {
      boolean isFirstRow = true;

      @Override
      public boolean filterRowKey(Row result) {
        if (isFirstRow) {
          isFirstRow = false;
          return false;
        }
        return true;
      }

      @Override
      public boolean hasFilterRow() {
        return true;
      }

      @Override
      public boolean hasFilterCell() {
        return false;
      }
    };

    scan.setFilter(filter);
    Scanner scanner = null;
    try {
      scanner = mt.getScanner(scan);
      int count = 0;
      for (Row result : scanner) {
        count++;
      }
      assertEquals(1, count);

      scan.setReturnKeysFlag(false);
      scanner = mt.getScanner(scan);
      count = 0;
      for (Row result : scanner) {
        count++;
      }
      assertEquals(1, count);

    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  static boolean isFirstRow = true;

  @Test
  public void testFilterRowCalled() {
    int numRows = 10;
    MTable mt = createTableAndPutRows("testRowFilterCalled", numRows);
    Scan scan = new Scan();
    isFirstRow = true;
    Filter filter = new FilterBase() {
      @Override
      public boolean filterRow() {
        if (isFirstRow) {
          isFirstRow = false;
          return false;
        }
        return true;
      }

      @Override
      public boolean hasFilterRow() {
        return true;
      }

      @Override
      public boolean hasFilterCell() {
        return false;
      }
    };

    scan.setFilter(filter);
    Scanner scanner = null;
    try {
      scanner = mt.getScanner(scan);
      int count = 0;
      while (scanner.next() != null) {
        count++;
      }
      assertEquals(1, count);
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  @Test
  public void testTransformRowCalled() {
    int numRows = 10;
    Long timeStamp = System.currentTimeMillis();
    MTable mt = createTableAndPutRows("testTransformRowCalled", numRows);

    Scan scan = new Scan();
    Filter filter = new FilterBase() {

      @Override
      public Row transformResult(Row v) {
        v = new RowImpl(v.getRowId(), timeStamp, v.getCells());
        return v;
      }

      @Override
      public boolean hasFilterRow() {
        return true;
      }

      @Override
      public boolean hasFilterCell() {
        return false;
      }
    };

    scan.setFilter(filter);
    Scanner scanner = null;
    try {
      scanner = mt.getScanner(scan);
      for (Row result : scanner) {
        assertEquals(timeStamp, result.getRowTimeStamp());
      }

      scan.setReturnKeysFlag(false);
      scanner = mt.getScanner(scan);
      for (Row result : scanner) {
        assertEquals(timeStamp, result.getRowTimeStamp());
      }
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  @Test
  public void testFilterRowNotCalled() {
    int numRows = 10;
    MTable mt = createTableAndPutRows("testFilterRowNotCalled", numRows);

    Scan scan = new Scan();
    Filter filter = new FilterBase() {

      @Override
      public boolean filterRow() {
        throw new MFilterException("Should not be called");
      }

      @Override
      public boolean hasFilterRow() {
        return false;
      }

      @Override
      public boolean hasFilterCell() {
        return false;
      }
    };

    scan.setFilter(filter);
    Scanner scanner = null;
    try {
      scanner = mt.getScanner(scan);
      for (Row result : scanner);
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  @Test
  public void testFilterCellCalled() {
    int numRows = 10;
    MTable mt = createTableAndPutRows("testFilterCellCalled", numRows);

    Scan scan = new Scan();
    Filter filter = new FilterBase() {
      boolean isFirstCell = true;

      @Override
      public void reset() {
        isFirstCell = true;
      }

      @Override
      public ReturnCode filterCell(Cell v, byte[] rowKey) {
        if (isFirstCell) {
          isFirstCell = false;
          return ReturnCode.INCLUDE;
        }
        return ReturnCode.NEXT_ROW;
      }

      @Override
      public boolean hasFilterRow() {
        return false;
      }

      @Override
      public boolean hasFilterCell() {
        return true;
      }
    };

    scan.setFilter(filter);
    Scanner scanner = null;
    try {
      scanner = mt.getScanner(scan);
      for (Row result : scanner) {
        assertEquals(10, result.size() - 1);
      }

      scan.setReturnKeysFlag(false);
      scanner = mt.getScanner(scan);
      for (Row result : scanner) {
        assertEquals(10, result.size() - 1);
      }
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  @Test
  public void testTransformCellCalled() {
    int numRows = 10;
    final byte[] newValue = Bytes.toBytes("newValue");
    MTable mt = createTableAndPutRows("testTransformCellCalled", numRows);

    Scan scan = new Scan();
    Filter filter = new FilterBase() {

      @Override
      public Cell transformCell(Cell v, byte[] rowKey) {
        v = new CellRef(v.getColumnName(), v.getColumnType(), newValue);
        return v;
      }

      @Override
      public boolean hasFilterRow() {
        return false;
      }

      @Override
      public boolean hasFilterCell() {
        return true;
      }
    };

    scan.setFilter(filter);
    Scanner scanner = null;
    try {
      scanner = mt.getScanner(scan);
      for (Row result : scanner) {
        for (int k = 0; k < result.getCells().size() - 1; k++) {
          assertEquals(Bytes.toString(newValue),
              Bytes.toString((byte[]) result.getCells().get(k).getColumnValue()));
        }
      }

      scan.setReturnKeysFlag(false);
      scanner = mt.getScanner(scan);
      for (Row result : scanner) {
        for (int k = 0; k < result.getCells().size() - 1; k++) {
          assertEquals(Bytes.toString(newValue),
              Bytes.toString((byte[]) result.getCells().get(k).getColumnValue()));
        }
      }
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  @Test
  public void testTransformRowCellsCalled() {
    int numRows = 10;
    MTable mt = createTableAndPutRows("testTransformRowCellsCalled", numRows);

    Scan scan = new Scan();
    Filter filter = new FilterBase() {

      @Override
      public List<Cell> filterRowCells(List<Cell> kvs) {
        throw new MFilterException("filterRowsCalled");
      }

      @Override
      public boolean hasFilterRow() {
        return false;
      }

      @Override
      public boolean hasFilterCell() {
        return true;
      }
    };

    scan.setFilter(filter);
    Scanner scanner = null;
    try {
      scanner = mt.getScanner(scan);
      for (Row result : scanner);
    } catch (Throwable e) {
      assert e.getCause() instanceof MFilterException;
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  @Test
  public void testFilterCellNotCalled() {
    int numRows = 10;
    MTable mt = createTableAndPutRows("testFilterCellNotCalled", numRows);

    Scan scan = new Scan();
    Filter filter = new FilterBase() {

      @Override
      public ReturnCode filterCell(Cell v, byte[] rowKey) {
        throw new MFilterException("Should not be called");
      }

      @Override
      public boolean hasFilterRow() {
        return false;
      }

      @Override
      public boolean hasFilterCell() {
        return false;
      }
    };

    scan.setFilter(filter);
    Scanner scanner = null;
    try {
      scanner = mt.getScanner(scan);
      for (Row result : scanner);
    } catch (Throwable e) {
      Assert.fail("No exception expected");
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  @Test
  public void testFilterCellWithSelCol() {
    int numRows = 10;
    MTable mt = createTableAndPutRows("testFilterCellWithSelCol", numRows);

    Scan scan = new Scan();
    scan.addColumn(QUALIFIERS[1]);
    scan.addColumn(QUALIFIERS[3]);
    scan.addColumn(QUALIFIERS[5]);
    Filter filter = new FilterBase() {
      boolean isFirstCell = true;

      @Override
      public void reset() {
        isFirstCell = true;
      }

      @Override
      public ReturnCode filterCell(Cell v, byte[] rowKey) {
        if (isFirstCell) {
          isFirstCell = false;
          return ReturnCode.INCLUDE;
        }
        return ReturnCode.NEXT_ROW;
      }

      @Override
      public boolean hasFilterRow() {
        return false;
      }

      @Override
      public boolean hasFilterCell() {
        return true;
      }
    };

    scan.setFilter(filter);
    Scanner scanner = null;
    try {
      scanner = mt.getScanner(scan);
      for (Row result : scanner) {
        assertEquals(3, result.size());
      }

      scan.setReturnKeysFlag(false);
      scanner = mt.getScanner(scan);
      for (Row result : scanner) {
        assertEquals(3, result.size());
      }
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  static byte[] stopRow = {36, 61, 22, 97, -79, -116, -6, 7},
      startRow = {-33, 34, -73, -57, 122, -67, -64, -20};
  static int filtercount = 0;

  @Test
  public void testFilterwithEmptyBatch() {
    MTable mt = createTable("testFilterwithEmptyBatch", 3);

    byte[][] rowkeys = {{79, -30, -122, 121, -11, -2, 127, -26}, {16, -18, 48, 29, 116, 32, 63, 60},
        {17, 30, -64, -79, -27, 10, 68, 5}, {26, 27, -10, 35, 16, 100, 109, 16},
        {67, -113, 39, -31, -1, 49, -128, -70}, {20, -79, -10, 71, -99, 36, 73, 33},
        {45, 85, 53, -125, 26, -13, -111, -88}, {43, -10, 98, -49, 42, 42, 2, -124},
        {30, -78, -18, 100, 17, 67, -42, -122}, {75, -65, -83, -14, 44, 27, 69, -10},
        {62, 88, -125, 13, -59, 85, -15, -24}, {4, 22, -98, 19, -9, -19, -46, 2},
        {43, 87, 39, 120, 69, -93, 24, -90}, {56, 97, 40, 46, -37, 76, -29, 42},
        {14, -72, -49, 104, -19, -108, -89, -61}, {47, -31, -42, -128, -3, -119, -63, 32},
        {36, 61, 22, 97, -79, -116, -6, 7}, {32, 101, -38, 88, -52, 50, 40, -109},
        {21, 52, 121, 104, 24, 127, 65, 72}, {46, -4, 30, -114, 117, 88, -101, 101},
        {-101, 77, -82, -39, -37, 79, 88, -126}, {89, -14, 8, -33, -36, 68, 54, -6},
        {-96, -66, 18, 59, -119, 79, -119, -56}, {97, 112, 98, -13, -124, -106, 72, 56},
        {124, 59, 111, -120, -76, 13, -122, -79}, {-103, 84, -37, -27, 22, -62, 81, 43},
        {-95, -6, -78, 99, -45, -6, -5, 113}, {123, 19, -102, -14, -109, -10, -82, -25},
        {96, -35, -125, 67, -114, 108, 61, 20}, {123, 54, 28, 56, 113, -47, -13, 63},
        {-125, 120, 51, 104, -103, 3, 77, -42}, {-95, -64, -116, 110, 6, 65, 36, -51},
        {-101, 0, -49, 39, -126, 90, 46, -98}, {-128, -71, 94, 52, 114, -68, -46, -87},
        {-128, -86, -36, -30, -41, -9, 67, 71}, {-111, 54, 13, -1, 103, 8, -112, -21},
        {104, 74, -31, -123, 121, -80, -66, 32}, {-86, 18, 29, -11, -52, 13, 13, -85},
        {102, -75, -84, 121, 92, -9, 114, 73}, {-90, -7, -80, 5, 95, -116, 47, 42},
        {-19, 39, -113, 73, -6, -80, 120, 125}, {-80, -11, 11, -33, -117, 63, 34, -70},
        {-80, -121, -121, -20, 86, 12, 103, -91}, {-76, -118, -21, -91, 80, -8, -81, 117},
        {-41, 75, 115, -10, -90, -41, -85, 61}, {-2, 8, -64, -49, 1, 86, -9, -33},
        {-52, -78, 79, -19, -89, 61, -58, 19}, {-49, 4, 115, 58, -46, 111, -38, -39},
        {-19, 127, 63, 79, 90, 71, 91, -78}, {-58, 30, -31, 74, -96, -4, 22, -12},
        {-74, 74, 40, 72, 110, 65, 109, 14}, {-33, 125, -112, 7, -113, 54, 96, -97},
        {-74, 114, 94, 40, -86, 102, -100, 92}, {-69, 114, 2, -92, 89, 10, -80, 98},
        {-67, -89, -98, 5, -88, -55, 9, 57}, {-48, -106, 58, 24, 62, -74, 36, 95},
        {-37, -111, 50, 34, -60, -32, 61, 126}, {-56, -16, -48, 57, -113, -66, -22, -79},
        {-32, -23, -103, -38, -82, 100, -2, -10}, {-33, 34, -73, -57, 122, -67, -64, -20}};

    for (byte[] rowkey : rowkeys) {
      Put put = new Put(rowkey);
      for (int j = 0; j < QUALIFIERS.length; j++) {
        put.addColumn(QUALIFIERS[j], Bytes.toBytes(VALUE + j));
      }
      mt.put(put);
    }

    Scan scan = new Scan();
    scan.setClientQueueSize(10);
    Filter filter = new FilterBase() {
      @Override
      public boolean filterRowKey(Row result) {
        if (Bytes.compareTo(result.getRowId(), stopRow) <= 0
            || Bytes.compareTo(result.getRowId(), startRow) > 0) {
          return false;
        }
        return true;
      }

      @Override
      public boolean hasFilterRow() {
        return true;
      }

      @Override
      public boolean hasFilterCell() {
        return false;
      }
    };

    scan.setFilter(filter);
    Scanner scanner = null;
    try {
      scanner = mt.getScanner(scan);
      int resultcount = 0;
      for (Row result : scanner) {
        resultcount++;
      }
      assertEquals(15, resultcount);

      scan = new Scan();
      scan.setClientQueueSize(10);
      scan.setFilter(filter);
      scan.setReturnKeysFlag(false);
      scanner = mt.getScanner(scan);
      resultcount = 0;
      for (Row result : scanner) {
        resultcount++;
      }
      assertEquals(15, resultcount);
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

}
