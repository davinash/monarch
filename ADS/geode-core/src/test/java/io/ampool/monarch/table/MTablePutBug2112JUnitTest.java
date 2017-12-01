package io.ampool.monarch.table;

import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

@Category(MonarchTest.class)
public class MTablePutBug2112JUnitTest {
  private static String COLUMNNAME_PREFIX = "COL", VALUE_PREFIX = "VAL";
  private static int NUM_COLUMNS = 5;

  private MCache createCache() {
    return (new MCacheFactory(this.createLonerProperties())).create();
  }

  private Properties createLonerProperties() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    return props;
  }

  private MTable createTable(String tableName) {
    MCache amplServerCache = this.createCache();

    MTableDescriptor mTableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    mTableDescriptor.setMaxVersions(1);
    for (int i = 0; i < NUM_COLUMNS; i++) {
      mTableDescriptor.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + i));
    }

    MTable mTable = amplServerCache.getAdmin().createMTable(tableName, mTableDescriptor);

    return mTable;
  }

  private void _testPutForBug2112(boolean isNullColumnValues, final String tableName) {
    java.util.function.BiConsumer<MTable, String[]> verifyGet1 = (table, keys) -> {
      Exception e = null;
      Get get = null;
      for (String key : keys) {
        try {
          get = new Get(key);
        } catch (Exception e1) {
          e = e1;
        }

        if (key == null || key.length() == 0) {
          assertTrue(e != null);
          assertTrue(e instanceof IllegalStateException);
        } else {
          assertTrue(e == null);
          Row result = table.get(get);
          List<Cell> cells = result.getCells();
          assertTrue(cells.size() > 0);
          // for (Cell mcell : cells) {
          for (int i = 0; i < cells.size() - 1; i++) {
            if (isNullColumnValues) {
              assertTrue(cells.get(i).getColumnValue() == null);
            } else {
              assertTrue(cells.get(i).getColumnValue() != null);
            }
          }
        }
      }

      // verify get all
      List<Get> gets = new ArrayList<>();
      for (String key : keys) {
        e = null;
        try {
          get = new Get(key);
        } catch (Exception e1) {
          e = e1;
        }

        if (key == null || key.length() == 0) {
          assertTrue(e != null);
          assertTrue(e instanceof IllegalStateException);
        } else {
          assertTrue(e == null);
          gets.add(get);
        }
      }

      Row[] results = table.get(gets);
      for (Row result : results) {
        List<Cell> cells = result.getCells();
        assertTrue(cells.size() > 0);
        // for (Cell mcell : cells) {
        for (int i = 0; i < cells.size() - 1; i++) {
          if (isNullColumnValues) {
            assertTrue(cells.get(i).getColumnValue() == null);
          } else {
            assertTrue(cells.get(i).getColumnValue() != null);
          }
        }
      }
    };

    /* Valid puts: Verify that all columns have valid data */
    java.util.function.BiConsumer<MTable, String[]> verifyGet2 = (table, keys) -> {
      Exception e = null;
      Get get = null;
      for (String key : keys) {
        try {
          get = new Get(key);
          // TODO: add setRowkey and use here.
        } catch (Exception e1) {
          e = e1;
        }

        if (key == null || key.length() == 0) {
          assertTrue(e != null);
          assertTrue(e instanceof IllegalStateException);
        } else {
          assertTrue(e == null);
          Row result = table.get(get);
          // getCurrentTableType
          List<Cell> cells = result.getCells();
          assertTrue(cells.size() > 0);
          for (int j = 0; j < cells.size() - 1 /* GEN-1696 */; j++) {
            assertTrue((VALUE_PREFIX + j)
                .compareTo(Bytes.toString((byte[]) cells.get(j).getColumnValue())) == 0);
          }
        }
      }

      // verify get all
      List<Get> gets = new ArrayList<>();
      for (String key : keys) {
        e = null;
        try {
          get = new Get(key);
        } catch (Exception e1) {
          e = e1;
        }

        if (key == null || key.length() == 0) {
          assertTrue(e != null);
          assertTrue(e instanceof IllegalStateException);
        } else {
          assertTrue(e == null);
          gets.add(get);
        }
      }

      Row[] results = table.get(gets);
      for (Row result : results) {
        List<Cell> cells = result.getCells();
        assertTrue(cells.size() > 0);
        for (int j = 0; j < cells.size() - 1 /* GEN- 1696 */; j++) {
          assertTrue((VALUE_PREFIX + j)
              .compareTo(Bytes.toString((byte[]) cells.get(j).getColumnValue())) == 0);
        }
      }
    };

    // ======================== Test starts here ============================
    final String[] keys = {"006", "008", "", null};
    MTable table = createTable(tableName);
    Exception e = null;
    List<Put> putList = new ArrayList<Put>();
    // null put
    e = null;
    putList.clear();
    putList.add(null);
    putList.add(null);
    try {
      table.put(putList);
    } catch (Exception e1) {
      e = e1;
    }

    assertTrue(e != null);
    assertTrue(e instanceof IllegalArgumentException);

    // no puts in list
    putList.clear();
    e = null;
    try {
      table.put(putList);
    } catch (Exception e1) {
      e = e1;
    }

    assertTrue(e != null);
    assertTrue(e instanceof IllegalArgumentException);

    putList.clear();

    for (int i = 0; i < keys.length; i++) {
      final String key = keys[i];

      // Valid puts
      e = null;
      try {
        Put put = new Put(key);
        for (int j = 0; j < NUM_COLUMNS; j++) {
          put.addColumn(COLUMNNAME_PREFIX + j, Bytes.toBytes(VALUE_PREFIX + j));
        }
        System.out.println("MTableAllGetPutDunit.run: key in MPut is:" + key);
        putList.add(put);

      } catch (Exception e1) {
        e = e1;
      }
      if (key == null || key.length() == 0) {
        System.out.println("Exception caught: " + e.getClass().toString());
        assertTrue(e != null);
        assertTrue(e instanceof IllegalStateException);
      } else {
        assertTrue(e == null);
      }

    }
    e = null;
    try {
      table.put(putList);
    } catch (Exception e1) {
      e = e1;
    }

    // The put should not fail.
    assertTrue(e == null);
    verifyGet2.accept(table, keys);

    // table = truncateTable(tableName);
    try {
      MCacheFactory.getAnyInstance().getAdmin().truncateMTable(tableName);
    } catch (Exception e1) {
      e1.printStackTrace();
    }
    putList.clear();

    for (int i = 0; i < keys.length; i++) {
      final String key = keys[i];
      // Empty put

      e = null;
      try {
        Put put1 = new Put(key);
        if (isNullColumnValues) {
          for (int j = 0; j < NUM_COLUMNS; j++) {
            put1.addColumn(COLUMNNAME_PREFIX + j, null);
          }
        }
        // No columns in Put(Empty Put)
        System.out.println("MTableAllGetPutDunit.run: key in MPut is:" + key);
        putList.add(put1);

      } catch (Exception e1) {
        e = e1;
      }
      if (key == null || key.length() == 0) {
        System.out.println("Exception caught: " + e.getClass().toString());
        assertTrue(e != null);
        assertTrue(e instanceof IllegalStateException);
      } else {
        assertTrue(e == null);
      }
    }
    e = null;
    try {
      table.put(putList);
    } catch (Exception e1) {
      e = e1;
    }

    // The put should not fail.
    if (isNullColumnValues) {
      assertTrue(e == null);
    } else {
      assertTrue(e instanceof IllegalArgumentException);
    }
    verifyGet1.accept(table, keys);

    putList.clear();

    MCacheFactory.getAnyInstance().getAdmin().deleteMTable(tableName);
  }

  /**
   * Update a existing row with key only. No columns are added. Put put = new Put(rowKey)
   * table.put(put).
   */
  @Test
  public void testPutWithOnlyKeyNoColumns_Bug2112() {
    _testPutForBug2112(false, "testPutWithOnlyKeyNoColumns_Bug2112");
  }

  @Test
  public void testPutWithKeyAndNullColumnValues_Bug2112() {
    _testPutForBug2112(true, "testPutWithKeyAndNullColumnValues_Bug2112");
  }

}
