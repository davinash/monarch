package io.ampool.monarch.table;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.ampool.monarch.table.internal.MTableRow;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.cache.*;
import org.apache.geode.test.junit.categories.MonarchTest;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import io.ampool.monarch.table.exceptions.MTableExistsException;
import io.ampool.monarch.table.internal.AdminImpl;

@Category(MonarchTest.class)
public class MTableCreationJUnitTest {

  @Rule
  public TestName testName = new TestName();

  private final String key = "key";
  private final Integer val = new Integer(1);


  @Before
  public void setUp() {

  }

  @After
  public void tearDown() throws Exception {

  }


  @Test
  public void testPARTITION() throws Exception {
    MCache amplCache = createCache();
    MTableDescriptor mTableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    MTable mTable = amplCache.getAdmin().createTable("testPARTITION_UNORDERED", mTableDescriptor);
    assertEquals(mTable.getTableDescriptor().getTableType(), MTableType.UNORDERED);
    assertEquals(DataPolicy.PARTITION,
        amplCache.getRegion("testPARTITION_UNORDERED").getAttributes().getDataPolicy());
    assertNotNull(
        amplCache.getRegion("testPARTITION_UNORDERED").getAttributes().getPartitionAttributes());
    assertEquals(0, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(0, amplCache.getRegion("testPARTITION_UNORDERED").getAttributes()
        .getPartitionAttributes().getRedundantCopies());
    amplCache.getAdmin().deleteTable("testPARTITION_UNORDERED");
    amplCache.close();
  }

  @Test
  public void testPARTITION_ORDERED() throws Exception {
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    MTable mTable = mCache.getAdmin().createTable("testPARTITION_ORDERED", mTableDescriptor);
    assertEquals(mTable.getTableDescriptor().getTableType(), MTableType.ORDERED_VERSIONED);
    assertEquals(DataPolicy.PARTITION,
        geodeCache.getRegion("testPARTITION_ORDERED").getAttributes().getDataPolicy());
    assertNotNull(
        geodeCache.getRegion("testPARTITION_ORDERED").getAttributes().getPartitionAttributes());
    assertEquals(0, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(0, geodeCache.getRegion("testPARTITION_ORDERED").getAttributes()
        .getPartitionAttributes().getRedundantCopies());
    mCache.getAdmin().deleteTable("testPARTITION_ORDERED");
    mCache.close();
  }


  @Test
  public void testPARTITION_REDUNDANT() throws Exception {
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    mTableDescriptor.setRedundantCopies(1);
    MTable mTable = mCache.getAdmin().createTable("testPARTITION_REDUNDANT", mTableDescriptor);
    assertEquals(mTable.getTableDescriptor().getTableType(), MTableType.ORDERED_VERSIONED);
    assertEquals(DataPolicy.PARTITION,
        geodeCache.getRegion("testPARTITION_REDUNDANT").getAttributes().getDataPolicy());
    assertNotNull(
        geodeCache.getRegion("testPARTITION_REDUNDANT").getAttributes().getPartitionAttributes());
    assertEquals(1, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(1, geodeCache.getRegion("testPARTITION_REDUNDANT").getAttributes()
        .getPartitionAttributes().getRedundantCopies());
    mCache.getAdmin().deleteTable("testPARTITION_REDUNDANT");
    mCache.close();
  }

  @Test
  public void testPARTITION_PERSISTENT() throws Exception {
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    mTableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    MTable mTable = mCache.getAdmin().createTable("testPARTITION_PERSISTENT", mTableDescriptor);
    assertEquals(mTable.getTableDescriptor().getTableType(), MTableType.ORDERED_VERSIONED);
    assertEquals(DataPolicy.PERSISTENT_PARTITION,
        geodeCache.getRegion("testPARTITION_PERSISTENT").getAttributes().getDataPolicy());
    assertNotNull(
        geodeCache.getRegion("testPARTITION_PERSISTENT").getAttributes().getPartitionAttributes());
    assertEquals(0, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(0, geodeCache.getRegion("testPARTITION_PERSISTENT").getAttributes()
        .getPartitionAttributes().getRedundantCopies());
    mCache.getAdmin().deleteTable("testPARTITION_PERSISTENT");
    mCache.close();
  }

  @Test
  public void testPARTITION_REDUNDANT_PERSISTENT() throws Exception {
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    mTableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    mTableDescriptor.setRedundantCopies(1);
    MTable mTable =
        mCache.getAdmin().createTable("testPARTITION_REDUNDANT_PERSISTENT", mTableDescriptor);
    assertEquals(mTable.getTableDescriptor().getTableType(), MTableType.ORDERED_VERSIONED);
    assertEquals(DataPolicy.PERSISTENT_PARTITION,
        geodeCache.getRegion("testPARTITION_REDUNDANT_PERSISTENT").getAttributes().getDataPolicy());
    assertNotNull(geodeCache.getRegion("testPARTITION_REDUNDANT_PERSISTENT").getAttributes()
        .getPartitionAttributes());
    assertEquals(1, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(1, geodeCache.getRegion("testPARTITION_REDUNDANT_PERSISTENT").getAttributes()
        .getPartitionAttributes().getRedundantCopies());
    mCache.getAdmin().deleteTable("testPARTITION_REDUNDANT_PERSISTENT");
    mCache.close();
  }

  @Test
  public void testMTableCreationMultiThread() throws Exception {
    String table_name = "multithreadMTable";
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    mTableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    mTableDescriptor.setRedundantCopies(1);

    ExecutorService es = Executors.newFixedThreadPool(10);

    final Callable thread = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return createTableFromThread(mCache, table_name, mTableDescriptor);
      }
    };

    final Callable thread2 = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        MTableDescriptor mTableDescriptor = new MTableDescriptor();
        mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
        mTableDescriptor.setRedundantCopies(1);

        return createTableFromThread(mCache, table_name, mTableDescriptor);
      }
    };

    final Future<Boolean> future = es.submit(thread);
    final Future<Boolean> future2 = es.submit(thread2);

    Boolean res1 = future.get();
    Boolean res2 = future2.get();

    final MTable mTable = mCache.getTable(table_name);

    assertEquals(mTable.getTableDescriptor().getTableType(), MTableType.ORDERED_VERSIONED);
    if (res1) {
      assertEquals(DataPolicy.PERSISTENT_PARTITION,
          geodeCache.getRegion(table_name).getAttributes().getDataPolicy());
    } else if (res2) {
      assertEquals(DataPolicy.PARTITION,
          geodeCache.getRegion(table_name).getAttributes().getDataPolicy());
    }

    assertNotNull(geodeCache.getRegion(table_name).getAttributes().getPartitionAttributes());

    assertEquals(1, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(1, geodeCache.getRegion(table_name).getAttributes().getPartitionAttributes()
        .getRedundantCopies());

    mCache.getAdmin().deleteTable(table_name);

    mCache.close();
  }

  @Test
  public void testTwoMTableCreationMultiThread() throws Exception {
    String table_name = "multithreadMTable";
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    mTableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    mTableDescriptor.setRedundantCopies(1);

    ExecutorService es = Executors.newFixedThreadPool(10);

    final Callable thread = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return createTableFromThread(mCache, table_name + "1", mTableDescriptor);
      }
    };

    final Callable thread2 = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        // MTableDescriptor mTableDescriptor = new MTableDescriptor();
        // mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
        // mTableDescriptor.setRedundantCopies(1);

        return createTableFromThread(mCache, table_name + "2", mTableDescriptor);
      }
    };

    final Future<Boolean> future = es.submit(thread);
    final Future<Boolean> future2 = es.submit(thread2);

    Boolean res1 = future.get();
    Boolean res2 = future2.get();

    MTable mTable = null;
    if (res1) {
      mTable = mCache.getTable(table_name + "1");
      assertNotNull(mTable);
      assertEquals(mTable.getTableDescriptor().getTableType(), MTableType.ORDERED_VERSIONED);
      assertEquals(DataPolicy.PERSISTENT_PARTITION,
          geodeCache.getRegion(table_name + "1").getAttributes().getDataPolicy());

      assertNotNull(
          geodeCache.getRegion(table_name + "1").getAttributes().getPartitionAttributes());

      assertEquals(1, mTable.getTableDescriptor().getRedundantCopies());
      assertEquals(1, geodeCache.getRegion(table_name + "1").getAttributes()
          .getPartitionAttributes().getRedundantCopies());
    }
    if (res2) {
      mTable = mCache.getTable(table_name + "2");
      assertNotNull(mTable);
      assertEquals(mTable.getTableDescriptor().getTableType(), MTableType.ORDERED_VERSIONED);
      assertEquals(DataPolicy.PERSISTENT_PARTITION,
          geodeCache.getRegion(table_name + "2").getAttributes().getDataPolicy());

      assertNotNull(
          geodeCache.getRegion(table_name + "2").getAttributes().getPartitionAttributes());

      assertEquals(1, mTable.getTableDescriptor().getRedundantCopies());
      assertEquals(1, geodeCache.getRegion(table_name + "2").getAttributes()
          .getPartitionAttributes().getRedundantCopies());
    }
    mCache.getAdmin().deleteTable(table_name + "1");
    mCache.getAdmin().deleteTable(table_name + "2");
    mCache.close();
  }

  private boolean createTableFromThread(final MCache mCache, final String table_name,
      final MTableDescriptor mTableDescriptor) {
    MTable mTable = null;
    Throwable expectedException = null;
    try {
      mTable = mCache.getAdmin().createTable(table_name, mTableDescriptor);
    } catch (Throwable t) {
      System.out
          .println("MTableCreationJUnitTest.createTableFromThread :: " + "excpetion occured..");
      expectedException = t;
      assertTrue(t instanceof MTableExistsException);
    }
    if (mTable != null) {
      assertNull(expectedException);
      return true;
    } else {
      assertNotNull(expectedException);
      return false;
    }
  }


  @Ignore
  @Test
  public void testPARTITION_OVERFLOW() throws Exception {
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    MTable mTable =
        ((AdminImpl) mCache.getAdmin()).createTable("testPARTITION_OVERFLOW", mTableDescriptor);
    assertEquals(mTable.getTableDescriptor().getTableType(), MTableType.ORDERED_VERSIONED);
    assertEquals(DataPolicy.PARTITION,
        geodeCache.getRegion("testPARTITION_OVERFLOW").getAttributes().getDataPolicy());
    assertNotNull(
        geodeCache.getRegion("testPARTITION_OVERFLOW").getAttributes().getPartitionAttributes());
    assertEquals(0, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(0, geodeCache.getRegion("testPARTITION_OVERFLOW").getAttributes()
        .getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.LOCAL_DESTROY),
        geodeCache.getRegion("testPARTITION_OVERFLOW").getAttributes().getEvictionAttributes());
    mCache.getAdmin().deleteTable("testPARTITION_OVERFLOW");
    mCache.close();
  }

  @Ignore
  @Test
  public void testPARTITION_REDUNDANT_OVERFLOW() throws Exception {
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    mTableDescriptor.setRedundantCopies(1);
    MTable mTable = ((AdminImpl) mCache.getAdmin()).createTable("testPARTITION_REDUNDANT_OVERFLOW",
        mTableDescriptor);
    assertEquals(mTable.getTableDescriptor().getTableType(), MTableType.ORDERED_VERSIONED);
    assertEquals(DataPolicy.PARTITION,
        geodeCache.getRegion("testPARTITION_REDUNDANT_OVERFLOW").getAttributes().getDataPolicy());
    assertNotNull(geodeCache.getRegion("testPARTITION_REDUNDANT_OVERFLOW").getAttributes()
        .getPartitionAttributes());
    assertEquals(1, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(1, geodeCache.getRegion("testPARTITION_REDUNDANT_OVERFLOW").getAttributes()
        .getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.LOCAL_DESTROY),
        geodeCache.getRegion("testPARTITION_REDUNDANT_OVERFLOW").getAttributes()
            .getEvictionAttributes());
    mCache.getAdmin().deleteTable("testPARTITION_REDUNDANT_OVERFLOW");
    mCache.close();
  }


  @Ignore
  @Test
  public void testPARTITION_PERSISTENT_OVERFLOW() throws Exception {
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    mTableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    MTable mTable = ((AdminImpl) mCache.getAdmin()).createTable("testPARTITION_PERSISTENT_OVERFLOW",
        mTableDescriptor);
    assertEquals(mTable.getTableDescriptor().getTableType(), MTableType.ORDERED_VERSIONED);
    assertEquals(DataPolicy.PERSISTENT_PARTITION,
        geodeCache.getRegion("testPARTITION_PERSISTENT_OVERFLOW").getAttributes().getDataPolicy());
    assertNotNull(geodeCache.getRegion("testPARTITION_PERSISTENT_OVERFLOW").getAttributes()
        .getPartitionAttributes());
    assertEquals(0, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(0, geodeCache.getRegion("testPARTITION_PERSISTENT_OVERFLOW").getAttributes()
        .getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.LOCAL_DESTROY),
        geodeCache.getRegion("testPARTITION_PERSISTENT_OVERFLOW").getAttributes()
            .getEvictionAttributes());
    mCache.getAdmin().deleteTable("testPARTITION_PERSISTENT_OVERFLOW");
    mCache.close();
  }

  @Ignore
  @Test
  public void testPARTITION_REDUNDANT_PERSISTENT_OVERFLOW() throws Exception {
    MCache mCache = createCache();
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    mTableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    mTableDescriptor.setRedundantCopies(1);
    MTable mTable = ((AdminImpl) mCache.getAdmin()).createTable("testPARTITION_PERSISTENT_OVERFLOW",
        mTableDescriptor);
    assertEquals(mTable.getTableDescriptor().getTableType(), MTableType.ORDERED_VERSIONED);
    assertEquals(DataPolicy.PERSISTENT_PARTITION,
        mCache.getRegion("testPARTITION_PERSISTENT_OVERFLOW").getAttributes().getDataPolicy());
    assertNotNull(mCache.getRegion("testPARTITION_PERSISTENT_OVERFLOW").getAttributes()
        .getPartitionAttributes());
    assertEquals(1, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(1, mCache.getRegion("testPARTITION_PERSISTENT_OVERFLOW").getAttributes()
        .getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.LOCAL_DESTROY),
        mCache.getRegion("testPARTITION_PERSISTENT_OVERFLOW").getAttributes()
            .getEvictionAttributes());
    mCache.getAdmin().deleteTable("testPARTITION_PERSISTENT_OVERFLOW");
    mCache.close();
  }

  private Properties createLonerProperties() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    return props;
  }

  @Test
  public void test1111() {
    final int NUM_OF_COLUMNS = 10;
    final String COLUMN_NAME_PREFIX = "COLUMN";
    final int NUM_OF_ROWS = 10;
    final String KEY_PREFIX = "KEY";
    final String VALUE_PREFIX = "VALUE";

    MCache amplServerCache = createCache();
    MTableDescriptor tableDescriptor = null;
    tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    MTable table = amplServerCache.getAdmin().createMTable("TEST1111", tableDescriptor);

    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }

    byte[] value = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, /* TimeStamp */
        0, 0, 0, 6, /* Column1 Length */
        0, 0, 0, 6, /* Column2 Length */
        0, 0, 0, 6, /* Column3 Length */
        0, 0, 0, 6, /* Column4 Length */
        0, 0, 0, 6, /* Column5 Length */
        0, 0, 0, 6, /* Column6 Length */
        0, 0, 0, 6, /* Column7 Length */
        0, 0, 0, 6, /* Column8 Length */
        0, 0, 0, 6, /* Column9 Length */
        0, 0, 0, 6, /* Column10 Length */
        0, 0, 0, 6, 86, 65, 76, 85, 69, 48, /* Column1 Value */
        86, 65, 76, 85, 69, 49, /* Column2 Value */
        86, 65, 76, 85, 69, 50, /* Column3 Value */
        86, 65, 76, 85, 69, 51, /* Column4 Value */
        86, 65, 76, 85, 69, 52, /* Column5 Value */
        86, 65, 76, 85, 69, 53, /* Column6 Value */
        86, 65, 76, 85, 69, 54, /* Column7 Value */
        86, 65, 76, 85, 69, 55, /* Column8 Value */
        86, 65, 76, 85, 69, 56, /* Column9 Value */
        86, 65, 76, 85, 69, 57, /* Column10 Value */
        86, 65, 76, 85, 69, 58};

    tableDescriptor = amplServerCache.getMTableDescriptor("TEST1111");
    assertNotNull(tableDescriptor);

    Map<MColumnDescriptor, byte[]> resultExpected = new LinkedHashMap<MColumnDescriptor, byte[]>();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      byte[] expectedValue = new byte[6];
      System.arraycopy(value, 52 + (colmnIndex * 6), expectedValue, 0, 6);

      resultExpected.put(tableDescriptor.getAllColumnDescriptors().get(colmnIndex), expectedValue);
    }

    MTableRow mcvo = new MTableRow(value, true, 11);

    Map<MColumnDescriptor, Object> result = mcvo.getColumnValue(tableDescriptor);
    if (result == null) {
      Assert.fail("Result got is null");
    }
    int columnIndex = 0;
    Iterator entries = resultExpected.entrySet().iterator();

    Iterator resultIterator = result.entrySet().iterator();
    for (int i = 0; i < result.entrySet().size() - 1; i++) {
      Map.Entry<MColumnDescriptor, Object> column =
          (Map.Entry<MColumnDescriptor, Object>) resultIterator.next();
      // for (Map.Entry<MColumnDescriptor, Object> column : result.entrySet()) {
      if (columnIndex > 10) {
        Assert.fail("Something is Wrong !!!");
      }
      Map.Entry<MColumnDescriptor, byte[]> thisEntry =
          (Map.Entry<MColumnDescriptor, byte[]>) entries.next();

      byte[] actualColumnName = column.getKey().getColumnName();
      byte[] expectedColumnName = thisEntry.getKey().getColumnName();


      if (!Bytes.equals(actualColumnName, expectedColumnName)) {
        System.out.println("ACTUAL   COLUMN NAME => " + new String(actualColumnName));
        System.out.println("EXPECTED COLUMN NAME => " + new String(expectedColumnName));

        Assert.fail("getColumnAt failed");
      }

      byte[] actualValue = (byte[]) column.getValue();
      byte[] expectedValue = resultExpected.get(column.getKey());


      if (!Bytes.equals(actualValue, expectedValue)) {
        System.out.println("ACTUAL   COLUMN VALUE => " + new String(actualValue));
        System.out.println("EXPECTED COLUMN VALUE => " + new String(expectedValue));
        Assert.fail("getColumnAt failed");
      }
      columnIndex++;
    }

    amplServerCache.close();
  }

  private MCache createCache() {
    return new MCacheFactory(createLonerProperties()).create();
  }

  @Test
  public void testSetMaxVersions() {
    Scan scan = new Scan();
    scan.setMaxVersions();
    Assert.assertFalse(scan.isOldestFirst());
  }
}
