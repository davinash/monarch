package io.ampool.monarch.table.facttable.junit;

import io.ampool.internal.RegionDataOrder;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.MPartitionResolver;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.Region;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(FTableTest.class)
public class FTableCreationJUnitTest {

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
  public void testPARTITION_PERSISTENT() throws Exception {
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    FTableDescriptor mTableDescriptor = new FTableDescriptor();
    mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    FTable mTable = mCache.getAdmin().createFTable("testPARTITION_PERSISTENT", mTableDescriptor);
    Region<Object, Object> geodeRegion = geodeCache.getRegion("testPARTITION_PERSISTENT");
    assertEquals(DataPolicy.PERSISTENT_PARTITION, geodeRegion.getAttributes().getDataPolicy());
    assertEquals(RegionDataOrder.IMMUTABLE,
        ((AmpoolTableRegionAttributes) geodeRegion.getAttributes().getCustomAttributes())
            .getRegionDataOrder());
    assertNotNull(geodeRegion.getAttributes().getPartitionAttributes());
    assertEquals(0, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(0, geodeRegion.getAttributes().getPartitionAttributes().getRedundantCopies());
    mCache.getAdmin().deleteFTable("testPARTITION_PERSISTENT");
    mCache.close();
  }

  @Test
  public void testPARTITION_RESOLVER() throws Exception {
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    FTableDescriptor mTableDescriptor = new FTableDescriptor();
    mTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    FTable mTable = mCache.getAdmin().createFTable("testPARTITION_PERSISTENT", mTableDescriptor);
    Region<Object, Object> geodeRegion = geodeCache.getRegion("testPARTITION_PERSISTENT");
    mCache.getAdmin().deleteFTable("testPARTITION_PERSISTENT");
    /** delete the table and then do assertions.. as failures may skip deletion **/
    assertEquals("Expected default partition-resolver.", MPartitionResolver.DEFAULT_RESOLVER,
        geodeRegion.getAttributes().getPartitionAttributes().getPartitionResolver());
    assertEquals(DataPolicy.PERSISTENT_PARTITION, geodeRegion.getAttributes().getDataPolicy());
    assertEquals(RegionDataOrder.IMMUTABLE,
        ((AmpoolTableRegionAttributes) geodeRegion.getAttributes().getCustomAttributes())
            .getRegionDataOrder());
    assertNotNull(geodeRegion.getAttributes().getPartitionAttributes());
    assertEquals(0, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(0, geodeRegion.getAttributes().getPartitionAttributes().getRedundantCopies());
    mCache.close();
  }

  @Test
  public void testPARTITION_REDUNDANT_PERSISTENT() throws Exception {
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    fTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    fTableDescriptor.setRedundantCopies(1);
    FTable mTable =
        mCache.getAdmin().createFTable("testPARTITION_REDUNDANT_PERSISTENT", fTableDescriptor);
    Region<Object, Object> geodeRegion = geodeCache.getRegion("testPARTITION_REDUNDANT_PERSISTENT");
    assertEquals(DataPolicy.PERSISTENT_PARTITION, geodeRegion.getAttributes().getDataPolicy());
    assertEquals(RegionDataOrder.IMMUTABLE,
        ((AmpoolTableRegionAttributes) geodeRegion.getAttributes().getCustomAttributes())
            .getRegionDataOrder());
    assertNotNull(geodeRegion.getAttributes().getPartitionAttributes());
    assertEquals(1, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(1, geodeRegion.getAttributes().getPartitionAttributes().getRedundantCopies());
    mCache.getAdmin().deleteFTable("testPARTITION_REDUNDANT_PERSISTENT");
    mCache.close();
  }

  @Test
  public void testPARTITION_REDUNDANT_PERSISTENT_EVICTION_ACTION() throws Exception {
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    fTableDescriptor.addColumn(Bytes.toBytes("testcol"));
    fTableDescriptor.setRedundantCopies(1);
    FTable mTable =
        mCache.getAdmin().createFTable("testPARTITION_REDUNDANT_PERSISTENT", fTableDescriptor);
    Region<Object, Object> geodeRegion = geodeCache.getRegion("testPARTITION_REDUNDANT_PERSISTENT");
    assertEquals(DataPolicy.PERSISTENT_PARTITION, geodeRegion.getAttributes().getDataPolicy());
    assertEquals(RegionDataOrder.IMMUTABLE,
        ((AmpoolTableRegionAttributes) geodeRegion.getAttributes().getCustomAttributes())
            .getRegionDataOrder());
    assertNotNull(geodeRegion.getAttributes().getPartitionAttributes());
    assertEquals(1, mTable.getTableDescriptor().getRedundantCopies());
    assertEquals(1, geodeRegion.getAttributes().getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAction.OVERFLOW_TO_TIER,
        geodeRegion.getAttributes().getEvictionAttributes().getAction());

    mCache.getAdmin().deleteFTable("testPARTITION_REDUNDANT_PERSISTENT");
    mCache.close();
  }

  private Properties createLonerProperties() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    return props;
  }

  private MCache createCache() {
    return new MCacheFactory(createLonerProperties()).create();
  }


  // Test to verify GEN-1574
  @Test
  public void testFTableSpecificTest() {
    MCache geodeCache = createCache();
    MCache mCache = MCacheFactory.getAnyInstance();
    final String TABLE_NAME = "testFTableSpecificTest";

    Object[][] columnNameTypes = {{"ID", BasicTypes.INT}, {"NAME", BasicTypes.STRING},
        {"AGE", BasicTypes.INT}, {"SEX", BasicTypes.STRING}, {"DESIGNATION", BasicTypes.STRING}};

    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    fTableDescriptor.setTotalNumOfSplits(1);
    for (int i = 0; i < columnNameTypes.length; i++) {
      fTableDescriptor.addColumn((String) columnNameTypes[i][0],
          (BasicTypes) columnNameTypes[i][1]);
    }
    if (mCache.getFTable(TABLE_NAME) != null) {
      mCache.getAdmin().deleteFTable(TABLE_NAME);
    }

    FTable fTable = mCache.getAdmin().createFTable(TABLE_NAME, fTableDescriptor);

    Object[][] values = {{1, "Phoebe", 34, "F", "PM"}, {2, "Chandler", 33, "M", "Team Leader"},
        {3, "Rachel", 16, "F", "Software Developer"}, {4, "Ross", 27, "M", "Software Developer"},
        {5, "Monica", 38, "F", "ARCH"}, {6, "Joey", 33, "M", "ARCH"}, {7, "Janice", 29, "M", "HR"},
        {8, "Gunther", 33, "M", "CUSTOMER SUPPORT"}, {9, "Jack", 54, "M", "GM"},
        {10, "Judy", 56, "F", "CEO"}};

    for (int i = 0; i < values.length; i++) {
      Record record = new Record();
      for (int j = 0; j < columnNameTypes.length; j++) {
        record.add((String) columnNameTypes[j][0], values[i][j]);
      }
      fTable.append(record);
    }
  }

}
