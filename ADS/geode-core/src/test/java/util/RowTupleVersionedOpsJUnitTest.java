package util;

import org.apache.geode.test.junit.categories.MonarchTest;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Created by nilkanth on 27/11/15.
 */
@Category(MonarchTest.class)
public class RowTupleVersionedOpsJUnitTest
    extends TestCase {/*
                       * private Cache cache;
                       * 
                       * protected Properties getDSProps() { Properties props = new Properties();
                       * props.put("mcast-port", "0"); props.put("locators", "");
                       * props.put("log-level", "config"); //props.put("log-file",
                       * "/work/code/ampool/hbaseJunit.log"); return props; }
                       * 
                       * public void setUp() throws Exception { Properties props = getDSProps();
                       * cache = new CacheFactory(props).create(); }
                       * 
                       * public void tearDown() throws Exception {
                       * 
                       * cache.close();
                       * 
                       * }
                       * 
                       * boolean compareByteArray(byte[] left, byte[] right) { if (left == null) {
                       * return false; } if (right == null) { return false; } if (left.length !=
                       * right.length) { return false; } for (int i = 0; i < left.length; i++) { if
                       * (left[i] != right[i]) { return false; } } return true; }
                       */
  @Test
  public void testVersionedOps() {/*
                                   * RegionFactory rf =
                                   * cache.createRegionFactory(RegionShortcut.PARTITION)
                                   * .setRegionDataOrder(true);
                                   * 
                                   *//*
                                     * public static ArrayList<MColumnDescriptor>
                                     * createEmployeeTableSchema() { ArrayList<MColumnDescriptor>
                                     * columnDescriptors = new ArrayList<MColumnDescriptor>();
                                     * columnDescriptors.add(new
                                     * MColumnDescriptor(Bytes.toBytes("NAME"),
                                     * IMObjectType.STRING)); columnDescriptors.add(new
                                     * MColumnDescriptor(Bytes.toBytes("ID"), BasicType.INT));
                                     * columnDescriptors.add(new
                                     * MColumnDescriptor(Bytes.toBytes("AGE"), BasicType.INT));
                                     * columnDescriptors.add(new
                                     * MColumnDescriptor(Bytes.toBytes("SALARY"),
                                     * IMObjectType.INT)); columnDescriptors.add(new
                                     * MColumnDescriptor(Bytes.toBytes("DEPT"), BasicType.INT));
                                     * columnDescriptors.add(new
                                     * MColumnDescriptor(Bytes.toBytes("DOJ"), BasicType.DATE));
                                     * 
                                     * return columnDescriptors;
                                     *//*
                                       * byte value10[] = new byte[]{0, 0, 0, 0, 0, 0, 0, 100, 0, 0,
                                       * 0, 10, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0,
                                       * 0, 0, 10, 78, 105, 108, 107, 97, 110, 116, 104, 49, 49, 0,
                                       * 0, 0, 1, 0, 0, 0, 11, 0, 0, 3, -24, 0, 0, 0, 1, 48, 49, 45,
                                       * 48, 49, 45, 50, 48, 49, 49}; byte value11[] = new byte[]{0,
                                       * 0, 0, 0, 0, 0, 3, -24, 0, 0, 0, 10, 0, 0, 0, 4, 0, 0, 0, 4,
                                       * 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 10, 78, 105, 108, 107, 97,
                                       * 110, 116, 104, 49, 50, 0, 0, 0, 2, 0, 0, 0, 12, 0, 0, 7,
                                       * -48, 0, 0, 0, 2, 48, 49, 45, 48, 49, 45, 50, 48, 49, 50};
                                       * byte value12[] = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                       * 0, 10, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0,
                                       * 0, 0, 10, 78, 105, 108, 107, 97, 110, 116, 104, 49, 51, 0,
                                       * 0, 0, 3, 0, 0, 0, 13, 0, 0, 11, -72, 0, 0, 0, 3, 48, 49,
                                       * 45, 48, 49, 45, 50, 48, 49, 51};
                                       * 
                                       * byte value20[] = new byte[]{0, 0, 0, 0, 0, 0, 0, -56, 0, 0,
                                       * 0, 10, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0,
                                       * 0, 0, 10, 78, 105, 108, 107, 97, 110, 116, 104, 50, 49, 0,
                                       * 0, 0, 17, 0, 0, 0, 21, 0, 0, 19, -120, 0, 0, 0, 1, 48, 49,
                                       * 45, 48, 49, 45, 50, 48, 50, 49}; byte value21[] = new
                                       * byte[]{0, 0, 0, 0, 0, 0, 7, -48, 0, 0, 0, 10, 0, 0, 0, 4,
                                       * 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 10, 78, 105,
                                       * 108, 107, 97, 110, 116, 104, 50, 50, 0, 0, 0, 18, 0, 0, 0,
                                       * 22, 0, 0, 23, 112, 0, 0, 0, 2, 48, 49, 45, 48, 49, 45, 50,
                                       * 48, 50, 50}; byte value22[] = new byte[]{0, 0, 0, 0, 0, 0,
                                       * 0, 0, 0, 0, 0, 10, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0,
                                       * 0, 0, 4, 0, 0, 0, 10, 78, 105, 108, 107, 97, 110, 116, 104,
                                       * 50, 51, 0, 0, 0, 19, 0, 0, 0, 23, 0, 0, 27, 88, 0, 0, 0, 3,
                                       * 48, 49, 45, 48, 49, 45, 50, 48, 50, 51};
                                       * 
                                       * byte value30[] = new byte[]{0, 0, 0, 0, 0, 0, 1, 44, 0, 0,
                                       * 0, 10, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0,
                                       * 0, 0, 10, 78, 105, 108, 107, 97, 110, 116, 104, 51, 49, 0,
                                       * 0, 0, 25, 0, 0, 0, 31, 0, 0, 39, 16, 0, 0, 0, 1, 48, 49,
                                       * 45, 48, 49, 45, 50, 48, 51, 49}; byte value31[] = new
                                       * byte[]{0, 0, 0, 0, 0, 0, 11, -72, 0, 0, 0, 10, 0, 0, 0, 4,
                                       * 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 10, 78, 105,
                                       * 108, 107, 97, 110, 116, 104, 51, 50, 0, 0, 0, 26, 0, 0, 0,
                                       * 32, 0, 0, 46, -32, 0, 0, 0, 2, 48, 49, 45, 48, 49, 45, 50,
                                       * 48, 51, 50}; byte value32[] = new byte[]{0, 0, 0, 0, 0, 0,
                                       * 0, 0, 0, 0, 0, 10, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0,
                                       * 0, 0, 4, 0, 0, 0, 10, 78, 105, 108, 107, 97, 110, 116, 104,
                                       * 51, 51, 0, 0, 0, 27, 0, 0, 0, 33, 0, 0, 50, -56, 0, 0, 0,
                                       * 3, 48, 49, 45, 48, 49, 45, 50, 48, 51, 51};
                                       * 
                                       * Region r = rf.create("XYZ");
                                       * 
                                       * //puts 3 rows (keys) MTableKey key1 = new
                                       * MTableKey("Key1".getBytes()); { key1.setTimeStamp(100L);
                                       * r.put(key1, value10);
                                       * 
                                       * key1.setTimeStamp(1000L); r.put(key1, value11);
                                       * 
                                       * r.put(key1, value12); }
                                       * 
                                       * MTableKey key2 = new MTableKey("Key2".getBytes()); {
                                       * key2.setTimeStamp(200L); r.put(key2, value20);
                                       * 
                                       * key2.setTimeStamp(2000L); r.put(key2, value21);
                                       * 
                                       * r.put(key2, value22); }
                                       * 
                                       * MTableKey key3 = new MTableKey("Key3".getBytes()); {
                                       * key3.setTimeStamp(300L); r.put(key3, value30);
                                       * 
                                       * key3.setTimeStamp(3000L); r.put(key3, value31);
                                       * 
                                       * r.put(key3, value32); }
                                       * 
                                       * System.out.println("Region Size = " + r.size());
                                       * 
                                       * //Delete keys //key2
                                       * 
                                       * Region metaRegion =
                                       * cache.createRegionFactory(RegionShortcut.REPLICATE).create(
                                       * "__MONARCH_ROW_TUPLE_REGION_META_DATA__");
                                       * 
                                       * MTableDescriptor td = new MTableDescriptor();
                                       * td.addColumn(Bytes.toBytes("NAME"))
                                       * .addColumn(Bytes.toBytes("ID"))
                                       * .addColumn(Bytes.toBytes("AGE"))
                                       * .addColumn(Bytes.toBytes("SALARY"))
                                       * .addColumn(Bytes.toBytes("DEPT"))
                                       * .addColumn(Bytes.toBytes("DOJ"));
                                       * 
                                       * metaRegion.put("XYZ", td);
                                       * 
                                       * MTableDescriptor mtd = (MTableDescriptor)
                                       * metaRegion.get("XYZ");
                                       * Iterator<Map.Entry<MColumnDescriptor, Integer>> iterColDes
                                       * = mtd.getColumnDescriptorsMap().entrySet().iterator();
                                       * Map<MTableKey, byte[]> columnValueMap = new HashMap<>();
                                       * columnValueMap.put(new MTableKey((Bytes.toBytes("NAME"))),
                                       * Bytes.toBytes("Nilkanth22")); columnValueMap.put(new
                                       * MTableKey((Bytes.toBytes("ID"))), Bytes.toBytes("022"));
                                       * columnValueMap.put(new MTableKey((Bytes.toBytes("AGE"))),
                                       * Bytes.toBytes("22")); columnValueMap.put(new
                                       * MTableKey((Bytes.toBytes("SALARY"))),
                                       * Bytes.toBytes("6000")); columnValueMap.put(new
                                       * MTableKey((Bytes.toBytes("DEPT"))), Bytes.toBytes("02"));
                                       * columnValueMap.put(new MTableKey((Bytes.toBytes("DOJ"))),
                                       * Bytes.toBytes("01-01-2022"));
                                       * 
                                       * //Testcase 1: //delete entire row r.destroy(key1);
                                       * System.out.
                                       * println("Deleted rows successfully! Updated size = " +
                                       * r.size()); Assert.assertNull(r.get(key1));
                                       * Assert.assertEquals(2, r.size());
                                       * Assert.assertFalse(r.containsKey(key1));
                                       * 
                                       * 
                                       * //Delete versioned entry (specified by timestamp) of a row.
                                       * //Example: //Testcase 2: //delete a specified cols within a
                                       * row with specified timestamp, ts. // Note: this should
                                       * delete entries having timestamp < ts as well. //Example::
                                       * //r.put(key, (t1, v1)) //r.put(key, (t2, v2)) //r.put(key,
                                       * (t3, v3)) //r.put(key, (t4, v4)) //r.put(key, (t5, v5))
                                       * --latest update //then, //r.delete(key, t3), should have
                                       * following state for the key, //[key, (t4, v4)] and [key,
                                       * (t5, v5)] key2.setIsDeleteOp( true);
                                       * key2.setTimeStamp(2000L); //execute destroy as a special
                                       * case of PUT. r.put(key2, new byte[0]); System.out.
                                       * println("Deleted key2 with timestamp 2000L successfully! Updated size = "
                                       * + r.size());
                                       * 
                                       * //reset the versionStamp. key2 = null; key2 = new
                                       * MTableKey("Key2".getBytes()); byte[] value =
                                       * (byte[])r.get(key2);
                                       * 
                                       * System.out.
                                       * println("Nilkanth DELETE Versioned Entry GET val = " +
                                       * Arrays.toString(value));
                                       * 
                                       *//*
                                         * List<byte[]> deletedColumns = new ArrayList<byte[]>();
                                         * deletedColumns.add(Bytes.toBytes("AGE"));
                                         * deletedColumns.add(Bytes.toBytes("DEPT")); int
                                         * noColsTobeDeleted = deletedColumns.size();
                                         * 
                                         * // Testcase 3: //delete a specified cols within a row
                                         * //Note: Should we support a timestamp with this case?? if
                                         * (noColsTobeDeleted > 0 && noColsTobeDeleted <
                                         * mtd.getColumnDescriptorsMap().size()) { //Set this put is
                                         * delete op key2.setIsDeleteOp(true);
                                         * 
                                         * Map<MColumnDescriptor, Integer> listOfColumns =
                                         * mtd.getColumnDescriptorsMap(); MColumnDescriptor current
                                         * = new MColumnDescriptor(); for (byte[] columnName :
                                         * deletedColumns) { current.setColumnName(columnName); int
                                         * position = listOfColumns.get(current);
                                         * System.out.println("Delele column position = " +
                                         * position); key2.addColumnPosition(position); }
                                         * key2.setTimeStamp(2000L); r.put(key2, new byte[0]);
                                         * System.out.println("Deleted (as PUT) done successfully!"
                                         * );
                                         * 
                                         * byte[] value = (byte[])r.get(key2);
                                         * 
                                         * System.out.println("Nilkanth GET val = " +
                                         * Arrays.toString(value));
                                         * 
                                         * //TODO //Verify the get result
                                         * 
                                         * 
                                         * return; }
                                         */

  }


}
