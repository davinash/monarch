package io.ampool.internal;

import static io.ampool.internal.MPartList.DES_ROW;
import static io.ampool.internal.MPartList.ROW;
import static io.ampool.monarch.types.BasicTypes.BINARY;
import static io.ampool.monarch.types.BasicTypes.DOUBLE;
import static io.ampool.monarch.types.BasicTypes.INT;
import static io.ampool.monarch.types.BasicTypes.LONG;
import static io.ampool.monarch.types.BasicTypes.STRING;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.Encoding;
import io.ampool.monarch.table.internal.InternalRow;
import io.ampool.monarch.table.internal.MOpInfo;
import io.ampool.monarch.table.internal.MOperation;
import io.ampool.monarch.table.internal.MTableStorageFormatter;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.MValue;
import io.ampool.monarch.table.region.FTableByteUtils;
import io.ampool.monarch.table.region.ScanContext;
import io.ampool.monarch.table.region.ScanUtils;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.TypeUtils;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.interfaces.Function3;
import io.ampool.store.StoreRecord;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class MPartListTest {
  @Rule
  public TestName testName = new TestName();
  static HeapDataOutputStream hdos = new HeapDataOutputStream(102400, null);

  private static final byte[] NULL_BYTES = null;
  private static final byte[] key = new byte[] {'k', 'e', 'y'};

  /**
   * Serialize the part-list using the ValueWriter and then deserialize the same. Simulating the
   * server-to-client communication.
   *
   * @param values the values to be sent
   * @return the value object retrieved after deserialization
   * @throws IOException if not able to write the serialized object
   * @throws ClassNotFoundException if the object class was not found
   */
  private static MPartList writeAndRead(final MPartList values)
      throws IOException, ClassNotFoundException {
    hdos.reset();
    BlobHelper.serializeTo(values, hdos);
    values.clear();

    MPartList ret;
    try (DataInputStream dis = new DataInputStream(hdos.getInputStream())) {
      ret = DataSerializer.readObject(dis);
    }
    return ret;
  }

  public static Object[] dataValueWriter() {
    return new Object[][] {{NULL_BYTES, Collections.emptyList(), NULL_BYTES,}, ////
        {NULL_BYTES, Arrays.asList(0, 1, 2), NULL_BYTES,}, ////
        // {new byte[] {0, 0, 0, 1, 0, 0, 0, 32, 0, 0, 4, 75, 2, -114, -42, 6, 0, 0, 0, 0, 0, 0, 0,
        // 8,
        // 0, 0, 0, 4, 83, 116, 114, 105, 110, 103, 95, 49, 0, 0, 0, 11},
        // Collections.emptyList(),
        // new byte[] {0, 0, 0, 1, 0, 0, 0, 32, 0, 0, 4, 75, 2, -114, -42, 6, 0, 0, 0, 0, 0, 0, 0,
        // 8, 0, 0, 0, 4, 83, 116, 114, 105, 110, 103, 95, 49, 0, 0, 0, 11},},
        // {new byte[] {0, 0, 0, 1, 0, 0, 0, 32, 0, 0, 4, 75, 2, -114, -42, 6, 0, 0, 0, 0, 0, 0, 0,
        // 8,
        // 0, 0, 0, 4, 83, 116, 114, 105, 110, 103, 95, 49, 0, 0, 0, 11},
        // Arrays.asList(0, 1, 2),
        // new byte[] {0, 0, 0, 1, 0, 0, 0, 32, 0, 0, 4, 75, 2, -114, -42, 6, 0, 0, 0, 0, 0, 0, 0,
        // 8, 0, 0, 0, 4, 83, 116, 114, 105, 110, 103, 95, 49, 0, 0, 0, 11},},
        {new byte[] {1, 1, 1, 0, 0, 0, 26, 104, 36, 32, -91, 22, 7, 0, 0, 0, 111, 64, 116, -43, 83,
            -9, -50, -39, 23, 97, 98, 99, 95, 49, 50, 51, 0, 0, 0, 25},
            Collections.singletonList(0),
            new byte[] {1, 1, 1, 0, 0, 0, 26, 104, 36, 32, -91, 22, 1, 0, 0, 0, 111}},
        {new byte[] {1, 1, 1, 0, 0, 0, 26, 104, 36, 32, -91, 22, 7, 0, 0, 0, 111, 64, 116, -43, 83,
            -9, -50, -39, 23, 97, 98, 99, 95, 49, 50, 51, 0, 0, 0, 25},
            Collections.singletonList(1),
            new byte[] {1, 1, 1, 0, 0, 0, 26, 104, 36, 32, -91, 22, 2, 0, 0, 0, 7, 97, 98, 99, 95,
                49, 50, 51}},
        {new byte[] {1, 1, 1, 0, 0, 0, 26, 104, 36, 32, -91, 22, 7, 0, 0, 0, 111, 64, 116, -43, 83,
            -9, -50, -39, 23, 97, 98, 99, 95, 49, 50, 51, 0, 0, 0, 25},
            Collections.singletonList(2),
            new byte[] {1, 1, 1, 0, 0, 0, 26, 104, 36, 32, -91, 22, 4, 64, 116, -43, 83, -9, -50,
                -39, 23}},
        {new byte[] {1, 1, 1, 0, 0, 0, 26, 104, 36, 32, -91, 22, 7, 0, 0, 0, 111, 64, 116, -43, 83,
            -9, -50, -39, 23, 97, 98, 99, 95, 49, 50, 51, 0, 0, 0, 25},
            Collections.singletonList(0),
            new byte[] {1, 1, 1, 0, 0, 0, 26, 104, 36, 32, -91, 22, 1, 0, 0, 0, 111}},
        // TODO disabling for now, should be back when we use Manish optimizations
        /*
         * {new byte[] {0, 0, 0, 1, 0, 0, 0, 32, 0, 0, 4, 75, 2, -114, -42, 6, 0, 0, 0, 0, 0, 0, 0,
         * 8, 0, 0, 0, 4, 83, 116, 114, 105, 110, 103, 95, 49, 0, 0, 0, 11},
         * Collections.singletonList(1), new byte[] {0, 0, 0, 1, 0, 0, 0, 32, 0, 0, 4, 75, 2, -114,
         * -42, 6, 0, 0, 0, 8, 83, 116, 114, 105, 110, 103, 95, 49}}, {new byte[] {0, 0, 0, 1, 0, 0,
         * 0, 32, 0, 0, 4, 75, 2, -114, -42, 6, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 4, 83, 116, 114,
         * 105, 110, 103, 95, 49, 0, 0, 0, 11}, Arrays.asList(0, 1), new byte[] {0, 0, 0, 1, 0, 0,
         * 0, 32, 0, 0, 4, 75, 2, -114, -42, 6, 0, 0, 0, 0, 0, 0, 0, 8, 83, 116, 114, 105, 110, 103,
         * 95, 49}}, {new byte[] {0, 0, 0, 1, 0, 0, 0, 40, 0, 0, 4, 75, 34, -74, 97, 74, 0, 0, 0, 8,
         * 0, 0, 0, 8, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 83, 116, 114, 105, 110, 103, 95, 53, 0,
         * 0, 0, 55}, Collections.singletonList(2), new byte[] {0, 0, 0, 1, 0, 0, 0, 40, 0, 0, 4,
         * 75, 34, -74, 97, 74, 0, 0, 0, 4, 0, 0, 0, 55}},
         */};
  }

  @Test
  @Parameters(method = "dataValueWriter")
  public void testValueWriter(byte[] rowBytes, List<Integer> selectedColumns,
      byte[] expectedRowBytes) throws IOException, ClassNotFoundException, InterruptedException {
    MTableDescriptor td = new MTableDescriptor();
    td.setSchema(
        new Schema(new String[] {"c_1", "c_2", "c_3"}, new DataType[] {INT, STRING, DOUBLE}));
    Scan scan = new Scan();
    scan.setColumns(selectedColumns);
    ScanContext sc = new ScanContext(null, null, scan, td, null, null);

    final boolean hasKeys = (boolean) TypeUtils.getRandomValue(BasicTypes.BOOLEAN);
    final byte[] dKey = hasKeys ? key : null;

    /* create part-list and add the row */
    final MPartList values = new MPartList(1, hasKeys, sc);
    addRow(values, scan, td, key, rowBytes);

    /* serialize the part-list via respective value-writer and then deserialize */
    final MPartList readValues = writeAndRead(values);

    assertEquals("Incorrect size of part-list.", 1, readValues.size());
    assertArrayEquals("Incorrect key received on client side.", dKey,
        (byte[]) readValues.getKey(0));
    assertArrayEquals("Incorrect value received on client side.", expectedRowBytes,
        (byte[]) readValues.getValue(0));
  }
  // data:
  // [111, abc_123, 333.333]
  // data-bytes-ordered:
  // [1, 1, 1, 0, 0, 0, 26, 104, 36, 32, -91, 22, 7, 0, 0, 0, 111, 64, 116, -43, 83, -9, -50, -39,
  // 23, 97, 98, 99, 95, 49, 50, 51, 0, 0, 0, 25]

  private static final String[] COLUMNS_F = {"c_1", "c_2", "c_3", "c_4", "__INSERTION_TIMESTAMP__"};
  private static final DataType[] TYPES_F = {INT, STRING, DOUBLE, STRING, LONG};
  private static final Object[] VALUES_F = new Object[] {111, "abc_123", 333.333, "efg", 99999L};
  private static final String[] COLUMNS = {"c_1", "c_2", "c_3", "c_4", MTableUtils.KEY_COLUMN_NAME};
  private static final DataType[] TYPES = {INT, STRING, DOUBLE, STRING, BINARY};
  private static final Object[] VALUES = new Object[] {111, "abc_123", 333.333, "efg", null};

  public static Object[] dataValueWriterWithTypes() {
    return new Object[][] { ////
        {Collections.emptyList(), new Object[] {111, "abc_123", 333.333, "efg"}},
        {Collections.singletonList(0), new Object[] {111}},
        {Collections.singletonList(1), new Object[] {"abc_123"}},
        {Collections.singletonList(2), new Object[] {333.333}},
        {Collections.singletonList(3), new Object[] {"efg"}},
        {Arrays.asList(0, 1), new Object[] {111, "abc_123"}},
        {Arrays.asList(0, 2), new Object[] {111, 333.333}},
        {Arrays.asList(1, 2), new Object[] {"abc_123", 333.333}},
        {Arrays.asList(1, 3), new Object[] {"abc_123", "efg"}},};
  }

  @Test
  @Parameters(method = "dataValueWriterWithTypes")
  public void testValueWriterWithTypes(final List<Integer> columnList,
      final Object[] expectedValues)
      throws IOException, ClassNotFoundException, InterruptedException {
    MTableDescriptor td = new MTableDescriptor();
    td.setSchema(new Schema(COLUMNS, TYPES));
    Scan scan = new Scan();
    scan.setColumns(columnList);
    ScanContext sc = new ScanContext(null, null, scan, td, null, null);

    byte[] bytes = getByteArrayMTable(td, VALUES);
    final MPartList list = new MPartList(1, false, sc);
    addRow(list, scan, td, key, bytes);

    /* serialize the part-list via respective value-writer and then deserialize */
    final byte[] readBytes = (byte[]) writeAndRead(list).getValue(0);

    Row row = ScanUtils.getRowProducer(scan, td).apply(null, readBytes);
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals("Incorrect column value.", expectedValues[i],
          row.getCells().get(i).getColumnValue());
    }
  }

  @Test
  @Parameters(method = "dataValueWriterWithTypes")
  public void testValueWriterWithTypesFTable(final List<Integer> columnList,
      final Object[] expectedValues)
      throws IOException, ClassNotFoundException, InterruptedException {
    FTableDescriptor td = new FTableDescriptor();
    td.setSchema(new Schema(COLUMNS_F, TYPES_F));
    Scan scan = new Scan();
    scan.setColumns(columnList);
    ScanContext sc = new ScanContext(null, null, scan, td, null, null);

    byte[] bytes = getByteArrayFTable(td, VALUES_F);
    final MPartList list = new MPartList(1, false, sc);
    addRow(list, scan, td, key, bytes);

    /* serialize the part-list via respective value-writer and then deserialize */
    final byte[] readBytes = (byte[]) writeAndRead(list).getValue(0);
    Row row = ScanUtils.getRowProducer(scan, td).apply(null, readBytes);
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals("Incorrect column value.", expectedValues[i],
          row.getCells().get(i).getColumnValue());
    }
  }

  /**
   * Basic test that verifies write and read with columns via MPartList works as expected with
   * de-serialized rows similar to serialized rows.
   *
   * @param columnList the list of column ids
   * @param expectedValues the list of expected values
   * @throws IOException if not able to write the serialized object
   * @throws ClassNotFoundException if the object class was not found
   * @throws InterruptedException the the executing thread was interrupted
   */
  @Test
  @Parameters(method = "dataValueWriterWithTypes")
  public void testValueWriterWithTypesDesRow(final List<Integer> columnList,
      final Object[] expectedValues)
      throws IOException, ClassNotFoundException, InterruptedException {
    FTableDescriptor td = new FTableDescriptor();
    td.setSchema(new Schema(COLUMNS_F, TYPES_F));
    Scan scan = new Scan();
    scan.setColumns(columnList);
    ScanContext sc = new ScanContext(null, null, scan, td, null, null);

    final MPartList list = new MPartList(1, false, sc);
    final StoreRecord record = new StoreRecord(td.getNumOfColumns());
    if (columnList.isEmpty()) {
      System.arraycopy(expectedValues, 0, record.getValues(), 0, expectedValues.length);
    } else {
      for (int i = 0; i < expectedValues.length; i++) {
        record.setValue(columnList.get(i), expectedValues[i]);
      }
    }

    list.addPart(new byte[0], record, DES_ROW, null);

    /* serialize the part-list via respective value-writer and then deserialize */
    final byte[] readBytes = (byte[]) writeAndRead(list).getValue(0);
    Row row = ScanUtils.getRowProducer(scan, td).apply(null, readBytes);
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals("Incorrect column value.", expectedValues[i], row.getValue(i));
    }
  }

  private Row getRow(final byte[] bytes, final TableDescriptor td, final List<Integer> columns)
      throws IOException, ClassNotFoundException, InterruptedException {
    Scan scan = new Scan();
    scan.setColumns(columns);
    ScanContext sc = new ScanContext(null, null, scan, td, null, null);

    final MPartList list = new MPartList(1, false, sc);
    addRow(list, scan, td, key, bytes);

    /* serialize the part-list via respective value-writer and then deserialize */
    final byte[] readBytes = (byte[]) writeAndRead(list).getValue(0);

    return ScanUtils.getRowProducer(scan, td).apply(key, readBytes);
  }

  @Test
  public void testValueWriterWithNullValues_MTable()
      throws InterruptedException, IOException, ClassNotFoundException {
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    /* create table with following schema */
    final Object[][] columns = new Object[][] {{"c_1", BasicTypes.LONG}, {"c_2", BasicTypes.STRING},
        {"c_3", BasicTypes.INT}};
    final String[] names = {"c_1", "c_2", "c_3", "__ROW__KEY__COLUMN__"};
    final DataType[] types = {BasicTypes.LONG, BasicTypes.STRING, BasicTypes.INT, BINARY};
    td.setSchema(new Schema(names, types));
    final Object[] expectedValues = {null, "String_1", 11, null};

    final byte[] bytes = getByteArrayMTable(td, expectedValues);

    /* get columns one-by-one */
    assertEquals("Incorrect column value.", expectedValues[0],
        getRow(bytes, td, Collections.singletonList(0)).getCells().get(0).getColumnValue());

    assertEquals("Incorrect column value.", expectedValues[1],
        getRow(bytes, td, Collections.singletonList(1)).getCells().get(0).getColumnValue());

    assertEquals("Incorrect column value.", expectedValues[2],
        getRow(bytes, td, Collections.singletonList(2)).getCells().get(0).getColumnValue());

    assertArrayEquals("Incorrect column value.", key,
        (byte[]) getRow(bytes, td, Collections.singletonList(3)).getCells().get(0)
            .getColumnValue());

    final List<Cell> cells = getRow(bytes, td, Arrays.asList(0, 1, 2)).getCells();
    assertEquals("Incorrect column value.", expectedValues[0], cells.get(0).getColumnValue());
    assertEquals("Incorrect column value.", expectedValues[2], cells.get(2).getColumnValue());
    assertEquals("Incorrect column value.", expectedValues[1], cells.get(1).getColumnValue());

  }

  @Test
  public void testValueWriterWithNullValues_FTable()
      throws InterruptedException, IOException, ClassNotFoundException {
    FTableDescriptor td = new FTableDescriptor();
    /* create table with following schema */
    final Object[][] columns = new Object[][] {{"c_1", BasicTypes.LONG}, {"c_2", BasicTypes.STRING},
        {"c_3", BasicTypes.INT}};
    final String[] names = {"c_1", "c_2", "c_3", "__ROW__KEY__COLUMN__"};
    final DataType[] types = {BasicTypes.LONG, BasicTypes.STRING, BasicTypes.INT, BINARY};
    td.setSchema(new Schema(names, types));
    final Object[] expectedValues = {null, "String_1", 11, null};

    final byte[] bytes = getByteArrayFTable(td, expectedValues);

    /* get columns one-by-one */
    assertEquals("Incorrect column value.", 0L,
        getRow(bytes, td, Collections.singletonList(0)).getCells().get(0).getColumnValue());

    assertEquals("Incorrect column value.", expectedValues[1],
        getRow(bytes, td, Collections.singletonList(1)).getCells().get(0).getColumnValue());

    assertEquals("Incorrect column value.", expectedValues[2],
        getRow(bytes, td, Collections.singletonList(2)).getCells().get(0).getColumnValue());

    assertArrayEquals("Incorrect column value.", null,
        (byte[]) getRow(bytes, td, Collections.singletonList(3)).getCells().get(0)
            .getColumnValue());

    final List<Cell> cells = getRow(bytes, td, Arrays.asList(0, 1, 2)).getCells();
    assertEquals("Incorrect column value.", 0L, cells.get(0).getColumnValue());
    assertEquals("Incorrect column value.", expectedValues[2], cells.get(2).getColumnValue());
    assertEquals("Incorrect column value.", expectedValues[1], cells.get(1).getColumnValue());
  }

  /**
   * Generate the actual byte-array for the specified descriptor and values, if any. If values were
   * not provided then random values, for the respective types, will be used.
   *
   * @param td the table descriptor
   * @param values the column values
   * @return the byte-array representation of the row
   */
  private static byte[] getByteArrayMTable(final MTableDescriptor td, Object[] values) {
    Put put = new Put("key");
    boolean useRandomValue = false;
    if (values == null) {
      values = new Object[td.getColumnDescriptors().size()];
      useRandomValue = true;
    }
    int i = 0;
    for (MColumnDescriptor cd : td.getColumnDescriptors()) {
      put.addColumn(cd.getColumnNameAsString(),
          useRandomValue ? TypeUtils.getRandomValue(cd.getColumnType()) : values[i]);
      i++;
    }
    final MValue val = MValue.fromPut(td, put, MOperation.PUT);
    final byte[] f = td.getStorageFormatterIdentifiers();
    MTableStorageFormatter fmt = new MTableStorageFormatter(f[0], f[1], f[2]);
    return (byte[]) fmt.performPutOperation(td, val, MOpInfo.fromValue(val), null);
  }

  /**
   * Generate the actual byte-array for the specified descriptor and values, if any. If values were
   * not provided then random values, for the respective types, will be used.
   *
   * @param td the table descriptor
   * @param values the column values
   * @return the byte-array representation of the row
   */
  private static byte[] getByteArrayFTable(final FTableDescriptor td, Object[] values) {
    Record rec = new Record();
    boolean useRandomValue = false;
    if (values == null) {
      values = new Object[td.getColumnDescriptors().size()];
      Arrays.fill(values, 0);
      useRandomValue = true;
    }
    int i = 0;
    for (MColumnDescriptor cd : td.getColumnDescriptors()) {
      if (values[i] != null)
        rec.add(cd.getColumnNameAsString(),
            useRandomValue ? TypeUtils.getRandomValue(cd.getColumnType()) : values[i]);
      i++;
    }
    return FTableByteUtils.fromRecord(td, rec);
  }

  /**
   * Basic test that verifies write and read with columns via MPartList works as expected for
   * MTable.
   *
   * @throws InterruptedException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @Test
  public void testWriteMTable() throws InterruptedException, IOException, ClassNotFoundException {
    Schema schema =
        new Schema(new String[] {"c_1", "c_2", "c_3", "c_4", "c_5", "c_6", "c_7", "c_8"});
    MTableDescriptor td = new MTableDescriptor();
    td.setSchema(schema);

    /*
     * Sample byte-array: [1, 1, 1, 0, 0, 0, 13, -26, -118, -63, -120, -31, -1, 37, 113, -52, -86,
     * 50, 106, -67, 41, -63, -70, 15, 53, 120, -65, -72, 85, 76, 25, -42, 101, -115, 60, 105, 0, 0,
     * 0, 35, 0, 0, 0, 31, 0, 0, 0, 28, 0, 0, 0, 27, 0, 0, 0, 26, 0, 0, 0, 19, 0, 0, 0, 18, 0, 0, 0,
     * 13]
     */
    byte[] v0 = getByteArrayMTable(td, null);

    Scan scan = new Scan();
    scan.setColumns(Arrays.asList(0, 1, 2, 3));
    ScanContext sc = new ScanContext(null, null, scan, td, null, null);

    final int count = 100;
    final int size = 100;
    final MPartList values = new MPartList(size + 1, false, sc);
    InternalRow[] rows = new InternalRow[size];
    Function3<byte[], Object, Encoding, InternalRow> rp = ScanUtils.serverRowProducer(scan, td);
    for (int i = 0; i < size; i++) {
      rows[i] = rp.apply(null, null, td.getEncoding());
    }

    MPartList readValues = null;
    long l1 = System.nanoTime();
    for (int i = 0; i < count; i++) {
      for (int j = 0; j < size; j++) {
        rows[j].reset(key, v0, td.getEncoding(), null);
        values.addPart(null, rows[j], ROW, null);
      }
      readValues = writeAndRead(values);
      values.clear();
    }
    System.out.println("### Time= " + (System.nanoTime() - l1) / 1_000_000_000.0);
    final byte[] readValueBytes = (byte[]) readValues.getValue(0);
    final int len = Bytes.toInt(readValueBytes, 13); /* skip header till column bits */
    assertTrue("Incorrect value read", Bytes.equals(v0, 13, len, readValueBytes, 13 + 4, len));
  }

  @Test
  public void testWriteMTableWithNullValuesInBatch_1()
      throws InterruptedException, IOException, ClassNotFoundException {
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    Schema schema = new Schema(new String[] {"c_int", "c_string", "c_int1"},
        new DataType[] {BasicTypes.INT, BasicTypes.STRING, BasicTypes.INT});
    td.setSchema(schema);

    final String val = "01234";
    byte[] v0 = {2, 1, 1, 0, 0, 0, 30, -14, -113, -14, 121, -125, 7, 0, 0, 0, 1, 0, 0, 0, 35, 48,
        49, 50, 51, 52, 0, 0, 0, 21};
    byte[] v1 = {2, 1, 1, 0, 0, 0, 30, -14, -113, -14, 121, -125, 5, 0, 0, 0, 1, 0, 0, 0, 35};

    Scan scan = new Scan();
    scan.setColumns(Collections.singletonList(1));
    ScanContext sc = new ScanContext(null, null, scan, td, null, null);

    final int count = 1;
    final int size = 2;
    final MPartList values = new MPartList(size + 1, false, sc);

    MPartList readValues = null;
    for (int i = 0; i < count; i++) {
      addRow(values, scan, td, null, v0);
      addRow(values, scan, td, null, v1);
      readValues = writeAndRead(values);
      values.clear();
    }

    Row row = ScanUtils.getRowProducer(scan, td).apply(key, readValues.getValue(0));
    assertEquals("Incorrect column value for: name", val, row.getCells().get(0).getColumnValue());
    row = ScanUtils.getRowProducer(scan, td).apply(key, readValues.getValue(1));
    assertEquals("Incorrect column value for: name", null, row.getCells().get(0).getColumnValue());
  }

  @Test
  public void testWriteMTableWithNullValuesInBatch_Row()
      throws InterruptedException, IOException, ClassNotFoundException {
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    Schema schema = new Schema(new String[] {"c_int", "c_string", "c_int1"},
        new DataType[] {BasicTypes.INT, BasicTypes.STRING, BasicTypes.INT});
    td.setSchema(schema);

    final String val = "01234";
    byte[] v0 = {2, 1, 1, 0, 0, 0, 30, -14, -113, -14, 121, -125, 7, 0, 0, 0, 1, 0, 0, 0, 35, 48,
        49, 50, 51, 52, 0, 0, 0, 21};
    byte[] v1 = {2, 1, 1, 0, 0, 0, 30, -14, -113, -14, 121, -125, 5, 0, 0, 0, 1, 0, 0, 0, 35};
    byte[] v2 = {2, 1, 1, 0, 0, 0, 30, -14, -113, -14, 121, -125, 7, 0, 0, 0, 1, 0, 0, 0, 35, 48,
        49, 50, 51, 52, 0, 0, 0, 21};

    Scan scan = new Scan();
    scan.setColumns(Collections.singletonList(1));
    ScanContext sc = new ScanContext(null, null, scan, td, null, null);

    final int count = 1;
    final int size = 3;
    final MPartList values = new MPartList(size + 1, false, sc);

    MPartList readValues = null;
    for (int i = 0; i < count; i++) {
      addRow(values, scan, td, null, v0);
      addRow(values, scan, td, null, v1);
      addRow(values, scan, td, null, v2);
      readValues = writeAndRead(values);
      values.clear();
    }

    Row row = ScanUtils.getRowProducer(scan, td).apply(key, readValues.getValue(0));
    assertEquals("Incorrect column value for: name", val, row.getCells().get(0).getColumnValue());
    row = ScanUtils.getRowProducer(scan, td).apply(key, readValues.getValue(1));
    assertEquals("Incorrect column value for: name", null, row.getCells().get(0).getColumnValue());
    row = ScanUtils.getRowProducer(scan, td).apply(key, readValues.getValue(2));
    assertEquals("Incorrect column value for: name", val, row.getCells().get(0).getColumnValue());
  }

  @Test
  public void testWriteMTableWithNullValuesInBatch_2()
      throws InterruptedException, IOException, ClassNotFoundException {
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    Schema schema = new Schema(new String[] {"c_int", "c_string", "c_int1"},
        new DataType[] {BasicTypes.INT, BasicTypes.STRING, BasicTypes.INT});
    td.setSchema(schema);

    final int val = 1;
    byte[] v0 = {2, 1, 1, 0, 0, 0, 30, -14, -113, -14, 121, -125, 7, 0, 0, 0, 1, 0, 0, 0, 35, 48,
        49, 50, 51, 52, 0, 0, 0, 21};
    byte[] v1 = {2, 1, 1, 0, 0, 0, 30, -14, -113, -14, 121, -125, 6, 0, 0, 0, 35, 48, 49, 50, 51,
        52, 0, 0, 0, 17};
    byte[] v2 = {2, 1, 1, 0, 0, 0, 30, -14, -113, -14, 121, -125, 7, 0, 0, 0, 1, 0, 0, 0, 35, 48,
        49, 50, 51, 52, 0, 0, 0, 21};

    Scan scan = new Scan();
    scan.setColumns(Collections.singletonList(0));
    ScanContext sc = new ScanContext(null, null, scan, td, null, null);

    final int count = 1;
    final int size = 3;
    final MPartList values = new MPartList(size + 1, false, sc);

    MPartList readValues = null;
    for (int i = 0; i < count; i++) {
      addRow(values, scan, td, null, v0);
      addRow(values, scan, td, null, v1);
      addRow(values, scan, td, null, v2);
      readValues = writeAndRead(values);
      values.clear();
    }

    Row row = ScanUtils.getRowProducer(scan, td).apply(key, readValues.getValue(0));
    assertEquals("Incorrect column value for: name", val, row.getCells().get(0).getColumnValue());
    row = ScanUtils.getRowProducer(scan, td).apply(key, readValues.getValue(1));
    assertEquals("Incorrect column value for: name", null, row.getCells().get(0).getColumnValue());
    row = ScanUtils.getRowProducer(scan, td).apply(key, readValues.getValue(2));
    assertEquals("Incorrect column value for: name", val, row.getCells().get(0).getColumnValue());
  }

  @Test
  public void testWriteMTableWithMultipleByteBitMap()
      throws InterruptedException, IOException, ClassNotFoundException {
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    Schema.Builder sb = new Schema.Builder();
    final int columnCount = 10;
    Object[] v = new Object[columnCount];
    for (int i = 0; i < columnCount; i++) {
      sb.column("c_" + i, BasicTypes.BYTE);
      v[i] = (byte) i;
    }
    td.setSchema(sb.build());

    byte[] v0 = MPartListTest.getByteArrayMTable(td, v);

    assertEquals("Incorrect column value for: c_0", v[0],
        getRow(v0, td, Collections.singletonList(0)).getCells().get(0).getColumnValue());
    assertEquals("Incorrect column value for: c_2", v[2],
        getRow(v0, td, Collections.singletonList(2)).getCells().get(0).getColumnValue());
    assertEquals("Incorrect column value for: c_9", v[9],
        getRow(v0, td, Collections.singletonList(9)).getCells().get(0).getColumnValue());

    Row row = getRow(v0, td, Arrays.asList(0, 9));
    assertEquals("Incorrect column value for: c_0", v[0], row.getCells().get(0).getColumnValue());
    assertEquals("Incorrect column value for: c_9", v[9], row.getCells().get(1).getColumnValue());
  }

  /**
   * Basic test that verifies write and read with columns via MPartList works as expected for
   * FTable.
   *
   * @throws InterruptedException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @Test
  public void testWriteFTable() throws InterruptedException, IOException, ClassNotFoundException {
    final String[] cs = {"c_1", "c_2", "c_3", "c_4", "c_5", "c_6", "c_7", "c_8"};
    Schema.Builder sb = new Schema.Builder();
    for (String c : cs) {
      sb.column(c);
    }
    sb.column("__INSERTION_TIMESTAMP__", LONG);
    FTableDescriptor td = new FTableDescriptor();
    td.setSchema(sb.build());

    /**
     * sample byte-array: [58, 127, 90, -103, 35, 67, -40, 7, -71, 121, 91, -104, -67, 17, 70, 65,
     * 40, -29, -46, -55, 79, 31, 34, -101, 66, 123, 43, -89, 0, 0, 0, 23, 0, 0, 0, 22, 0, 0, 0, 22,
     * 0, 0, 0, 18, 0, 0, 0, 18, 0, 0, 0, 17, 0, 0, 0, 13, 0, 0, 0, 8]
     */
    byte[] v0 = getByteArrayFTable(td, null);

    Scan scan = new Scan();
    scan.setColumns(Arrays.asList(0, 1, 2, 3));
    ScanContext sc = new ScanContext(null, null, scan, td, null, null);

    final int count = 100;
    final int size = 100;
    final MPartList values = new MPartList(size + 1, false, sc);
    InternalRow[] rows = new InternalRow[size];
    Function3<byte[], Object, Encoding, InternalRow> rp = ScanUtils.serverRowProducer(scan, td);
    for (int i = 0; i < size; i++) {
      rows[i] = rp.apply(null, null, td.getEncoding());
    }

    MPartList readValues = null;
    long l1 = System.nanoTime();
    for (int i = 0; i < count; i++) {
      for (int j = 0; j < size; j++) {
        rows[j].reset(null, v0, td.getEncoding(), null);
        values.addPart(null, rows[j], ROW, null);
      }
      readValues = writeAndRead(values);
      values.clear();
    }
    System.out.println("### Time= " + (System.nanoTime() - l1) / 1_000_000_000.0);
    final byte[] readValueBytes = (byte[]) readValues.getValue(0);
    int len = Bytes.toInt(readValueBytes);
    assertTrue("Incorrect value read", Bytes.equals(v0, 8, len, readValueBytes, 4, len));
  }

  /**
   * Convert the byte array to an InternalRow and then write to the specified part-list. -- Much
   * expensive; it creates a new row for each call.. to be used only from tests.
   *
   * @param list the part list to be written to
   * @param scan the scan object
   * @param td the table descriptor
   * @param k the key
   * @param v the value byte-array
   */
  private void addRow(final MPartList list, final Scan scan, final TableDescriptor td,
      final byte[] k, final byte[] v) {
    list.addPart(k, ScanUtils.serverRowProducer(scan, td).apply(k, v, td.getEncoding()), ROW, null);
  }
}
