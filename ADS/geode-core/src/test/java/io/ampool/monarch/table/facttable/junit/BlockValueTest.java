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

import static io.ampool.monarch.table.ftable.FTableDescriptor.BlockFormat.ORC_BYTES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.Encoding;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.DataTypeFactory;
import io.ampool.monarch.types.StructType;
import io.ampool.orc.AColumnStatistics;
import io.ampool.orc.OrcUtils;
import io.ampool.store.StoreRecord;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.test.junit.categories.FTableTest;
import org.apache.orc.ColumnStatistics;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(FTableTest.class)
@RunWith(JUnitParamsRunner.class)
public class BlockValueTest {

  @Test
  public void testBlockValueDelta() throws IOException {

    int size = 4991;

    BlockValue blockValueP = new BlockValue(size);
    BlockValue blockValueS = new BlockValue(size);

    for (int i = 1; i <= size; i++) {

      byte[] value = new byte[i];

      blockValueP.checkAndAddRecord(value);

      if (i == 1) {
        blockValueS.checkAndAddRecord(value);
      }

      if (blockValueP.hasDelta()) {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        blockValueP.toDelta(dos);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));

        blockValueS.fromDelta(dis);

        assertEquals(i, blockValueP.getCurrentIndex());
        assertEquals(blockValueP.getCurrentIndex(), blockValueS.getCurrentIndex());

        assertArrayEquals(value, blockValueP.getRecord(i - 1));
        assertArrayEquals(value, blockValueS.getRecord(i - 1));

      }
    }
    assertEquals(size, blockValueP.getCurrentIndex());
    assertEquals(size, blockValueS.getCurrentIndex());
  }

  @Test
  public void testBlockValuePDelta() throws IOException {

    int size = 4991;

    BlockValue blockValueM = new BlockValue(size);
    BlockValue blockValueD = new BlockValue(size);

    for (int i = 1; i <= size; i++) {

      byte[] value = new byte[i];



      if (i == 1) {
        blockValueM.checkAndAddRecord(value);
        blockValueD.checkAndAddRecord(value);
        blockValueM.resetPersistenceDelta();
      } else {
        blockValueM.checkAndAddRecord(value);
      }

      if (blockValueM.hasPersistenceDelta()) {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        blockValueM.toPersistenceDelta(dos);
        blockValueM.resetPersistenceDelta();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));

        blockValueD.fromPersistenceDelta(dis);

        assertEquals(i, blockValueM.getCurrentIndex());
        assertEquals(blockValueM.getCurrentIndex(), blockValueD.getCurrentIndex());

        assertArrayEquals(value, blockValueM.getRecord(i - 1));
        assertArrayEquals(value, blockValueD.getRecord(i - 1));

      }
    }
    assertEquals(size, blockValueM.getCurrentIndex());
    assertEquals(size, blockValueD.getCurrentIndex());
  }

  private static Object getValue(final int rowId, final int columnId) {
    return columnId == 0 ? rowId : columnId == 1 ? 10_000L + rowId : "String_" + rowId;
  }

  private static final int MAX_STRIDE_SIZE = 1111;
  private static final FTableDescriptor td = new FTableDescriptor();
  private static final BlockValue BLOCK_VALUE = setupBlockValue();

  private static BlockValue setupBlockValue() {
    final StructType aSchema =
        (StructType) DataTypeFactory.getTypeFromString("struct<f1:INT,f2:LONG,f3:STRING>");
    final Properties props = new Properties();
    props.setProperty("orc.row.index.stride", String.valueOf(MAX_STRIDE_SIZE));

    td.setBlockFormat(ORC_BYTES).setBlockProperties(props)
        .setSchema(new Schema(aSchema.getColumnNames(), aSchema.getColumnTypes()));

    final int totalCount = 5_000;
    final StoreRecord row = new StoreRecord(td.getNumOfColumns());

    final BlockValue bv = new BlockValue(totalCount);
    final Encoding enc = td.getEncoding();

    for (int i = 0; i < totalCount; i++) {
      for (int j = 0; j < td.getNumOfColumns(); j++) {
        row.setValue(j, getValue(i, j));
      }
      bv.checkAndAddRecord(enc.serializeValue(td, row));
    }
    bv.close(td);

    return bv;
  }

  public static Object[] dataOrcFormatCloseAndIterator() {
    return new Object[][] {
        {MAX_STRIDE_SIZE,
            new FilterList(FilterList.Operator.MUST_PASS_ALL)
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.GREATER, 0))
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.LESS, 10))},
        {MAX_STRIDE_SIZE,
            new FilterList(FilterList.Operator.MUST_PASS_ALL)
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.GREATER, 1500))
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.LESS, 1600))},
        {5_000 - (4 * MAX_STRIDE_SIZE),
            new FilterList(FilterList.Operator.MUST_PASS_ALL)
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.GREATER, 4500))
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.LESS, 4700))},};
  }

  /**
   * Assert that the specified row-index-stride-size is honoured at the time of closing blocks. The
   * subsequent scan would retrieve all rows from the respective row-groups with matching filters.
   *
   * @param expectedCount expected row count
   * @param filter filters to be used when iterating the block
   */
  @Test
  @Parameters(method = "dataOrcFormatCloseAndIterator")
  public void testOrcFormatCloseAndIterator(final int expectedCount, final Filter filter) {
    final OrcUtils.OrcOptions orcOptions = new OrcUtils.OrcOptions(filter, td);
    final Iterator<Object> itr = BLOCK_VALUE.iterator(orcOptions);
    int count = 0;
    while (itr.hasNext()) {
      final Object next = itr.next();
      count++;
    }
    assertEquals("Incorrect number of records retrieved.", expectedCount, count);
  }

  @Test
  public void testWriteAndReadWithNullValues() throws IOException {
    final int expectedCount = 10;
    final StoreRecord sr = new StoreRecord(td.getNumOfColumns());
    final BlockValue bv = new BlockValue(expectedCount);
    for (int rId = 0; rId < expectedCount; rId++) {
      if (rId % 3 == 0) {
        for (int i = 0; i < td.getNumOfColumns(); i++) {
          sr.setValue(i, null);
        }
      } else {
        for (int cId = 0; cId < td.getNumOfColumns(); cId++) {
          sr.setValue(cId, getValue(rId, cId));
        }
      }
      bv.checkAndAddRecord(td.getEncoding().serializeValue(td, sr), td);
    }
    bv.close(td);

    final Iterator<Object> itr = bv.iterator();
    int count = 0;
    while (itr.hasNext()) {
      final Object next = itr.next();
      count++;
    }
    assertEquals("Incorrect number of rows retrieved from scan.", expectedCount, count);
  }

  public static Object[] dataWriteAndReadWithColumnStatistics() {
    return new Object[][] {{false, new SingleColumnValueFilter("f1", CompareOp.EQUAL, 12)},
        {true, new SingleColumnValueFilter("f1", CompareOp.EQUAL, 0)},
        {true, new SingleColumnValueFilter("f1", CompareOp.EQUAL, 9)},
        {true,
            new FilterList(FilterList.Operator.MUST_PASS_ALL)
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.GREATER, 0))
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.LESS, 10))},
        {true,
            new FilterList(FilterList.Operator.MUST_PASS_ONE)
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.GREATER, 0))
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.LESS, -100))},
        {true,
            new FilterList(FilterList.Operator.MUST_PASS_ALL)
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.GREATER, 0))
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.LESS, 16))},
        {false,
            new FilterList(FilterList.Operator.MUST_PASS_ALL)
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.GREATER, 45))
                .addFilter(new SingleColumnValueFilter("f1", CompareOp.LESS, 47))},};
  }

  @Test
  @Parameters(method = "dataWriteAndReadWithColumnStatistics")
  public void testWriteAndReadWithColumnStatistics(final boolean expected, final Filter filter)
      throws IOException {
    final int count = 10;
    final StoreRecord sr = new StoreRecord(td.getNumOfColumns());
    final BlockValue bv = new BlockValue(count);
    for (int rId = 0; rId < count; rId++) {
      for (int cId = 0; cId < td.getNumOfColumns(); cId++) {
        sr.setValue(cId, getValue(rId, cId));
      }
      bv.checkAndAddRecord(td.getEncoding().serializeValue(td, sr), td);
    }

    final boolean needed = OrcUtils.isBlockNeeded(new OrcUtils.OrcOptions(filter, td), bv);
    assertEquals("Incorrect status for Block using filters.", expected, needed);
  }

  private static final Schema SCHEMA_CS =
      Schema.fromString("struct<f1:INT,f2:LONG,f3:STRING,__INSERTION_TIMESTAMP__:LONG>");

  @Test
  public void testMergeBlockValuesWithColumnStatistics() {
    final Object[] values1 = new Object[] {11, 1111L, "String_1", 0L};
    final Object[] values2 = new Object[] {22, 222L, "String_2", 0L};
    final String[] expected1 = new String[] {"11", "1111", "String_1", "0"};
    final String[] expected2 = new String[] {"22", "222", "String_2", "0"};
    final String[] expectedMin = new String[] {"11", "222", "String_1", "0"};
    final String[] expectedMax = new String[] {"22", "1111", "String_2", "0"};

    FTableDescriptor td = new FTableDescriptor();
    td.setSchema(SCHEMA_CS);

    final Record record1 = new Record();
    final Record record2 = new Record();
    for (int i = 0; i < td.getNumOfColumns(); i++) {
      record1.add(td.getColumnDescriptorByIndex(i).getColumnName(), values1[i]);
      record2.add(td.getColumnDescriptorByIndex(i).getColumnName(), values2[i]);
    }

    final BlockValue temp = new BlockValue(1);
    temp.addAndUpdateStats(record1, td);
    assertColumnStatistics(expected1, expected1, temp.getColumnStatistics());

    final BlockValue bv = new BlockValue(1);
    bv.addAndUpdateStats(record2, td);
    assertColumnStatistics(expected2, expected2, bv.getColumnStatistics());

    bv.checkAndAddRecord(temp, td);
    assertColumnStatistics(expectedMin, expectedMax, bv.getColumnStatistics());
  }

  @Test
  public void testMergeBlockValuesUsingDelta() throws IOException, ClassNotFoundException {
    final Object[] values1 = new Object[] {11, 1111L, "String_1", 0L};
    final Object[] values2 = new Object[] {22, 222L, "String_2", 0L};
    final String[] expected1 = new String[] {"11", "1111", "String_1", "0"};
    final String[] expectedMin = new String[] {"11", "222", "String_1", "0"};
    final String[] expectedMax = new String[] {"22", "1111", "String_2", "0"};

    FTableDescriptor td = new FTableDescriptor();
    td.setSchema(SCHEMA_CS);
    td.setColumnStatisticsEnabled(true);

    final Record record1 = new Record();
    final Record record2 = new Record();
    for (int i = 0; i < td.getNumOfColumns(); i++) {
      record1.add(td.getColumnDescriptorByIndex(i).getColumnName(), values1[i]);
      record2.add(td.getColumnDescriptorByIndex(i).getColumnName(), values2[i]);
    }

    final BlockValue temp = new BlockValue(1);
    temp.checkAndAddRecord(td.getEncoding().serializeValue(td, record1), td);
    temp.checkAndAddRecord(td.getEncoding().serializeValue(td, record1), td);
    assertColumnStatistics(expected1, expected1, temp.getColumnStatistics());

    final HeapDataOutputStream out = new HeapDataOutputStream(1024, null);
    DataSerializer.writeObject(temp, out);

    final BlockValue newBV = BlockValue.fromBytes(out.toByteArray());
    assertColumnStatistics(expected1, expected1, newBV.getColumnStatistics());

    temp.resetDelta();
    temp.checkAndAddRecord(td.getEncoding().serializeValue(td, record2), td);
    temp.checkAndAddRecord(td.getEncoding().serializeValue(td, record2), td);
    assertColumnStatistics(expectedMin, expectedMax, temp.getColumnStatistics());
    out.reset();
    if (temp.hasDelta()) {
      temp.toDelta(out);
    }

    final byte[] bytes = out.toByteArray();
    out.close();
    assertNotEquals("Expected delta of size must be > 0.", 0, bytes.length);

    newBV.fromDelta(new DataInputStream(new ByteArrayInputStream(bytes)));
    assertEquals("Incorrect number of records.", temp.size(), newBV.size());
    assertColumnStatistics(expectedMin, expectedMax, newBV.getColumnStatistics());
  }

  private void assertColumnStatistics(final String[] expectedMin, final String[] expectedMax,
      final AColumnStatistics acs) {
    for (int i = 0; i < td.getNumOfColumns(); i++) {
      final ColumnStatistics stats = acs.getColumnStatistics(i);
      assertEquals("Incorrect min value for columnId= " + i, expectedMin[i],
          AColumnStatistics.get(stats, "getMinimum"));
      assertEquals("Incorrect max value for columnId= " + i, expectedMax[i],
          AColumnStatistics.get(stats, "getMaximum"));
    }
  }
}
