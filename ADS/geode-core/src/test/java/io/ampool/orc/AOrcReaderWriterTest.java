/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.ampool.orc;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Date;
import java.util.*;
import java.util.function.BiFunction;

import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.types.*;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.commons.io.FileUtils;
import org.apache.geode.test.junit.categories.FTableTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


@Category(FTableTest.class)
@RunWith(JUnitParamsRunner.class)
public class AOrcReaderWriterTest {
  private static final String schema =
      "struct<f1:int,f2:bigint,f3:array<double>,f4:string,f5:binary>";
  private static final SettableStructObjectInspector ROW_OI;
  private static final SettableStructObjectInspector ROW_OI_R;

  private static final int SIZE = 10;
  private static final Object[][] VALUES = new Object[SIZE][];
  private static final Object[][] VALUES_R = new Object[SIZE][];

  private static final int ALL_STRIDES_SIZE = 5_000;
  private static final int MAX_STRIDE_SIZE = 1_000;
  public static final String DUMMY_ = "_dummy_";
  private static byte[] ORC_BYTES_WITH_MULTIPLE_STRIDES = null;
  private static final BiFunction<Integer, Integer, Object> GET_VALUE = (rId, cId) -> {
    Object ret = VALUES[rId % SIZE][cId];
    switch (cId) {
      case 0:
        ret = rId;
        break;
      case 1:
        ret = 10_000L + rId;
        break;
      case 3:
        ret = "String_" + (20_000 + rId);
        break;
    }
    return ret;
  };
  private static final FTableDescriptor TD = new FTableDescriptor();
  private static final StructType aSchema = (StructType) DataTypeFactory
      .getTypeFromString("struct<f1:INT,f2:LONG,f3:array<DOUBLE>,f4:STRING,f5:BINARY>");

  static {
    final TypeInfo ti = TypeInfoUtils.getTypeInfoFromTypeString(schema);
    ROW_OI = (SettableStructObjectInspector) TypeInfoUtils
        .getStandardJavaObjectInspectorFromTypeInfo(ti);
    ROW_OI_R = (SettableStructObjectInspector) OrcStruct.createObjectInspector(ti);
    for (int j = 0; j < SIZE; j++) {
      VALUES[j] = new Object[] {(j), ((j + 1) * 11L),
          Arrays.asList((j * 100 + (0.1)), (j * 100 + (0.2)), (j * 100 + (0.3))), ("String_" + j),
          (new byte[] {0, (byte) j})};
      VALUES_R[j] = new Object[] {new IntWritable(j), new LongWritable((j + 1) * 11L),
          Arrays.asList(new DoubleWritable(j * 100 + (0.1)), new DoubleWritable(j * 100 + (0.2)),
              new DoubleWritable(j * 100 + (0.3))),
          new Text("String_" + j), new BytesWritable(new byte[] {0, (byte) j})};
    }

    TD.setSchema(new Schema(aSchema.getColumnNames(), aSchema.getColumnTypes()));

    /* Create ORC buffer with multiple row-index-strides */
    try {
      final OrcFile.WriterOptions wOpts = OrcFile.writerOptions(new Configuration())
          .rowIndexStride(MAX_STRIDE_SIZE).inspector(ROW_OI);
      AWriter aWriter = OrcUtils.createWriter(wOpts);
      List<? extends StructField> fields = ROW_OI.getAllStructFieldRefs();
      List x = (List) ROW_OI.create();
      for (int rId = 0; rId < ALL_STRIDES_SIZE; rId++) {
        for (int cId = 0; cId < fields.size(); cId++) {
          ROW_OI.setStructFieldData(x, fields.get(cId), GET_VALUE.apply(rId, cId));
        }
        aWriter.addRow(x);
      }
      aWriter.close();
      ORC_BYTES_WITH_MULTIPLE_STRIDES = aWriter.getBytes();
    } catch (Exception e) {
      System.out.println("Failed to create ORC memory buffer for row-index-strides.");
    }
  }

  /**
   * Write the rows using the specified ORC writer.
   *
   * @param writer the ORC writer
   * @throws IOException if not able to write the data
   */
  private static void writeUsingOrcWriter(final Writer writer) throws IOException {
    List<? extends StructField> fields = ROW_OI.getAllStructFieldRefs();
    List x = (List) ROW_OI.create();
    for (int rId = 0; rId < SIZE; rId++) {
      for (int cId = 0; cId < fields.size(); cId++) {
        ROW_OI.setStructFieldData(x, fields.get(cId), VALUES[rId][cId]);
      }
      writer.addRow(x);
    }
    writer.close();
  }

  /**
   * Read the rows from ORC reader and assert that the correct values are retrieved comparing with
   * expected values.
   *
   * @param reader the ORC reader
   * @throws IOException if not able to read the data
   */
  private static void readAndAssertUsingReader(final RecordReader reader) throws IOException {
    int counter = 0;
    final List<? extends StructField> fields = ROW_OI_R.getAllStructFieldRefs();
    Object row = null;
    while (reader.hasNext()) {
      row = reader.next(row);
      for (int i = 0; i < fields.size(); i++) {
        assertEquals("Incorrect value: row= " + counter + ", column= " + i, VALUES_R[counter][i],
            ROW_OI_R.getStructFieldData(row, fields.get(i)));
      }
      counter++;
    }
  }

  private ExprNodeDesc getExprNodeDesc(final TypeInfo colType, final String colName,
      final Object value, final TypeInfo retType, final GenericUDF udf) {
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(retType, udf, new ArrayList<>(2));
    exprNodeDesc.getChildren().add(new ExprNodeColumnDesc(colType, colName, colName, false));
    exprNodeDesc.getChildren().add(new ExprNodeConstantDesc(colType, value));
    return exprNodeDesc;
  }

  /**
   * Write the data using the Ampool ORC writer that writes ORC data to memory and then read the ORC
   * rows using reader from the memory bytes.
   *
   * @throws IOException if not able to write or read the data
   */
  @Test
  public void testWriteOrcToMemoryAndRead() throws IOException {
    final OrcFile.WriterOptions wOpts =
        OrcFile.writerOptions(new Configuration()).inspector(ROW_OI);
    final OrcFile.ReaderOptions rOpts = new OrcFile.ReaderOptions(new Configuration());

    AWriter aWriter = OrcUtils.createWriter(wOpts);
    writeUsingOrcWriter(aWriter);
    final byte[] memOrcBytes = aWriter.getBytes();

    readAndAssertUsingReader(OrcUtils.createReader(memOrcBytes, rOpts).rows());
  }

  /**
   * Convert rows into ORC bytes and write to a file. Then read using ORC file reader and assert the
   * expected values.
   *
   * @throws IOException if not able to write or read the data
   */
  @Test
  public void testWriteOrcBytesAndReadAsFile() throws IOException {
    final File file = new File("/tmp/tmp_1.orc");
    file.deleteOnExit();

    final OrcFile.WriterOptions wOpts =
        OrcFile.writerOptions(new Configuration()).inspector(ROW_OI);
    final OrcFile.ReaderOptions rOpts = new OrcFile.ReaderOptions(new Configuration());

    final AWriter aWriter = OrcUtils.createWriter(wOpts);
    writeUsingOrcWriter(aWriter);
    final byte[] memOrcBytes = aWriter.getBytes();

    FileUtils.writeByteArrayToFile(file, memOrcBytes);

    readAndAssertUsingReader(OrcFile.createReader(new Path(file.toString()), rOpts).rows());
  }

  /**
   * Write ORC file using generic file base ORC writer and then read bytes of the file and read and
   * assert using memory (byte-array) based reader.
   *
   * @throws IOException if not able to write or read the data
   */
  @Test
  public void testWriteFileAndReadAsBytes() throws IOException {
    final File file = new File("/tmp/tmp_2.orc");
    file.deleteOnExit();

    final OrcFile.WriterOptions wOpts =
        OrcFile.writerOptions(new Configuration()).inspector(ROW_OI);
    final OrcFile.ReaderOptions rOpts = new OrcFile.ReaderOptions(new Configuration());

    final Writer writer = OrcFile.createWriter(new Path(file.toString()), wOpts);
    writeUsingOrcWriter(writer);
    final byte[] memOrcBytes = FileUtils.readFileToByteArray(file);

    readAndAssertUsingReader(OrcUtils.createReader(memOrcBytes, rOpts).rows());
  }

  public static Object[] dataWithPredicates() {
    return new Object[][] {{null, ALL_STRIDES_SIZE},
        {new SingleColumnValueFilter("f1", CompareOp.LESS, 2400), 3 * MAX_STRIDE_SIZE},
        {new SingleColumnValueFilter("f2", CompareOp.GREATER_OR_EQUAL, 14999),
            1 * MAX_STRIDE_SIZE},
        {new SingleColumnValueFilter("f2", CompareOp.GREATER_OR_EQUAL, 14999),
            1 * MAX_STRIDE_SIZE},
        {new SingleColumnValueFilter("f4", CompareOp.EQUAL, "String_22222"), 1 * MAX_STRIDE_SIZE},
        {new SingleColumnValueFilter("f4", CompareOp.GREATER_OR_EQUAL, "String_22222"),
            3 * MAX_STRIDE_SIZE},
        {new SingleColumnValueFilter("f2", CompareOp.NOT_EQUAL, 11111), ALL_STRIDES_SIZE},
        {new FilterList(FilterList.Operator.MUST_PASS_ALL)
            .addFilter(new SingleColumnValueFilter("f1", CompareOp.LESS_OR_EQUAL, 2400)).addFilter(
                new SingleColumnValueFilter("f2", CompareOp.GREATER_OR_EQUAL, 12500)),
            1 * MAX_STRIDE_SIZE},
        {new FilterList(FilterList.Operator.MUST_PASS_ALL)
            .addFilter(new SingleColumnValueFilter("f1", CompareOp.LESS_OR_EQUAL, 5000))
            .addFilter(new SingleColumnValueFilter("f2", CompareOp.GREATER_OR_EQUAL, 10000)),
            ALL_STRIDES_SIZE},
        {new FilterList(FilterList.Operator.MUST_PASS_ONE)
            .addFilter(new SingleColumnValueFilter("f1", CompareOp.LESS_OR_EQUAL, 2400))
            .addFilter(new SingleColumnValueFilter("f2", CompareOp.GREATER_OR_EQUAL, 12500)),
            ALL_STRIDES_SIZE},
        {new FilterList(FilterList.Operator.MUST_PASS_ONE)
            .addFilter(new SingleColumnValueFilter("f1", CompareOp.LESS_OR_EQUAL, 1400))
            .addFilter(new SingleColumnValueFilter("f2", CompareOp.GREATER_OR_EQUAL, 13500)),
            4 * MAX_STRIDE_SIZE},};
  }

  /**
   * Test to verify that with predicates (filters) pushed to ORC reader layer, it returns all rows
   * from the respective row groups where the row is supposed to be found. It does not return the
   * specific rows as the statistics is maintained per row-group.
   *
   * @param f the filters to be evaluated
   * @param expectedCount expected number of rows returned by ORC reader
   * @throws IOException in case there was an error reading/writing
   */
  @Test
  @Parameters(method = "dataWithPredicates")
  public void testWithPredicates(final Filter f, final int expectedCount) throws IOException {
    final OrcFile.ReaderOptions rOpts = new OrcFile.ReaderOptions(new Configuration());

    final OrcUtils.OrcOptions orcOptions = new OrcUtils.OrcOptions(f, TD);

    final RecordReader rows = OrcUtils.createReader(ORC_BYTES_WITH_MULTIPLE_STRIDES, rOpts)
        .rowsOptions(orcOptions.getOptions());
    int count = 0;
    Object row = null;
    while (rows.hasNext()) {
      row = rows.next(row);
      count++;
    }
    rows.close();
    assertEquals("Incorrect number of rows retrieved from scan.", expectedCount, count);
  }

  private static final BlockValue BLOCK_VALUE = new BlockValue(1);
  private static final FTableDescriptor BLOCK_VALUE_TD = new FTableDescriptor();
  static {
    BLOCK_VALUE_TD.setSchema(Schema.fromString(
        "struct<f1:INT,f2:LONG,f3:STRING,f4:FLOAT,f5:BOOLEAN,__INSERTION_TIMESTAMP__:LONG>"));
    final Object[] VALUES_1 = new Object[] {11, 1111L, "String_1", 11.11f, false, 0L};
    final Record record1 = new Record();
    for (int i = 0; i < BLOCK_VALUE_TD.getNumOfColumns(); i++) {
      record1.add(BLOCK_VALUE_TD.getColumnDescriptorByIndex(i).getColumnName(), VALUES_1[i]);
    }
    BLOCK_VALUE.addAndUpdateStats(record1, BLOCK_VALUE_TD);
  }

  public static Object[] dataIsBlockNeeded() {
    return new Object[][] { ///
        {null, true}, ////
        {new SingleColumnValueFilter("f1", CompareOp.EQUAL, 11), true},
        {new SingleColumnValueFilter("f1", CompareOp.NOT_EQUAL, 11), false},
        {new SingleColumnValueFilter("f1", CompareOp.LESS, 1), false},
        {new SingleColumnValueFilter("f2", CompareOp.GREATER_OR_EQUAL, 1111L), true},
        {new SingleColumnValueFilter("f2", CompareOp.LESS, 1111L), false},
        {new SingleColumnValueFilter("f3", CompareOp.EQUAL, "String_1"), true},
        {new SingleColumnValueFilter("f3", CompareOp.GREATER, "String_0"), true},
        {new SingleColumnValueFilter("f3", CompareOp.GREATER, "String_2"), false},
        {new SingleColumnValueFilter("f4", CompareOp.EQUAL, 11.11f), true},
        {new SingleColumnValueFilter("f4", CompareOp.NOT_EQUAL, 11.11f), false},
        {new SingleColumnValueFilter("f5", CompareOp.NOT_EQUAL, false), false},
        {new SingleColumnValueFilter("f5", CompareOp.EQUAL, false), true},
        {new SingleColumnValueFilter("f5", CompareOp.EQUAL, true), false}, ///
    };
  }

  @Test
  @Parameters(method = "dataIsBlockNeeded")
  public void testIsBlockNeeded(final Filter f, final boolean expected) {
    assertEquals("Incorrect status for isBlockNeeded.", expected,
        OrcUtils.isBlockNeeded(new OrcUtils.OrcOptions(f, BLOCK_VALUE_TD), BLOCK_VALUE));
  }

  @Test
  public void testConvertOrcBytes() {
    final String str =
        "struct<col_tinyint:BYTE,col_smallint:SHORT,col_int:INT,col_bigint:LONG,col_boolean:BOOLEAN,col_float:FLOAT,col_double:DOUBLE,col_string:STRING,col_int_array:array<INT>,col_string_array:array<STRING>,col_map:map<INT,STRING>,col_struct:struct<id:STRING,name:STRING,val:INT>,col_timestamp:TIMESTAMP,col_binary:BINARY,col_decimal:BIG_DECIMAL(10,5),col_char:CHARS(10),col_varchar:VARCHAR(26),col_date:DATE,__INSERTION_TIMESTAMP__:LONG>";
    final String orcSchema =
        "struct<col_tinyint:tinyint,col_smallint:smallint,col_int:int,col_bigint:bigint,col_boolean:boolean,col_float:float,col_double:double,col_string:string,col_int_array:array<int>,col_string_array:array<string>,col_map:map<int,string>,col_struct:struct<id:string,name:string,val:int>,col_timestamp:timestamp,col_binary:binary,col_decimal:decimal(10,5),col_char:string(10),col_varchar:varchar(26),col_date:date,__INSERTION_TIMESTAMP__:bigint>";
    final FTableDescriptor td = new FTableDescriptor();
    td.setSchema(Schema.fromString(str));
    td.setBlockSize(1);
    td.setBlockFormat(FTableDescriptor.BlockFormat.ORC_BYTES);
    assertEquals("Incorrect ORC schema.", orcSchema, td.getOrcSchema());

    Record record = new Record();
    final Object[][] expectedValues = new Object[2][];
    final BlockValue bv = new BlockValue(1);
    expectedValues[0] = addToRecord(td, record);
    bv.addAndUpdateStats(record, td);
    record.clear();
    expectedValues[1] = addToRecord(td, record);
    bv.addAndUpdateStats(record, td);
    bv.close(td);

    final Iterator<Object> iterator = bv.iterator();
    int rId = 0;
    while (iterator.hasNext()) {
      final Row row = (Row) iterator.next();
      final Object[] values = expectedValues[rId++];
      assertEquals("Incorrect number of column returned.", td.getNumOfColumns(), row.size());
      for (int j = 0; j < td.getNumOfColumns(); j++) {
        final Object value = row.getValue(j);
        final Object expected = values[j];
        if (expected == null || value == null) {
          continue;
        }
        if (expected.getClass().isArray()) {
          assertEquals("Incorrect value.length for columnId= " + j, Array.getLength(expected),
              Array.getLength(value));
          for (int k = 0; k < Array.getLength(value); k++) {
            assertEquals("Incorrect value.length for columnId= " + j + ", at index= " + k,
                Array.get(expected, k), Array.get(value, k));
          }
        } else {
          if (td.getColumnDescriptorByIndex(j).getColumnType().equals(BasicTypes.VARCHAR)) {
            assertEquals("Incorrect value for columnId= " + j, ((String) expected).substring(0, 26),
                value);
          } else if (td.getColumnDescriptorByIndex(j).getColumnType() instanceof MapType) {
            assertEquals("Incorrect value for columnId= " + j, MAP_STRING, value.toString());
          } else if (value instanceof BigDecimal) {
            BigDecimal e = ((BigDecimal) expected).stripTrailingZeros();
            BigDecimal a = ((BigDecimal) value).stripTrailingZeros();
            assertEquals("Incorrect value for columnId= " + j, e, a);
          } else {
            assertEquals("Incorrect value for columnId= " + j, expected, value);
          }
        }
      }
    }
  }

  /* ORC reader gives HashMap and not LinkedHashMap.. need to update if fixed in ORC-reader */
  private static final String MAP_STRING = "{20=val2, 10=val1}";

  private Object[] addToRecord(FTableDescriptor td, Record record) {
    Object[] values = new Object[td.getNumOfColumns()];
    int i = 0;
    for (MColumnDescriptor cd : td.getSchema().getColumnDescriptors()) {
      values[i++] = TypeUtils.getRandomValue(cd.getColumnType());
      if (cd.getColumnType().equals(BasicTypes.BIG_DECIMAL)) {
        BigDecimal bd = (BigDecimal) values[i - 1];
        values[i - 1] = new BigDecimal(bd.toBigInteger(), 5, new MathContext(10));
      } else if (cd.getColumnType().equals(BasicTypes.VARCHAR)) {
        final String s = (String) values[i - 1];
        final StringBuilder ns = new StringBuilder(s);
        final int len = s.length();
        int al = len;
        while (true) {
          ns.append(s);
          al += len;
          if (al > 40) {
            break;
          }
        }
        values[i - 1] = ns.toString();
      } else if (cd.getColumnType().equals(BasicTypes.DATE)) {
        values[i - 1] = Date.valueOf(values[i - 1].toString());
      } else if (cd.getColumnType() instanceof MapType) {
        Map<Integer, String> map = new LinkedHashMap<>(2);
        map.put(10, "val1");
        map.put(20, "val2");
        values[i - 1] = map;
      }
      record.add(cd.getColumnNameAsString(), values[i - 1]);
    }
    return values;
  }
}
