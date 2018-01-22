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
package io.ampool.orc;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.ftable.internal.ReaderOptions;
import io.ampool.monarch.table.internal.Encoding;
import io.ampool.monarch.table.internal.InternalRow;
import io.ampool.monarch.table.internal.ServerRow;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.DataTypeFactory;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.interfaces.TypePredicateOp;
import io.ampool.store.StoreRecord;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.logging.LogService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.logging.log4j.Logger;
import org.apache.orc.FileMetaInfo;
import org.apache.orc.Reader;
import org.iq80.snappy.Snappy;

/**
 * Utility methods to help handling of ORC format data.
 *
 */
public class OrcUtils {
  private static final Logger logger = LogService.getLogger();
  private static final String VARCHAR_TYPE = getBase(TypeInfoFactory.varcharTypeInfo.getTypeName());
  private static final String DECIMAL_TYPE = getBase(TypeInfoFactory.decimalTypeInfo.getTypeName());

  private static String getBase(final String typeStr) {
    final int index = typeStr.indexOf('(');
    return index > 0 ? typeStr.substring(0, index) : typeStr;
  }

  public static final Map<String, Function<Object, Object>> OrcReadFunctionMap =
      new HashMap<String, Function<Object, Object>>(20) {
        {
          put(TypeInfoFactory.stringTypeInfo.getTypeName(), Object::toString);
          put(VARCHAR_TYPE, Object::toString);
          put(TypeInfoFactory.stringTypeInfo.getTypeName(), Object::toString);
          put(TypeInfoFactory.charTypeInfo.getTypeName(),
              e -> ((HiveCharWritable) e).getHiveChar().getValue().charAt(0));
          put(TypeInfoFactory.intTypeInfo.getTypeName(), e -> ((IntWritable) e).get());
          put(TypeInfoFactory.longTypeInfo.getTypeName(), e -> ((LongWritable) e).get());
          put(TypeInfoFactory.doubleTypeInfo.getTypeName(), e -> ((DoubleWritable) e).get());
          put(TypeInfoFactory.binaryTypeInfo.getTypeName(), e -> ((BytesWritable) e).copyBytes());
          put(TypeInfoFactory.booleanTypeInfo.getTypeName(), e -> ((BooleanWritable) e).get());
          put(TypeInfoFactory.byteTypeInfo.getTypeName(), e -> ((ByteWritable) e).get());
          put(TypeInfoFactory.dateTypeInfo.getTypeName(), e -> ((DateWritable) e).get());
          put(TypeInfoFactory.floatTypeInfo.getTypeName(), e -> ((FloatWritable) e).get());
          put(TypeInfoFactory.shortTypeInfo.getTypeName(), e -> ((ShortWritable) e).get());
          put(TypeInfoFactory.timestampTypeInfo.getTypeName(),
              e -> ((TimestampWritable) e).getTimestamp());
          put(DECIMAL_TYPE, e -> ((HiveDecimalWritable) e).getHiveDecimal().bigDecimalValue());
        }
      };
  public static final Map<String, TypeInfo> OrcTypeMap = new HashMap<String, TypeInfo>(20) {
    {
      put(BasicTypes.STRING.toString(), TypeInfoFactory.stringTypeInfo);
      put(BasicTypes.VARCHAR.toString(), TypeInfoFactory.varcharTypeInfo);
      put(BasicTypes.CHARS.toString(), TypeInfoFactory.stringTypeInfo);
      put(BasicTypes.CHAR.toString(), TypeInfoFactory.charTypeInfo);
      put(BasicTypes.O_INT.toString(), TypeInfoFactory.intTypeInfo);
      put(BasicTypes.O_LONG.toString(), TypeInfoFactory.longTypeInfo);
      put(BasicTypes.INT.toString(), TypeInfoFactory.intTypeInfo);
      put(BasicTypes.LONG.toString(), TypeInfoFactory.longTypeInfo);
      put(BasicTypes.DOUBLE.toString(), TypeInfoFactory.doubleTypeInfo);
      put(BasicTypes.BINARY.toString(), TypeInfoFactory.binaryTypeInfo);
      put(BasicTypes.BOOLEAN.toString(), TypeInfoFactory.booleanTypeInfo);
      put(BasicTypes.BYTE.toString(), TypeInfoFactory.byteTypeInfo);
      put(BasicTypes.DATE.toString(), TypeInfoFactory.dateTypeInfo);
      put(BasicTypes.FLOAT.toString(), TypeInfoFactory.floatTypeInfo);
      put(BasicTypes.SHORT.toString(), TypeInfoFactory.shortTypeInfo);
      put(BasicTypes.TIMESTAMP.toString(), TypeInfoFactory.timestampTypeInfo);
      put(BasicTypes.BIG_DECIMAL.name(), TypeInfoFactory.decimalTypeInfo);
    }
  };
  public static final Map<String, String> OrcTypeStringMap = new HashMap<String, String>(20) {
    {
      put(BasicTypes.STRING.toString(), TypeInfoFactory.stringTypeInfo.getTypeName());
      put(BasicTypes.VARCHAR.toString(), VARCHAR_TYPE);
      put(BasicTypes.CHARS.toString(), TypeInfoFactory.stringTypeInfo.getTypeName());
      put(BasicTypes.CHAR.toString(), TypeInfoFactory.charTypeInfo.getTypeName());
      put(BasicTypes.O_INT.toString(), TypeInfoFactory.intTypeInfo.getTypeName());
      put(BasicTypes.O_LONG.toString(), TypeInfoFactory.longTypeInfo.getTypeName());
      put(BasicTypes.INT.toString(), TypeInfoFactory.intTypeInfo.getTypeName());
      put(BasicTypes.LONG.toString(), TypeInfoFactory.longTypeInfo.getTypeName());
      put(BasicTypes.DOUBLE.toString(), TypeInfoFactory.doubleTypeInfo.getTypeName());
      put(BasicTypes.BINARY.toString(), TypeInfoFactory.binaryTypeInfo.getTypeName());
      put(BasicTypes.BOOLEAN.toString(), TypeInfoFactory.booleanTypeInfo.getTypeName());
      put(BasicTypes.BYTE.toString(), TypeInfoFactory.byteTypeInfo.getTypeName());
      put(BasicTypes.DATE.toString(), TypeInfoFactory.dateTypeInfo.getTypeName());
      put(BasicTypes.FLOAT.toString(), TypeInfoFactory.floatTypeInfo.getTypeName());
      put(BasicTypes.SHORT.toString(), TypeInfoFactory.shortTypeInfo.getTypeName());
      put(BasicTypes.TIMESTAMP.toString(), TypeInfoFactory.timestampTypeInfo.getTypeName());
      put(BasicTypes.BIG_DECIMAL.name(), DECIMAL_TYPE);
    }
  };

  /**
   * Predicate converter map: From Ampool to ORC predicates.
   **/
  public static final Map<TypePredicateOp, BiFunction<SingleColumnValueFilter, TableDescriptor, ExprNodeDesc>> UDF_CONVERT_MAP =
      new HashMap<TypePredicateOp, BiFunction<SingleColumnValueFilter, TableDescriptor, ExprNodeDesc>>() {
        {
          put(CompareOp.EQUAL, (f, td) -> convertFilter(f, td, new GenericUDFOPEqual()));
          put(CompareOp.NOT_EQUAL, (f, td) -> convertFilter(f, td, new GenericUDFOPNotEqual()));
          put(CompareOp.LESS, (f, td) -> convertFilter(f, td, new GenericUDFOPLessThan()));
          put(CompareOp.LESS_OR_EQUAL,
              (f, td) -> convertFilter(f, td, new GenericUDFOPEqualOrLessThan()));
          put(CompareOp.GREATER, (f, td) -> convertFilter(f, td, new GenericUDFOPGreaterThan()));
          put(CompareOp.GREATER_OR_EQUAL,
              (f, td) -> convertFilter(f, td, new GenericUDFOPEqualOrGreaterThan()));
          // put(CompareOp.REGEX, (f, td) -> convertFilter(f, td, new UDFLike()));
        }
      };

  public static String getOrcSchema(final Schema schema) {
    return DataTypeFactory.convertTypeString(schema.toString(), OrcTypeStringMap);
  }

  /**
   * Create an ORC writer that can write the ORC data, with the specified schema, to an in-memory
   * byte-array rather than to a file.
   *
   * @param opts the ORC writer options
   * @return the ORC writer
   * @throws IOException if failed to create the ORC writer
   */
  public static AWriter createWriter(final OrcFile.WriterOptions opts) throws IOException {
    final Path dummyPath = AReaderImpl.PATH;
    FileSystem fs = opts.getFileSystem() == null ? dummyPath.getFileSystem(opts.getConfiguration())
        : opts.getFileSystem();

    return new AWriter(fs, dummyPath, opts);
  }

  /**
   * Create an ORC reader that can read the ORC data from the provided byte-array rather than
   * reading from a file.
   *
   * @param bytes the binary ORC data
   * @param options the ORC reader options
   * @return the ORC reader
   * @throws IOException if failed to create the ORC reader
   */
  public static AReaderImpl createReader(final byte[] bytes, final OrcFile.ReaderOptions options)
      throws IOException {
    final FileMetaInfo fileMetaInfo = AReaderImpl.extractMetaInfoFromFooter(bytes);
    AFileMetadata metadata = new AFileMetadata(fileMetaInfo);
    options.fileMetadata(metadata);
    options.fileMetaInfo(fileMetaInfo);
    return new AReaderImpl(bytes, options);
  }

  /**
   * Convert the provided BlockValue to ORC format byte-array using the specified table-descriptor.
   * All the rows are interpreted using the relevant encoding and then converted/written as ORC
   * format.
   *
   * @param bv the block value
   * @param td the table descriptor
   * @return the ORC format representation of the block of rows
   */
  public static byte[] convertToOrcBytes(final BlockValue bv, final FTableDescriptor td) {
    final SettableStructObjectInspector rowOI =
        (SettableStructObjectInspector) getStandardJavaObjectInspectorFromTypeInfo(
            getTypeInfoFromTypeString(td.getOrcSchema()));

    final List<? extends StructField> fields = rowOI.getAllStructFieldRefs();
    final OrcFile.WriterOptions wOpts =
        OrcFile.writerOptions(td.getBlockProperties(), new Configuration()).inspector(rowOI);
    final Encoding enc = Encoding.getEncoding(bv.getRowHeader().getEncoding());
    try {
      final AWriter writer = createWriter(wOpts);

      final InternalRow aRow = ServerRow.create(td);
      final List oRow = (List) rowOI.create();
      for (int i = 0; i < bv.getCurrentIndex(); i++) {
        aRow.reset(null, bv.getRecord(i), enc, null);
        int j = 0;
        for (final Cell cell : aRow.getCells()) {
          if (cell.getColumnType().equals(BasicTypes.BIG_DECIMAL)) {
            rowOI.setStructFieldData(oRow, fields.get(j++),
                HiveDecimal.create((BigDecimal) cell.getColumnValue()));
          } else {
            rowOI.setStructFieldData(oRow, fields.get(j++), cell.getColumnValue());
          }
        }
        writer.addRow(oRow);
      }
      return writer.getBytes();
    } catch (IOException e) {
      logger.warn("Error in writing ORC row... closing of block skipped...", e);
    }
    return null;
  }

  public static byte[] compressSnappy(final BlockValue bv, final FTableDescriptor td) {
    byte[] bytes = null;
    final int totalSize = bv.getValueSize() + (bv.size() * 4);
    try (HeapDataOutputStream hdos = new HeapDataOutputStream(totalSize, null, true)) {
      for (final byte[] rec : bv.getRecords()) {
        hdos.writeInt(rec.length);
        hdos.write(rec);
      }
      // bytes = Snappy.compress(hdos.toByteArray());
      bytes = hdos.toByteArray();
      byte[] compressedOut = new byte[Snappy.maxCompressedLength(bytes.length)];
      int compressedSize = Snappy.compress(bytes, 0, bytes.length, compressedOut, 0);
      bytes = Arrays.copyOf(compressedOut, compressedSize);
    } catch (IOException e) {
      logger.error("Error converting to Snappy format.", e);
    }
    return bytes;
  }

  public static byte[] decompressSnappy(final byte[] input) {
    return Snappy.uncompress(input, 0, input.length);
  }

  /**
   * Convert Ampool SingleColumnValueFilter to the respective ORC predicate.
   *
   * @param f the Ampool filter
   * @param td the table descriptor
   * @param udf the generic UDF corresponding to the Ampool filter
   * @return the ORC predicate
   */
  private static ExprNodeDesc convertFilter(final SingleColumnValueFilter f,
      final TableDescriptor td, final GenericUDF udf) {
    final String cName = f.getColumnNameString();
    final String aType = td.getColumnByName(cName).getColumnType().toString();
    ExprNodeDesc exprNodeDesc =
        new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, udf, new ArrayList<>(2));
    final TypeInfo ti = OrcTypeMap.get(aType);
    final ExprNodeDesc c1 = new ExprNodeColumnDesc(ti, cName, cName, false);
    final ExprNodeDesc c2 = new ExprNodeConstantDesc(ti,
        ti == TypeInfoFactory.floatTypeInfo ? ((Number) f.getValue()).doubleValue() : f.getValue());
    exprNodeDesc.getChildren().add(c1);
    exprNodeDesc.getChildren().add(c2);
    return exprNodeDesc;
  }

  /**
   * Convert generic Ampool filter(s) to the corresponding generic UDF(s).
   *
   * @param filter the Ampool filters
   * @param td the Ampool table descriptor
   * @return the generic ORC predicates
   */
  public static ExprNodeDesc convertToExpression(final Filter filter, final TableDescriptor td)
      throws IOException {
    if (filter instanceof FilterList) {
      FilterList fl = (FilterList) filter;
      ExprNodeDesc expression = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
          fl.getOperator() == FilterList.Operator.MUST_PASS_ALL ? new GenericUDFOPAnd()
              : new GenericUDFOPOr(),
          new ArrayList<>());
      for (Filter f : fl.getFilters()) {
        expression.getChildren().add(convertToExpression(f, td));
      }
      return expression;
    } else if (filter instanceof SingleColumnValueFilter) {
      SingleColumnValueFilter cf = (SingleColumnValueFilter) filter;
      if (!UDF_CONVERT_MAP.containsKey(cf.getOperator())) {
        throw new IOException("Failed to convert ComparisonOperator: " + cf.getOperator());
      }
      return UDF_CONVERT_MAP.get(cf.getOperator()).apply(cf, td);
    } else {
      return null;
    }
  }

  /**
   * Create an array of columns names as required by ORC SArg. It needs a dummy entry for each
   * non-primitive column. It seems it maintains additional index-stats for such entries thus
   * trailing columns have additional offset of 1 (per such column).
   *
   * TODO: Verified with struct and array column types; need to verify/adjust for others.
   *
   * @param td the table descriptor
   * @return an array of column names as required by ORC reader
   */
  public static String[] getSArgColumns(final TableDescriptor td) {
    final List<String> list = new ArrayList<>(td.getNumOfColumns());
    list.add("_dummy_");
    for (MColumnDescriptor cd : td.getColumnDescriptors()) {
      final DataType type = cd.getColumnType();
      if (type.getCategory() != DataType.Category.Basic) {
        list.add("_dummy_");
      }
      list.add(cd.getColumnNameAsString());
    }
    return list.toArray(new String[0]);
  }

  /**
   * Convert the Ampool filters into ORC SearchArgument using the table descriptor. The table
   * descriptor only gives the respective columns types.
   *
   * @param filter the filters
   * @param td the table descriptor
   * @return the search argument
   */
  public static SearchArgument getSArg(final Filter filter, final TableDescriptor td) {
    ExprNodeDesc expr = null;
    try {
      expr = OrcUtils.convertToExpression(filter, td);
    } catch (IOException e) {
      logger.debug("Failed to convert filters= {} to ORC expression.", filter, e);
    } catch (Exception e) {
      logger.warn("Failed to convert filters= {} to ORC expression.", filter, e);
    }
    if (expr == null || td == null) {
      return null;
    }

    try {
      final String str = SerializationUtilities.serializeExpression((ExprNodeGenericFuncDesc) expr);
      Configuration conf = new Configuration();
      conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, str);
      return ConvertAstToSearchArg.createFromConf(conf);
    } catch (Exception e) {
      logger.warn("Failed to create SearchArgument: expression= {}, filters= {}", expr, filter, e);
      return null;
    }
  }

  public static class DummyRow {
    private byte[] bytes;
    private int offset;
    private int length;

    public DummyRow(final byte[] bytes) {
      this.bytes = bytes;
      this.offset = 0;
      this.length = 0;
    }

    public DummyRow reset(final int offset, final int length) {
      this.offset = offset;
      this.length = length;
      return this;
    }

    public byte[] getBytes() {
      return bytes;
    }

    public int getOffset() {
      return offset;
    }

    public int getLength() {
      return length;
    }

    public byte[] getCopy() {
      return Arrays.copyOfRange(bytes, offset, offset + length);
    }

    @Override
    public String toString() {
      return "DummyRow{offset=" + offset + ",length=" + length + "}";
    }
  }
  public static class RowIterator implements Iterator<Object> {
    private byte[] bytes;
    private int offset = 0;
    private DummyRow row;
    private boolean doCopy = true;

    public RowIterator(final byte[] bytes) {
      this.bytes = bytes;
      this.row = new DummyRow(this.bytes);
    }

    public RowIterator(final byte[] bytes, final boolean doCopy) {
      this.bytes = bytes;
      this.doCopy = doCopy;
      this.row = new DummyRow(this.bytes);
    }

    @Override
    public boolean hasNext() {
      return this.offset < this.bytes.length;
    }

    @Override
    public Object next() {
      final int len = Bytes.toInt(this.bytes, this.offset);
      this.offset += Bytes.SIZEOF_INT;
      row.reset(this.offset, len);
      this.offset += len;
      return this.doCopy ? row.getCopy() : row;
    }
  }

  /**
   * A simple wrapper that holds the required ORC search arguments and column names.
   */
  public static class OrcOptions implements ReaderOptions {
    private final Reader.Options options = new Reader.Options();
    private int[] filterColumns;

    public OrcOptions(final Filter filter, final TableDescriptor td) {
      final SearchArgument sArg = OrcUtils.getSArg(filter, td);
      if (sArg != null) {
        this.filterColumns = sArg.getLeaves().stream()
            .mapToInt(e -> td.getColumnByName(e.getColumnName()).getIndex()).toArray();
      }
      this.options.searchArgument(sArg, OrcUtils.getSArgColumns(td));
    }

    public Reader.Options getOptions() {
      return options;
    }

    /**
     * Get the indices of the columns used in predicates/filters. The column-index of the respective
     * column, in query, is maintained at the respective position.
     *
     * @return an array column indices for respective filters
     */
    public int[] getFilterColumns() {
      return this.filterColumns;
    }

    @Override
    public String toString() {
      return "OrcOptions{" + "options=" + options + ", filterColumns="
          + Arrays.toString(filterColumns) + '}';
    }
  }

  /**
   * Return whether the block contains at least one row matching the provided filters. It will
   * return false only when not even a single row would match the filters.
   *
   * @param opts the options containing the filters
   * @param bv the block value
   * @return true if the block contains at least one matching row; false otherwise
   */
  public static boolean isBlockNeeded(final OrcUtils.OrcOptions opts, final BlockValue bv) {
    final SearchArgument sa = opts.getOptions().getSearchArgument();
    final int[] filterColumns = opts.getFilterColumns();
    final AColumnStatistics stats = bv.getColumnStatistics();
    if (sa == null || sa.getLeaves().isEmpty() || filterColumns.length == 0 || stats == null) {
      return true;
    }
    final List<PredicateLeaf> leaves = sa.getLeaves();

    SearchArgument.TruthValue[] truthValues = new SearchArgument.TruthValue[leaves.size()];
    for (int i = 0; i < leaves.size(); i++) {
      truthValues[i] = RecordReaderImpl
          .evaluatePredicate(stats.getColumnStatistics(filterColumns[i]), leaves.get(i), null);
    }
    return sa.evaluate(truthValues).isNeeded();
  }

  /**
   * Convert the value using the ObjectInspector. The Writable values are converted to their
   * respective Java objects from using the provided inspector.
   *
   * @param oi the field object inspector
   * @param value the value
   * @return the corresponding Java object value
   */
  public static Object convert(final ObjectInspector oi, final Object value) {
    if (value == null) {
      return null;
    }
    Object outValue = null;
    switch (oi.getCategory()) {
      case PRIMITIVE:
        final Function<Object, Object> type = OrcReadFunctionMap.computeIfAbsent(oi.getTypeName(),
            k -> OrcReadFunctionMap.get(getBase(k)));
        if (type == null) {
          logger.error("No converter found for ORC type: {}; returning null..", oi.getTypeName());
          return null;
        }
        outValue = type.apply(value);
        break;
      case LIST:
        final ListObjectInspector loi = (ListObjectInspector) oi;
        final ObjectInspector eoi = loi.getListElementObjectInspector();
        outValue =
            loi.getList(value).stream().map(e -> convert(eoi, e)).collect(Collectors.toList());
        break;
      case MAP:
        final MapObjectInspector moi = (MapObjectInspector) oi;
        final ObjectInspector koi = moi.getMapKeyObjectInspector();
        final ObjectInspector voi = moi.getMapValueObjectInspector();
        outValue = moi.getMap(value).entrySet().stream()
            .collect(Collectors.toMap(e -> convert(koi, e.getKey()),
                e -> convert(voi, e.getValue()), throwingMerger(), LinkedHashMap::new));
        break;
      case STRUCT:
        final StructObjectInspector soi = (StructObjectInspector) oi;
        outValue = soi.getAllStructFieldRefs().stream()
            .map(e -> convert(e.getFieldObjectInspector(), soi.getStructFieldData(value, e)))
            .toArray();
        break;
      case UNION:
        final UnionObjectInspector uoi = (UnionObjectInspector) oi;
        final List<? extends ObjectInspector> ois = uoi.getObjectInspectors();
        final byte tag = uoi.getTag(value);
        outValue = new Object[] {tag, convert(ois.get(tag), uoi.getField(value))};
        break;
    }
    return outValue;
  }

  private static <T> BinaryOperator<T> throwingMerger() {
    return (u, v) -> {
      throw new IllegalStateException(String.format("Duplicate key %s", u));
    };
  }

  /**
   * An ORC iterator that can help iterate over the ORC format data row-by-row. It creates an ORC
   * reader from bytes and converts each ORC row to a StoreRecord.
   */
  public static class OrcIterator implements Iterator<Object> {
    private RecordReader reader;
    private Object row = null;
    private SettableStructObjectInspector rowOI;
    private List<? extends StructField> fields;
    private StoreRecord desRow;

    public OrcIterator(final byte[] bytes, final OrcOptions opts) {
      try {
        final AReaderImpl rdr = createReader(bytes, new OrcFile.ReaderOptions(new Configuration()));
        this.reader = opts == null ? rdr.rows() : rdr.rowsOptions(opts.getOptions());
        this.rowOI = (SettableStructObjectInspector) rdr.getObjectInspector();
        this.fields = rowOI.getAllStructFieldRefs();
      } catch (IOException e) {
        logger.error("Failed to create ORC reader.", e);
      }
    }

    public OrcIterator(final byte[] bytes) {
      this(bytes, null);
    }

    @Override
    public boolean hasNext() {
      try {
        return reader.hasNext();
      } catch (IOException e) {
        return false;
      }
    }

    @Override
    public Object next() {
      try {
        /* convert ORC row to Ampool row */
        this.row = this.reader.next(this.row);
        int i = 0;
        this.desRow = new StoreRecord(this.fields.size());
        for (final StructField field : fields) {
          this.desRow.setValue(i++,
              convert(field.getFieldObjectInspector(), rowOI.getStructFieldData(row, field)));
        }
        return this.desRow;
      } catch (Exception e) {
        logger.error("Failed to retrieve/convert row via ORC reader. row= {}", row, e);
        return null;
      }
    }
  }
}
