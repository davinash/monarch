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

package io.ampool.monarch.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import io.ampool.monarch.Utils.HiveUtils;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.types.*;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.interfaces.TypePredicateOp;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Specification for handling Hive predicates in context of Monarch.
 * It decides which predicates can be executed on Monarch and rest are
 * executed in Hive query engine itself.
 * <p>
 * Created on: 2015-12-23
 * Since version: 0.2.0
 */
public class MonarchPredicateHandler {
  /**
   * Allowed predicates on Monarch
   **/
  public static final Map<String, TypePredicateOp> FUNC_HIVE_TO_MONARC_MAP = new
    HashMap<String, TypePredicateOp>(5) {{
    put(GenericUDFOPEqual.class.getName(), CompareOp.EQUAL);
    put(GenericUDFOPNotEqual.class.getName(), CompareOp.NOT_EQUAL);
    put(GenericUDFOPLessThan.class.getName(), CompareOp.LESS);
    put(GenericUDFOPEqualOrLessThan.class.getName(), CompareOp.LESS_OR_EQUAL);
    put(GenericUDFOPGreaterThan.class.getName(), CompareOp.GREATER);
    put(GenericUDFOPEqualOrGreaterThan.class.getName(), CompareOp.GREATER_OR_EQUAL);
    put(UDFLike.class.getName(), CompareOp.REGEX);
  }};

  public static final Map<String, String> TYPE_HIVE_TO_MONARCH_MAP = new HashMap<String, String>(15) {{
    put(serdeConstants.INT_TYPE_NAME, BasicTypes.INT.name());
    put(serdeConstants.BIGINT_TYPE_NAME, BasicTypes.LONG.name());
    put(serdeConstants.STRING_TYPE_NAME, BasicTypes.STRING.name());
    put(serdeConstants.BOOLEAN_TYPE_NAME, BasicTypes.BOOLEAN.name());
    put(serdeConstants.SMALLINT_TYPE_NAME, BasicTypes.SHORT.name());
    put(serdeConstants.CHAR_TYPE_NAME, BasicTypes.CHARS.name());
    put(serdeConstants.VARCHAR_TYPE_NAME, BasicTypes.VARCHAR.name());
    put(serdeConstants.DATE_TYPE_NAME, BasicTypes.DATE.name());
    put(serdeConstants.TIMESTAMP_TYPE_NAME, BasicTypes.TIMESTAMP.name());
    put(serdeConstants.FLOAT_TYPE_NAME, BasicTypes.FLOAT.name());
    put(serdeConstants.DOUBLE_TYPE_NAME, BasicTypes.DOUBLE.name());
    put(serdeConstants.DECIMAL_TYPE_NAME, BasicTypes.BIG_DECIMAL.name());
    put(serdeConstants.TINYINT_TYPE_NAME, BasicTypes.BYTE.name());
    put(serdeConstants.BINARY_TYPE_NAME, BasicTypes.BINARY.name());
    put(serdeConstants.UNION_TYPE_NAME, UnionType.NAME);
  }};

  public static boolean isMonarchTypeSupported(final ObjectInspector oi) {
    if (ObjectInspector.Category.PRIMITIVE.equals(oi.getCategory())) {
      /** handle primitive type definitions like decimal(20,20) or varchar(100) **/
      String typeStr = oi.getTypeName();
      final int argPos = typeStr.indexOf('(');
      if (argPos > 0) {
        typeStr = typeStr.substring(0, argPos);
      }
      return TYPE_HIVE_TO_MONARCH_MAP.containsKey(typeStr);
    } else if (oi instanceof ListObjectInspector) {
      ListObjectInspector loi = (ListObjectInspector)oi;
      return isMonarchTypeSupported(loi.getListElementObjectInspector());
    } else if (oi instanceof MapObjectInspector) {
      MapObjectInspector moi = (MapObjectInspector)oi;
      return isMonarchTypeSupported(moi.getMapKeyObjectInspector()) &&
        isMonarchTypeSupported(moi.getMapValueObjectInspector());
    } else if (oi instanceof StructObjectInspector) {
      return ((StructObjectInspector) oi).getAllStructFieldRefs().stream()
        .map(StructField::getFieldObjectInspector)
        .allMatch(MonarchPredicateHandler::isMonarchTypeSupported);
    } else if (oi instanceof UnionObjectInspector) {
      return ((UnionObjectInspector) oi).getObjectInspectors().stream()
        .allMatch(MonarchPredicateHandler::isMonarchTypeSupported);
    }
    return false;
  }

  private static final Logger logger = LoggerFactory.getLogger(MonarchPredicateHandler.class);

  /**
   * Decompose the predicates (filter expressions) provided in hive query and if some
   * predicates can be pushed down to the Monarch, use them at query time reduce the
   * data queried from Monarch (Geode). The residual predicates (the ones that cannot
   * be executed on Monarch/Geode) will need to be executed in hive query engine.
   * <p>
   * The predicates to be executed on Monarch are decided by the column-type and
   * predicate operations. Following is the current list supported for execution
   * on Monarch/Geode side (as of 2015-12-23):
   * - Predicate Operations:
   * -- EQUAL
   * -- LESS THAN
   * -- LESS THAN OR EQUAL
   * - Column Types:
   * -- INT
   * -- LONG
   * -- STRING
   *
   * @param jobConf      the job configuration
   * @param deserializer the deserializer
   * @param exprNodeDesc the hive expression to be decpomposed
   * @return the decomposed predicate indicating which predicates will be executed on Monarch
   * and which predicates (residual) will be by Hive query engine
   */
  public static DecomposedPredicate decomposePredicate(final JobConf jobConf,
                                                       final MonarchSerDe deserializer,
                                                       final ExprNodeDesc exprNodeDesc) {
    List<IndexSearchCondition> indexSearchConditions = new ArrayList<>(5);
    IndexPredicateAnalyzer ipa = getIndexPredicateAnalyzer(deserializer);
    ExprNodeDesc residual = ipa.analyzePredicate(exprNodeDesc, indexSearchConditions);
    ipa.clearAllowedColumnNames();
    if (indexSearchConditions.isEmpty()) {
      if (logger.isDebugEnabled())
        logger.debug("nothing to decompose. Returning");
      return null;
    }

    DecomposedPredicate dp = new DecomposedPredicate();
    dp.pushedPredicate = ipa.translateSearchConditions(indexSearchConditions);
    dp.residualPredicate = (ExprNodeGenericFuncDesc) residual;
    dp.pushedPredicateObject = null;

    if (logger.isDebugEnabled()) {
      logger.debug("[To Monarch -->] PushedPredicate= {}", dp.pushedPredicate);
      logger.debug("[In Hive    -->] ResidualPredicate= {}", dp.residualPredicate);
    }
    return dp;
  }

  /**
   * Provide the {@link IndexPredicateAnalyzer} that will analyze the queries and determine
   * what predicates and what column types are supported by Monarch.
   *
   * @param deserializer the deserializer
   * @return the index predicate analyzer
   */
  protected static IndexPredicateAnalyzer getIndexPredicateAnalyzer(final MonarchSerDe deserializer) {
    IndexPredicateAnalyzer ipa = new IndexPredicateAnalyzer();

    /** support the only columns for which predicate test is allowed
     *   in Monarch, for the respective column-type **/
    List<String> columnNameList = deserializer.getColumnNames();
    List<TypeInfo> columnTypeList = deserializer.getColumnTypes();
    int size = columnNameList.size();
    if (size != columnTypeList.size()) {
      logger.warn("Column names and types do not match. Skipping predicate push-down.");
    } else {
      for (int i = 0; i < size; i++) {
        if (TYPE_HIVE_TO_MONARCH_MAP.get(columnTypeList.get(i).toString()) != null) {
          ipa.allowColumnName(columnNameList.get(i));
        }
      }
    }

    /** support following operations for push-down **/
    FUNC_HIVE_TO_MONARC_MAP.keySet().forEach(ipa::addComparisonOp);
    return ipa;
  }

  /**
   * Provide the required predicates to be pushed-down to Monarch, in serializable form
   * so that these could be transferred.
   *
   * @param expression the serialized expression for the predicates to be pushed-down
   * @param columns an array of columns for the table
   * @return an array of predicates to be tested on Monarch
   */
  public static MPredicateHolder[] getPushDownPredicates(final String expression, final String[] columns) {
    Objects.requireNonNull(expression, "Valid expression should be provided.");
    Objects.requireNonNull(columns, "Valid list of columns should be provided.");
    ExprNodeDesc exprNodeDesc = HiveUtils.deserializeExpression(expression);
    logger.debug("Hive push-down expression= {}" + exprNodeDesc);
    if(exprNodeDesc == null){
      return null;
    }

    /** mimic all columns as supported **/
    IndexPredicateAnalyzer ipa = new IndexPredicateAnalyzer();
    ipa.clearAllowedColumnNames();
    final Map<String, Integer> columnIndexMap = new HashMap<>(columns.length);
    final AtomicInteger index = new AtomicInteger(0);
    Arrays.stream(columns).forEach(column -> {
      ipa.allowColumnName(column);
      columnIndexMap.put(column, index.getAndIncrement());
    });

    /** support following operations for push-down **/
    FUNC_HIVE_TO_MONARC_MAP.keySet().forEach(ipa::addComparisonOp);

    List<IndexSearchCondition> iscList = new ArrayList<>(5);
    ipa.analyzePredicate(exprNodeDesc, iscList);

    if (iscList.isEmpty()) {
      if (logger.isDebugEnabled())
        logger.debug("nothing to decompose. Returning");
      return null;
    }

    MPredicateHolder[] predicateHolders = iscList.stream()
      .filter(HIVE_TO_MONARCH_CONDITION)
      .map(e -> newPredicate(columnIndexMap, e))
      .toArray(MPredicateHolder[]::new);

    if (logger.isDebugEnabled()) {
      logger.debug("Predicates pushed to Monarch: {}", Arrays.toString(predicateHolders));
    }
    return predicateHolders;
  }

  /**
   * Provide the required predicates to be pushed-down to Monarch, in serializable form
   * so that these could be transferred.
   *
   * @param expression the serialized expression for the predicates to be pushed-down
   * @param columns an array of columns for the table
   * @return an array of predicates to be tested on Monarch
   */
  public static FilterList getPushDownFilters(final String expression, final String[] columns) {
    Objects.requireNonNull(expression, "Valid expression should be provided.");
    Objects.requireNonNull(columns, "Valid list of columns should be provided.");
    ExprNodeDesc exprNodeDesc = HiveUtils.deserializeExpression(expression);
    logger.debug("Hive push-down expression= {}" + exprNodeDesc);
    if(exprNodeDesc == null){
      return null;
    }

    /** mimic all columns as supported **/
    IndexPredicateAnalyzer ipa = new IndexPredicateAnalyzer();
    ipa.clearAllowedColumnNames();

    Arrays.stream(columns).forEach(column -> {
      ipa.allowColumnName(column);
    });

    /** support following operations for push-down **/
    FUNC_HIVE_TO_MONARC_MAP.keySet().forEach(ipa::addComparisonOp);

    List<IndexSearchCondition> iscList = new ArrayList<>(5);
    ipa.analyzePredicate(exprNodeDesc, iscList);

    if (iscList.isEmpty()) {
      if (logger.isDebugEnabled())
        logger.debug("nothing to decompose. Returning");
      return null;
    }

    Filter[] filterHolders = iscList.stream()
      .filter(HIVE_TO_MONARCH_CONDITION)
      .map(e -> newFilter(e))
      .toArray(Filter[]::new);

    if (logger.isDebugEnabled()) {
      logger.debug("Predicates pushed to Monarch: {}", Arrays.toString(filterHolders));
    }

    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    for (int i = 0; i < filterHolders.length; i++) {
      filterList.addFilter(filterHolders[i]);
    }
    return filterList;
  }

  /**
   * Condition to allow the hive predicates to be pushed-down to Monarch.
   * It restricts the search conditions, from pushed-down, with unsupported
   * column-types and predicates/functions.
   */
  private static final Predicate<IndexSearchCondition> HIVE_TO_MONARCH_CONDITION =
    e -> TYPE_HIVE_TO_MONARCH_MAP.containsKey(e.getColumnDesc().getTypeString())
      && FUNC_HIVE_TO_MONARC_MAP.containsKey(e.getComparisonOp());

  /**
   * Create new {@link MPredicateHolder} that maps Hive {@link IndexSearchCondition} to
   * Monarch understandable format.
   *
   * @param columnIndexMap the column to index map
   * @param isc            the index-search-condition
   * @return the mapped predicate
   */
  private static MPredicateHolder newPredicate(final Map<String, Integer> columnIndexMap,
                                               final IndexSearchCondition isc) {
    Object constantValue = isc.getConstantDesc().getValue();
    /** convert SQL like query to Java-regex syntax **/
    if (CompareOp.REGEX.equals(FUNC_HIVE_TO_MONARC_MAP.get(isc.getComparisonOp()))) {
      constantValue = UDFLike.likePatternToRegExp(constantValue.toString());
    } else {
      constantValue = convertObject(isc.getConstantDesc().getTypeInfo(),
        isc.getColumnDesc().getTypeInfo(), constantValue);
    }
    return new MPredicateHolder(columnIndexMap.get(isc.getColumnDesc().getColumn()),
      MonarchPredicateHandler.getMonarchFieldType(isc.getColumnDesc().getTypeString()),
      FUNC_HIVE_TO_MONARC_MAP.get(isc.getComparisonOp()), constantValue);
  }


  /**
   * Create new {@link Filter} that maps Hive {@link IndexSearchCondition} to
   * Monarch understandable format.
   *
   * @param isc            the index-search-condition
   * @return the mapped predicate
   */
  private static Filter newFilter(final IndexSearchCondition isc) {
    Object constantValue = isc.getConstantDesc().getValue();
    /** convert SQL like query to Java-regex syntax **/
    if (CompareOp.REGEX.equals(FUNC_HIVE_TO_MONARC_MAP.get(isc.getComparisonOp()))) {
      constantValue = UDFLike.likePatternToRegExp(constantValue.toString());
    } else {
      constantValue = convertObject(isc.getConstantDesc().getTypeInfo(),
        isc.getColumnDesc().getTypeInfo(), constantValue);
    }
    Filter filter =  new SingleColumnValueFilter(isc.getColumnDesc().getColumn(),
      (CompareOp) FUNC_HIVE_TO_MONARC_MAP.get(isc.getComparisonOp()) , constantValue);

    return filter;
  }

  /**
   * Convert an object value to the correct type (the type of column) so that the comparison
   * is done using the same type. The conversion is done by using the respective Hive's
   * object inspectors.
   *
   * @param tiIn   the type-info of the object
   * @param tiOut  the expected type-info of the object
   * @param object the object that is to be converted
   * @return the object converted from input type to output type
   */
  private static Object convertObject(final TypeInfo tiIn, final TypeInfo tiOut,
                                      final Object object) {
    final ObjectInspector oi0 = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(tiIn);
    final ObjectInspector oi1 = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(tiOut);
    return ObjectInspectorConverters.getConverter(oi0, oi1).convert(object);
  }

  public static DataType getMonarchFieldType(final String typeStr) {
    return DataTypeFactory.getTypeFromString(typeStr, TYPE_HIVE_TO_MONARCH_MAP);
  }
}
