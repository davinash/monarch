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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.*;

import io.ampool.monarch.Utils.HiveUtils;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.MPredicateHolder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMinus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for MonarchPredicateHandler..
 * <p>
 * Created on: 2015-12-23
 * Since version: 0.2.0
 */
public class MonarchPredicateHandlerTest {
  /**
   * Simple utility class to assert null or non-null objects.
   */
  private enum NullCheck {
    Null,
    NotNull;

    public void check(final Object object) {
      switch (this) {
        case Null:
          assertNull(object);
          break;
        case NotNull:
          assertNotNull(object);
          break;
      }
    }
  }

  ;

  /**
   * Create an expression with one function and two arguments one column name and other with value to
   * be checked against using the specified function.
   *
   * @param colType the column type
   * @param colName the column name
   * @param value   the constant value to be checked against
   * @param retType the return type of the function
   * @param udf     the function to be executed using column name and constant
   * @return the expression node
   */
  private ExprNodeDesc getExprNodeDesc(final TypeInfo colType, final String colName, final Object value,
                                       final TypeInfo retType, final GenericUDF udf) {
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(retType, udf, new ArrayList<>(2));
    exprNodeDesc.getChildren().add(new ExprNodeColumnDesc(colType, colName, colName, false));
    exprNodeDesc.getChildren().add(new ExprNodeConstantDesc(colType, value));
    return exprNodeDesc;
  }

  /**
   * Data provider for a predicate with single function.
   *
   * @return the require arguments: an expression node and respective null/non-null check
   * for the pushed predicate and residual predicate
   */
  @DataProvider
  private Object[][] getDataForSinglePredicate() {
    return new Object[][]{
      {getExprNodeDesc(TypeInfoFactory.intTypeInfo, "c1", 10, TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqual()), NullCheck.NotNull, NullCheck.Null,
        new SingleColumnValueFilter("c1", CompareOp.EQUAL, 10)},
      {getExprNodeDesc(TypeInfoFactory.intTypeInfo, "c1", 10, TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPLessThan()), NullCheck.NotNull, NullCheck.Null,
        new SingleColumnValueFilter("c1", CompareOp.LESS, 10)},
      {getExprNodeDesc(TypeInfoFactory.stringTypeInfo, "c1", "10", TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqual()), NullCheck.NotNull, NullCheck.Null,
        new SingleColumnValueFilter("c1", CompareOp.EQUAL, "10")},
      {getExprNodeDesc(TypeInfoFactory.intTypeInfo, "c1", 10, TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqualOrLessThan()), NullCheck.NotNull, NullCheck.Null,
        new SingleColumnValueFilter("c1", CompareOp.LESS_OR_EQUAL, "10")},
      {getExprNodeDesc(TypeInfoFactory.intTypeInfo, "c1", 10, TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPGreaterThan()), NullCheck.NotNull, NullCheck.Null,
        new SingleColumnValueFilter("c1", CompareOp.GREATER, 10)},
      {getExprNodeDesc(TypeInfoFactory.intTypeInfo, "c1", 10, TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqualOrGreaterThan()), NullCheck.NotNull, NullCheck.Null,
        new SingleColumnValueFilter("c1", CompareOp.GREATER_OR_EQUAL, 10)},
      {getExprNodeDesc(TypeInfoFactory.intTypeInfo, "c1", 10, TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPNotEqual()), NullCheck.NotNull, NullCheck.Null,
        new SingleColumnValueFilter("c1", CompareOp.NOT_EQUAL, 10)},
      {getExprNodeDesc(TypeInfoFactory.intTypeInfo, "c1", 10, TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPMultiply()), NullCheck.Null, NullCheck.NotNull,
        null
      }
    };
  }

  /**
   * properties used for SerDe
   **/
  private Properties properties = new Properties() {{
    setProperty("columns", "c1,c2,c3");
    setProperty("columns.type", "int:int:struct<s1:bigint,s2:map<string,array<float>>,s3:boolean>");
  }};
  /**
   * the SerDe used during decomposing the predicates
   **/
  private MonarchSerDe serDe;

  /**
   * Create instance of SerDe.
   *
   * @throws SerDeException
   */
  @BeforeClass
  public void setUpBeforeClass() throws SerDeException {
    serDe = new MonarchSerDe();
    serDe.initialize(new Configuration(), properties);
  }

  /**
   * Test single function predicates.
   *
   * @param exprNodeDesc  the node expression
   * @param checkPushed   the null check for pushed predicate
   * @param checkResidual the null check for residual predicate
   * @throws Exception
   */
  @Test(dataProvider = "getDataForSinglePredicate")
  public void testPredicate_Single(final ExprNodeDesc exprNodeDesc,
                                   final NullCheck checkPushed, final NullCheck checkResidual,
                                   final Filter expectedPredicate) throws
    Exception {

    DecomposedPredicate dp = MonarchPredicateHandler.decomposePredicate(null, serDe, exprNodeDesc);
    //assertNotNull(dp);
    //checkPushed.check(dp.pushedPredicate);
    //checkResidual.check(dp.residualPredicate);

    if (checkPushed.equals(NullCheck.NotNull)) {
      final String expression = Utilities.serializeExpression(dp.pushedPredicate);
      final String[] cols = Arrays.stream(properties.getProperty("columns").split(",")).toArray(String[]::new);
      MPredicateHolder[] phs1 = MonarchPredicateHandler.getPushDownPredicates(expression, cols);
      FilterList phs = MonarchPredicateHandler.getPushDownFilters(expression, cols);
      assertEquals(phs.getFilters().size(), 1);
      assertEquals(phs.getFilters().get(0).toString(), expectedPredicate.toString());
    }
  }

  @Test
  public void testPredicate_Multiple_1() {
    ExprNodeDesc expr1 = getExprNodeDesc(TypeInfoFactory.intTypeInfo, "c1", 10, TypeInfoFactory.booleanTypeInfo, new GenericUDFOPEqual());
    ExprNodeDesc expr2 = getExprNodeDesc(TypeInfoFactory.intTypeInfo, "c1", 10, TypeInfoFactory.booleanTypeInfo, new GenericUDFOPEqual());
    ExprNodeDesc exprT = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFOPAnd(), new ArrayList<>(2));
    exprT.getChildren().add(expr1);
    exprT.getChildren().add(expr2);
    DecomposedPredicate dp = MonarchPredicateHandler.decomposePredicate(null, serDe, exprT);
    assertNotNull(dp);
    assertNotNull(dp.pushedPredicate);
    assertNull(dp.residualPredicate);
    assertEquals(dp.pushedPredicate.toString(), exprT.toString());
  }

  @Test
  public void testPredicate_Multiple_2() {
    ExprNodeDesc expr1 = getExprNodeDesc(TypeInfoFactory.intTypeInfo, "c1", 10, TypeInfoFactory.booleanTypeInfo, new GenericUDFOPEqual());
    ExprNodeDesc expr2 = getExprNodeDesc(TypeInfoFactory.intTypeInfo, "c1", 100, TypeInfoFactory.booleanTypeInfo, new GenericUDFOPLessThan());
    ExprNodeDesc exprT = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFOPAnd(), new ArrayList<>(2));
    exprT.getChildren().add(expr1);
    exprT.getChildren().add(expr2);
    DecomposedPredicate dp = MonarchPredicateHandler.decomposePredicate(null, serDe, exprT);
    assertNotNull(dp);
    assertNotNull(dp.pushedPredicate);
    assertNull(dp.residualPredicate);
    assertEquals(dp.pushedPredicate.toString(), exprT.toString());
  }

  @Test
  public void testPredicate_Multiple_3() {
    ExprNodeDesc expr1 = getExprNodeDesc(TypeInfoFactory.intTypeInfo, "c1", 10, TypeInfoFactory.booleanTypeInfo, new GenericUDFOPEqual());
    ExprNodeDesc expr2 = getExprNodeDesc(TypeInfoFactory.intTypeInfo, "c1", 10, TypeInfoFactory.booleanTypeInfo, new GenericUDFOPMinus());
    ExprNodeDesc exprT = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFOPAnd(), new ArrayList<>(2));
    exprT.getChildren().add(expr1);
    exprT.getChildren().add(expr2);
    DecomposedPredicate dp = MonarchPredicateHandler.decomposePredicate(null, serDe, exprT);
    assertNotNull(dp);
    assertNotNull(dp.pushedPredicate);
    assertNotNull(dp.residualPredicate);

    assertEquals(dp.pushedPredicate.toString(), expr1.toString());
    assertEquals(dp.residualPredicate.toString(), expr2.toString());
  }

  /**
   * Data provider for various type tests..
   *
   * @return the required arguments for the test
   * {@link MonarchPredicateHandlerTest#testIsMonarchTypeSupported(TypeInfo, boolean)}
   */
  @DataProvider
  public static Object[][] getTypeSupportData() {
    return new Object[][]{
      {TypeInfoFactory.intTypeInfo, true},
      {TypeInfoFactory.binaryTypeInfo, true},
      {TypeInfoFactory.longTypeInfo, true},
      {TypeInfoFactory.floatTypeInfo, true},
//      {TypeInfoFactory.unknownTypeInfo, false},
      {TypeInfoFactory.getDecimalTypeInfo(20, 10), true},
      {TypeInfoFactory.getCharTypeInfo(200), true},
      {TypeInfoFactory.getStructTypeInfo(Arrays.asList("c1", "c2"),
        Arrays.asList(TypeInfoFactory.floatTypeInfo, TypeInfoFactory.getUnionTypeInfo(
          Collections.singletonList(TypeInfoFactory.longTypeInfo)))), true},
      {TypeInfoFactory.getStructTypeInfo(Arrays.asList("c1", "c2"),
        Arrays.asList(TypeInfoFactory.dateTypeInfo, TypeInfoFactory.decimalTypeInfo)), true},
      {TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.timestampTypeInfo), true},
      {TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.doubleTypeInfo,
        TypeInfoFactory.getCharTypeInfo(100)), true},
      {TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.doubleTypeInfo,
        TypeInfoFactory.getVarcharTypeInfo(100)), true},
      {TypeInfoFactory.getListTypeInfo(
        TypeInfoFactory.getListTypeInfo(TypeInfoFactory.shortTypeInfo)), true},
      {TypeInfoFactory.getStructTypeInfo(Arrays.asList("c1", "c2"),
        Arrays.asList(TypeInfoFactory.floatTypeInfo, TypeInfoFactory.getUnionTypeInfo(
          Arrays.asList(TypeInfoFactory.decimalTypeInfo,
            TypeInfoFactory.getListTypeInfo(TypeInfoFactory.shortTypeInfo))))), true},
      {TypeInfoFactory.getVarcharTypeInfo(200), true},
    };
  }

  /**
   * Test for various types supported/not-supported as native types with MTable.
   *
   * @param ti       the hive type info
   * @param expected true if the respective type is supported using MTable; false otherwise
   */
  @Test(dataProvider = "getTypeSupportData")
  public void testIsMonarchTypeSupported(final TypeInfo ti, final boolean expected) {
    final ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(ti);
    assertEquals(MonarchPredicateHandler.isMonarchTypeSupported(oi), expected);
  }
}