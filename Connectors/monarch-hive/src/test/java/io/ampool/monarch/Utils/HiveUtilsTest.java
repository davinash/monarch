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

package io.ampool.monarch.Utils;

import static org.junit.Assert.*;

import io.ampool.monarch.hive.MonarchPredicateHandler;
import io.ampool.monarch.hive.MonarchPredicateHandlerTest;
import io.ampool.monarch.hive.MonarchSerDe;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.types.CompareOp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;

public class HiveUtilsTest {

  @Test
  public void deserializeExpression() throws Exception {
    assertEquals(HiveUtils.getHiveUtilitiesClassName(), "org.apache.hadoop.hive.ql.exec.Utilities");
    //HiveUtils.deserializeExpression(""); //exception
    //HiveUtils.deserializeExpression(null); //exception
    final String expression = "AQEAamF2YS51dGlsLkFycmF5TGlz9AECAQFvcmcuYXBhY2hlLmhhZG9vcC5oaXZlLn"
        + "FsLnBsYW4uRXhwck5vZGVDb2x1bW5EZXPjAQFjsQAABQECb3JnLmFwYWNoZS5oYWRvb3AuaGl2ZS5zZXJkZTIu"
        + "dHlwZWluZm8uUHJpbWl0aXZlVHlwZUluZu8BAWlu9AEDb3JnLmFwYWNoZS5oYWRvb3AuaGl2ZS5xbC5wbGFuLk"
        + "V4cHJOb2RlQ29uc3RhbnREZXPjAQECBgIUAQRvcmcuYXBhY2hlLmhhZG9vcC5oaXZlLnFsLnVkZi5nZW5lcmlj"
        + "LkdlbmVyaWNVREZPUEVxdWHsAQAAAYI9AUVRVUHMAQVvcmcuYXBhY2hlLmhhZG9vcC5pby5Cb29sZWFuV3JpdG"
        + "FibOUBAAABAgEBYm9vbGVh7g==";
    ExprNodeGenericFuncDesc exprNodeGenericFuncDesc = HiveUtils.deserializeExpression(expression);
    assertNotNull(exprNodeGenericFuncDesc);
  }
}