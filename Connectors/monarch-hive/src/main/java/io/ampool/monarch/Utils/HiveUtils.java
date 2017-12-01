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

import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hive.common.util.HiveVersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


/**
 * Utility class for loading the hive utility methods.
 */
public final class HiveUtils {
  private static final Logger logger = LoggerFactory.getLogger(HiveUtils.class);
  private static String HIVE_UTILITIES_CLASS_NAME = "org.apache.hadoop.hive.ql.exec.Utilities";
  private static final String HIVE20_UTILITIES_CLASS_NAME = "org.apache.hadoop.hive.ql.exec.SerializationUtilities";
  private static final String HIVE_UTILITIES_DESER_METHOD_NAME = "deserializeExpression";
  private static Method deserializeMethod = null;


  /**
   * get the utility class used
   * @return name of the utility class
   */
  public static String getHiveUtilitiesClassName(){
    return HIVE_UTILITIES_CLASS_NAME;
  }

  static {
    try {
      Class<?> utilitiesClass = Class.forName(HIVE_UTILITIES_CLASS_NAME);
      deserializeMethod = utilitiesClass.getMethod(HIVE_UTILITIES_DESER_METHOD_NAME, String.class);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      logger.error("Exception in loading default serialization classes. Exception {}" , e);
    }

    if(deserializeMethod == null){
      try {
        HIVE_UTILITIES_CLASS_NAME = HIVE20_UTILITIES_CLASS_NAME;
        Class<?> utilitiesClass = Class.forName(HIVE_UTILITIES_CLASS_NAME);
        deserializeMethod = utilitiesClass.getMethod(HIVE_UTILITIES_DESER_METHOD_NAME, String.class);
      } catch (ClassNotFoundException | NoSuchMethodException e) {
        logger.error("Exception in loading serialization classes. Exception {}" , e);
      }
    }

    if(deserializeMethod == null){
      try {
        deserializeMethod = HiveUtils.class.getMethod("deserializeExpressionDummy", String.class);
      } catch (NoSuchMethodException e) {
        logger.error("Exception in loading serialization classes. Exception {}" , e);
      }
    }

  }


  /**
   * Deserialize the the expression using the hive version specific utility classes
   * @param expression expression to deserialize
   * @return the deserialized ExprNodeGenericFuncDesc
   */
  public static ExprNodeGenericFuncDesc deserializeExpression(String expression){
    try {
      return (ExprNodeGenericFuncDesc) deserializeMethod.invoke(null, expression);
    } catch (IllegalAccessException | InvocationTargetException e) {
      logger.error("Exception in invoking deserialization method. Exception {}" , e);
    }
    logger.info("Pushdown filters are skipped as the respective deserialization expression method not found.");
    return null;
  }

  public static ExprNodeGenericFuncDesc deserializeExpressionDummy(String expression){
    logger.info("Pushdown filters are skipped as the respective deserialization expression method not found.");
    return null;
  }

}
