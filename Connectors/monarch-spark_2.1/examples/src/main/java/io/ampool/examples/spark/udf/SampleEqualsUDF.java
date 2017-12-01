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

package io.ampool.examples.spark.udf;

import org.apache.spark.sql.api.java.UDF2;

/**
 * A sample Spark UDF that does comparison of two strings.
 * It returns true if the supplied strings are equal, false otherwise.
 * <p>
 * Once the UDF is compiled, it can be used in Spark SQL queries.
 * Following is an example demonstrating how to use it in Spark SQL.
 * <p>
 * {{
 *    val df = sqlContext.read.format("io.ampool").load("my_table")
 *
 *     df.registerTempTable("my_table")
 *
 *     // register the UDF with SQL Context with appropriate return type.
 *     sqlContext.udf.register("MyEquals", SampleEqualsUDF.SAMPLE_EQUALS_UDF, DataTypes.BooleanType)
 *
 *     // once registered, use the UDF in SQL queries in the same SQL Context
 *     sqlContext.sql("select * from my_table where MyEquals(column_1, 'constant_value')").show()
 * }}
 *
 * <p>
 * Created on: 2016-06-28
 * Since version: 0.3.2.0
 */
public class SampleEqualsUDF implements UDF2<String, String, Boolean> {
  public static final SampleEqualsUDF SAMPLE_EQUALS_UDF = new SampleEqualsUDF();

  /**
   * Tell if the supplied two strings are equal or not.
   *
   * @param s1 a string
   * @param s2 another string
   * @return true if both strings are equal; false otherwise
   * @throws Exception
   */
  @Override
  public Boolean call(final String s1, final String s2) throws Exception {
    return s1 == null ? s2 == null : s1.equals(s2);
  }
}