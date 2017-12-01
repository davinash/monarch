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

package io.ampool.examples.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * An example demonstrating how to save an existing data-frame created by reading CSV to Ampool
 * and load an existing Ampool table into Spark as a data-frame.
 * Once an Ampool table is loaded as a data-frame in Spark, one can
 * perform all operations that are supported on a data-frame. It includes,
 * for example, the filter operations, column selection, and aggregation
 * operations. These are now transparent to the user and these are
 * redirected to Ampool wherever required.
 * <p>
 * The options can be provided for writing/loading a data-frame to/from
 * Ampool as a map of Strings (i.e. Map<String,String>):
 *   ampool.locator.host -- the Ampool locator hostanme
 *   ampool.locator.port -- the Ampool locator port number
 *   ampool.batch.size   -- the batch-size to be used during read/write
 *
 *   By Default FTable is used to save an existing data-frame to Ampool and
 *   load an existing Ampool FTable into spark as a data-frame.
 *   Set following options to work with Ampool MTable.
 *
 *   ampool.table.type   -- "ordered" for ORDERED MTable
 *   ampool.table.type   --  "unordered" for UNORDERED MTable.
 *
 *   To work with kerberised ampool cluster, set the following options,
 *   security.enable.kerberos.authc       --  "true" to enable kerberos authentication
 *   security.kerberos.service.principal  --  "Ampool locator service principal"
 *   security.kerberos.user.principal     --  "user principal"
 *   security.kerberos.user.keytab        --  "user keytab"
 *
 * <p>
 * Compile and run the example:
 * <code>
 *   bash$ mvn clean install
 *   bash$ mvn exec:java -Dspark.ampool.jar=<spark-ampool-jar-path> -Dexec.mainClass="io.ampool.examples.spark.SparkExampleDFUsingCSV" -Dexec.args="localhost 10334"
 * </code>
 *
 * <p>
 * Created on: 2016-04-18
 * Since version: 0.3.2.0
 */
public class SparkExampleDFUsingCSV {

  /**
   * Main method..
   *
   * @param args the arguments
   */
  public static void main(final String[] args) {
    final String tableName = "SparkExampleDFUsingCSV";

    /** get the locator host/port from arguments, if specified.. **/
    final String locatorHost = args.length > 0 ? args[0] : "localhost";
    final int locatorPort = args.length > 1 ? Integer.valueOf(args[1]) : 10334;

    /** create SparkContext **/
    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkExampleDFUsingCSV");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    StructType customSchema = new StructType(new StructField[] {
            new StructField("year", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("make", DataTypes.StringType, true, Metadata.empty()),
            new StructField("model", DataTypes.StringType, true, Metadata.empty()),
            new StructField("comment", DataTypes.StringType, true, Metadata.empty()),
            new StructField("blank", DataTypes.StringType, true, Metadata.empty())
    });

    DataFrame df = sqlContext.read()
            .format("com.databricks.spark.csv")
            .schema(customSchema)
            .option("header", "true")
            .load("cars.csv");


    /** print schema of the data-frame **/
    df.printSchema();

    df.show();

    Map<String, String> options = new HashMap<>(3);
    options.put("ampool.locator.host", locatorHost);
    options.put("ampool.locator.port", String.valueOf(locatorPort));

    /** overwrite existing table, if specified.. **/
    SaveMode saveMode = Boolean.getBoolean("overwrite") ? SaveMode.Overwrite : SaveMode.ErrorIfExists;

    /** save the dataFrame to Ampool as `tableName' **/
    df.write().format("io.ampool").options(options).mode(saveMode).save(tableName);

    System.out.println("########## DATA FROM AMPOOL ############");

    /** load the data-frame from Ampool `tableName' **/
    DataFrame df1 = sqlContext.read().format("io.ampool").options(options).load(tableName);

    /** show the contents of loaded data-frame **/
    df1.show();

    /** show the total number of rows in data-frame **/
    System.out.println("# NumberOfRowsInDataFrame= " + df1.count());

    /** data-frame with filter **/
    df1.filter("year > 1997").show();

    /** data-frame with selected columns **/
    df1.select("year", "make", "model", "comment").show();

    df1.registerTempTable("temp_table");

    sqlContext.sql("select * from temp_table order by year").show();
  }
}
