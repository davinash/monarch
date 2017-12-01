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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * An example demonstrating how to save an existing data-frame to Ampool
 * and load an existing Ampool table into Spark as a data-frame.
 * Once an Ampool table is loaded as a data-frame in Spark, one can
 * execute ML algorithms on it.
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
 *   bash$ mvn exec:java -Dspark.ampool.jar=<spark-ampool-jar-path> -Dexec.mainClass="io.ampool.examples.spark.SparkExampleML" -Dexec.args="localhost 10334"
 * </code>
 *
 * <p>
 *
 * Created on: 2016-05-06
 * Since version: 0.3.3.0
 */
public class SparkExampleML {
  private static final List<LabeledPoint> SAMPLE_ML_DATA = new ArrayList<LabeledPoint>(5) {{
    add(new LabeledPoint(1.0, Vectors.dense(0.0, 0.0, 0.0)));
    add(new LabeledPoint(2.0, Vectors.dense(0.1, 0.1, 0.1)));
    add(new LabeledPoint(3.0, Vectors.dense(0.2, 0.2, 0.2)));
    add(new LabeledPoint(4.0, Vectors.dense(9.0, 9.0, 9.0)));
    add(new LabeledPoint(5.0, Vectors.dense(9.1, 9.1, 9.1)));
    add(new LabeledPoint(6.0, Vectors.dense(9.2, 9.2, 9.2)));
  }};

  public static void main(final String[] args) {
    final String tableName = "SparkExampleML";

    /** get the locator host/port from arguments, if specified.. **/
    final String locatorHost = args.length > 0 ? args[0] : "localhost";
    final int locatorPort = args.length > 1 ? Integer.valueOf(args[1]) : 10334;

    int numClusters = Integer.getInteger("numClusters", 2);
    int numIterations = Integer.getInteger("numIterations", 20);

    /** create SparkContext **/
    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkExampleDF");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    /** create data-frame from sample ML data **/
    Dataset df = sqlContext.createDataFrame(jsc.parallelize(SAMPLE_ML_DATA), LabeledPoint.class);
    df.show();

    Map<String, String> options = new HashMap<>(2);
    options.put("ampool.locator.host", locatorHost);
    options.put("ampool.locator.port", String.valueOf(locatorPort));

    /** overwrite existing table, if specified.. **/
    SaveMode saveMode = Boolean.getBoolean("overwrite") ? SaveMode.Overwrite : SaveMode.ErrorIfExists;

    /** save the dataFrame to Ampool as `tableName' **/
    df.write().format("io.ampool").options(options).mode(saveMode).save(tableName);

    /** load the data-frame from Ampool `tableName' **/
    Dataset df1 = sqlContext.read().format("io.ampool").options(options).load(tableName);

    System.out.println("########## DATA FROM AMPOOL ############");
    df1.show();

    /** execute KMeans fit on the data loaded from Ampool **/
    KMeans kMeans = new KMeans().setK(numClusters).setMaxIter(numIterations)
      .setFeaturesCol("features").setPredictionCol("prediction");
    KMeansModel model = kMeans.fit(df1);

    Vector[] cost = model.clusterCenters();
    System.out.println("# Sum of Squared Errors = " + Arrays.toString(cost));
  }
}
