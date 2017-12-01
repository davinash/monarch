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
package io.ampool.falkonry

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.PeriodFormatterBuilder

import scala.collection.immutable.HashMap
import scala.util.Random

/**
  * Created on: 2016-09-08
  * Since version: 1.0.3
  */
class Utils extends Serializable {

  val roundByPeriod = (time: Long, period: String) => {
    val formatter = new PeriodFormatterBuilder()
      .appendMillis().appendSuffix("ms")
      .appendSeconds().appendSuffix("s")
      .appendMinutes().appendSuffix("m")
      .appendHours().appendSuffix("h")
      .appendDays().appendSuffix("d")
      .toFormatter
    val millis = formatter.parsePeriod(period).toStandardDuration.getMillis
    (time / millis) * millis
  }

  val thing = (s: String, location: String, delimiter: String) => {
    if (location == "prefix")
      s.splitAt(s.indexOf(delimiter))._2.tail
    else
      s.splitAt(s.lastIndexOf(delimiter))._1
  }

  val value = udf { (s: String, v: String, name: String, location: String, delimiter: String) =>
    val signal = if (location == "prefix")
      s.splitAt(s.indexOf(delimiter))._1
    else
      s.splitAt(s.lastIndexOf(delimiter))._2.tail

    if (signal == name) v
    else null
  }

  val trimOrNullOrQuote = udf { (s: String) =>
    if (s == null || s.trim.isEmpty)
      null
    else
      s.trim.replaceAll("\"", "")
  }

}

class AmpoolIO() {

  private val host = "localhost"
  private val port = FalkonryTestSuite.LocatorPort
  private val saveMode = "append"
  private val storageMode = "async"
  private val numPartitions = 8
  private val options = HashMap[String, String](
    ("ampool.locator.host", host), ("ampool.locator.port", port), ("ampool.enable.persistence", storageMode)
  )

  private def tableName(tenant: String, measurement: String): String = {
    val tenantStr = if (tenant == null) "" else tenant + "_"
    tenantStr + measurement
  }

  def read(sqlContext: SQLContext, tenant: String, measurement: String): DataFrame = {
    val table = tableName(tenant, measurement)
    sqlContext.read.format("io.ampool").options(options).load(table)
  }

  def write(tenant: String, measurement: String, data: DataFrame, partitionBy: List[String]): Unit = {
    val table = tableName(tenant, measurement)
    data.repartition(numPartitions, partitionBy.map(data.apply(_)): _*)
      .write
      .format("io.ampool")
      .mode(saveMode)
      .options(HashMap[String, String](("ampool.batch.prefix", Random.alphanumeric take 20 mkString (""))) ++ (options))
      .save(table)
  }
}

object HistorianTest {

  def main(args: Array[String]): Unit = {
    //val config = ConfigFactory.parseFile(new File("/etc/tercel/tercel.properties"))
    val seriesAdapter = {
      //val ampoolConfig = config.getConfig("tercel.io.timeSeries.ampool")
      new AmpoolIO()
    }
    val utils = new Utils

    val conf = new SparkConf()
      .setAppName("test-1")
      .set("spark.ui.showConsoleProgress", "false")
      .set("spark.sql.shuffle.partitions", "8")
      .set("spark.driver.allowMultipleContexts", "true")
      .setIfMissing("spark.master", "local[*,8]")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    sqlContext.udf.register("roundByPeriod", new Utils().roundByPeriod)

    //    val tempDf = sqlContext.read
    //      .format("com.databricks.spark.csv")
    //      .option("header", "true")
    //      .option("parserLib", "univocity")
    //      .option("ignoreLeadingWhiteSpace", "true")
    //      .option("ignoreTrailingWhiteSpace", "true")
    //      .load("/Users/phagunbaya/Desktop/histo.csv")

    val tempDf = sqlContext.read.json(FalkonryTestSuite.HistJsonFile)

    val timeCol = tempDf.col("time")
    val tagCol = tempDf.col("tag")
    val valueCol = tempDf.col("value")
    val location = "suffix"
    val delimiter = "_"

    val thingCol = {
      val thing = udf { (x: String) => {
        utils.thing(x, location, delimiter)
      }
      }
      utils.trimOrNullOrQuote(thing(tagCol)).as("thing")
    }

    val signals = Seq[String]("metric1", "metric2", "metric3", "metric10")

    val inputCols = signals.map { input =>
      val nameCol = lit(input)
      val inputCol = utils.value(tagCol, valueCol, nameCol, lit(location), lit(delimiter))
      inputCol.cast(DoubleType).as(input)
    }

    val columns = List(timeCol, thingCol) ++ inputCols

    val parsedData = tempDf.select(columns: _*)

    //tempDf.show()

    println("====== writing ======")
    val tenant = Random.alphanumeric take 5 mkString ("")
    val measurement = Random.alphanumeric take 5 mkString ("")

    seriesAdapter.write(tenant, measurement, parsedData, List.apply("thing"))


    println("====== reading ======")
    val table = tenant + "_" + measurement
    val readDf = seriesAdapter.read(sqlContext, tenant, measurement)

    println("====== Printing All Data ======")
//    readDf.collect().foreach(println)
    println("====== Printing All Data Done ======")

    readDf.registerTempTable(table)
    val query_response = readDf.sqlContext.sql("select first(`time`) as `time`, avg(`metric1`) as `value` from `" + table + "` where `time` >= 1459050600000 and `time` <= 1459914600000 and `thing`=\"machine1\" group by roundByPeriod(`time`, \"10m\") order by `time` asc")
//    query_response.collect().foreach(println)

    /** assert that the result data is as expected **/
    FalkonryTestSuite.AssertOnData(readDf, query_response)

    if(!sparkContext.isStopped)
      sparkContext.stop()
  }
}
