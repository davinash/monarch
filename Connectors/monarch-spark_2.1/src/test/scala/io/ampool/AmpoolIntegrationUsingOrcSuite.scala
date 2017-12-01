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
package io.ampool

import java.security.SecureRandom

import io.ampool.monarch.table.client.MClientCacheFactory
import io.ampool.monarch.table.exceptions.{MCacheClosedException, MCacheInternalErrorException, RowKeyOutOfRangeException}
import io.ampool.monarch.table.filter.{FilterList, SingleColumnValueFilter}
import io.ampool.monarch.table.ftable.FTableDescriptor
import io.ampool.monarch.table.{MTableDescriptor, Scan}
import io.ampool.monarch.types.{BasicTypes, CompareOp, TypeHelper}
import org.apache.geode.internal.cache.MonarchCacheImpl
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConversions._
import scala.runtime.ScalaRunTime.stringOf

/**
 * The integration tests for Spark-Ampool integration with FTable.
 *
 *
 * Since version: 1.1.1
 */
@RunWith(classOf[JUnitRunner])
class AmpoolIntegrationUsingOrcSuite extends FunSuite with BeforeAndAfterAll with Matchers {
  var sqlContext: SQLContext = _
  var testBase: TestBase = _
  var params: Map[String, String] = _
  var baseDataFrame: DataFrame = null

  /** defaults **/
  val tableName: String = "test_immutable_with_orc"
  val sampleJson: String = ClassLoader.getSystemResource("sample.json").getFile

  val blockOptions = Map((Constants.AmpoolBlockFormat, "ORC_BYTES"), (Constants.AmpoolBlockSize, "1"))
  /**
   * Create SparkContext, SQLContext, and Geode Locator/Servers before tests are run..
   */
  override def beforeAll {
    /** start the DUnit base **/
    testBase = new TestBase("AmpoolIntegrationUsingOrcSuite")
    testBase.setUp()

    /** create spark-context **/
    val sparkConf = new SparkConf()
    sparkConf.set(Constants.AmpoolLocatorPortKey, testBase.getLocatorPort)
    sparkConf.set(Constants.AmpoolLogFile, "dunit/t.log")
    sparkConf.set(Constants.AmpoolReadTimeout, "12345")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    val sparkContext = new SparkContext("local[*]", "AmpoolIntegrationSuite", sparkConf)
    Logger.getRootLogger.setLevel(Level.OFF)
    sqlContext = new SQLContext(sparkContext)

    /** options, if any **/
    params = Map((Constants.AmpoolLocatorPortKey, testBase.getLocatorPort),
      (Constants.AmpoolLogFile, "dunit/t.log"),(Constants.AmpoolBatchSize, "10"),
      (Constants.AmpoolRedundancy, "3"),(Constants.AmpoolReadTimeout, "12345")) ++ blockOptions

    /** delete old table, if any, to avoid any clashes **/
    val conn: MTableWrapper = new MTableWrapper(tableName, params)
    if (conn.getCache.getAdmin.fTableExists(tableName))
      conn.getCache.getAdmin.deleteFTable(tableName)

    /** load sample JSON data and save into Ampool for various tests.. **/
    baseDataFrame = loadJsonData(sampleJson)
    baseDataFrame.write.format("io.ampool").options(params).mode(SaveMode.Overwrite).save(tableName)

    /* assert on the read-timeout */
    val cache = MonarchCacheImpl.getGeodeCacheInstance
    cache.getDefaultPool.getReadTimeout shouldEqual 12345
  }

  /**
   * Cleanup the required.. tear-down the DUnit..
   */
  override def afterAll {
    val cache = MClientCacheFactory.getAnyInstance
    if (cache != null && !cache.isClosed) {
      if (cache.getAdmin.existsFTable(tableName)) cache.getAdmin.deleteFTable(tableName)
      cache.close()
    }
    testBase.tearDown2()
    sqlContext.sparkSession.stop()
  }

  /**
   * Load sample JSON data into data-frame and return the data-frame.
   *
   * @param fileName the file-name to load data from
   * @return the data frame created from the sample json data
   */
  def loadJsonData(fileName: String): DataFrame = {
    sqlContext.read.format("json").load(fileName)
  }

  def dataFrameToString(dataFrame: DataFrame, isAmpoolTable: Boolean = false): Array[String] = {
    if (isAmpoolTable) {
      //Discard "INSERTION_TIMESTAMP" from the dataframe. As of now it is the first column so drop it
      dataFrame.drop(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME).collect().map(e => e.toString()).sorted
    } else {
      dataFrame.collect().map(e => TypeHelper.deepToString(e)).sorted
    }
  }

  def mTableToString(tableName: String): Array[String] = {
    val cache = getAmpoolCache(params)
    val scan = new Scan()
    val buckets = new java.util.HashSet[Integer]
    (0 to 112).foreach(e=>buckets.add(e.asInstanceOf[java.lang.Integer]))
    scan.setBucketIds(buckets)
    val scanner = cache.getFTable(tableName).getScanner(scan).iterator()
    val builder = Array.newBuilder[String]

    while (scanner.hasNext) {
      val nxt = scanner.next
      builder += ("[" + nxt.getCells.dropRight(1).map(_.getColumnValue).map(stringOf(_)).mkString(",") + "]")
    }
    builder.result.sorted
  }

  /**
   * Save a data-frame to Ampool and assert that the contents are
   * same as the original data-frame.
   *
   * @param df1 the data-frame to be saved to Ampool
   * @param tableName the table-name in Ampool
   */
  def assertDataFrameResults(df1: DataFrame, tableName: String): Unit = {
    /** save ML data-frame to Ampool **/
    df1.write.format("io.ampool").options(blockOptions).mode(SaveMode.Overwrite).save(tableName)

    /** load the saved data-frame from Ampool and also read from MTable.. **/
    val df2 = sqlContext.read.format("io.ampool").load(tableName)

    val results1 = TestHelper.dataFrameToStringComplexTypes(df1)
    val results2 = TestHelper.mTableToStringComplexTypes(tableName, params)
    val results3 = TestHelper.dataFrameToStringComplexTypes(df2)

    /** assert on the required.. **/
    results2.length shouldEqual results1.length
    results3.length shouldEqual results1.length
    results2 shouldEqual results1
    results3 shouldEqual results1
  }

  test("BasicWrite") {
    val dataFrame = loadJsonData(sampleJson)
    val resultsFromDataFrame = dataFrameToString(dataFrame)
    val resultsFromMTable = mTableToString(tableName)

    /** assert on the required.. **/
    resultsFromMTable.length shouldEqual resultsFromDataFrame.length
    resultsFromMTable shouldEqual resultsFromDataFrame
  }

  test("BasicReadWriteWithTimeStampQuery") {
    var newTable = tableName + "_01"
    //Load a JSON document as a DF
    val dataFrame = loadJsonData(sampleJson)
    dataFrame.count shouldEqual 32

    //save a DF as a FTable
    dataFrame.write.format("io.ampool").options(blockOptions).save(newTable)
    println("DF successfully save as a FTable!")

    //Do scan and verify the count
    val resultsFromMTable = mTableToString(newTable)
    resultsFromMTable.length shouldEqual 32

    //load a FTable as a DF
    var dfFromTable = sqlContext.read.format("io.ampool").load(newTable)
    dfFromTable.drop(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME).show(50)
    dfFromTable.count shouldEqual 32

    //Save a dfFromTable to again new FTable
    dfFromTable.write.format("io.ampool").options(blockOptions).save(newTable+"11")

    sqlContext.read.format("io.ampool").load(newTable+"11").registerTempTable("my_table")

    //var results = sqlContext.sql("select * from my_table WHERE __INSERTION_TIMESTAMP__ > 123456").drop(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME).collect
    var results = sqlContext.sql("select * from my_table WHERE " + FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME + " > 123").collect
    results.length shouldEqual 32
    getAmpoolCache(params).getAdmin.deleteFTable(newTable)
  }

  test("BasicRead") {
    val readFromJson = loadJsonData(sampleJson)
    val readFromTable = sqlContext.read.format("io.ampool").load(tableName)

    val fromJson = dataFrameToString(readFromJson)
    val fromTable = dataFrameToString(readFromTable, true)

    readFromTable.show

    /** assert on the required.. **/
    fromTable.length shouldEqual fromJson.length
    fromTable shouldEqual fromJson
  }

  test("ReadWithFilter") {
    val df = sqlContext.read.format("io.ampool").load(tableName).filter("size = 4096")
    val results = df.drop(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME).collect

    df.count shouldEqual 13
    results.length shouldEqual 13
    results.map(_.toString()) should not contain ",4096,"
  }
  test("ReadWithFilter_1") {
    val df = sqlContext.read.format("io.ampool").load(tableName).filter("size = 4096")
    val results = df.drop(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME).collect

    df.count shouldEqual 13
    results.length shouldEqual 13
    results.map(_.toString()) should not contain ",4096,"
  }

  test("ReadWithAggregation") {
    val results = sqlContext.read.format("io.ampool").load(tableName)
      .agg(("size", "sum")).collect

    results(0).get(0) shouldEqual 129662
  }

  test("ReadWithSelectedColumns") {
    val results = sqlContext.read.format("io.ampool").load(tableName).select("size", "path").collect

    results.length shouldEqual 32
    results(0).length shouldEqual 2
    results(0).get(0) shouldBe a[java.lang.Long]
    results(0).get(1) shouldBe a[java.lang.String]
  }

  test("ReadAsTableWithSQL") {
    sqlContext.read.format("io.ampool").load(tableName).registerTempTable("my_table")

    var results = sqlContext.sql("select * from my_table").drop(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME).collect
    results.length shouldEqual 32

    results = sqlContext.sql("select * from my_table where size = 4096").drop(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME).collect
    results.length shouldEqual 13

    results = sqlContext.sql("select * from my_table where perms = '-rw-rw-r--'").drop(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME).collect
    results.length shouldEqual 19
    results = sqlContext.sql("select * from my_table where perms = '-rw-rw-r--' or size = 4096").drop(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME).collect
    results.length shouldEqual 32
  }

  test("ReadAsTableWithSQL_SelectedColumns") {
    sqlContext.read.format("io.ampool").load(tableName).registerTempTable("my_table")

    var results = sqlContext.sql("select size from my_table").collect
    results.length shouldEqual 32

    results = sqlContext.sql("select perms,size from my_table where size = 4096").collect
    results.length shouldEqual 13

    results = sqlContext.sql("select size,perms from my_table where perms = '-rw-rw-r--'").collect
    results.length shouldEqual 19
  }

  /** a simple test to make sure that scanner with predicate works as expected **/
  test("ScanWithPredicates") {
    val cache = getAmpoolCache(params)
    val scan = new Scan()
    /*scan.setPredicates(Array(new MPredicateHolder(3, BasicTypes.LONG,
      CompareOp.EQUAL, 4096)))*/
    val filter = new SingleColumnValueFilter("size",CompareOp.EQUAL, 4096)
    val filterList = new FilterList()
    filterList.addFilter(filter)
    scan.setFilter(filterList)
    val set = new java.util.HashSet[Integer]
    (0 to 112).foreach(e=>set.add(e.asInstanceOf[java.lang.Integer]))
    scan.setBucketIds(set)
    val scanner = cache.getFTable(tableName).getScanner(scan).iterator()

    var count: Int = 0
    while (scanner.hasNext) {
      count += 1
      scanner.next()
    }
    count shouldEqual 13
  }

  test("Write_Overwrite") {
    val newTableName = tableName + "_NEW"

    baseDataFrame.write.format("io.ampool").options(blockOptions).save(newTableName)

    /** save the data-frame to Ampool.. **/
    val df1 = sqlContext.read.format("io.ampool").load(newTableName)

    df1.count shouldEqual baseDataFrame.count

    /** overwrite the table with different data and then assert on the required.. **/
    val df2 = baseDataFrame.filter("size = 4096")
    df2.write.format("io.ampool").options(blockOptions).mode(SaveMode.Overwrite).save(newTableName)

    val df3 = sqlContext.read.format("io.ampool").load(newTableName)

    df3.count shouldEqual df2.count
    df3.count should not equal baseDataFrame.count
    getAmpoolCache(params).getAdmin.deleteFTable(newTableName)
  }

  /*
  write_Overwrite test with user-defined partitioning column set
   */
  test("Write_OverwriteWithPartitionColumn") {
    val newTableName = tableName + "_NEW_PC"
    val params = Map((Constants.AmpoolPartitioningColumn, "inode"))
    baseDataFrame.write.format("io.ampool").options(params ++ blockOptions).save(newTableName)

    /** save the data-frame to Ampool.. **/
    val df1 = sqlContext.read.format("io.ampool").load(newTableName)

    df1.count shouldEqual baseDataFrame.count

    /** overwrite the table with different data and then assert on the required.. **/
    val df2 = baseDataFrame.filter("size = 4096")
    df2.write.format("io.ampool").options(params ++ blockOptions).mode(SaveMode.Overwrite).save(newTableName)

    val df3 = sqlContext.read.format("io.ampool").load(newTableName)

    df3.count shouldEqual df2.count
    df3.count should not equal baseDataFrame.count
    getAmpoolCache(params).getAdmin.deleteFTable(newTableName)
  }

  /** Test for some complex types.. i.e. struct/map/array **/
  test("WriteAndReadComplexTypes") {
    val newTableName = tableName + "_COMPLEX"
    val fileName = ClassLoader.getSystemResource("sample.parquet").getFile
    val df1 = sqlContext.read.load(fileName)
    df1.write.format("io.ampool").options(blockOptions).mode(SaveMode.Overwrite).save(newTableName)

    val df2 = sqlContext.read.format("io.ampool").load(newTableName)
    df2.show()

    val results1 = TestHelper.dataFrameToStringComplexTypes(df1)
    val results2 = TestHelper.mTableToStringComplexTypes(newTableName, params)
    val results3 = TestHelper.dataFrameToStringComplexTypes(df2)

    /** assert on the required.. **/
    results2.length shouldEqual results1.length
    results3.length shouldEqual results1.length
    results2.mkString(", ") shouldEqual results1.mkString(", ")
    results3.mkString(", ") shouldEqual results1.mkString(", ")
    getAmpoolCache(params).getAdmin.deleteFTable(newTableName)
  }

  /** Test for some complex types with SQL and filters **/
  test("ReadComplexTypesSQL") {
    val newTableName = tableName + "_COMPLEX"
    val fileName = ClassLoader.getSystemResource("sample.parquet").getFile
    val df1 = sqlContext.read.load(fileName)
    df1.write.format("io.ampool").options(blockOptions).mode(SaveMode.Overwrite).save(newTableName)

    val dfA = sqlContext.read.format("io.ampool").load(newTableName)

    /** register data-frame for use in Spark SQL **/
    dfA.registerTempTable("my_table")

    sqlContext.sql("select * from my_table").show(false)

    val dfS1 = sqlContext.sql("select * from my_table where sStruct.anInt = 2")
    dfS1.count shouldEqual 1

    val dfS2 = sqlContext.sql("select * from my_table where sArray[0].anInt = 1")
    dfS2.select("iArray").take(1).apply(0).getSeq[Int](0).sorted shouldEqual Seq(1,2,3)

    val dfS3 = sqlContext.sql("select * from my_table where sArray[0].anArray[0] = 1")
    dfS3.count shouldEqual 1
    dfS3.select("sStruct").take(1).apply(0).getAs[Row](0).getInt(0) shouldEqual 1

    val dfS4 = sqlContext.sql("select sArray[0].anArray[0] as MyColumn from my_table where sArray[0].anArray[0] >= 0")
    dfS4.count shouldEqual 2
    dfS4.select("MyColumn").collect.map(_.getDouble(0)).sorted shouldEqual Array(1.0,5.0)

    /** negative.. should return no records **/
    val dfS5 = sqlContext.sql("select * from my_table where sArray[0].anArray[0] >= 100")
    dfS5.count shouldEqual 0
    getAmpoolCache(params).getAdmin.deleteFTable(newTableName)
  }

  /** Test for Spark-ML data-frame with Sparse and Dense vectors.. **/
  test("WriteAndRead_SparkML_Vector") {
    val tableName = "ml_table"
    val denseSeq = Seq((1, Vectors.dense(0.0, 0.0, 0.0)), (2, Vectors.dense(0.1, 0.1, 0.1)),
      (3, Vectors.dense(0.2, 0.2, 0.2)), (4, Vectors.dense(9.0, 9.0, 9.0)),
      (5, Vectors.dense(9.1, 9.1, 9.1)), (6, Vectors.dense(9.2, 9.2, 9.2)))

    assertDataFrameResults(sqlContext.createDataFrame(denseSeq).toDF("id", "features"), tableName)
    getAmpoolCache(params).getAdmin.deleteFTable(tableName)

    val sparseSeq = Seq((1, Vectors.sparse(3, Seq((0, 1.0), (2, 1.2)))),
      (2, Vectors.sparse(3, Seq((1, 2.1), (2, 2.2)))),
      (3, Vectors.sparse(3, Seq((0, 3.0), (1, 3.1)))))

    assertDataFrameResults(sqlContext.createDataFrame(sparseSeq).toDF("id", "features"), tableName)

    getAmpoolCache(params).getAdmin.deleteFTable(tableName)
  }

//  /** Test with persistence enabled **/
//  test("BasicWriteWithPersistenceAndRestart") {
//    val options = Map(("ampool.enable.persistence", "async"))
//    val newTableName = tableName + "_PERSIST"
//    baseDataFrame.write.format("io.ampool").options(options ++ blockOptions).save(newTableName)
//
//    testBase.restart()
//
//    val resultsFromDataFrame = dataFrameToString(baseDataFrame)
//    val resultsFromMTable = dataFrameToString(sqlContext.read.format("io.ampool").load(newTableName), true)
//
//    /** assert on the required.. **/
//    resultsFromMTable.length shouldEqual resultsFromDataFrame.length
//    resultsFromMTable shouldEqual resultsFromDataFrame
//    getAmpoolCache(params).getAdmin.deleteFTable(newTableName)
//  }

//  /** Test for some complex types.. i.e. struct/map/array with persistence enabled **/
//  test("WriteAndReadComplexTypesWithPersistenceAndRestart") {
//    val newTableName = tableName + "_COMPLEX_PERSIST"
//    val fileName = ClassLoader.getSystemResource("sample.parquet").getFile
//    val df1 = sqlContext.read.load(fileName)
//    val options = Map(("ampool.enable.persistence", "async"))
//    df1.write.format("io.ampool").mode(SaveMode.Overwrite).options(options ++ blockOptions).save(newTableName)
//
//    val df2 = sqlContext.read.format("io.ampool").load(newTableName)
//
//    val results1 = dataFrameToString1(df1)
//    val results2 = mTableToString1(newTableName)
//    val results3 = dataFrameToString1(df2)
//
//    /** assert on the required.. **/
//    results2.length shouldEqual results1.length
//    results3.length shouldEqual results1.length
//    results2 shouldEqual results1
//    results3 shouldEqual results1
//
//    testBase.restart()
//
//    val df3 = sqlContext.read.format("io.ampool").load(newTableName)
//
//    val results4 = dataFrameToString1(df1)
//    val results5 = mTableToString1(newTableName)
//    val results6 = dataFrameToString1(df3)
//
//    /** assert on the required.. **/
//    results5.length shouldEqual results4.length
//    results6.length shouldEqual results4.length
//    results5 shouldEqual results4
//    results6 shouldEqual results4
//    getAmpoolCache(params).getAdmin.deleteFTable(newTableName)
//  }
}