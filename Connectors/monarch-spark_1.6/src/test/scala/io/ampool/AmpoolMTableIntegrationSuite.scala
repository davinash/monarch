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

import io.ampool.client.AmpoolClient
import io.ampool.monarch.table.client.MClientCacheFactory
import io.ampool.monarch.table.exceptions.{MCacheClosedException, MCacheInternalErrorException, RowKeyOutOfRangeException}
import io.ampool.monarch.table.filter.{FilterList, SingleColumnValueFilter}
import io.ampool.monarch.table.internal.MTableUtils
import io.ampool.monarch.table.{MTableDescriptor, Scan}
import io.ampool.monarch.types.{BasicTypes, CompareOp}
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
 * The integration tests for Spark-Ampool integration.
 *
 * Created on: 2016-02-26
 * Since version: 0.2.0
 */
@RunWith(classOf[JUnitRunner])
class AmpoolMTableIntegrationSuite extends FunSuite with BeforeAndAfterAll with Matchers {
  var sqlContext: SQLContext = _
  var testBase: TestBase = _
  var params: Map[String, String] = _
  var baseDataFrame: DataFrame = null

  /** defaults **/
  val tableName: String = "test"
  val sampleJson: String = ClassLoader.getSystemResource("sample.json").getFile

  /**
   * Create SparkContext, SQLContext, and Geode Locator/Servers before tests are run..
   */
  override def beforeAll {
    /** start the DUnit base **/
    testBase = new TestBase("AmpoolMTableIntegrationSuite")
    testBase.setUp()

    /** create spark-context **/
    val sparkConf = new SparkConf()
    sparkConf.set(Constants.AmpoolLocatorPortKey, testBase.getLocatorPort)
    sparkConf.set(Constants.AmpoolLogFile, "dunit/t.log")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    val sparkContext = new SparkContext("local[*]", "AmpoolMTableIntegrationSuite", sparkConf)
    Logger.getRootLogger.setLevel(Level.OFF)
    sqlContext = new SQLContext(sparkContext)

    /** options, if any **/
    params = Map((Constants.AmpoolLocatorPortKey, testBase.getLocatorPort),
      (Constants.AmpoolLogFile, "dunit/t.log"),(Constants.AmpoolBatchSize, "10"), (Constants.AmpoolRedundancy, "3"),
      (Constants.AmpoolTableType, "UNORDERED"))

    /** delete old table, if any, to avoid any clashes **/
    val conn: MTableWrapper = new MTableWrapper(tableName, params)
    if (conn.getCache.getAdmin.tableExists(tableName))
      conn.getCache.getAdmin.deleteTable(tableName)

    /** load sample JSON data and save into Ampool for various tests.. **/
    baseDataFrame = loadJsonData(sampleJson)
    baseDataFrame.write.format("io.ampool").options(params).mode(SaveMode.Overwrite).save(tableName)

  }

  /**
   * Cleanup the required.. tear-down the DUnit..
   */
  override def afterAll {
    val cache = MClientCacheFactory.getAnyInstance
    if (cache != null && !cache.isClosed) cache.close()
    testBase.tearDown2()
    sqlContext.sparkContext.stop();

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

  def dataFrameToString(dataFrame: DataFrame): Array[String] = {
    dataFrame.drop(MTableUtils.KEY_COLUMN_NAME).collect().map(e => e.toString()).sorted
  }

  def mTableToString(tableName: String): Array[String] = {
    val cache = getAmpoolCache(params)
    val scan = new Scan()
    val buckets = new java.util.HashSet[Integer]
    (0 to 112).foreach(e=>buckets.add(e.asInstanceOf[java.lang.Integer]))
    scan.setBucketIds(buckets)
    val scanner = cache.getTable(tableName).getScanner(scan).iterator()
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
    val params = Map((Constants.AmpoolTableType, "UNORDERED"))

    /** save ML data-frame to Ampool **/
    df1.write.format("io.ampool").mode(SaveMode.Overwrite).options(params).save(tableName)

    /** load the saved data-frame from Ampool and also read from MTable.. **/
    val df2 = sqlContext.read.format("io.ampool").load(tableName)

    val results1 = dataFrameToString(df1)
    val results2 = mTableToString(tableName)
    val results3 = dataFrameToString(df2)

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

  /** test for write/read using spark-context configuration **/
  test("WriteUsingSaveToAmpool") {
    val params = Map((Constants.AmpoolTableType, "UNORDERED"))
    baseDataFrame.saveToAmpool(tableName, params, overwrite = true)
    val resultsFromDataFrame = dataFrameToString(baseDataFrame)
    val resultsFromMTable = dataFrameToString(sqlContext.read.format("io.ampool").load(tableName))

    /** assert on the required.. **/
    resultsFromMTable.length shouldEqual resultsFromDataFrame.length
    resultsFromMTable shouldEqual resultsFromDataFrame
  }

  /** negative test to assert that specified configuration overrides spark-configuration **/
  test("WriteUsingSaveToAmpool_Negative") {
    val params = Map((Constants.AmpoolLocatorHostKey, "my_host"),
      (Constants.AmpoolLocatorPortKey, "11111"),
      (Constants.AmpoolTableType, "UNORDERED"))
    try {
      baseDataFrame.saveToAmpool(tableName, params, overwrite = true)
    } catch {
      case e: MCacheClosedException =>
    }
  }

  test("BasicRead") {
    val readFromJson = loadJsonData(sampleJson)
    val readFromTable = sqlContext.read.format("io.ampool").load(tableName)

    val fromJson = dataFrameToString(readFromJson)
    val fromTable = dataFrameToString(readFromTable)

    readFromTable.show

    /** assert on the required.. **/
    fromTable.length shouldEqual fromJson.length
    fromTable shouldEqual fromJson
  }

  test("ReadWithFilter") {
    val df = sqlContext.read.format("io.ampool").load(tableName).filter("size = 4096")
    val results = df.collect

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
    val df = sqlContext.read.format("io.ampool").load(tableName).select("inode", "path")

    val results = df.collect

    results.length shouldEqual 32
    results(0).length shouldEqual 2
    results(0).get(0) shouldBe a[java.lang.Long]
    results(0).get(1) shouldBe a[java.lang.String]
  }

  test("ReadAsTableWithSQL") {
    sqlContext.read.format("io.ampool").load(tableName).registerTempTable("my_table")

    var results = sqlContext.sql("select * from my_table").collect
    results.length shouldEqual 32

    results = sqlContext.sql("select * from my_table where size = 4096").collect
    results.length shouldEqual 13

    results = sqlContext.sql("select * from my_table where perms = '-rw-rw-r--'").collect
    results.length shouldEqual 19
    results = sqlContext.sql("select * from my_table where perms = '-rw-rw-r--' or size = 4096").collect
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
    val scanner = cache.getTable(tableName).getScanner(scan).iterator()

    var count: Int = 0
    while (scanner.hasNext) {
      count += 1
      scanner.next()
    }
    count shouldEqual 13
  }

  test("Write_ErrorIfExists") {
    try {
      baseDataFrame.write.format("io.ampool").mode(SaveMode.ErrorIfExists).save(tableName)
    } catch {
      case e: Exception => e.getMessage should startWith("Table already exists:")
    }
  }
  test("Write_Ignore") {
    baseDataFrame.filter("size = 4096").write.format("io.ampool").mode(SaveMode.Ignore).save(tableName)

    val newDF = sqlContext.read.format("io.ampool").load(tableName)

    newDF.count shouldEqual baseDataFrame.count
  }

  test("Write_Overwrite") {
    val newTableName = tableName + "_NEW"
    val params = Map((Constants.AmpoolTableType, "UNORDERED"))
    baseDataFrame.write.format("io.ampool").options(params).save(newTableName)

    /** save the data-frame to Ampool.. **/
    val df1 = sqlContext.read.format("io.ampool").load(newTableName)

    df1.count shouldEqual baseDataFrame.count

    /** overwrite the table with different data and then assert on the required.. **/
    val df2 = baseDataFrame.filter("size = 4096")
    df2.write.format("io.ampool").mode(SaveMode.Overwrite).options(params).save(newTableName)

    val df3 = sqlContext.read.format("io.ampool").load(newTableName)

    df3.count shouldEqual df2.count
    df3.count should not equal baseDataFrame.count
    getAmpoolCache(params).getAdmin.deleteTable(newTableName)
  }

  /** Test for Appending data-frames to Ampool table **/
  test("Write_Append") {
    val newTableName = tableName + "_APPEND"
    val o1 = Map((Constants.AmpoolTableAppend, "true"), (Constants.AmpoolTableType, "UNORDERED"))

    baseDataFrame.write.format("io.ampool").options(o1).save(newTableName)

    /** read and assert the total number of records **/
    val df1 = sqlContext.read.format("io.ampool").load(newTableName)
    df1.count shouldEqual baseDataFrame.count

    /** append the same data-frame one more time and then assert on the count **/
    baseDataFrame.write.format("io.ampool").options(o1).mode(SaveMode.Append).save(newTableName)

    val df2 = sqlContext.read.format("io.ampool").load(newTableName)
    df2.count shouldEqual (baseDataFrame.count*2)

    /** append one more time and assert on the count (3*original) **/
    baseDataFrame.write.format("io.ampool").options(o1).mode(SaveMode.Append).save(newTableName)

    val df3 = sqlContext.read.format("io.ampool").load(newTableName)
    df3.count shouldEqual (baseDataFrame.count*3)

    getAmpoolCache(params).getAdmin.deleteTable(newTableName)
    getAmpoolCache(params).getAdmin.deleteTable(newTableName + Constants.MetaTableSuffix)
  }

  /** Test for some complex types.. i.e. struct/map/array **/
  test("WriteAndReadComplexTypes") {
    val newTableName = tableName + "_COMPLEX"
    val fileName = ClassLoader.getSystemResource("sample.parquet").getFile
    val df1 = sqlContext.read.load(fileName)
    val params = Map((Constants.AmpoolTableType, "UNORDERED"))
    df1.write.format("io.ampool").mode(SaveMode.Overwrite).options(params).save(newTableName)

    val df2 = sqlContext.read.format("io.ampool").load(newTableName)

    val results1 = dataFrameToString(df1)
    val results2 = mTableToString(newTableName)
    val results3 = dataFrameToString(df2)

    /** assert on the required.. **/
    results2.length shouldEqual results1.length
    results3.length shouldEqual results1.length
    results2 shouldEqual results1
    results3 shouldEqual results1
    getAmpoolCache(params).getAdmin.deleteTable(newTableName)
  }

  /** Test for some complex types with SQL and filters **/
  test("ReadComplexTypesSQL") {
    val newTableName = tableName + "_COMPLEX"
    val fileName = ClassLoader.getSystemResource("sample.parquet").getFile
    val df1 = sqlContext.read.load(fileName)
    val params = Map((Constants.AmpoolTableType, "UNORDERED"))
    df1.write.format("io.ampool").mode(SaveMode.Overwrite).options(params).save(newTableName)

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
    getAmpoolCache(params).getAdmin.deleteTable(newTableName)
  }

  /** Test for Spark-ML data-frame with Sparse and Dense vectors.. **/
  test("WriteAndRead_SparkML_Vector") {
    val tableName = "ml_table"
    val denseSeq = Seq((1, Vectors.dense(0.0, 0.0, 0.0)), (2, Vectors.dense(0.1, 0.1, 0.1)),
      (3, Vectors.dense(0.2, 0.2, 0.2)), (4, Vectors.dense(9.0, 9.0, 9.0)),
      (5, Vectors.dense(9.1, 9.1, 9.1)), (6, Vectors.dense(9.2, 9.2, 9.2)))

    assertDataFrameResults(sqlContext.createDataFrame(denseSeq).toDF("id", "features"), tableName)
    getAmpoolCache(params).getAdmin.deleteTable(tableName)

    val sparseSeq = Seq((1, Vectors.sparse(3, Seq((0, 1.0), (2, 1.2)))),
      (2, Vectors.sparse(3, Seq((1, 2.1), (2, 2.2)))),
      (3, Vectors.sparse(3, Seq((0, 3.0), (1, 3.1)))))

    assertDataFrameResults(sqlContext.createDataFrame(sparseSeq).toDF("id", "features"), tableName)

    getAmpoolCache(params).getAdmin.deleteTable(tableName)
  }

  /** Test for unsupported data-types for both read and write.. **/
  test("UnsupportedTypes") {
    val tableName = "ml_table"
    val random = new SecureRandom()
    val cache = getAmpoolCache(params)
    val td = new MTableDescriptor
    td.addColumn("column_1", BasicTypes.BIG_DECIMAL)
    cache.getAdmin.createTable(tableName, td)
    /** throw an exception for unsupported type **/
    try {
      sqlContext.read.format("io.ampool").load(tableName)
    } catch {
      case e: IllegalArgumentException =>
      case _: Throwable => fail("Expected IllegalArgumentException.")
    }

    /** similarly, throw an exception when saving unsupported type **/
    val rdd: RDD[Row] = sqlContext.sparkContext.parallelize(Seq[Row]())
    val schema = StructType(Seq(StructField("value", DataTypes.createDecimalType(17, 13))))
    val df = sqlContext.createDataFrame(rdd, schema)
    try {
      df.write.format("io.ampool").mode(SaveMode.Overwrite).save(tableName)
    } catch {
      case e: IllegalArgumentException =>
      case _: Throwable => fail("Expected IllegalArgumentException.")
    }
  }

  /** Test with persistence enabled **/
  test("BasicWriteWithPersistenceAndRestart") {
    val options = Map(("ampool.enable.persistence", "async"), (Constants.AmpoolTableType, "UNORDERED"))
    val newTableName = tableName + "_PERSIST"
    baseDataFrame.write.format("io.ampool").options(options).save(newTableName)

    testBase.restart()

    val resultsFromDataFrame = dataFrameToString(baseDataFrame)
    val resultsFromMTable = dataFrameToString(sqlContext.read.format("io.ampool").load(newTableName))

    /** assert on the required.. **/
    resultsFromMTable.length shouldEqual resultsFromDataFrame.length
    resultsFromMTable shouldEqual resultsFromDataFrame
    getAmpoolCache(params).getAdmin.deleteTable(newTableName)
  }

  /** Test with incorrect redundancy value (excluding 0, 1, 2, 3 value) **/
  test("IncorrectRedundancy") {
    val options = Map((Constants.AmpoolRedundancy, "100"))
    val newTableName = tableName + "_REDUNDANCY"
    val random = new SecureRandom()
    val cache = getAmpoolCache(params)
    val td = new MTableDescriptor
    val redundancy = options.getOrElse(Constants.AmpoolRedundancy, "0").toInt
    //println(redundancy)
    td.setRedundantCopies(redundancy)
    td.addColumn("column_1", BasicTypes.INT)
    try {
      cache.getAdmin.createTable(newTableName, td)
    } catch {

      case e: MCacheInternalErrorException =>
      case _: Throwable => fail("Expected MCacheInternalErrorException.")
    }
  }

  /** Test without persistence enabled -- no data should be read after restart **/
  test("BasicWriteWithoutPersistenceAndRestart") {
    val options = Map(("junk", "junk"), (Constants.AmpoolTableType, "UNORDERED"))
    val newTableName = tableName + "_NO_PERSIST"
    baseDataFrame.write.format("io.ampool").options(options).save(newTableName)

    testBase.restart()

    val results = dataFrameToString(sqlContext.read.format("io.ampool").load(newTableName))

    /** assert on the required.. **/
    results.length shouldEqual 0
    getAmpoolCache(params).getAdmin.deleteTable(newTableName)
  }

  /** Test for some complex types.. i.e. struct/map/array with persistence enabled **/
  test("WriteAndReadComplexTypesWithPersistenceAndRestart") {
    val newTableName = tableName + "_COMPLEX_PERSIST"
    val fileName = ClassLoader.getSystemResource("sample.parquet").getFile
    val df1 = sqlContext.read.load(fileName)
    val options = Map(("ampool.enable.persistence", "async"), (Constants.AmpoolTableType, "UNORDERED"))
    df1.write.format("io.ampool").mode(SaveMode.Overwrite).options(options).save(newTableName)

    val df2 = sqlContext.read.format("io.ampool").load(newTableName)

    val results1 = dataFrameToString(df1)
    val results2 = mTableToString(newTableName)
    val results3 = dataFrameToString(df2)

    /** assert on the required.. **/
    results2.length shouldEqual results1.length
    results3.length shouldEqual results1.length
    results2 shouldEqual results1
    results3 shouldEqual results1

    testBase.restart()

    val df3 = sqlContext.read.format("io.ampool").load(newTableName)

    val results4 = dataFrameToString(df1)
    val results5 = mTableToString(newTableName)
    val results6 = dataFrameToString(df3)

    /** assert on the required.. **/
    results5.length shouldEqual results4.length
    results6.length shouldEqual results4.length
    results5 shouldEqual results4
    results6 shouldEqual results4
    getAmpoolCache(params).getAdmin.deleteTable(newTableName)
  }

  /** Make sure that loading invalid/non-existent table throws an exception **/
  test("NonExistentTable") {
    try {
      sqlContext.read.format("io.ampool").load("ABC_DEF_XYZ")
    } catch {
      case e: IllegalArgumentException =>
      case _: Throwable => fail("Expected IllegalArgumentException.")
    }
  }

  /** Test for Ordered table with key-range **/
  test("WriteOrderedWithKeyRange") {
    val newTableName = tableName + "_ORDERED"

    /** test using key-range as byte-array **/
    val o1 = Map((Constants.AmpoolTableType, "ORDERED_VERSIONED"),("ampool.key.range", "0-0,99999-999999999"), (Constants.AmpoolRedundancy, "2"))
    baseDataFrame.write.format("io.ampool").mode(SaveMode.Overwrite).options(o1).save(newTableName)

    val df1 = sqlContext.read.format("io.ampool").load(newTableName)
    df1.count shouldEqual 32

    /** test using key-range as int **/
        val o2 = Map((Constants.AmpoolTableType, "ORDERED_VERSIONED"), (Constants.AmpoolKeyColumns, "size"),
          ("ampool.key.range", "0,999999999"), ("ampool.key.range.type", "int"))
        baseDataFrame.write.format("io.ampool").mode(SaveMode.Overwrite).options(o2).save(newTableName)

        val df2 = sqlContext.read.format("io.ampool").load(newTableName)
        df2.count shouldEqual 19

        /** test using key-range as long **/
        val o3 = Map((Constants.AmpoolTableType, "ORDERED_VERSIONED"), (Constants.AmpoolKeyColumns, "inode"),
          ("ampool.key.range", "0,999999999"), ("ampool.key.range.type", "long"))
        baseDataFrame.write.format("io.ampool").mode(SaveMode.Overwrite).options(o3).save(newTableName)

        val df3 = sqlContext.read.format("io.ampool").load(newTableName)
        df3.count shouldEqual 31

    getAmpoolCache(params).getAdmin.deleteTable(newTableName)
  }

  /** Test for Ordered table with key-range **/
  test("WriteOrderedWithKeyRangeNegative") {
    val newTableName = tableName + "_ORDERED"

    /** test using key-range as byte-array **/
    val o1 = Map((Constants.AmpoolTableType, "ORDERED_VERSIONED"),("ampool.key.range", "0-5,99999-999999999"), (Constants.AmpoolRedundancy, "3"))
    try {
      baseDataFrame.write.format("io.ampool").mode(SaveMode.Overwrite).options(o1).save(newTableName)
    } catch {
      case e: Exception =>
        e.getCause.isInstanceOf[RowKeyOutOfRangeException] shouldEqual true
    }

        /** test using invalid input for key-range.. so default key-range should be honoured **/
        val o2 = Map((Constants.AmpoolTableType, "ORDERED_VERSIONED"), (Constants.AmpoolKeyColumns, "size"),
          ("ampool.key.range", "0-0,99999-999999"), ("ampool.key.range.type", "int"))
        baseDataFrame.write.format("io.ampool").mode(SaveMode.Overwrite).options(o2).save(newTableName)

        val df1 = sqlContext.read.format("io.ampool").load(newTableName)
        df1.count shouldEqual 19

    getAmpoolCache(params).getAdmin.deleteTable(newTableName)
  }

  /** Test for write with column(s) as row-key **/
  test("WriteWithCustomKeyUsingColumnAsRowKey") {
    val newTableName = tableName + "_CUSTOM_KEY"
    val o1 = Map((Constants.AmpoolKeyColumns, "size"), (Constants.AmpoolTableType, "UNORDERED"))

        /** only rows having unique value for "size" are stored **/
        baseDataFrame.write.format("io.ampool").options(o1).save(newTableName)

        val df1 = sqlContext.read.format("io.ampool").load(newTableName)
        df1.count shouldEqual 19

        /** only rows having unique value for "inode" are stored **/
        val o2 = Map((Constants.AmpoolKeyColumns, "inode"), (Constants.AmpoolTableType, "UNORDERED"))
        baseDataFrame.write.format("io.ampool").options(o2).mode(SaveMode.Overwrite).save(newTableName)

        val df2 = sqlContext.read.format("io.ampool").load(newTableName)
        df2.count shouldEqual 31

        /** only rows having unique value for "perms" are stored **/
        val o3 = Map((Constants.AmpoolKeyColumns, "perms"), (Constants.AmpoolTableType, "UNORDERED"))
        baseDataFrame.write.format("io.ampool").options(o3).mode(SaveMode.Overwrite).save(newTableName)

        val df3 = sqlContext.read.format("io.ampool").load(newTableName)
        df3.count shouldEqual 2

        /** only rows having unique value for "size" and "perms" are stored **/
        val o4 = Map((Constants.AmpoolKeyColumns, "perms,size"), (Constants.AmpoolTableType, "UNORDERED"))
        baseDataFrame.write.format("io.ampool").options(o4).mode(SaveMode.Overwrite).save(newTableName)

        val df4 = sqlContext.read.format("io.ampool").load(newTableName)
        df4.count shouldEqual 19

        getAmpoolCache(params).getTable(newTableName + Constants.MetaTableSuffix) shouldEqual null
        getAmpoolCache(params).getAdmin.deleteTable(newTableName)
  }


  /** Test for write with column(s) as row-key **/
  test("WriteWithCustomKeyUsingColumnAsRowKeyForOrderedTable") {

    baseDataFrame.show()

    val newTableName = tableName + "_CUSTOM_KEY_ORDERED"
    val o1 = Map((Constants.AmpoolKeyColumns, "size"), (Constants.AmpoolTableType, "ORDERED_VERSIONED"),
      (Constants.AmpoolMaxVersions, "99"), (Constants.AmpoolBatchSize, "1"))

    /** only rows having unique value for "size" are stored **/
    baseDataFrame.write.format("io.ampool").options(o1).save(newTableName)

    val ac = new AmpoolClient("localhost", testBase.getLocatorPort.toInt)
    val scan = new Scan().setMaxVersions(99, false)
    val scanner = ac.getMTable(newTableName).getScanner(scan)
    var rowCount = 0
    var explodedCount = 0
    import scala.collection.JavaConversions._
    for (row <- scanner) {
      rowCount += 1
      explodedCount += row.getAllVersions.size
    }

    println(rowCount)
    println(explodedCount)

    val o11 = Map((Constants.AmpoolKeyColumns, "size"), (Constants.AmpoolTableType, "ORDERED_VERSIONED"),
      (Constants.AmpoolMaxVersions, "99"))
    val df1 = sqlContext.read.format("io.ampool").options(o11).load(newTableName).select("inode", "path","perms","size")
    df1.printSchema()
    df1.show()
    df1.count shouldEqual 32

    // without filter, plain count
    val df11 = sqlContext.read.format("io.ampool").options(o11).load(newTableName)
    df11.count shouldEqual 32


    /** only rows having unique value for "inode" are stored **/
    val o2 = Map((Constants.AmpoolKeyColumns, "inode"), (Constants.AmpoolTableType, "ORDERED_VERSIONED"),
      (Constants.AmpoolMaxVersions, "99"), (Constants.AmpoolBatchSize, "1"))
    baseDataFrame.write.format("io.ampool").options(o2).mode(SaveMode.Overwrite).save(newTableName)

    val o22 = Map((Constants.AmpoolKeyColumns, "inode"), (Constants.AmpoolTableType, "ORDERED_VERSIONED"),
      (Constants.AmpoolMaxVersions, "99"))
    val df2 = sqlContext.read.format("io.ampool").options(o22).load(newTableName).filter("inode > 12976997")
    df2.count shouldEqual 30

    df2.show()

    val df22 = sqlContext.read.format("io.ampool").options(o22).load(newTableName)
    df22.count shouldEqual 32

    /** only rows having unique value for "perms" are stored **/
    val o3 = Map((Constants.AmpoolKeyColumns, "perms"), (Constants.AmpoolTableType, "ORDERED_VERSIONED"),
      (Constants.AmpoolMaxVersions, "99"),  (Constants.AmpoolBatchSize, "1"))
    baseDataFrame.write.format("io.ampool").options(o3).mode(SaveMode.Overwrite).save(newTableName)

    val o33 = Map((Constants.AmpoolKeyColumns, "perms"), (Constants.AmpoolTableType, "ORDERED_VERSIONED"),
      (Constants.AmpoolMaxVersions, "99"))
    val df3 = sqlContext.read.format("io.ampool").options(o33).load(newTableName).filter("perms = 'drwxrwxr-x'")
    df3.count shouldEqual 13

    val df33 = sqlContext.read.format("io.ampool").options(o33).load(newTableName)
    df3.count shouldEqual 13

    /** only rows having unique value for "size" and "perms" are stored **/
    val o4 = Map((Constants.AmpoolKeyColumns, "perms,size"), (Constants.AmpoolTableType, "ORDERED_VERSIONED"),
      (Constants.AmpoolMaxVersions, "99"),   (Constants.AmpoolBatchSize, "1"))
    baseDataFrame.write.format("io.ampool").options(o4).mode(SaveMode.Overwrite).save(newTableName)

    val o44 = Map((Constants.AmpoolKeyColumns, "perms,size"), (Constants.AmpoolTableType, "ORDERED_VERSIONED"),
      (Constants.AmpoolMaxVersions, "99"))
    val df4 = sqlContext.read.format("io.ampool").options(o44).load(newTableName).filter("size > 100 and perms = 'drwxrwxr-x'")
    df4.count shouldEqual 13

    getAmpoolCache(params).getTable(newTableName + Constants.MetaTableSuffix) shouldEqual null
    getAmpoolCache(params).getAdmin.deleteTable(newTableName)
  }

  /** Test for write with custom row-key using batch-prefix **/
  test("WriteWithCustomKeyUsingBatchPrefix") {
    val newTableName = tableName + "_CUSTOM_KEY_1"
    val o1 = Map((Constants.AmpoolBatchPrefix, "0"), (Constants.AmpoolTableType, "UNORDERED"))

    /** rows are saved with the provided prefix **/
    baseDataFrame.write.format("io.ampool").options(o1).save(newTableName)

    val df1 = sqlContext.read.format("io.ampool").load(newTableName)
    df1.count shouldEqual 32

    /** with different prefix records are appended to the table **/
    val o2 = Map((Constants.AmpoolBatchPrefix, "1"), (Constants.AmpoolTableType, "UNORDERED"))
    baseDataFrame.write.format("io.ampool").options(o2).mode(SaveMode.Append).save(newTableName)

    val df2 = sqlContext.read.format("io.ampool").load(newTableName)
    df2.count shouldEqual 64

    /** in case the prefix is reused, a set of records/rows will be overwritten **/
    val o3 = Map((Constants.AmpoolBatchPrefix, "0"), (Constants.AmpoolTableType, "UNORDERED"))
    baseDataFrame.write.format("io.ampool").options(o3).mode(SaveMode.Append).save(newTableName)

    val df3 = sqlContext.read.format("io.ampool").load(newTableName)
    df3.count shouldEqual 64

    /** with another unique prefix, another set of records will be appended to the table **/
    val o4 = Map((Constants.AmpoolBatchPrefix, "2"), (Constants.AmpoolTableType, "UNORDERED"))
    baseDataFrame.write.format("io.ampool").options(o4).mode(SaveMode.Append).save(newTableName)

    val df4 = sqlContext.read.format("io.ampool").load(newTableName)
    df4.count shouldEqual 96

    getAmpoolCache(params).getTable(newTableName + Constants.MetaTableSuffix) shouldEqual null
    getAmpoolCache(params).getAdmin.deleteTable(newTableName)
  }

  /** Test for write with column(s) as row-key **/
  test("WriteWithCustomKeyUsingColumnAsRowKeyForUnOrderedTable") {

    baseDataFrame.show()

    val newTableName = tableName + "_CUSTOM_KEY_ORDERED"
    val o1 = Map((Constants.AmpoolKeyColumns, "size"), (Constants.AmpoolTableType, "UNORDERED"),
      (Constants.AmpoolMaxVersions, "99"), (Constants.AmpoolBatchSize, "1"))

    /** only rows having unique value for "size" are stored **/
    baseDataFrame.write.format("io.ampool").options(o1).save(newTableName)

    val ac = new AmpoolClient("localhost", testBase.getLocatorPort.toInt)
    val scan = new Scan().setMaxVersions(99, false)
    val scanner = ac.getMTable(newTableName).getScanner(scan)
    var rowCount = 0
    var explodedCount = 0
    import scala.collection.JavaConversions._
    for (row <- scanner) {
      rowCount += 1
      explodedCount += row.getAllVersions.size
    }

    println(rowCount)
    println(explodedCount)

    val o11 = Map((Constants.AmpoolKeyColumns, "size"), (Constants.AmpoolTableType, "UNORDERED"),
      (Constants.AmpoolMaxVersions, "99"))
    val df1 = sqlContext.read.format("io.ampool").options(o11).load(newTableName).select("inode", "path","perms","size")
    df1.printSchema()
    df1.show()
    df1.count shouldEqual 32

    // without filter, plain count
    val df11 = sqlContext.read.format("io.ampool").options(o11).load(newTableName)
    df11.count shouldEqual 32


    /** only rows having unique value for "inode" are stored **/
    val o2 = Map((Constants.AmpoolKeyColumns, "inode"), (Constants.AmpoolTableType, "UNORDERED"),
      (Constants.AmpoolMaxVersions, "99"), (Constants.AmpoolBatchSize, "1"))
    baseDataFrame.write.format("io.ampool").options(o2).mode(SaveMode.Overwrite).save(newTableName)

    val o22 = Map((Constants.AmpoolKeyColumns, "inode"), (Constants.AmpoolTableType, "UNORDERED"),
      (Constants.AmpoolMaxVersions, "99"))
    val df2 = sqlContext.read.format("io.ampool").options(o22).load(newTableName).filter("inode > 12976997")
    df2.count shouldEqual 30

    df2.show()

    val df22 = sqlContext.read.format("io.ampool").options(o22).load(newTableName)
    df22.count shouldEqual 32

    /** only rows having unique value for "perms" are stored **/
    val o3 = Map((Constants.AmpoolKeyColumns, "perms"), (Constants.AmpoolTableType, "UNORDERED"),
      (Constants.AmpoolMaxVersions, "99"),  (Constants.AmpoolBatchSize, "1"))
    baseDataFrame.write.format("io.ampool").options(o3).mode(SaveMode.Overwrite).save(newTableName)

    val o33 = Map((Constants.AmpoolKeyColumns, "perms"), (Constants.AmpoolTableType, "UNORDERED"),
      (Constants.AmpoolMaxVersions, "99"))
    val df3 = sqlContext.read.format("io.ampool").options(o33).load(newTableName).filter("perms = 'drwxrwxr-x'")
    df3.count shouldEqual 13

    val df33 = sqlContext.read.format("io.ampool").options(o33).load(newTableName)
    df3.count shouldEqual 13

    /** only rows having unique value for "size" and "perms" are stored **/
    val o4 = Map((Constants.AmpoolKeyColumns, "perms,size"), (Constants.AmpoolTableType, "UNORDERED"),
      (Constants.AmpoolMaxVersions, "99"),   (Constants.AmpoolBatchSize, "1"))
    baseDataFrame.write.format("io.ampool").options(o4).mode(SaveMode.Overwrite).save(newTableName)

    val o44 = Map((Constants.AmpoolKeyColumns, "perms,size"), (Constants.AmpoolTableType, "UNORDERED"),
      (Constants.AmpoolMaxVersions, "99"))
    val df4 = sqlContext.read.format("io.ampool").options(o44).load(newTableName).filter("size > 100 and perms = 'drwxrwxr-x'")
    df4.count shouldEqual 13

    getAmpoolCache(params).getTable(newTableName + Constants.MetaTableSuffix) shouldEqual null
    getAmpoolCache(params).getAdmin.deleteTable(newTableName)
  }

}