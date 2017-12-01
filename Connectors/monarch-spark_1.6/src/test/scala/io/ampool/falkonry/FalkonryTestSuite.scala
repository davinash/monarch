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

import io.ampool.TestBase
import io.ampool.monarch.table.client.MClientCacheFactory
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.BufferedSource

/**
  * A wrapper test class to test Falkonry's tests:
  *   - HistorianTest
  *
  * Following modifications are made to the test provided by Falkonry:
  *   - Used FalkonryTestSuite.LocatorPort instead of default port in AmpoolIO
  *   - Added property spark.driver.allowMultipleContexts=true when creating SparkContext
  *     so as to allow multiple SparkContexts in HistorianTest
  *   - Used FalkonryTestSuite.HistJsonFile for location of hist.json in HistorianTest
  *   - Commented the println statements in HistorianTest so that everything is displayed
  *   - Added assertion (FalkonryTestSuite.AssertOnData) at the end of HistorianTest
  *
  * Created on: 2016-09-08
  * Since version: 1.0.3
  */
//@RunWith(classOf[JUnitRunner])
class FalkonryTestSuite extends FunSuite with BeforeAndAfterAll with Matchers {
  var testBase: TestBase = _

  /**
    * Create SparkContext, SQLContext, and Geode Locator/Servers before tests are run..
    */
  override def beforeAll {
    /** start the DUnit base **/
    testBase = new TestBase("FalkonryTestSuite")
    testBase.setUp()
    FalkonryTestSuite.LocatorPort = testBase.getLocatorPort
  }

  /**
    * Cleanup the required.. tear-down the DUnit..
    */
  override def afterAll {
    val cache = MClientCacheFactory.getAnyInstance
    if (cache != null && !cache.isClosed) cache.close()
    testBase.tearDown2()
  }

  /** Test the Falkonry's use-case as provided by them **/
  test("HistorianTest") {
    HistorianTest.main(Array[String]())
  }
}

object FalkonryTestSuite {
  var LocatorPort: String = "10334"
  val HistJsonFile = ClassLoader.getSystemResource("hist.json").getFile
  val AllDataSortedFile = ClassLoader.getSystemResource("all_data_sorted.txt").getFile
  val QueryResponseSortedFile = ClassLoader.getSystemResource("query_response_sorted.txt").getFile

  /**
    * Return the contents of a file.
    *
    * @param file the file-name
    * @return the contents of the file
    */
  def getFileContents(file: String): String = {
    var f : BufferedSource = null
    try {
      f = scala.io.Source.fromFile(file, "UTF-8")
      f.getLines().mkString("\n")
    } finally {
      if (f != null) f.close
    }
  }

  /**
    * Assert that the result data is same as data from resource files.
    *
    * @param readDf the complete data
    * @param query_response the query response
    */
  def AssertOnData(readDf: DataFrame, query_response: DataFrame) = {
    assert(
      readDf.sort("time").collect().mkString("\n").equals(
        FalkonryTestSuite.getFileContents(FalkonryTestSuite.AllDataSortedFile))
    )
    assert(
      query_response.sort("time").collect().mkString("\n").equals(
        FalkonryTestSuite.getFileContents(FalkonryTestSuite.QueryResponseSortedFile))
    )
  }
}
