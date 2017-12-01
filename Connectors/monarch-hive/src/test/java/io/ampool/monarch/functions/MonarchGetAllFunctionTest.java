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

package io.ampool.monarch.functions;

import io.ampool.client.AmpoolClient;
import io.ampool.monarch.RMIException;
import io.ampool.monarch.TestBase;
import io.ampool.monarch.hive.MonarchUtils;
import io.ampool.monarch.hive.TestHelper;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.MPredicateHolder;
import io.ampool.monarch.types.interfaces.DataType;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

//import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;

/**
 * A sample test to show how predicate push-down can work with Monarch.
 *   The data-set from the file is stored as 2-D byte-array into Monarch region.
 *   Then the different predicates are executed on top the data, via
 *   custom GetAll function, to assert that only the matching data is returned
 *   by Monarch rather than whole data-set.
 *
 * Created on: 2015-12-15
 * Since version: 0.2.0
 */
public class MonarchGetAllFunctionTest extends TestBase {
  DataType[] dataTypes = new DataType[]{BasicTypes.LONG, BasicTypes.STRING,
    BasicTypes.INT, BasicTypes.STRING};
//  IObjectType[] dataTypes = new IObjectType[]{HiveObjectType.LONG, HiveObjectType.STRING, HiveObjectType.INT, HiveObjectType.STRING};
  final String file = "data/sample1.txt";
  final String regionName = "region_get_all_test";

  @DataProvider
  public Object[][] getData() {
    return new Object[][]{
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.GREATER_OR_EQUAL, 0), getConfiguration(), 31},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.LESS_OR_EQUAL, 0), getConfiguration(), 0},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.GREATER, Integer.MAX_VALUE), getConfiguration(), 0},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.LESS, Integer.MAX_VALUE), getConfiguration(), 31},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.LESS, 4096), getConfiguration(), 11},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.EQUAL, 4096), getConfiguration(), 12},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.NOT_EQUAL, 4096), getConfiguration(), 19},
      {new MPredicateHolder(2, BasicTypes.INT, CompareOp.LESS_OR_EQUAL, 4096), getConfiguration(), 23},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.GREATER_OR_EQUAL, 0L), getConfiguration(), 31},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.LESS_OR_EQUAL, 0L), getConfiguration(), 0},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.GREATER, Long.MAX_VALUE), getConfiguration(), 0},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.LESS, Long.MAX_VALUE), getConfiguration(), 31},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.EQUAL, 12978724L), getConfiguration(), 1},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.NOT_EQUAL, 12978724L), getConfiguration(), 30},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.EQUAL, 12978724123L), getConfiguration(), 0},
      {new MPredicateHolder(0, BasicTypes.LONG, CompareOp.NOT_EQUAL, 12978724123L), getConfiguration(), 31},
      {new MPredicateHolder(3, BasicTypes.STRING, CompareOp.EQUAL, "target/warehouse"), getConfiguration(), 1},
      {new MPredicateHolder(3, BasicTypes.STRING, CompareOp.NOT_EQUAL, "target/warehouse"), getConfiguration(), 30},
      {new MPredicateHolder(1, BasicTypes.STRING, CompareOp.EQUAL, "-rw-rw-r--"), getConfiguration(), 19},
      {new MPredicateHolder(1, BasicTypes.STRING, CompareOp.NOT_EQUAL, "-rw-rw-r--"), getConfiguration(), 12},
      {new MPredicateHolder(1, BasicTypes.STRING, CompareOp.EQUAL, "drwxrwxr-x"), getConfiguration(), 12},
      {new MPredicateHolder(1, BasicTypes.STRING, CompareOp.REGEX, "d.wxr[xw]{2}[r]-x"), getConfiguration(), 12},
      {new MPredicateHolder(3, BasicTypes.STRING, CompareOp.REGEX, ".*spark.*"), getConfiguration(), 6},
    };
  }

  /**
   * Setup method.. Create the region and put data in the region.
   *
   * @throws Exception
   */
  @BeforeClass
  private void setUpMethod() throws Exception {
//    System.out.println("MonarchGetAllFunctionTest.setUpMethod");
    testBase.createRegionOnServer(regionName);
    putDataInRegion();
  }

  /**
   * Clean-up method.. Destroy the region on server. It should delete the data as well.
   * @throws Exception
   */
  @AfterClass
  public void cleanUpMethod() throws Exception {
//    System.out.println("MonarchGetAllFunctionTest.cleanUpMethod");
    testBase.destroyRegionOnServer(regionName);
  }

  /**
   * Test various predicate-operations with pre-existing data-set. The data-provider
   *    provides the data as well as the output count to assert on.
   * The predicate contains:
   *    - the column-id on which to execute the predicate
   *    - the argument type {@link BasicTypes}
   *    - the predicate operation {@link CompareOp} like <, <=, >, >=, == !=
   *    - the initial value to be compared with other values
   *
   * @param ph the {@link MPredicateHolder} containing the required predicate data
   * @param conf the configuration required to connect to monarch
   * @param expectedResultCount the expected result count after executing the specified predicate
   * @throws RMIException
   * @throws IOException
   */
  @Test(dataProvider = "getData")
  public void test_Predicates(final MPredicateHolder ph, final Configuration conf,
                    final int expectedResultCount) throws RMIException, IOException {
//    System.out.println("MonarchGetAllFunctionTest.test_Predicates :: " + ph.toString()
//      + " -- expectedResultCount= " + expectedResultCount);
    MonarchGetAllFunction func = new MonarchGetAllFunction();
    final AmpoolClient aClient = MonarchUtils.getConnectionFromConf(conf);
    Execution exec = FunctionService
      .onServer(((GemFireCacheImpl)aClient.getGeodeCache()).getDefaultPool())
      .withArgs(new Object[]{regionName, ph});
    ResultCollector rc = exec.execute(func);
    List o = (List) rc.getResult();

    assertTrue(o.get(0) instanceof List);
    assertEquals(expectedResultCount, ((List) o.get(0)).size());
  }

  /**
   * Get the required configuration for connecting to Monarch. It includes:
   *   - regionName (region_xx)
   *   - locator port (locator port from DUnit launcher)
   *
   * @return the configuration
   */
  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set(MonarchUtils.REGION, regionName);
    conf.set(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());
    return conf;
  }

  /**
   * Put the specified data in region as 2-D byte array (byte[][]).
   *
   * @throws IOException
   * @throws RMIException
   */
  private void putDataInRegion() throws IOException, RMIException {
    int id = 0;
    for (final String line : TestHelper.getResourceAsString(file)) {
      String[] cols = line.split(",");
      byte[][] bytes = new byte[cols.length][];
      byte[] bs;
      int i = 0;
      for (final String col : cols) {
        bs = dataTypes[i].serialize(getObjectFromString(dataTypes[i], col));
        bytes[i++] = bs;
      }
      testBase.putInRegionOnServer(regionName, id++, bytes);
    }
  }

  private Object getObjectFromString(DataType dataType, String col) {
    if (BasicTypes.INT.equals(dataType)) {
      return Integer.valueOf(col);
    } else if (BasicTypes.LONG.equals(dataType)) {
      return Long.valueOf(col);
    } else {
      return col;
    }
  }
}