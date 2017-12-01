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
package io.ampool.monarch.hive.itests;

import static java.lang.ClassLoader.getSystemResource;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.google.common.collect.Sets;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveProperties;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import io.ampool.monarch.RMIException;
import io.ampool.monarch.hive.TestHelper;
//import io.ampool.store.hive.HiveStoreConfig;
import junit.framework.TestCase;
import org.apache.commons.collections.MapUtils;
import org.junit.*;
import org.junit.runner.RunWith;

/**
 * Hive Runner Reference implementation.
 * <p/>
 * All HiveRunner tests should run with the StandaloneHiveRunner
 */
@RunWith(StandaloneHiveRunner.class)
public class CreateTableTestWithTez extends TestCase {

  //private MonarchDUnitBase testBase;
  @ClassRule
  public static TestBaseJUnit testBase = new TestBaseJUnit("ComplexTypesTest");

  /**
   * Explicit test class configuration of the HiveRunner runtime.
   * See {@link HiveRunnerConfig} for further details.
   */
  @HiveRunnerSetup
  public final HiveRunnerConfig CONFIG = new HiveRunnerConfig(){{
    setHiveExecutionEngine("tez");
    setTimeoutEnabled(true);
    setTimeoutSeconds(300);
  }};

  /**
   * Cater for all the parameters in the script that we want to test.
   * Note that the "hadoop.tmp.dir" is one of the dirs defined by the test harness
   */
  @HiveProperties
  public Map hiveProperties = MapUtils.putAll(new HashMap(), new Object[]{
    "MY.HDFS.DIR", "${hadoop.tmp.dir}",
    "my.schema", "ampool",
    //"hive.optimize.ppd", "false"
  });

  /**
   * In this example, the scripts under test expects a schema to be already present in hive so
   * we do that with a setup script.
   * <p/>
   * There may be multiple setup scripts but the order of execution is undefined.
   */
  @HiveSetupScript
  private String createSchemaScript = "create schema ${hiveconf:my.schema}";

  /**
   * Create some data in the target directory. Note that the 'targetFile' references the
   * same dir as the create table statement in the script under test.
   * <p/>
   * This example is for defining the data in in a resource file.
   */
  @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/ampool/u.data")
  private File dataFromFile =
    new File(getSystemResource("data/sample_u_data.txt").getPath());

  @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/ampool/datatypes.data")
  private File dataFromFile1 =
    new File(ClassLoader.getSystemResource("data/sample_datatypes.txt").getPath());

  /**
   * Define the script files under test. The files will be loaded in the given order.
   * <p/>
   * The HiveRunner instantiate and inject the HiveShell
   */
  @HiveSQL(files = {
  }, encoding = "UTF-8")
  private HiveShell hiveShell;
  private List<String> testData;

  @Before
  public void setUp() throws Exception {
//    testBase = new MonarchDUnitBase("CreateTableTestWithTez");
//    testBase.setUp();
    testData = new ArrayList<>();
    testData.add("1");
    testData.add("12");
    testData.add("123");
    testData.add("123");
    testData.add("true");
    testData.add("12.34");
    testData.add("1234.1234");
    testData.add("hello world");
    testData.add("[10,20,30,40]");
    testData.add("[\"a\",\"b\",\"c\"]");
    testData.add("{10:\"val1\",20:\"val2\"}");
    testData.add("{\"id\":\"a1\",\"name\":\"name\",\"val\":1234}");
    testData.add("2011-05-06 07:08:09.123");
    testData.add("12.34568");
    testData.add("abcdefghi ");
    testData.add("abcdefghijklmnopqrstuvwxyz");
    testData.add("2016-01-27");
  }

  @After
  public void tearDown() throws Exception {
    //testBase.tearDown2();
//      System.out.println("CreateTableTestWithTez tearDown " + testBase.getLocatorPort());
//      Configuration conf = new Configuration();
//      conf.set("monarch.locator.port", testBase.getLocatorPort());
  }

  @Test
  public void testTablesCreated() throws RMIException, IOException {
    final String sqlFile = "sql/test_u_data.sql";

//    Map<String, String> map = new HashMap<>();
//    map.put("10334", testBase.getLocatorPort());
    hiveShell.execute(TestHelper.getResourceAsString(sqlFile, testBase.getMap()));

    HashSet<String> expected = Sets.newHashSet("p1", "u_data", "p2", "p3");
    HashSet<String> actual = Sets.newHashSet(hiveShell.executeQuery("show tables"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("100000");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from u_data"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("87092");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from u_data where userid > 123"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("87146");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from u_data where userid >= 123"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from u_data where unixtime is null"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("200000");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from p1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("200000");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from p2"));
    Assert.assertEquals(expected, actual);

    /** query using selective columns -- reader fetches only selected column **/
    expected = Sets.newHashSet("200000");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(age) from p1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("85106026");
    actual = Sets.newHashSet(hiveShell.executeQuery("select sum(age) from p1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1\t878542420");
    actual = Sets.newHashSet(hiveShell.executeQuery("select id, name from p3 where name = '878542420'"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("87092");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from p3 where id > 123"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("87146");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from p3 where id >= 123"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from p3 where name is null"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("100000");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from p3"));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testTablesCreatedWithMTable() throws RMIException, IOException {
    final String sqlFile = "sql/test_u_data_MTable.sql";

//    Map<String, String> map = new HashMap<>();
//    map.put("10334", testBase.getLocatorPort());

    hiveShell.execute(TestHelper.getResourceAsString(sqlFile, testBase.getMap()));

    HashSet<String> expected = Sets.newHashSet("m1", "u_data", "m2");
    HashSet<String> actual = Sets.newHashSet(hiveShell.executeQuery("show tables"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("100000");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from u_data"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("87092");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from u_data where userid > 123"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("87146");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from u_data where userid >= 123"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from u_data where unixtime is null"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("100000");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from m1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("100000");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from m2"));
    Assert.assertEquals(expected, actual);

    /** query using selective columns -- reader fetches only selected column **/
    expected = Sets.newHashSet("100000");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(age) from m1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("42553013");
    actual = Sets.newHashSet(hiveShell.executeQuery("select sum(age) from m1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("87092");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from m2 where id > 123"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("87146");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from m2 where id >= 123"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from m2 where name is null"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1\t878542420");
    actual = Sets.newHashSet(hiveShell.executeQuery("select id, name from m2 where name = '878542420'"));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDataTypes_MTable() throws IOException, RMIException {
    final String sqlFile = "sql/test_datatypes_MTable.sql";
//    Map<String, String> map = new HashMap<>();
//    map.put("10334", testBase.getLocatorPort());
    hiveShell.execute(TestHelper.getResourceAsString(sqlFile, testBase.getMap()));

    HashSet<String> expected = Sets.newHashSet("1");
    HashSet<String> actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_tinyint from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("12");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_smallint from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_bigint from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("123\t123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint from Mdatatypes1 where col_int = col_bigint and col_tinyint = 1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("123\t123\t1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint, col_tinyint from Mdatatypes1 where col_int = col_bigint and col_tinyint = 1 and col_bigint = 123"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("123\t123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint from Mdatatypes1 where col_int = col_bigint or col_tinyint = 1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("true");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_boolean from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(col_float) from Mdatatypes1 where col_float==12.34"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("1234.1234");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_double from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("hello world");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_string from Mdatatypes1"));
    assertEquals(expected, actual);


    expected = Sets.newHashSet("[10,20,30,40]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int_array from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("[\"a\",\"b\",\"c\"]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_string_array from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("{10:\"val1\",20:\"val2\"}");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_map from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("{\"id\":\"a1\",\"name\":\"name\",\"val\":1234}");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_struct from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2011-05-06 07:08:09.123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_timestamp from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2011");
    actual = Sets.newHashSet(hiveShell.executeQuery("select year(col_timestamp) from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("5");
    actual = Sets.newHashSet(hiveShell.executeQuery("select month(col_timestamp) from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("6");
    actual = Sets.newHashSet(hiveShell.executeQuery("select day(col_timestamp) from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("7");
    actual = Sets.newHashSet(hiveShell.executeQuery("select hour(col_timestamp) from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("8");
    actual = Sets.newHashSet(hiveShell.executeQuery("select minute(col_timestamp) from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("9");
    actual = Sets.newHashSet(hiveShell.executeQuery("select second(col_timestamp) from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2011-05-06");
    actual = Sets.newHashSet(hiveShell.executeQuery("select to_date(col_timestamp) from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("6");
    actual = Sets.newHashSet(hiveShell.executeQuery("select dayofmonth(col_timestamp) from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("18");
    actual = Sets.newHashSet(hiveShell.executeQuery("select weekofyear(col_timestamp) from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(col_binary) from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("12.34568");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_decimal from Mdatatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("abcdefghi ");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_char from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("abcdefghijklmnopqrstuvwxyz");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_varchar from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2016-01-27");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_date from Mdatatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2016");
    actual = Sets.newHashSet(hiveShell.executeQuery("select year(col_date) from Mdatatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select month(col_date) from Mdatatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("27");
    actual = Sets.newHashSet(hiveShell.executeQuery("select day(col_date) from Mdatatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_tinyint from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("12");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_smallint from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_bigint from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("123\t123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint from Mdatatypes2 where col_int = col_bigint and col_tinyint = 1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("123\t123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint from Mdatatypes2 where col_int = col_bigint or col_tinyint = 1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("true");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_boolean from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(col_float) from Mdatatypes2 where col_float==12.34"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("1234.1234");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_double from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("hello world");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_string from Mdatatypes2"));
    assertEquals(expected, actual);


    expected = Sets.newHashSet("[10,20,30,40]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int_array from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("[\"a\",\"b\",\"c\"]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_string_array from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("{10:\"val1\",20:\"val2\"}");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_map from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("{\"id\":\"a1\",\"name\":\"name\",\"val\":1234}");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_struct from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2011-05-06 07:08:09.123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_timestamp from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2011");
    actual = Sets.newHashSet(hiveShell.executeQuery("select year(col_timestamp) from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("5");
    actual = Sets.newHashSet(hiveShell.executeQuery("select month(col_timestamp) from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("6");
    actual = Sets.newHashSet(hiveShell.executeQuery("select day(col_timestamp) from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("7");
    actual = Sets.newHashSet(hiveShell.executeQuery("select hour(col_timestamp) from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("8");
    actual = Sets.newHashSet(hiveShell.executeQuery("select minute(col_timestamp) from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("9");
    actual = Sets.newHashSet(hiveShell.executeQuery("select second(col_timestamp) from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2011-05-06");
    actual = Sets.newHashSet(hiveShell.executeQuery("select to_date(col_timestamp) from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("6");
    actual = Sets.newHashSet(hiveShell.executeQuery("select dayofmonth(col_timestamp) from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("18");
    actual = Sets.newHashSet(hiveShell.executeQuery("select weekofyear(col_timestamp) from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("12.34568");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_decimal from Mdatatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("abcdefghi ");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_char from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("abcdefghijklmnopqrstuvwxyz");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_varchar from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2016-01-27");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_date from Mdatatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2016");
    actual = Sets.newHashSet(hiveShell.executeQuery("select year(col_date) from Mdatatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select month(col_date) from Mdatatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("27");
    actual = Sets.newHashSet(hiveShell.executeQuery("select day(col_date) from Mdatatypes2"));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDataTypes() throws IOException, RMIException {
    final String sqlFile = "sql/test_datatypes.sql";

    hiveShell.execute(TestHelper.getResourceAsString(sqlFile, testBase.getMap()));

    HashSet<String> expected = Sets.newHashSet("1");
    HashSet<String> actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_tinyint from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("12");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_smallint from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_bigint from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("123\t123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint from datatypes1 where col_int = col_bigint and col_tinyint = 1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("123\t123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint from datatypes1 where col_int = col_bigint or col_tinyint = 1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("true");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_boolean from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(col_float) from datatypes1 where col_float==12.34"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1234.1234");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_double from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("hello world");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_string from datatypes1"));
    Assert.assertEquals(expected, actual);


    expected = Sets.newHashSet("[10,20,30,40]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int_array from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("[\"a\",\"b\",\"c\"]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_string_array from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("{10:\"val1\",20:\"val2\"}");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_map from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("{\"id\":\"a1\",\"name\":\"name\",\"val\":1234}");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_struct from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("2011-05-06 07:08:09.123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_timestamp from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("2011");
    actual = Sets.newHashSet(hiveShell.executeQuery("select year(col_timestamp) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("5");
    actual = Sets.newHashSet(hiveShell.executeQuery("select month(col_timestamp) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("6");
    actual = Sets.newHashSet(hiveShell.executeQuery("select day(col_timestamp) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("7");
    actual = Sets.newHashSet(hiveShell.executeQuery("select hour(col_timestamp) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("8");
    actual = Sets.newHashSet(hiveShell.executeQuery("select minute(col_timestamp) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("9");
    actual = Sets.newHashSet(hiveShell.executeQuery("select second(col_timestamp) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("2011-05-06");
    actual = Sets.newHashSet(hiveShell.executeQuery("select to_date(col_timestamp) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("6");
    actual = Sets.newHashSet(hiveShell.executeQuery("select dayofmonth(col_timestamp) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("18");
    actual = Sets.newHashSet(hiveShell.executeQuery("select weekofyear(col_timestamp) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(col_binary) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("abcdefghi ");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_char from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("abcdefghijklmnopqrstuvwxyz");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_varchar from datatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2016-01-27");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_date from datatypes1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2016");
    actual = Sets.newHashSet(hiveShell.executeQuery("select year(col_date) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select month(col_date) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("27");
    actual = Sets.newHashSet(hiveShell.executeQuery("select day(col_date) from datatypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("12.34568");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_decimal from datatypes1"));
    Assert.assertEquals(expected, actual);

    // With persistence enabled.

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_tinyint from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("12");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_smallint from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_bigint from datatypes2"));
    Assert.assertEquals(expected, actual);


    expected = Sets.newHashSet("123\t123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint from datatypes2 where col_int = col_bigint and col_tinyint = 1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("123\t123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint from datatypes2 where col_int = col_bigint or col_tinyint = 1"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("true");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_boolean from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select count(col_float) from datatypes2 where col_float==12.34"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1234.1234");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_double from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("hello world");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_string from datatypes2"));
    Assert.assertEquals(expected, actual);


    expected = Sets.newHashSet("[10,20,30,40]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_int_array from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("[\"a\",\"b\",\"c\"]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_string_array from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("{10:\"val1\",20:\"val2\"}");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_map from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("{\"id\":\"a1\",\"name\":\"name\",\"val\":1234}");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_struct from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("2011-05-06 07:08:09.123");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_timestamp from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("2011");
    actual = Sets.newHashSet(hiveShell.executeQuery("select year(col_timestamp) from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("5");
    actual = Sets.newHashSet(hiveShell.executeQuery("select month(col_timestamp) from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("6");
    actual = Sets.newHashSet(hiveShell.executeQuery("select day(col_timestamp) from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("7");
    actual = Sets.newHashSet(hiveShell.executeQuery("select hour(col_timestamp) from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("8");
    actual = Sets.newHashSet(hiveShell.executeQuery("select minute(col_timestamp) from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("9");
    actual = Sets.newHashSet(hiveShell.executeQuery("select second(col_timestamp) from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("2011-05-06");
    actual = Sets.newHashSet(hiveShell.executeQuery("select to_date(col_timestamp) from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("6");
    actual = Sets.newHashSet(hiveShell.executeQuery("select dayofmonth(col_timestamp) from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("18");
    actual = Sets.newHashSet(hiveShell.executeQuery("select weekofyear(col_timestamp) from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("abcdefghi ");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_char from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("abcdefghijklmnopqrstuvwxyz");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_varchar from datatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2016-01-27");
    actual = Sets.newHashSet(hiveShell.executeQuery("select col_date from datatypes2"));
    assertEquals(expected, actual);

    expected = Sets.newHashSet("2016");
    actual = Sets.newHashSet(hiveShell.executeQuery("select year(col_date) from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("1");
    actual = Sets.newHashSet(hiveShell.executeQuery("select month(col_date) from datatypes2"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("27");
    actual = Sets.newHashSet(hiveShell.executeQuery("select day(col_date) from datatypes2"));
    Assert.assertEquals(expected, actual);
  }
}
