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
import io.ampool.monarch.hive.MonarchUtils;
import io.ampool.monarch.hive.TestHelper;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.commons.collections.MapUtils;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(StandaloneHiveRunner.class)
public class DataTypesTest {
  /**
   * create and setup testBase only once per class..
   **/
  @ClassRule
  public static TestBaseJUnit testBase = new TestBaseJUnit("DataTypesTest");


  /**
     * Explicit test class configuration of the HiveRunner runtime.
     * See {@link HiveRunnerConfig} for further details.
     */
    @HiveRunnerSetup
    public final HiveRunnerConfig CONFIG = new HiveRunnerConfig(){{
        setHiveExecutionEngine("mr");
        setTimeoutEnabled(true);
        setTimeoutSeconds(300);
    }};

    /**
     * Cater for all the parameters in the script that we want to test.
     * Note that the "hadoop.tmp.dir" is one of the dirs defined by the test harness
     */
    @HiveProperties
    @SuppressWarnings("unchecked")
    public Map<String, String> hiveProperties = MapUtils.putAll(new HashMap(), new Object[]{
            "MY.HDFS.DIR", "${hadoop.tmp.dir}",
            "my.schema", "ampool",
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
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/ampool/datatypes.data")
    private File dataFromFile =
            new File(ClassLoader.getSystemResource("data/sample_datatypes.txt").getPath());


    /**
     * Define the script files under test. The files will be loaded in the given order.
     * <p/>
     * The HiveRunner instantiate and inject the HiveShell
     */
    @HiveSQL(files = {
//            "helloHiveRunner/datatypes.sql"
    }, encoding = "UTF-8")
    private HiveShell hiveShell;


    @Test
    public void testDataTypes() throws IOException, RMIException {
      /* close the cache, if any, so that new one gets created with client-read-timeout */
      try {
        MClientCacheFactory.getAnyInstance().close();
      } catch (Exception e) {
        e.printStackTrace();
      }
      final String[] types = new String[]{"immutable,AMP_BYTES", "immutable,AMP_SNAPPY", "immutable,ORC_BYTES"};
      for (final String type : types) {
        loadDataFromFile("sql/test_datatypes.sql", type);

        final GemFireCacheImpl cc = MonarchCacheImpl.getGeodeCacheInstance();
        assertEquals("Incorrect client-read-timeout.", 12345, cc.getDefaultPool().getReadTimeout());

        HashSet<String> expected = Sets.newHashSet("1");
        HashSet<String> actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("1");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_tinyint from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("12");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_smallint from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("123");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_int from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("123");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_bigint from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("123\t123");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint from datatypes1 where col_int = col_bigint and col_tinyint = 1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("123\t123\t1");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint, col_tinyint from datatypes1 where col_int = col_bigint and col_tinyint = 1 and col_bigint = 123"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("123\t123");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint from datatypes1 where col_int = col_bigint or col_tinyint = 1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("true");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_boolean from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("1");
        actual = Sets.newHashSet(hiveShell.executeQuery("select count(col_float) from datatypes1 where col_float==12.34"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("1234.1234");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_double from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("hello world");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_string from datatypes1"));
        assertEquals(expected, actual);


        expected = Sets.newHashSet("[10,20,30,40]");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_int_array from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("[\"a\",\"b\",\"c\"]");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_string_array from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("{10:\"val1\",20:\"val2\"}");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_map from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("{\"id\":\"a1\",\"name\":\"name\",\"val\":1234}");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_struct from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("2011-05-06 07:08:09.123");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_timestamp from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("2011");
        actual = Sets.newHashSet(hiveShell.executeQuery("select year(col_timestamp) from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("5");
        actual = Sets.newHashSet(hiveShell.executeQuery("select month(col_timestamp) from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("6");
        actual = Sets.newHashSet(hiveShell.executeQuery("select day(col_timestamp) from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("7");
        actual = Sets.newHashSet(hiveShell.executeQuery("select hour(col_timestamp) from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("8");
        actual = Sets.newHashSet(hiveShell.executeQuery("select minute(col_timestamp) from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("9");
        actual = Sets.newHashSet(hiveShell.executeQuery("select second(col_timestamp) from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("2011-05-06");
        actual = Sets.newHashSet(hiveShell.executeQuery("select to_date(col_timestamp) from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("6");
        actual = Sets.newHashSet(hiveShell.executeQuery("select dayofmonth(col_timestamp) from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("18");
        actual = Sets.newHashSet(hiveShell.executeQuery("select weekofyear(col_timestamp) from datatypes1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("1");
        actual = Sets.newHashSet(hiveShell.executeQuery("select count(col_binary) from datatypes1"));
        assertEquals(expected, actual);

//      This is correct test case but is failing due to hiveRunner
//      expected = Sets.newHashSet("01234567891011121314151617181920");
//      actual = Sets.newHashSet(hiveShell.executeQuery("select col_binary from datatypes1"));
//      Assert.assertEquals(expected, actual);

        expected = Sets.newHashSet("12.34568");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_decimal from datatypes1"));
        Assert.assertEquals(expected, actual);

        expected = Sets.newHashSet("abcdefghi ");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_char from datatypes1"));
        assertEquals(expected, actual);

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

      /*expected = Sets.newHashSet("10364055.81");
      actual = Sets.newHashSet(hiveShell.executeQuery("select col_decimal from datatypes1"));
      assertEquals(expected, actual);*/

        // With persistence enabled.

        expected = Sets.newHashSet("1");
        actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("1");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_tinyint from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("12");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_smallint from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("123");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_int from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("123");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_bigint from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("123\t123");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint from datatypes2 where col_int = col_bigint and col_tinyint = 1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("123\t123");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_int, col_bigint from datatypes2 where col_int = col_bigint or col_tinyint = 1"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("true");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_boolean from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("1");
        actual = Sets.newHashSet(hiveShell.executeQuery("select count(col_float) from datatypes2 where col_float==12.34"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("1234.1234");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_double from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("hello world");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_string from datatypes2"));
        assertEquals(expected, actual);


        expected = Sets.newHashSet("[10,20,30,40]");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_int_array from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("[\"a\",\"b\",\"c\"]");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_string_array from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("{10:\"val1\",20:\"val2\"}");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_map from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("{\"id\":\"a1\",\"name\":\"name\",\"val\":1234}");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_struct from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("2011-05-06 07:08:09.123");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_timestamp from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("2011");
        actual = Sets.newHashSet(hiveShell.executeQuery("select year(col_timestamp) from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("5");
        actual = Sets.newHashSet(hiveShell.executeQuery("select month(col_timestamp) from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("6");
        actual = Sets.newHashSet(hiveShell.executeQuery("select day(col_timestamp) from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("7");
        actual = Sets.newHashSet(hiveShell.executeQuery("select hour(col_timestamp) from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("8");
        actual = Sets.newHashSet(hiveShell.executeQuery("select minute(col_timestamp) from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("9");
        actual = Sets.newHashSet(hiveShell.executeQuery("select second(col_timestamp) from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("2011-05-06");
        actual = Sets.newHashSet(hiveShell.executeQuery("select to_date(col_timestamp) from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("6");
        actual = Sets.newHashSet(hiveShell.executeQuery("select dayofmonth(col_timestamp) from datatypes2"));
        assertEquals(expected, actual);

        expected = Sets.newHashSet("18");
        actual = Sets.newHashSet(hiveShell.executeQuery("select weekofyear(col_timestamp) from datatypes2"));
        assertEquals(expected, actual);

//        expected = Sets.newHashSet("1");
//        actual = Sets.newHashSet(hiveShell.executeQuery("select count(col_binary) from datatypes2"));
//        assertEquals(expected, actual);

//      This is correct test case but is failing due to hiveRunner
//      expected = Sets.newHashSet("01234567891011121314151617181920");
//      actual = Sets.newHashSet(hiveShell.executeQuery("select col_binary from datatypes2"));
//      Assert.assertEquals(expected, actual);

        expected = Sets.newHashSet("12.34568");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_decimal from datatypes1"));
        Assert.assertEquals(expected, actual);

        expected = Sets.newHashSet("abcdefghi ");
        actual = Sets.newHashSet(hiveShell.executeQuery("select col_char from datatypes2"));
        assertEquals(expected, actual);

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

      /*expected = Sets.newHashSet("10364055.81");
      actual = Sets.newHashSet(hiveShell.executeQuery("select col_decimal from datatypes2"));
      assertEquals(expected, actual);*/
      }
    }

  /**
   * Read SQL statements from a file and execute them to load the data..
   *
   * @param sqlFile the filename
   * @throws IOException
   */
  private void loadDataFromFile(String sqlFile, final String type) throws IOException {
    final String[] s = type.split(",");
    final Map<String, String> params = new HashMap<>(3);
    params.put(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());
    params.put(MonarchUtils.MONARCH_TABLE_TYPE, s[0]);
    if (s.length > 1) {
      params.put(MonarchUtils.BLOCK_FORMAT, s[1]);
    }
    hiveShell.execute(TestHelper.getResourceAsString1(sqlFile, params));
  }

  private void assertWithDataTypes() {
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

  private void assertWithPredicates() {
    final String queryPrefix = "select count(*) from data_types_external where ";
    /** the query and respective expected count of rows returned **/
    final String[][] qs = new String[][]{
      {"1", "col_tinyint == 1"},
      {"0", "col_tinyint < 1"},
      {"1", "col_tinyint > -1"},
      {"1", "col_tinyint <= 10"},
      {"1", "col_bigint > -10"},
      {"1", "col_bigint < 10000"},
      {"0", "col_bigint != 123"},
      {"1", "col_bigint > 0"},
      {"1", "col_boolean == true"},
      {"0", "col_boolean != true"},
      {"0", "col_boolean == false"},
      {"1", "col_boolean != false"},
      {"1", "col_char == 'abcdefghi'"},
      {"0", "col_string == 'abcdefghij'"},
      {"1", "col_string != 'abcdefghij'"},
      {"1", "col_string LIKE 'he_lo%'"},
      {"0", "col_string LIKE '%abcd%'"},
      {"1", "col_decimal == 12.34568"},
      {"0", "col_decimal <= 12.34560"},
      {"1", "col_date == '2016-01-27'"},
      {"1", "col_date >= '2016-01-01'"},
      {"0", "col_date < '2016-01-01'"},
      {"1", "col_timestamp == '2011-05-06 07:08:09.123'"}, /** TODO: support higher precision **/
      {"0", "col_timestamp != '2011-05-06 07:08:09.123'"},
      {"0", "col_timestamp < '2010-05-06 07:08:09'"},
      {"1", "col_timestamp >= '2010-05-06 07:08:09'"},
    };
    /** assert that the query returns correct number of rows.. **/
    for (final String[] q : qs) {
      assertEquals("Predicate: " + q[1], q[0], hiveShell.executeQuery(queryPrefix + q[1]).get(0));
    }
  }

  @Test
  public void testDataTypesForTables() throws IOException, RMIException {
    final String sqlFile = "sql/test_datatypes_MTable.sql";
    final String sqlFileE = "sql/test_external_MTable.sql";
    final String[] types = new String[]{"unordered", "ordered", "immutable,AMP_BYTES",
      "immutable,AMP_SNAPPY", "immutable,ORC_BYTES"};
    for (final String type : types) {
//      System.out.println("### Executing for type= " + type);
      loadDataFromFile(sqlFile, type);
      assertWithDataTypes();

      /* predicates */
      loadDataFromFile(sqlFileE, type);
      assertWithPredicates();
    }
  }

  @Test
  public void testComplexTypes() throws IOException {
    final String sqlFile = "sql/test_complex_types.sql";
    final String[] types = new String[]{"unordered", "ordered", "immutable,AMP_BYTES",
      "immutable,AMP_SNAPPY", "immutable,ORC_BYTES"};

    final String query = "select * from complex_types";

    /** column-name and expected value (as string) for the column.. **/
    final String[][] expected = new String[][]{
      {"c1", "1"},
      {"c21", "[[1,2,3],[4,5,6],[7,8,9]]"},
      {"c22", "[0.12346,1.23457,12.34568]"},
      {"c23", "{\"string_1\":0.12346}"},
      {"c24", "{\"c1\":true,\"c2\":12.34568}"},
      {"c25", "[\"abc  \",\"def  \"]"},
      {"c3", "[{\"string_1\":{\"c1\":true,\"c2\":1.23457,\"c3\":\"abc\"}},{\"string_2\":{\"c1\":false,\"c2\":12.34567,\"c3\":\"def\"}},{\"string_3\":{\"c1\":true,\"c2\":123.4567,\"c3\":\"ghi\"}}]"},
      {"c4", "[{\"c1\":\"string_1\",\"c3\":11.11},{\"c1\":\"string_3\",\"c3\":22.22},{\"c1\":\"string_2\",\"c3\":33.33}]"},
      {"c5", "[{\"c1\":\"string_11\",\"c2\":{11:\"value_11\",12:\"value_12\",13:\"value_13\"},\"c3\":123.456},{\"c1\":\"string_22\",\"c2\":{21:\"value_21\",22:\"value_22\",23:\"value_23\"},\"c3\":123.456}]"},
      {"c6", "{1:[\"string_1\",\"string_2\",\"string_3\"]}"},
      {"c7", "{\"key_1\":{\"c1\":11,\"c2\":\"string_11\"},\"key_2\":{\"c1\":12,\"c2\":\"string_12\"}}"},
      {"c8", "{\"s_1\":[{\"c1\":\"string_11\",\"c2\":{11:\"value_11\",12:\"value_12\",13:\"value_13\"},\"c3\":123.456}]}"},
      {"c9", "{\"c1\":{\"key_11\":[1,2,3],\"key_12\":[4,5,6]},\"c2\":[{11:11.111,12:11.222,13:11.333},{21:22.111,22:22.222,23:22.333},{32:33.222,33:33.333,31:33.111}],\"c3\":{\"c11\":111,\"c22\":\"string_222\"}}"},
      {"cl", "3"}
    };
    for (final String type : types) {
//      System.out.println("### Executing for type= " + type);
      loadDataFromFile(sqlFile, type);

      final String[] output = hiveShell.executeQuery(query).get(0).split("\t");
      int i = 0;
      for (final String columnValue : output) {
        assertEquals("Incorrect value for column: " + expected[i][0], expected[i++][1], columnValue);
      }
    }
  }


  /**
   * Test union type with other types (basic as well as complex) in it.
   *
   * @throws IOException
   */
  @Test
  public void testUnionType() throws IOException {
    final String sqlFile = "sql/test_union_type.sql";
    final String[] types = new String[]{"unordered", "ordered", "immutable,AMP_BYTES",
      "immutable,AMP_SNAPPY", "immutable,ORC_BYTES"};
    final Map<String, String> params = testBase.getMap();

    final String query = "select * from test_union_type";

    final String[][] expected = new String[][]{
      {"c1", "1"},
      {"c2", "{0:123}"},
      {"c3", "{1:\"string_111\"}"},
      {"c4", "{2:[{\"c11\":11,\"c12\":{\"key_11\":11.111,\"key_12\":22.111,\"key_13\":33.111},\"c13\":123.456},{\"c11\":22,\"c12\":{\"key_21\":22.111,\"key_22\":22.222,\"key_23\":33.222},\"c13\":456.321}]}"},
      {"cl", "{\"c1\":1234}"},
    };

    for (final String type : types) {
//      System.out.println("### Executing for type= " + type);
      params.put("__type__", type);
      loadDataFromFile(sqlFile, type);

      final String[] output = hiveShell.executeQuery(query).get(0).split("\t");
      int i = 0;
      for (final String columnValue : output) {
        assertEquals("Incorrect value for column: " + expected[i][0], expected[i++][1], columnValue);
      }
    }
  }
}
