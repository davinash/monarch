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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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
import junit.framework.TestCase;
import org.apache.commons.collections.MapUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

//import io.ampool.store.hive.HiveStoreConfig;

@RunWith(StandaloneHiveRunner.class)

public class ComplexTypesTest extends TestCase {
  @ClassRule
  public static TestBaseJUnit testBase = new TestBaseJUnit("ComplexTypesTest");

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
    "my.schema", "ampool"
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
    new File(ClassLoader.getSystemResource("data/sample_complextypes.txt").getPath());


  /**
   * Define the script files under test. The files will be loaded in the given order.
   * <p/>
   * The HiveRunner instantiate and inject the HiveShell
   */
  @HiveSQL(files = {
  }, encoding = "UTF-8")
  private HiveShell hiveShell;

  @Test
  public void testComplexTypes() throws IOException, RMIException {
    final String sqlFile = "sql/test_complextypes.sql";

    hiveShell.execute(TestHelper.getResourceAsString(sqlFile, testBase.getMap()));

    HashSet<String> expected = Sets.newHashSet("1");
    HashSet<String> actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from complextypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("3\t[[[[[[[[[[[[[[[[[[[[[[[0,1,2]]]]]]]]]]]]]]]]]]]]]]]\t[[[[[[[[[[[[[[[[[[[[[{\"k1\":\"v1\",\"k2\":\"v2\"}]]]]]]]]]]]]]]]]]]]]]\t[[[[[[[[[[[[[[[[[[[[[[{\"s\":\"a\",\"i\":10}]]]]]]]]]]]]]]]]]]]]]]\t2");
    actual = Sets.newHashSet(hiveShell.executeQuery("select * from complextypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("3");
    actual = Sets.newHashSet(hiveShell.executeQuery("select simple_int from complextypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("2");
    actual = Sets.newHashSet(hiveShell.executeQuery("select simple_string from complextypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("[[[[[[[[[[[[[[[[[[[[[[[0,1,2]]]]]]]]]]]]]]]]]]]]]]]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select max_nested_array from complextypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("[[[[[[[[[[[[[[[[[[[[[{\"k1\":\"v1\",\"k2\":\"v2\"}]]]]]]]]]]]]]]]]]]]]]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select max_nested_map from complextypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("[[[[[[[[[[[[[[[[[[[[[[{\"s\":\"a\",\"i\":10}]]]]]]]]]]]]]]]]]]]]]]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select max_nested_struct from complextypes1"));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testComplexTypesMTable() throws IOException, RMIException {
    final String sqlFile = "sql/test_complextypes_MTable.sql";
    final Map<String, String> params = testBase.getMap();
    final String[] types = new String[]{"ordered", "unordered"};
    for (final String type : types) {
      params.put("__type__", type);
      hiveShell.execute(TestHelper.getResourceAsString(sqlFile, params));
      assertComplexTypes();
    }
  }

  private void assertComplexTypes() {
    HashSet<String> expected = Sets.newHashSet("1");
    HashSet<String> actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from Mcomplextypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("3\t[[[[[[[[[[[[[[[[[[[[[[[0,1,2]]]]]]]]]]]]]]]]]]]]]]]\t[[[[[[[[[[[[[[[[[[[[[{\"k1\":\"v1\",\"k2\":\"v2\"}]]]]]]]]]]]]]]]]]]]]]\t[[[[[[[[[[[[[[[[[[[[[[{\"s\":\"a\",\"i\":10}]]]]]]]]]]]]]]]]]]]]]]\t2");
    actual = Sets.newHashSet(hiveShell.executeQuery("select * from Mcomplextypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("3");
    actual = Sets.newHashSet(hiveShell.executeQuery("select simple_int from Mcomplextypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("2");
    actual = Sets.newHashSet(hiveShell.executeQuery("select simple_string from Mcomplextypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("[[[[[[[[[[[[[[[[[[[[[[[0,1,2]]]]]]]]]]]]]]]]]]]]]]]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select max_nested_array from Mcomplextypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("[[[[[[[[[[[[[[[[[[[[[{\"k1\":\"v1\",\"k2\":\"v2\"}]]]]]]]]]]]]]]]]]]]]]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select max_nested_map from Mcomplextypes1"));
    Assert.assertEquals(expected, actual);

    expected = Sets.newHashSet("[[[[[[[[[[[[[[[[[[[[[[{\"s\":\"a\",\"i\":10}]]]]]]]]]]]]]]]]]]]]]]");
    actual = Sets.newHashSet(hiveShell.executeQuery("select max_nested_struct from Mcomplextypes1"));
    Assert.assertEquals(expected, actual);
  }
}
