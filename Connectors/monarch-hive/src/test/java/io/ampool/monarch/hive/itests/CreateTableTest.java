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
import io.ampool.monarch.table.ftable.FTableDescriptor;
import junit.framework.TestCase;
import org.apache.commons.collections.MapUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.lang.ClassLoader.getSystemResource;

/**
 * Hive Runner Reference implementation.
 * <p/>
 * All HiveRunner tests should run with the StandaloneHiveRunner
 */
@RunWith(StandaloneHiveRunner.class)
public class CreateTableTest extends TestCase {

    @ClassRule
    public static TestBaseJUnit testBase = new TestBaseJUnit("CreateTableTest");

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
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/ampool/u.data")
    private File dataFromFile =
            new File(getSystemResource("data/sample_u_data.txt").getPath());

    /**
     * Define the script files under test. The files will be loaded in the given order.
     * <p/>
     * The HiveRunner instantiate and inject the HiveShell
     */
    @HiveSQL(files = {
//            "helloHiveRunner/create_table.sql"
    }, encoding = "UTF-8")
    private HiveShell hiveShell;

    //private HiveStoreConfig hiveStoreConfig;

    @Test
    public void testTablesCreated() throws RMIException, IOException {
        final String sqlFile = "sql/test_u_data.sql";
        final Map<String, String> replaceMap = new HashMap<>(4);
        replaceMap.put(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());
        for (FTableDescriptor.BlockFormat blockFormat : FTableDescriptor.BlockFormat.values()) {
//            System.out.println("### Immutable table with blockFormat= " + blockFormat);
            replaceMap.put(MonarchUtils.BLOCK_FORMAT, blockFormat.name());
            hiveShell.execute(TestHelper.getResourceAsString1(sqlFile, replaceMap));
            assertTablesCreatedFTable();
        }
    }
    private void assertTablesCreatedFTable() {
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

        expected = Sets.newHashSet("100000");
        actual = Sets.newHashSet(hiveShell.executeQuery("select count(*) from p3"));
        Assert.assertEquals(expected, actual);

        /** query using selective columns -- reader fetches only selected column **/
        expected = Sets.newHashSet("200000");
        actual = Sets.newHashSet(hiveShell.executeQuery("select count(age) from p1"));
        Assert.assertEquals(expected, actual);

        expected = Sets.newHashSet("85106026");
        actual = Sets.newHashSet(hiveShell.executeQuery("select sum(age) from p1"));
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

        expected = Sets.newHashSet("1\t878542420");
        actual = Sets.newHashSet(hiveShell.executeQuery("select id, name from p3 where name = '878542420'"));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testTablesCreatedWithMTable() throws RMIException, IOException {
        final String sqlFile = "sql/test_u_data_MTable.sql";

        final Map<String, String> params = testBase.getMap();
        final String[] types = new String[]{"unordered", "ordered"};
        for (final String type : types) {
            params.put("__type__", type);
            hiveShell.execute(TestHelper.getResourceAsString(sqlFile, testBase.getMap()));
            assertCreateTables();
        }
    }

    private void assertCreateTables() {
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
}
