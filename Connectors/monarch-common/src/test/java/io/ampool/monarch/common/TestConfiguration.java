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
package io.ampool.monarch.common;

import io.ampool.monarch.table.MConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestConfiguration {
    @BeforeClass
    public static void setUpClass() throws Exception {
        //System.out.println("TestConfiguration::setUpClass");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        //System.out.println("TestConfiguration::tearDownClass");
    }

    @Test
    public void testUsingAPI() throws Exception {
        MConfiguration mConf = MConfiguration.create();
        mConf.setBoolean("boolean.prop", true);

        Assert.assertTrue(mConf.getBoolean("boolean.prop"));

    }

    @Test
    public void testConfiguration() throws Exception {
        MConfiguration mConf = MConfiguration.create();
        mConf.set("test.property.1", "value1");
        Assert.assertEquals(mConf.get("test.property.1"), "value1");


    }
}
