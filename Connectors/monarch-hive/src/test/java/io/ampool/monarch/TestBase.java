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
package io.ampool.monarch;

import io.ampool.monarch.hive.MonarchDUnitBase;
import io.ampool.monarch.hive.MonarchUtils;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.util.HashMap;

/**
 * Base test class to be inherited by all TestNG tests so that DUnit
 *   VM creation can be done only once and reused for all tests by
 *   having individual tests create unique region (based on test name)
 *   and all tests can be run parallel to boost the test case execution
 *   time.
 *
 */
public class TestBase {
  static public MonarchDUnitBase testBase;

  @BeforeSuite
  public void setUpBeforeClass() throws Exception {
//    System.out.println("@BeforeSuite -> TestBase.setUpBeforeClass");
    testBase = new MonarchDUnitBase("TestBase");
    testBase.setUp();
  }
  @AfterSuite
  public void setUpAfterClass() throws Exception {
//    System.out.println("@AfterSuite -> TestBase.setUpAfterClass");
    MonarchUtils.getConnection(new HashMap<String, String>(1){{
      put(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());
    }}).close();
    testBase.tearDown2();
  }
}
