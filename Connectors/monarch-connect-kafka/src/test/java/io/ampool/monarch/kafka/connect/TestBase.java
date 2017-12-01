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

package io.ampool.monarch.kafka.connect;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;


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
    /*MonarchUtils.getConnection(new HashMap<String, String>(1){{
      put(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());
    }}).close();*/
    MonarchUtils.getConnection(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort()).close();
    testBase.tearDown2();
  }
}
