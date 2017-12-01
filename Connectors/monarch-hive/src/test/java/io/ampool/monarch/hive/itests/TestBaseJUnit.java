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

import java.util.HashMap;
import java.util.Map;

import io.ampool.monarch.hive.MonarchDUnitBase;
import io.ampool.monarch.hive.MonarchUtils;
//import io.ampool.store.hive.HiveStoreConfig;
import org.junit.rules.ExternalResource;

/**
 * A base-class that can be loaded only once for JUnit tests..
 * <p>
 * Created on: 2016-01-21
 * Since version: 0.2.0
 */
public class TestBaseJUnit extends ExternalResource {
  private MonarchDUnitBase testBase;
  private String name = "TestBaseJUnit";

  public TestBaseJUnit(final String name) {
    this.name = name;
  }

  @Override
  protected void before() throws Throwable {
    testBase = new MonarchDUnitBase(name);
    testBase.setUp();
    setupHiveStore();
  }

  @Override
  protected void after() {
    try {
      Map<String, String> map = new HashMap<String, String>(1) {{
        put(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());
      }};
      MonarchUtils.getConnection(map).close();
      testBase.tearDown2();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void setupHiveStore() {
//    HiveStoreConfig hiveStoreConfig;
//    hiveStoreConfig = HiveStoreConfig.create();
//    hiveStoreConfig.set("hive.dbname", "ampool");
//
//    String port = System.getenv(testBase.HIVETEST_ENV_HIVEPORT);
//    if (port != null) {
//      hiveStoreConfig.set("hiveserver.port", port);
//    } else {
//      hiveStoreConfig.set("hiveserver.port", "10000");
//    }
//
//    String host = System.getenv(testBase.HIVETEST_ENV_HIVEHOSTNAME);
//    if (host != null) {
//      hiveStoreConfig.set("hiveserver.host", host);
//    } else {
//      hiveStoreConfig.set("hiveserver.host", "52.27.170.98");
//    }
//
//    String user = System.getenv(testBase.HIVETEST_ENV_HIVEUSERNAME);
//    if (user != null) {
//      hiveStoreConfig.set("hive.user", user);
//    } else {
//      hiveStoreConfig.set("hive.user", "hive");
//    }
//
//    String password = System.getenv(testBase.HIVETEST_ENV_HIVEPASSWD);
//    if (password != null) {
//      hiveStoreConfig.set("hive.password", password);
//    } else {
//      hiveStoreConfig.set("hive.password", "abc123");
//    }
//
//    testBase.createDBOnHiveServer(hiveStoreConfig);
  }
  public Map<String, String> getMap() {
    Map<String, String> map = new HashMap<>();
    map.put("10334", testBase.getLocatorPort());

//    String port = System.getenv(testBase.HIVETEST_ENV_HIVEPORT);
//    if (port != null)
//      map.put("10000", port);
//
//    String host = System.getenv(testBase.HIVETEST_ENV_HIVEHOSTNAME);
//    if (host != null)
//      map.put("52.27.170.98", host);
//
//    String user = System.getenv(testBase.HIVETEST_ENV_HIVEUSERNAME);
//    if (user != null)
//      map.put("hive", user);
//
//    String password = System.getenv(testBase.HIVETEST_ENV_HIVEPASSWD);
//    if (password != null)
//      map.put("abc123", user);

    return map;
  }
  public String getLocatorPort() {
    return testBase.getLocatorPort();
  }
}
