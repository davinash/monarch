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
package io.ampool.monarch.table.security;

import io.ampool.client.AmpoolClient;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.TableColumnAlreadyExists;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ServerStarterRule;

import java.util.Properties;

import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;

public class SecurityTestHelper {
  public static final int NUM_OF_COLUMNS = 10;
  public static final String TABLE_NAME = "MTableAllOpsDUnitTest";
  public static final String KEY_PREFIX = "KEY";
  public static final String VALUE_PREFIX = "VALUE";
  public static final int NUM_OF_ROWS = 10;
  public static final String COLUMN_NAME_PREFIX = "COLUMN";
  public static final int LATEST_TIMESTAMP = 300;
  public static final int MAX_VERSIONS = 5;
  public static final int TABLE_MAX_VERSIONS = 7;

  public static void startSecureServer(int locatorPort) {
    Properties props = new Properties();

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "user");
    props.setProperty("security-password", "user");

    VM server = getHost(0).getVM(1);
    server.invoke(() -> {
      ServerStarterRule serverStarter = new ServerStarterRule(props);
      assertThatThrownBy(() -> serverStarter.startServer(locatorPort))
          .isInstanceOf(GemFireSecurityException.class)
          .hasMessageContaining("user not authorized for CLUSTER:MANAGE");
    });
  }

  public static AmpoolClient createAmpoolClient(String locator_host, int locator_port,
      Properties props) {
    // Java client connect it programmatically.
    /*
     * MConfiguration mconf = MConfiguration.create();
     * mconf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, "127.0.0.1");
     * mconf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, locatorPort);
     * 
     * // String testName = getTestMethodName(); String logFileName = testName + "-client.log";
     * 
     * mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, logFileName);
     * 
     * MClientCache mClientCache = new MClientCacheFactory().create(mconf);
     * assertNotNull(mClientCache); return mClientCache;
     */

    return new AmpoolClient(locator_host, locator_port, props);
  }

  // create MTableDescriptor
  private static MTableDescriptor getMTableDescriptor() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();

    tableDescriptor.setMaxVersions(1);
    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    // add the same columns again
    Exception e = null;
    try {
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        tableDescriptor =
            tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + "-" + colmnIndex));
      }
    } catch (TableColumnAlreadyExists ex) {
      ex.printStackTrace();
      e = ex;
    }
    return tableDescriptor;
  }

  // create FTableDescriptor
  private static FTableDescriptor getFTableDescriptor() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    FTableDescriptor tableDescriptor = new FTableDescriptor();

    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    // add the same columns again
    Exception e = null;
    try {
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        tableDescriptor =
            tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + "-" + colmnIndex));
      }
    } catch (TableColumnAlreadyExists ex) {
      ex.printStackTrace();
      e = ex;
    }
    return tableDescriptor;
  }

  public static Properties getValidCredentials() {
    Properties props = new Properties();
    props.setProperty("security-username", "cluster");
    props.setProperty("security-password", "cluster");
    return props;
  }

  public static Properties getInValidCredentials() {
    Properties props = new Properties();
    props.setProperty("security-username", "user");
    props.setProperty("security-password", "invalid");
    return props;
  }

  public static Properties getInSufficientCredentials() {
    Properties props = new Properties();
    props.setProperty("security-username", "user");
    props.setProperty("security-password", "user");
    return props;
  }
}
