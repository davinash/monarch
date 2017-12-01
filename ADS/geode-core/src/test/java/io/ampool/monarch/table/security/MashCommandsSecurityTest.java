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

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.security.rules.MServerStarterRule;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.junit.categories.FTableTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Properties;

@Category({FTableTest.class, SecurityTest.class})
public class MashCommandsSecurityTest {

  protected static int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
  protected static int jmxPort = ports[0];
  protected static int httpPort = ports[1];

  static Properties properties = new Properties() {
    {
      setProperty(JMX_MANAGER_PORT, jmxPort + "");
      setProperty(HTTP_SERVICE_PORT, httpPort + "");
      setProperty(SECURITY_MANAGER, TestSecurityManager.class.getName());
      setProperty("security-json",
          "org/apache/geode/management/internal/security/cacheServer.json");

    }
  };
  @Rule
  public GfshShellConnectionRule gfshConnection =
      new GfshShellConnectionRule(jmxPort, GfshShellConnectionRule.PortType.jmxManger);

  @ClassRule
  public static MServerStarterRule serverStarter = new MServerStarterRule(properties);

  @BeforeClass
  public static void beforeClass() throws Exception {
    serverStarter.cache.createRegionFactory(RegionShortcut.REPLICATE).create("region1");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    // Do cleanup - diskStores, dir etc.
    // serverStarter.cache.
  }


  private void runCommandsWithAndWithout(String permission) throws Exception {
    List<MashTestCommand> allPermitted =
        MashTestCommand.getPermittedCommands(new WildcardPermission(permission, true));

    // All permitted commands
    for (MashTestCommand permitted : allPermitted) {
      LogService.getLogger().info("Processing authorized command: " + permitted.getCommand());


      CommandResult result = gfshConnection.executeCommand(permitted.getCommand());
      assertNotNull(result);

      if (result.getResultData() instanceof ErrorResultData) {
        assertNotEquals(ResultBuilder.ERRORCODE_UNAUTHORIZED,
            ((ErrorResultData) result.getResultData()).getErrorCode());
      } else {
        assertEquals(Result.Status.OK, result.getStatus());
      }
    }

    System.out.println("NIL MashCommandsSecurityTest.runCommandsWithAndWithout");
    List<MashTestCommand> others = MashTestCommand.getCommands();
    others.removeAll(allPermitted);
    for (MashTestCommand other : others) {
      // skip no permission commands
      if (other.getPermission() == null)
        continue;

      System.out.println("NIL Processing unauthorized command: " + other.getCommand());

      LogService.getLogger().info("NIL Processing unauthorized command: " + other.getCommand());
      CommandResult result = (CommandResult) gfshConnection.executeCommand(other.getCommand());
      int errorCode = ((ErrorResultData) result.getResultData()).getErrorCode();

      // for some commands there are pre execution checks to check for user input error, will skip
      // those commands
      if (errorCode == ResultBuilder.ERRORCODE_USER_ERROR) {
        LogService.getLogger().info("Skip user error: " + result.getContent());
        continue;
      }

      assertEquals(ResultBuilder.ERRORCODE_UNAUTHORIZED,
          ((ErrorResultData) result.getResultData()).getErrorCode());
      String resultMessage = result.getContent().toString();
      String permString = other.getPermission().toString();
      assertTrue(resultMessage + " does not contain " + permString,
          resultMessage.contains(permString));
    }
  }

  @Test
  @ConnectionConfiguration(user = "data-admin", password = "1234567")
  public void testValidCredentials() throws Exception {
    assertTrue(gfshConnection.isConnected());
  }

  @Test
  @ConnectionConfiguration(user = "data-manager", password = "1234567")
  public void testDataManager() throws Exception {
    runCommandsWithAndWithout("DATA:MANAGE");
  }

  @Test
  @ConnectionConfiguration(user = "data-reader", password = "1234567")
  public void testDataReader() throws Exception {
    runCommandsWithAndWithout("DATA:READ");
  }
}
