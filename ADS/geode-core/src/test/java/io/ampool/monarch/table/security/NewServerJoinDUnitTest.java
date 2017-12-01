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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.ampool.monarch.table.security.rules.MLocatorServerStartupRule;
import io.ampool.monarch.table.security.rules.MServerStarterRule;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.FTableTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category({FTableTest.class, SecurityTest.class})
public class NewServerJoinDUnitTest extends JUnit4DistributedTestCase {

  @Rule
  public MLocatorServerStartupRule lsRule = new MLocatorServerStartupRule();

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    lsRule.startLocatorVM(0, props);
  }

  // AuthC Test-cases for new node joining a DS.
  @Test
  public void testNewServerJoinWithoutCreds_ShouldFail() {
    Properties props = new Properties();
    // props required for peer-peer authC not provided
    VM server = getHost(0).getVM(1);
    server.invoke(() -> {
      MServerStarterRule serverStarter = new MServerStarterRule(props);
      // serverStarter.startServer(lsRule.getMember(0).getPort());
      assertThatThrownBy(() -> serverStarter.startServer(lsRule.getMember(0).getPort()))
          .isInstanceOf(GemFireSecurityException.class).hasMessageContaining(
              "Security check failed. Authentication error. Please check your credentials");
    });
  }

  // start server with invalid/insufficient (cluster/cluster) credentials. - Done
  @Test
  public void testNewServerJoinWithInvalidCreds_ShouldFail() {
    Properties props = new Properties();

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "user");
    props.setProperty("security-password", "invalidPass");

    VM server = getHost(0).getVM(1);
    server.invoke(() -> {
      MServerStarterRule serverStarter = new MServerStarterRule(props);
      assertThatThrownBy(() -> serverStarter.startServer(lsRule.getMember(0).getPort()))
          .isInstanceOf(GemFireSecurityException.class).hasMessageContaining(
              "Security check failed. Authentication error. Please check your credentials");
    });
  }

  // Unauthorized user trying to join a cluster.
  @Test
  public void testNewServerJoinWithInsufficientCreds_ShouldFail() {
    Properties props = new Properties();

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "user");
    props.setProperty("security-password", "user");

    VM server = getHost(0).getVM(1);
    server.invoke(() -> {
      MServerStarterRule serverStarter = new MServerStarterRule(props);
      assertThatThrownBy(() -> serverStarter.startServer(lsRule.getMember(0).getPort()))
          .isInstanceOf(GemFireSecurityException.class)
          .hasMessageContaining("Security check failed. user not authorized for CLUSTER:MANAGE");
    });
  }

  @Test
  public void testNewServerJoinWithValidCredentials_ShouldPass() throws Exception {
    Properties props = new Properties();

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster");
    props.setProperty("security-password", "cluster");

    lsRule.startServerVM(1, props);
  }
}
