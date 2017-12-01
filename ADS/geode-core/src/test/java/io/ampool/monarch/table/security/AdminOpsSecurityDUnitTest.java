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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.security.rules.MLocatorServerStartupRule;
import org.apache.geode.internal.Assert;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.FTableTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category({FTableTest.class, SecurityTest.class})
public class AdminOpsSecurityDUnitTest extends JUnit4DistributedTestCase {

  private static final String[] COLUMN_NAMES =
      new String[] {"NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ"};

  @Rule
  public MLocatorServerStartupRule lsRule = new MLocatorServerStartupRule();

  @Before
  public void before() throws Exception {
    Properties locProps = new Properties();
    locProps.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    lsRule.startLocatorVM(0, locProps);

    Properties serProps = new Properties();
    serProps.setProperty("security-username", "cluster");
    serProps.setProperty("security-password", "cluster");
    lsRule.startServerVM(1, serProps, lsRule.getMember(0).getPort());
  }

  private MTableDescriptor getMTableDescriptor() {
    final Schema schema = new Schema(COLUMN_NAMES);
    MTableDescriptor descriptor = new MTableDescriptor();
    descriptor.setSchema(schema);
    return descriptor;
  }

  private FTableDescriptor getFTableDescriptor() {
    final Schema schema = new Schema(COLUMN_NAMES);
    FTableDescriptor descriptor = new FTableDescriptor();
    descriptor.setSchema(schema);
    return descriptor;
  }

  // Test-cases for Client authC both for AuthCRequired and AuthCFailed cases...
  /**
   * Test-case: Unauthenticated user trying to perform MAdmin ops. (create, delete, list, truncate
   * etc...)
   *
   */
  @Test
  public void testAdminOpsWithoutCreds_ShouldFail() throws Exception {
    IgnoredException
        .addIgnoredException("org.apache.geode.security.AuthenticationRequiredException");
    System.out.println("Testing AdminOps without credentials....");
    Properties props = new Properties();
    // props.setProperty("security-username", "cluster");
    // props.setProperty("security-password", "cluster");
    // props.setProperty("security-client-auth-init",
    // "org.apache.geode.security.templates.UserPasswordAuthInit.create");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .createMTable("EmployeeTable", getMTableDescriptor()))
            .isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .createTable("EmployeeTable", getMTableDescriptor()))
            .isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .createFTable("EmployeeTable", getFTableDescriptor()))
            .isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .tableExists("EmployeeTable")).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .existsMTable("EmployeeTable")).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .existsFTable("EmployeeTable")).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .listTables()).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .listMTables()).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .listFTables()).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .listTableNames()).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .listMTableNames()).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .listFTableNames()).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .deleteTable("EmployeeTable")).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .deleteMTable("EmployeeTable")).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .deleteFTable("EmployeeTable")).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .existsMTable("EmployeeTable")).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getMTable("EmployeeTable")).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .existsFTable("EmployeeTable")).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getFTable("EmployeeTable")).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .truncateTable("EmployeeTable")).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .truncateMTable("EmployeeTable")).isInstanceOf(AuthenticationRequiredException.class)
            .hasMessageContaining("No security credentials are provided");

    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }
  }


  @Test
  public void testAdminOpsWithInvalidCreds_ShouldFail() throws Exception {
    IgnoredException.addIgnoredException("org.apache.geode.security.AuthenticationFailedException");
    System.out.println("Testing AdminOps without credentials....");
    Properties props = new Properties();
    props.setProperty("security-username", "user");
    props.setProperty("security-password", "invalid");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .createMTable("EmployeeTable", getMTableDescriptor()))
            .isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .createTable("EmployeeTable", getMTableDescriptor()))
            .isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .createFTable("EmployeeTable", getFTableDescriptor()))
            .isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .tableExists("EmployeeTable")).isInstanceOf(AuthenticationFailedException.class);


    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .existsMTable("EmployeeTable")).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .existsFTable("EmployeeTable")).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .listTables()).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .listMTables()).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .listFTables()).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .listTableNames()).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .listMTableNames()).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .listFTableNames()).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .deleteTable("EmployeeTable")).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .deleteMTable("EmployeeTable")).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .deleteFTable("EmployeeTable")).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .existsFTable("EmployeeTable")).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getFTable("EmployeeTable")).isInstanceOf(AuthenticationFailedException.class);


    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .existsMTable("EmployeeTable")).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getMTable("EmployeeTable")).isInstanceOf(AuthenticationFailedException.class);


    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .truncateTable("EmployeeTable")).isInstanceOf(AuthenticationFailedException.class);

    assertThatThrownBy(() -> SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin()
        .truncateMTable("EmployeeTable")).isInstanceOf(AuthenticationFailedException.class);

    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }
  }

  @Test
  public void testAdminOpsWithValidCreds_ShouldPass() throws Exception {

    System.out.println("Testing AdminOps without credentials....");
    Properties props = new Properties();
    props.setProperty("security-username", "data");
    props.setProperty("security-password", "data");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");

    SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getAdmin().createMTable("EmployeeTable_mtable", getMTableDescriptor());

    Assert.assertTrue(
        SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
            .existsMTable("EmployeeTable_mtable"));
    org.apache.geode.test.dunit.Assert.assertNotNull(
        SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
            .getMTable("EmployeeTable_mtable"));

    SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getAdmin().createTable("EmployeeTable", getMTableDescriptor());

    SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getAdmin().createFTable("EmployeeTable_ftable", getFTableDescriptor());

    Assert.assertTrue(
        SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
            .existsFTable("EmployeeTable_ftable"));
    org.apache.geode.test.dunit.Assert.assertNotNull(
        SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
            .getFTable("EmployeeTable_ftable"));

    SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getAdmin().tableExists("EmployeeTable");

    SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getAdmin().existsMTable("EmployeeTable_mtable");

    SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getAdmin().existsFTable("EmployeeTable_ftable");

    /*
     * SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
     * .getAdmin() .listTables();
     */

    /*
     * SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
     * .getAdmin() .listMTables();
     */

    /*
     * SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
     * .getAdmin() .listFTables();
     */


    SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getAdmin().listTableNames();


    SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getAdmin().listMTableNames();

    SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getAdmin().listFTableNames();

    SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getAdmin().deleteTable("EmployeeTable");


    SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getAdmin().deleteMTable("EmployeeTable_mtable");

    Assert.assertTrue(
        !SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
            .existsFTable("EmployeeTable_mtable"));
    org.apache.geode.test.dunit.Assert.assertNull(
        SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
            .getFTable("EmployeeTable_mtable"));


    SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
        .getAdmin().deleteFTable("EmployeeTable_ftable");

    Assert.assertTrue(
        !SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
            .existsFTable("EmployeeTable_ftable"));
    org.apache.geode.test.dunit.Assert.assertNull(
        SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
            .getFTable("EmployeeTable_ftable"));


    /*
     * SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
     * .getAdmin() .truncateTable("EmployeeTable");
     * 
     * SecurityTestHelper.createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props)
     * .getAdmin() .truncateMTable("EmployeeTable_mtable");
     * 
     * 
     */

    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }
  }
}
