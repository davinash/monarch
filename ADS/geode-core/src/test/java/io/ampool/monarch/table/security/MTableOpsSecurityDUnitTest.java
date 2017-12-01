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

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.security.rules.MLocatorServerStartupRule;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.FTableTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Category({FTableTest.class, SecurityTest.class})
public class MTableOpsSecurityDUnitTest extends JUnit4DistributedTestCase {
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


  @Test
  public void testAllOpsWithDataWriter() {
    Properties props = new Properties();
    props.setProperty("security-username", "dataManage");
    props.setProperty("security-password", "dataManage");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");

    {
      Admin admin = SecurityTestHelper
          .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();

      // TODO: For now createTable requires a DATA:WRITE, May need to change this to DATA:MANAGE,
      // refer GEN-1684.
      MTable table = admin.createMTable("EmployeeTable", getMTableDescriptor());

      System.out.println("Table [EmployeeTable] is created successfully!");

      if (MClientCacheFactory.getAnyInstance() != null) {
        MClientCacheFactory.getAnyInstance().close();
      }
    }

    props.setProperty("security-username", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataWrite");
    props.setProperty("security-password", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataWrite");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");


    Admin admin = SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();

    MTable table = MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable");

    System.out.println("NNN starting puts");
    doPuts(table);
    System.out.println("Table[EmployeeTable].puts completed successfully!");

    doCheckAndPut(table);
    System.out.println("Table[EmployeeTable].checkAndPut completed successfully!");

    doCheckAndDelete(table);
    System.out.println("Table[EmployeeTable].checkAndDelete completed successfully!");

    // Scan internally requires DATA:READ:.AMPOOL.MONARCH.TABLE.META.
    assertThatThrownBy(() -> doScan(table)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataWrite not authorized for DATA:READ");

    assertThatThrownBy(() -> doGets(table)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataWrite not authorized for DATA:READ");

    doDeleteRow(table);
    System.out.println("Table[EmployeeTable].deleteRow completed successfully!");

    // Coprocessor internally requires DATA:READ:.AMPOOL.MONARCH.TABLE.META.
    // NotAuthorizedException: dataWrite not authorized for
    // DATA:READ:.AMPOOL.MONARCH.TABLE.META.:EmployeeTable
    MExecutionRequest request = new MExecutionRequest();
    request.setTransactionId("trx-3");

    table.coprocessorService("io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodD",
        null, null, request);

    assertThatThrownBy(() -> table.isEmpty()).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataWrite not authorized for DATA:READ:EmployeeTable");

    doBatchPut(table);

    // doBatchGet(table);

    // NotAuthorizedException: dataWrite not authorized for DATA:READ:.AMPOOL.MONARCH.TABLE.META.
    // refer. Bug GEN-1684.

    try {
      admin.truncateMTable("EmployeeTable");
    } catch (Exception e) {
      System.out.println("Truncate Table got Exception: " + e.toString());
    }

    assertThatThrownBy(() -> admin.deleteMTable("EmployeeTable"))
        .isInstanceOf(NotAuthorizedException.class).hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataWrite not authorized for DATA:MANAGE");

    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }
  }


  @Test
  public void testAllOpsWithDataReader() throws InterruptedException {

    // Write client creates a table
    VM client1 = getHost(0).getVM(2);
    client1.invoke(() -> {
      Properties props = new Properties();
      props.setProperty("security-username", "dataManage");
      props.setProperty("security-password", "dataManage");
      props.setProperty("security-client-auth-init",
          "org.apache.geode.security.templates.UserPasswordAuthInit.create");
      Admin admin = SecurityTestHelper
          .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();

      MTable table = admin.createMTable("EmployeeTable", getMTableDescriptor());
      System.out.println("Table [EmployeeTable] is created successfully!");

      MTable table1 = admin.createMTable("EmployeeTable1", getMTableDescriptor());
      System.out.println("Table [EmployeeTable1] is created successfully!");

      MTable admintable = admin.createMTable("AdminTable", getMTableDescriptor());
      System.out.println("Table [AdminTable] is created successfully!");

      if (MClientCacheFactory.getAnyInstance() != null) {
        MClientCacheFactory.getAnyInstance().close();
      }
    });


    // reader client
    Properties props = new Properties();
    props.setProperty("security-username", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead");
    props.setProperty("security-password", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");

    Admin admin = SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();
    MTable table = MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable");



    ////////
    assertThatThrownBy(() -> doPuts(table)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead not authorized for DATA:WRITE");

    assertThatThrownBy(() -> doCheckAndPut(table)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead not authorized for DATA:WRITE");

    assertThatThrownBy(() -> doCheckAndDelete(table)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead not authorized for DATA:WRITE");

    // Scan internally requires DATA:READ:.AMPOOL.MONARCH.TABLE.META.
    doScan(table);

    doGets(table);
    System.out.println("Table[EmployeeTable].gets completed success");

    assertThatThrownBy(() -> doDeleteRow(table)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead not authorized for DATA:WRITE");

    MExecutionRequest request = new MExecutionRequest();
    request.setTransactionId("trx-3");
    assertThatThrownBy(
        () -> table.coprocessorService("io.ampool.monarch.table.coprocessor.TrxRegionEndpoint",
            "methodD", null, null, request)).isInstanceOf(NotAuthorizedException.class)
                .hasMessageContaining("dataRead not authorized for DATA:WRITE");

    table.isEmpty();

    assertThatThrownBy(() -> doBatchPut(table)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead not authorized for DATA:WRITE");

    assertThatThrownBy(() -> admin.truncateMTable("EmployeeTable"))
        .isInstanceOf(NotAuthorizedException.class).hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead not authorized for DATA:WRITE");

    // NotAuthorizedException: dataWrite not authorized for DATA:READ:.AMPOOL.MONARCH.TABLE.META.
    // refer. Bug GEN-1684.
    assertThatThrownBy(() -> admin.deleteMTable("EmployeeTable"))
        .isInstanceOf(NotAuthorizedException.class).hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead not authorized for DATA:MANAGE");

    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }

    // Thread.sleep(1000);

    props.setProperty("security-username",
        "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1");
    props.setProperty("security-password",
        "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");

    Admin admin1 = SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();
    MTable table1 = MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable1");

    MTable admintable = MClientCacheFactory.getAnyInstance().getMTable("AdminTable");

    System.out.println("Starting the test with EmployeeTable1.");

    doScan(table1);

    table1.isEmpty();

    assertThatThrownBy(() -> doBatchPut(table1)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1 not authorized for DATA:WRITE");

    // Checking denial of authorization for operation done on some inaccessible other table when
    // authorization
    // is granted at table level.
    assertThatThrownBy(() -> doScan(admintable)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1 not authorized for DATA:READ");

    assertThatThrownBy(() -> admintable.isEmpty()).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1 not authorized for DATA:READ");

    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }
  }


  @Test
  public void testAllOpsWithReaderWriterClients() {
    Properties props = new Properties();
    props.setProperty("security-username", "dataManage");
    props.setProperty("security-password", "dataManage");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");

    {
      Admin admin = SecurityTestHelper
          .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();

      MTable table = admin.createMTable("EmployeeTable", getMTableDescriptor());
      System.out.println("Table [EmployeeTable] is created successfully!");

      MTable table1 = admin.createMTable("EmployeeTable1", getMTableDescriptor());
      System.out.println("Table [EmployeeTable1] is created successfully!");

      MTable admintable = admin.createMTable("AdminTable", getMTableDescriptor());
      System.out.println("Table [AdminTable] is created successfully!");

      if (MClientCacheFactory.getAnyInstance() != null) {
        MClientCacheFactory.getAnyInstance().close();
      }
    }

    props.setProperty("security-username", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataWrite");
    props.setProperty("security-password", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataWrite");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");

    Admin admin = SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();

    MTable table = MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable");

    MTable table1 = MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable1");

    doPuts(table);

    doCheckAndPut(table);

    doCheckAndDelete(table);


    // org.apache.geode.security.NotAuthorizedException: dataWrite not authorized for
    // DATA:READ:.AMPOOL.MONARCH.TABLE.META.:EmployeeTable
    // table.scan ===> DATA:READ
    assertThatThrownBy(() -> doScan(table)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataWrite not authorized for DATA:READ");

    // org.apache.geode.security.NotAuthorizedException: dataWrite not authorized for
    // DATA:READ:EmployeeTable:io.ampool.monarch.table.internal.MTableKey@fdc22166
    // table.get() ==> DATA:READ
    assertThatThrownBy(() -> doGets(table)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataWrite not authorized for DATA:READ");

    // NotAuthorizedException: dataWrite not authorized for
    // DATA:READ:.AMPOOL.MONARCH.TABLE.META.:EmployeeTable
    MExecutionRequest request = new MExecutionRequest();
    request.setTransactionId("trx-3");
    // assertThatThrownBy(
    // () -> table.coprocessorService("io.ampool.monarch.table.coprocessor.TrxRegionEndpoint",
    // "methodD", null, null, request)).isInstanceOf(NotAuthorizedException.class)
    // .hasMessageContaining("dataWrite not authorized for DATA:READ");

    table.coprocessorService("io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodD",
        null, null, request);

    doDeleteRow(table);

    doPuts(table1);

    doCheckAndPut(table1);

    doCheckAndDelete(table1);


    VM client1 = getHost(0).getVM(2);
    client1.invoke(() -> {
      Properties cProps = new Properties();
      cProps.setProperty("security-username", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead");
      cProps.setProperty("security-password", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead");
      cProps.setProperty("security-client-auth-init",
          "org.apache.geode.security.templates.UserPasswordAuthInit.create");

      Admin admin2 = SecurityTestHelper
          .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), cProps).getAdmin();

      System.out.println("NNN starting gets from client-1");
      doGets(MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable"));

      // org.apache.geode.security.NotAuthorizedException: dataRead not authorized for DATA:MANAGE
      assertThatThrownBy(() -> admin2.deleteMTable("EmployeeTable"))
          .isInstanceOf(NotAuthorizedException.class).hasMessageContaining(
              "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead not authorized for DATA:MANAGE");


      // No security required..
      // doGetMTableLocationInfo(MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable"));

      MExecutionRequest request2 = new MExecutionRequest();
      request.setTransactionId("trx-3");

      assertThatThrownBy(() -> MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable")
          .coprocessorService("io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodD",
              null, null, request2)).isInstanceOf(NotAuthorizedException.class)
                  .hasMessageContaining("dataRead not authorized for DATA:WRITE");

      doScan(MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable"));

      assertThatThrownBy(
          () -> doDeleteRow(MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable")))
              .isInstanceOf(NotAuthorizedException.class)
              .hasMessageContaining("dataRead not authorized for DATA:WRITE");

      assertThatThrownBy(
          () -> doCheckAndPut(MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable")))
              .isInstanceOf(NotAuthorizedException.class)
              .hasMessageContaining("dataRead not authorized for DATA:WRITE");

      assertThatThrownBy(
          () -> doCheckAndDelete(MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable")))
              .isInstanceOf(NotAuthorizedException.class)
              .hasMessageContaining("dataRead not authorized for DATA:WRITE");

      assertThatThrownBy(() -> admin2.deleteMTable("EmployeeTable"))
          .isInstanceOf(NotAuthorizedException.class)
          .hasMessageContaining("dataRead not authorized for DATA:MANAGE");

      if (MClientCacheFactory.getAnyInstance() != null) {
        MClientCacheFactory.getAnyInstance().close();
      }

    });


    // reader client
    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }

    Properties cProps1 = new Properties();
    cProps1.setProperty("security-username",
        "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1");
    cProps1.setProperty("security-password",
        "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1");
    cProps1.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");
    Admin admin1 = SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), cProps1).getAdmin();

    System.out.println("Getting handle for EmployeeTable1");

    MTable table2 = MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable1");

    MTable admintable = MClientCacheFactory.getAnyInstance().getMTable("AdminTable");

    System.out.println("Testing scan for EmployeeTable1");

    doScan(table2);

    assertThatThrownBy(() -> doBatchPut(table2)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1 not authorized for DATA:WRITE");

    // Checking denial of authorization for operation done on some inaccessible other table when
    // authorization
    // is granted at table level.
    assertThatThrownBy(() -> doScan(admintable)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1 not authorized for DATA:READ");

    assertThatThrownBy(() -> admintable.isEmpty()).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1 not authorized for DATA:READ");

    VM client2 = getHost(0).getVM(3);
    client2.invoke(() -> {
      Properties cProps = new Properties();
      cProps.setProperty("security-username", "data");
      cProps.setProperty("security-password", "data");
      cProps.setProperty("security-client-auth-init",
          "org.apache.geode.security.templates.UserPasswordAuthInit.create");

      Admin admin3 = SecurityTestHelper
          .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), cProps).getAdmin();

      System.out.println("NNN starting scan from client-2");
      doScan(MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable"));

      MExecutionRequest request3 = new MExecutionRequest();
      request.setTransactionId("trx-3");
      Map<Integer, List<Object>> result =
          MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable").coprocessorService(
              "io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodD", null, null,
              request3);

      admin3.deleteMTable("EmployeeTable");

      if (MClientCacheFactory.getAnyInstance() != null) {
        MClientCacheFactory.getAnyInstance().close();
      }
    });

    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }
  }


  // Test: bug-1685, coprocessorService does not work with DATA:WRITE permission,
  // due to additional DATA:READ required for metaRegion.get().
  //
  /*
   * Test-case Description 1. client-1 (having role dataManage) creates a table "EmployeeTable" 2 .
   * client-2 (having role dataWrite) insert rows and execute co-processor.
   */
  @Test
  public void test_Bug1685() {

    // Write client creates a table
    VM client1 = getHost(0).getVM(2);
    client1.invoke(() -> {
      Properties props = new Properties();
      props.setProperty("security-username", "dataManage");
      props.setProperty("security-password", "dataManage");
      props.setProperty("security-client-auth-init",
          "org.apache.geode.security.templates.UserPasswordAuthInit.create");

      Admin admin = SecurityTestHelper
          .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();

      // refer GEN-1684.
      MTable table = admin.createMTable("EmployeeTable", getMTableDescriptor());

      System.out.println("Table [EmployeeTable] is created successfully!");

      if (MClientCacheFactory.getAnyInstance() != null) {
        MClientCacheFactory.getAnyInstance().close();
      }

    });

    Properties props = new Properties();
    props.setProperty("security-username", "dataWrite");
    props.setProperty("security-password", "dataWrite");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");


    Admin admin = SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();

    MTable table = MClientCacheFactory.getAnyInstance().getMTable("EmployeeTable");

    System.out.println("NNN starting puts");
    doPuts(table);
    System.out.println("Table[EmployeeTable].puts completed successfully!");

    MExecutionRequest request = new MExecutionRequest();
    request.setTransactionId("trx-3");

    table.coprocessorService("io.ampool.monarch.table.coprocessor.TrxRegionEndpoint", "methodD",
        null, null, request);

    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }
  }

  // End
  private void doPuts(MTable table) {
    for (int rowIndex = 0; rowIndex < 5; rowIndex++) {
      Put record = new Put(Bytes.toBytes("rowKey-" + rowIndex));
      for (int colIndex = 0; colIndex < COLUMN_NAMES.length; colIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAMES[colIndex]), Bytes.toBytes("val-" + colIndex));
      }
      table.put(record);
      System.out.println("Row inserted successfully");
    }
  }

  private void doBatchPut(MTable table) {
    List<Put> puts = new ArrayList<>();
    for (int rowIndex = 5; rowIndex < 20; rowIndex++) {
      Put record = new Put(Bytes.toBytes("rowKey-" + rowIndex));
      for (int colIndex = 0; colIndex < COLUMN_NAMES.length; colIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAMES[colIndex]), Bytes.toBytes("val-" + colIndex));
      }
      puts.add(record);
    }
    table.put(puts);
    System.out.println("Batch insert done successfully");
  }

  private void doDeleteRow(MTable table) {
    Delete delete = new Delete(Bytes.toBytes("rowKey-0"));
    table.delete(delete);
    System.out.println("Deleted a row entry");
  }

  private void doCheckAndPut(MTable table) {
    Put put = new Put("rowKey-1");
    // set the new value in put
    put.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth"));
    boolean result = table.checkAndPut(Bytes.toBytes("rowKey-1"), Bytes.toBytes("NAME"),
        Bytes.toBytes("val-0"), put);
    System.out.println("NNN checkAndPut result = " + result);
  }


  private void doCheckAndDelete(MTable table) {
    System.out.println("STEP:12 Deleting row [rowKey-2] if column name -NAME - matches val-0");
    Delete delete = new Delete(Bytes.toBytes("rowKey-2"));
    table.checkAndDelete(Bytes.toBytes("rowKey-2"), Bytes.toBytes("NAME"), Bytes.toBytes("val-0"),
        delete);
  }

  private void doGets(MTable table) {
    for (int rowIndex = 0; rowIndex < 5; rowIndex++) {
      Get get = new Get(Bytes.toBytes("rowKey-" + rowIndex));
      System.out.println("XXX Getting a key - " + "rowKey-" + rowIndex);
      Row result = table.get(get);
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (Cell cell : row) {
        Object o = cell.getColumnValue();
        if (o instanceof byte[]) {
          System.out.println("ColumnName   => " + Bytes.toString(cell.getColumnName())
              + " AND ColumnValue  => " + Bytes.toString((byte[]) cell.getColumnValue()));
        } else {
          System.out.println("ColumnName   => " + Bytes.toString(cell.getColumnName())
              + " AND ColumnValue  => " + cell.getColumnValue());
        }
        columnIndex++;
      }
      System.out.println("--------------------------------------");
    }
    System.out.println("Retrieved a row entry successfully!");
  }

  private void doBatchGet(MTable table) {
    List<Get> gets = new ArrayList<>();
    for (int rowIndex = 5; rowIndex < 20; rowIndex++) {
      Get get = new Get(Bytes.toBytes("rowKey-" + rowIndex));
      gets.add(get);
    }
    table.get(gets);
    System.out.println("Batch gets completed successfully!");
  }

  private void doScan(MTable table) {
    System.out.println("ZZZ Scan start()");
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    while (itr.hasNext()) {
      Row res = (Row) itr.next();
      System.out.println("Key" + Bytes.toString(res.getRowId()));
    }
    System.out.println("ZZZ scan ends");
  }

  private void doGetMTableLocationInfo(MTable table) {
    table.getMTableLocationInfo();
  }



}
