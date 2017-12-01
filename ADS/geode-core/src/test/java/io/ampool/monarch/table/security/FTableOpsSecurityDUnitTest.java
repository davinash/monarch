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

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.security.rules.MLocatorServerStartupRule;
import io.ampool.monarch.types.CompareOp;
import io.ampool.utils.TimestampUtil;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.FTableTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category({FTableTest.class, SecurityTest.class})
public class FTableOpsSecurityDUnitTest extends JUnit4DistributedTestCase {

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

  private FTableDescriptor getFTableDescriptor() {
    final Schema schema = new Schema(COLUMN_NAMES);
    FTableDescriptor descriptor = new FTableDescriptor();
    descriptor.setSchema(schema);
    return descriptor;
  }

  @Test
  public void testAllOpsWithReaderWriteClients() {
    Properties props = new Properties();
    props.setProperty("security-username", "dataManage");
    props.setProperty("security-password", "dataManage");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");

    {
      Admin admin = SecurityTestHelper
          .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();

      FTable table = admin.createFTable("EmployeeTable", getFTableDescriptor());
      System.out.println("Table [EmployeeTable] is created successfully!");

      FTable table1 = admin.createFTable("EmployeeTable1", getFTableDescriptor());
      System.out.println("Table [EmployeeTable1] is created successfully!");

      FTable admintable = admin.createFTable("AdminTable", getFTableDescriptor());
      System.out.println("Table [AdminTable] is created successfully!");

      FTable flushtable = admin.createFTable("FlushTable", getFTableDescriptor());
      System.out.println("Table [FlushTable] is created successfully!");

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

    FTable table = MClientCacheFactory.getAnyInstance().getFTable("EmployeeTable");

    FTable table1 = MClientCacheFactory.getAnyInstance().getFTable("EmployeeTable1");

    System.out.println("NNN starting appends to EmployeeTable1");

    doAppends(table);

    doBatchAppends(table);

    System.out.println("NNN starting appends to EmployeeTable1");

    doAppends(table1);

    doBatchAppends(table1);

    // org.apache.geode.security.NotAuthorizedException: dataWrite not authorized for
    // DATA:READ:.AMPOOL.MONARCH.TABLE.META.:EmployeeTable

    assertThatThrownBy(() -> doScanRecords(table)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataWrite not authorized for DATA:READ");


    assertThatThrownBy(() -> doScanRecords(table)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataWrite not authorized for DATA:READ");

    VM client = getHost(0).getVM(2);
    client.invoke(() -> {
      Properties cProps = new Properties();
      cProps.setProperty("security-username", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead");
      cProps.setProperty("security-password", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead");
      cProps.setProperty("security-client-auth-init",
          "org.apache.geode.security.templates.UserPasswordAuthInit.create");

      Admin admin1 = SecurityTestHelper
          .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), cProps).getAdmin();

      // org.apache.geode.security.NotAuthorizedException: dataRead not authorized for DATA:WRITE
      assertThatThrownBy(
          () -> doAppends(MClientCacheFactory.getAnyInstance().getFTable("EmployeeTable")))
              .isInstanceOf(NotAuthorizedException.class).hasMessageContaining(
                  "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead not authorized for DATA:WRITE");

      assertThatThrownBy(
          () -> doBatchAppends(MClientCacheFactory.getAnyInstance().getFTable("EmployeeTable")))
              .isInstanceOf(NotAuthorizedException.class).hasMessageContaining(
                  "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead not authorized for DATA:WRITE");

      doScanRecords(MClientCacheFactory.getAnyInstance().getFTable("EmployeeTable"));

      // truncateFTable call below produces NullPointerException in
      // io.ampool.tierstore.wal.WriteAheadLog.getAllFilesForTableWithBucketId
      // Keep it commented till that bug is fixed.
      //
      // Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
      // CompareOp.LESS, System.nanoTime());
      //
      // assertThatThrownBy(() -> admin1.truncateFTable("EmployeeTable", filter1))
      // .isInstanceOf(NotAuthorizedException.class).hasMessageContaining(
      // "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead not authorized for DATA:WRITE");

      // If security for metaRegion.get() disabled,
      // org.apache.geode.security.NotAuthorizedException: dataRead not authorized for DATA:MANAGE
      assertThatThrownBy(() -> admin1.deleteFTable("EmployeeTable"))
          .isInstanceOf(NotAuthorizedException.class).hasMessageContaining(
              "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataRead not authorized for DATA:MANAGE");

      if (MClientCacheFactory.getAnyInstance() != null) {
        MClientCacheFactory.getAnyInstance().close();
      }
    });


    try {
      admin.forceFTableEviction("EmployeeTable");
    } catch (Exception e) {
      System.out.println("Truncate Table got Exception: " + e.toString());
    }



    // truncateFTable call below produces NullPointerException in
    // io.ampool.tierstore.wal.WriteAheadLog.getAllFilesForTableWithBucketId
    // Keep the truncateFTable call commented till the bug for this is fixed
    //
    Filter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
        CompareOp.LESS, TimestampUtil.getCurrentTime());

    try {
      admin.truncateFTable("EmployeeTable", filter1);
    } catch (Exception e) {
      System.out.println("Truncate Table got Exception: " + e.toString());
    }

    // If security for metaRegion.get() disabled, org.apache.geode.security.NotAuthorizedException:
    // dataWrite not authorized for DATA:MANAGE
    // Else org.apache.geode.security.NotAuthorizedException: dataWrite not authorized for DATA:READ
    assertThatThrownBy(() -> admin.deleteFTable("EmployeeTable"))
        .isInstanceOf(NotAuthorizedException.class).hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,dataWrite not authorized for DATA:MANAGE");

    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }
    System.out.println("Table [EmployeeTable] is created successfully!");

    Properties cProps = new Properties();
    cProps.setProperty("security-username",
        "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1");
    cProps.setProperty("security-password",
        "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1");
    cProps.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");

    Admin admin1 = SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), cProps).getAdmin();
    FTable table2 = MClientCacheFactory.getAnyInstance().getFTable("EmployeeTable1");
    FTable admintable = MClientCacheFactory.getAnyInstance().getFTable("AdminTable");

    doScanRecords(table2);

    assertThatThrownBy(() -> doAppends(table2)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1 not authorized for DATA:WRITE");

    assertThatThrownBy(() -> doBatchAppends(table2)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1 not authorized for DATA:WRITE");

    // Checking denial of authorization for operation done on some inaccessible other table when
    // authorization
    // is granted at table level.
    assertThatThrownBy(() -> doScanRecords(admintable)).isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining(
            "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAREADEmployeeTable1 not authorized for DATA:READ");

    // ===== update with data manage

    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }

    props.setProperty("security-username", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAMANAGE");
    props.setProperty("security-password", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAMANAGE");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");
    Admin admin2 = SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();
    Map<byte[], Object> newColValues = new LinkedHashMap<>();
    Exception e = null;
    try {
      admin2.updateFTable("EmployeeTable", filter1, newColValues);
    } catch (Exception e1) {
      e1.printStackTrace();
      e = e1;
    }

    // update without data manage
    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }

    props.setProperty("security-username", "DATAREAD.AMPOOL.MONARCH.TABLE.META.");
    props.setProperty("security-password", "DATAREAD.AMPOOL.MONARCH.TABLE.META.");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");
    Admin admin3 = SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();
    assertThatThrownBy(() -> admin3.updateFTable("EmployeeTable", filter1, newColValues))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining("DATAREAD.AMPOOL.MONARCH.TABLE.META. not authorized for DATA:MANAGE");



    /********************************************/


    // ===== flush with data manage

    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }

    props.setProperty("security-username", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAMANAGE");
    props.setProperty("security-password", "DATAREAD.AMPOOL.MONARCH.TABLE.META.,DATAMANAGE");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");
    Admin admin4 = SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();

    Exception ex = null;
    try {
      admin4.forceFTableEviction("FlushTable");
    } catch (Exception e1) {
      e1.printStackTrace();
      ex = e1;
    }

    // flush with data read
    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }

    props.setProperty("security-username", "DATAREAD.AMPOOL.MONARCH.TABLE.META.");
    props.setProperty("security-password", "DATAREAD.AMPOOL.MONARCH.TABLE.META.");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");
    Admin admin5 = SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();
    assertThatThrownBy(() -> admin5.forceFTableEviction("FlushTable"))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining("DATAREAD.AMPOOL.MONARCH.TABLE.META. not authorized for DATA:MANAGE");


    // flush with data write
    if (MClientCacheFactory.getAnyInstance() != null) {
      MClientCacheFactory.getAnyInstance().close();
    }

    props.setProperty("security-username", "DATAWRITE.AMPOOL.MONARCH.TABLE.META.");
    props.setProperty("security-password", "DATAWRITE.AMPOOL.MONARCH.TABLE.META.");
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.UserPasswordAuthInit.create");
    Admin admin6 = SecurityTestHelper
        .createAmpoolClient("127.0.0.1", lsRule.getMember(0).getPort(), props).getAdmin();
    assertThatThrownBy(() -> admin6.forceFTableEviction("FlushTable"))
        .isInstanceOf(NotAuthorizedException.class).hasMessageContaining(
            "DATAWRITE.AMPOOL.MONARCH.TABLE.META. not authorized for DATA:MANAGE");

  }

  private void doAppends(FTable table) {
    for (int i = 0; i < 10; i++) {
      Record record = new Record();
      record.add(COLUMN_NAMES[0], Bytes.toBytes("NAME" + i));
      record.add(COLUMN_NAMES[1], Bytes.toBytes("ID" + i));
      record.add(COLUMN_NAMES[2], Bytes.toBytes(10 + i));
      record.add(COLUMN_NAMES[3], Bytes.toBytes(10000 * i));
      record.add(COLUMN_NAMES[4], Bytes.toBytes("DEPT"));
      record.add(COLUMN_NAMES[5], Bytes.toBytes("21/11/2000"));

      if (table == null) {
        System.out.println("NNNN .doAppends table is NULL");
      }
      table.append(record);
    }
  }

  private void doBatchAppends(FTable table) {
    Record[] records = new Record[10];
    for (int i = 0; i < 10; i++) {
      Record record = new Record();
      record.add(COLUMN_NAMES[0], Bytes.toBytes("NAME" + i));
      record.add(COLUMN_NAMES[1], Bytes.toBytes("ID" + i));
      record.add(COLUMN_NAMES[2], Bytes.toBytes(10 + i));
      record.add(COLUMN_NAMES[3], Bytes.toBytes(10000 * i));
      record.add(COLUMN_NAMES[4], Bytes.toBytes("DEPT"));
      record.add(COLUMN_NAMES[5], Bytes.toBytes("21/11/2000"));
      records[i] = record;
    }
    table.append(records);
  }

  private static void doScanRecords(FTable fTable) {
    // final FTable fTable = MClientCacheFactory.getAnyInstance().getFTable("EmployeeTable");
    final Scanner scanner = fTable.getScanner(new Scan());
    final Iterator<Row> iterator = scanner.iterator();

    int recordCount = 0;
    while (iterator.hasNext()) {
      recordCount++;
      final Row result = iterator.next();
      System.out.println("============= Record " + recordCount + " =============");
      // read the columns
      final List<Cell> cells = result.getCells();
      StringBuilder builder = new StringBuilder();
      for (Cell cell : cells) {
        builder.append(Bytes.toString(cell.getColumnName()) + ":");
        Object value = cell.getColumnValue();
        if (value instanceof byte[]) {
          builder.append(Bytes.toString((byte[]) value) + "\n");
        } else {
          builder.append(value);
        }
      }
      System.out.println(builder.toString());
    }
    System.out.println("Successfully scanned " + recordCount + "records.");
  }
}
