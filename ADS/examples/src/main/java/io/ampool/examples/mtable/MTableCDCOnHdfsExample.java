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

package io.ampool.examples.mtable;


import static io.ampool.security.SecurityConfigurationKeysPublic.ENABLE_KERBEROS_AUTHC;
import static io.ampool.security.SecurityConfigurationKeysPublic.KERBEROS_SECURITY_PASSWORD;
import static io.ampool.security.SecurityConfigurationKeysPublic.KERBEROS_SECURITY_USERNAME;
import static io.ampool.security.SecurityConfigurationKeysPublic.KERBEROS_SERVICE_PRINCIPAL;

import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.CDCConfig;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.distributed.internal.DistributionConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * This example shows MTable CDC listener writing secured hdfs cluster. It performs following table
 * operations. {@link MTableCDCHDFSListener} is used to listen and write change events.
 */

public class MTableCDCOnHdfsExample {
  public static void main(String args[]) throws InterruptedException {
    System.out.println("MTable CDC listener writing on to HDFS Example!");

    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length == 2) {
      locator_host = args[0];
      locator_port = Integer.parseInt(args[1]);
    }

    // Step:1 create a connection with monarch distributed system (DS).
    // create a configuration and connect to monarch locator.
    Properties props = new Properties();

    props.put(DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME,
        "io.ampool.security.AmpoolAuthInitClient.create");
    // Kerberos Service principal name
    props.put(KERBEROS_SERVICE_PRINCIPAL,
        "locator/sri-ampool-locator-for-hdp24-4gh5.c.ampool-141120.internal@TEST.AMPOOL.IO");
    // user principal same as above
    props.put(KERBEROS_SECURITY_USERNAME, "ampool@TEST.AMPOOL.IO");
    props.put(KERBEROS_SECURITY_PASSWORD, "/opt/ampool.keytab");

    props.setProperty(ENABLE_KERBEROS_AUTHC, "true");

    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, "/tmp/MTableClient.log");
    AmpoolClient aClient = new AmpoolClient(locator_host, locator_port, props);
    System.out.println("Connection to monarch distributed system is successfully done!");

    // Step:2 create a Table.
    List<String> columnNames = Arrays.asList("NAME", "ID", "AGE", "SALARY", "DEPT");
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setMaxVersions(1);
    Schema schema = new Schema.Builder().column(columnNames.get(0), BasicTypes.STRING)
        .column(columnNames.get(1), BasicTypes.INT).column(columnNames.get(2), BasicTypes.INT)
        .column(columnNames.get(3), BasicTypes.DOUBLE).column(columnNames.get(4), BasicTypes.STRING)
        .build();
    tableDescriptor.setSchema(schema);

    CDCConfig config = tableDescriptor.createCDCConfig();
    config.setPersistent(true);

    tableDescriptor.addCDCStream("EmployeeTableCDCListener",
        "io.ampool.examples.mtable.MTableCDCHDFSListener", config);

    Admin admin = aClient.getAdmin();
    String tableName = "EmployeeTableCDC";

    MTable table = admin.createMTable(tableName, tableDescriptor);
    System.out.println("Table [" + tableName + "] is created successfully!");

    // create
    for (int i = 0; i < 50; i++) {
      Put employeeRecord = new Put(Bytes.toBytes("Key-" + i));
      employeeRecord.addColumn(columnNames.get(0), "Name-" + i);
      employeeRecord.addColumn(columnNames.get(1), i);
      employeeRecord.addColumn(columnNames.get(2), (i + 1) * 10);
      employeeRecord.addColumn(columnNames.get(3), (i + 2) * 100.0);
      employeeRecord.addColumn(columnNames.get(4), "Dept-" + (i / 10));

      table.put(employeeRecord);
    }

    // update
    for (int i = 0; i < 50; i++) {
      Put employeeRecord = new Put(Bytes.toBytes("Key-" + i));
      employeeRecord.addColumn(columnNames.get(0), "UpdatedName-" + i);
      employeeRecord.addColumn(columnNames.get(1), i);
      employeeRecord.addColumn(columnNames.get(2), (i + 1) * 10);
      employeeRecord.addColumn(columnNames.get(3), (i + 2) * 100.0);
      employeeRecord.addColumn(columnNames.get(4), "Dept-" + (i / 10));

      table.put(employeeRecord);
    }
    //
    for (int i = 10; i < 20; i++) {
      Delete employeeRecord = new Delete(Bytes.toBytes("Key-" + i));
      table.delete(employeeRecord);
    }

    TimeUnit.SECONDS.sleep(20);

    // Step-5: Delete a table
    admin.deleteMTable(tableName);
    System.out.println("Table is deleted successfully!");

    // Step-6: close the Monarch connection
    aClient.close();
    System.out.println("Connection to monarch DS closed successfully!");
  }
}
