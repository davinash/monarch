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
package io.ampool.monarch.table;

import io.ampool.monarch.table.client.MClientCache;

public class TableHelper {

  public static MTable createEmployeeTable(MClientCache clientCache) {

    MTableDescriptor tableDescriptor = new MTableDescriptor();

    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"))
        .addColumn(Bytes.toBytes("DEPT")).addColumn(Bytes.toBytes("DOJ"));

    tableDescriptor.setRedundantCopies(1);

    Admin admin = clientCache.getAdmin();
    return admin.createTable("EmployeeTable", tableDescriptor);

  }

  public static Put createPutRecord(final String rowkey, final String name, final int id,
      final int age, final int salary, final int dept, final String date) {
    Put putRecord = new Put(Bytes.toBytes(rowkey));

    putRecord.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes(name));
    putRecord.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(id));
    putRecord.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(age));
    putRecord.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(salary));
    putRecord.addColumn(Bytes.toBytes("DEPT"), Bytes.toBytes(dept));
    putRecord.addColumn(Bytes.toBytes("DOJ"), Bytes.toBytes(date));

    return putRecord;

  }

}
