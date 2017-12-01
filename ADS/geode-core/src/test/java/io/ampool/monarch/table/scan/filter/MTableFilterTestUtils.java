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
package io.ampool.monarch.table.scan.filter;

import static org.junit.Assert.assertEquals;

import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.internal.MOperation;
import io.ampool.monarch.table.internal.MTableStorageFormatter;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.MValue;
import io.ampool.monarch.table.internal.MultiVersionValue;
import io.ampool.monarch.table.internal.StorageFormatter;
import io.ampool.monarch.table.internal.StorageFormatters;

import java.util.List;


public abstract class MTableFilterTestUtils {


  public static byte[] getRowBytes(MTableDescriptor tableDescriptor, List<Object> colValues,
      long timestamp) {
    List<MColumnDescriptor> columnDescriptors = tableDescriptor.getAllColumnDescriptors();


    Put put = new Put("testKey");
    put.setTimeStamp(1);

    int i = 0;
    for (MColumnDescriptor columnDescriptor : columnDescriptors) {
      put.addColumn(columnDescriptor.getColumnNameAsString(), colValues.get(i++));
    }

    StorageFormatter mtableFormatter = MTableUtils.getStorageFormatter(tableDescriptor);

    assertEquals(MTableStorageFormatter.class, mtableFormatter.getClass());

    MValue mValue = MValue.fromPut(tableDescriptor, put, MOperation.PUT);

    Object finalValue =
        mtableFormatter.performPutOperation(tableDescriptor, mValue, mValue.getOpInfo(), null);


    if (tableDescriptor.getMaxVersions() > 1) {
      return ((MultiVersionValue) finalValue).getLatestVersion();
    } else {
      return (byte[]) finalValue;
    }
  }

}
