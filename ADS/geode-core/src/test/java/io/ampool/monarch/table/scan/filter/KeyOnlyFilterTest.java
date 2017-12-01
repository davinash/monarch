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

import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.geode.test.junit.categories.MonarchTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.MTableColumnType;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.filter.KeyOnlyFilter;
import io.ampool.monarch.table.internal.AbstractTableDescriptor;
import io.ampool.monarch.table.results.FormatAwareRow;
import io.ampool.monarch.types.BasicTypes;

@Category(MonarchTest.class)
public class KeyOnlyFilterTest {

  @Test
  public void testTransformResult() throws Exception {

    String key = "key";
    String val = "Value";
    long timestamp = Timestamp.from(Instant.now()).getTime();
    MTableDescriptor mTableDescriptor = new MTableDescriptor();
    mTableDescriptor.addColumn("NAME", new MTableColumnType(BasicTypes.STRING));
    mTableDescriptor.addColumn("ID", new MTableColumnType(BasicTypes.STRING));
    mTableDescriptor.addColumn("AGE", new MTableColumnType(BasicTypes.INT));
    mTableDescriptor.addColumn("SALARY", new MTableColumnType(BasicTypes.LONG));
    // To create schema from addColumn
    ((AbstractTableDescriptor) mTableDescriptor).finalizeDescriptor();

    List<Object> colvalues = new ArrayList<>();
    colvalues.add("ABC");
    colvalues.add("A-1");
    colvalues.add(1);
    colvalues.add(1000L);

    byte[] valarray = MTableFilterTestUtils.getRowBytes(mTableDescriptor, colvalues, timestamp);
    Row result = new FormatAwareRow(key.getBytes(), valarray, mTableDescriptor, new ArrayList<>());
    KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
    Row transformed = keyOnlyFilter.transformResult(result);
    // In production row is again transformed after returning scan
    List<byte[]> names = new ArrayList<>();
    mTableDescriptor.getColumnsByName().keySet().stream()
        .forEach(cName -> names.add(cName.getByteArray()));

    assertEquals(result.getRowId(), transformed.getRowId());
    assertEquals(result.getRowTimeStamp(), transformed.getRowTimeStamp());
    assertEquals(0, transformed.getCells().size());
  }
}
