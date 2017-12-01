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
package io.ampool.monarch.table.facttable.junit;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.facttable.FTableDescriptorHelper;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.region.FTableByteUtils;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(FTableTest.class)
public class FTableByteUtilsTest {

  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void fromRecord() throws Exception {
    Assert.assertNull(FTableByteUtils.fromRecord(null, null));
    Assert.assertNull(FTableByteUtils.fromRecord(new FTableDescriptor(), null));
    Assert.assertNull(FTableByteUtils.fromRecord(null, new Record()));
    Assert.assertNull(FTableByteUtils.fromRecord(new FTableDescriptor(), new Record()));

    final FTableDescriptor fTableDescriptor = FTableDescriptorHelper.getFTableDescriptor();
    fTableDescriptor.finalizeDescriptor();
    final int valueSize = ((fTableDescriptor.getNumOfColumns() - 1) * 4) + Bytes.SIZEOF_LONG;
    byte[] bytes = FTableByteUtils.fromRecord(fTableDescriptor, new Record());
    Assert.assertNotNull(bytes);
    Assert.assertEquals(valueSize, bytes.length);


    Record record = new Record();
    int columnValueLength = 0;
    int varLengthColValueLen = 0;
    for (int i = 0; i < fTableDescriptor.getAllColumnDescriptors().size(); i++) {
      final MColumnDescriptor mColumnDescriptor = fTableDescriptor.getAllColumnDescriptors().get(i);
      if (!mColumnDescriptor.getColumnNameAsString()
          .equals(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) {
        record.add(mColumnDescriptor.getColumnName(), mColumnDescriptor.getColumnName());
        columnValueLength += mColumnDescriptor.getColumnName().length;
      }
      if (!mColumnDescriptor.getColumnType().isFixedLength()) {
        varLengthColValueLen += mColumnDescriptor.getColumnName().length + Bytes.SIZEOF_INT;
      }
    }

    bytes = FTableByteUtils.fromRecord(fTableDescriptor, record);
    Assert.assertNotNull(bytes);
    int expectedSize = fTableDescriptor.getMaxLengthForFixedSizeColumns() + varLengthColValueLen;
    Assert.assertEquals(expectedSize, bytes.length);

    columnValueLength = 0;
    record = new Record();
    varLengthColValueLen = 0;
    expectedSize = fTableDescriptor.getMaxLengthForFixedSizeColumns() + varLengthColValueLen;
    for (int i = 0; i < fTableDescriptor.getAllColumnDescriptors().size(); i++) {
      final MColumnDescriptor mColumnDescriptor = fTableDescriptor.getAllColumnDescriptors().get(i);
      if (!mColumnDescriptor.getColumnType().isFixedLength()) {
        varLengthColValueLen += mColumnDescriptor.getColumnName().length;
        varLengthColValueLen += (i % 4 == 0) ? 0 : Bytes.SIZEOF_INT;
      }
      if (i % 4 == 0) {
        continue;
      }
      if (!mColumnDescriptor.getColumnNameAsString()
          .equals(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) {
        record.add(mColumnDescriptor.getColumnName(), mColumnDescriptor.getColumnName());
        columnValueLength += mColumnDescriptor.getColumnName().length;
      }
    }

    bytes = FTableByteUtils.fromRecord(fTableDescriptor, record);
    Assert.assertNotNull(bytes);
    Assert.assertEquals(expectedSize + varLengthColValueLen, bytes.length);

  }

}
