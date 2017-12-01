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

package io.ampool.orc;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;

import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.Encoding;
import io.ampool.monarch.table.internal.ServerRow;
import io.ampool.store.StoreRecord;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.test.junit.categories.FTableTest;
import org.apache.orc.ColumnStatistics;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(FTableTest.class)
public class AColumnStatisticsTest {
  private static final String SCHEMA =
      "struct<c1:INT,c2:DOUBLE,c3:STRING,c4:DATE,c5:TIMESTAMP,c6:array<LONG>>";
  private static final TableDescriptor TD =
      new FTableDescriptor().setSchema(Schema.fromString(SCHEMA));
  private static final StoreRecord record = new StoreRecord(TD.getNumOfColumns());
  private static final ServerRow row = ServerRow.create(TD);
  private static final Encoding enc = TD.getEncoding();

  /**
   * Simple test that asserts the statistics are correctly stored for the types even with serialize
   * and de-serialize. Also assert that min changes accordingly when null values are passed as for
   * fixed length columns use default values.
   *
   * @throws IOException if not able to read/write
   * @throws ClassNotFoundException the class does not exist
   */
  @Test
  public void testStatisticsBasic() throws IOException, ClassNotFoundException {
    final Object[] values = new Object[] {1, 11.11, "abc_EFG", Date.valueOf("2017-10-10"),
        new Timestamp(123456789L), new Object[] {55L, 56L, 57L}};
    final String[] expected =
        new String[] {"1", "11.11", "abc_EFG", "2017-10-10", "1970-01-02 15:47:36.789", null};
    final String[] expectedMin =
        new String[] {"0", "0.0", "abc_EFG", "1970-01-01", "1970-01-01 05:30:00.0", null};

    final AColumnStatistics acs = new AColumnStatistics(TD);
    row.reset(null, enc.serializeValue(TD, record.reset(values)), enc, null);
    acs.updateRowStatistics(TD.getAllColumnDescriptors(), row);

    assertColumnStatistics(expected, expected, acs);

    final HeapDataOutputStream out = new HeapDataOutputStream(1024, null);
    DataSerializer.writeObject(acs, out);
    final byte[] dataBytes = out.toByteArray();

    AColumnStatistics ncs =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(dataBytes)));

    assertColumnStatistics(expected, expected, ncs);

    row.reset(null, enc.serializeValue(TD, record.reset(new Object[TD.getNumOfColumns()])), enc,
        null);
    acs.updateRowStatistics(TD.getAllColumnDescriptors(), row);
    assertColumnStatistics(expectedMin, expected, acs);
  }

  private void assertColumnStatistics(final String[] expectedMin, final String[] expectedMax,
      final AColumnStatistics acs) {
    for (int i = 0; i < TD.getNumOfColumns(); i++) {
      final ColumnStatistics stats = acs.getColumnStatistics(i);
      assertEquals("Incorrect min value for columnId= " + i, expectedMin[i],
          AColumnStatistics.get(stats, "getMinimum"));
      assertEquals("Incorrect max value for columnId= " + i, expectedMax[i],
          AColumnStatistics.get(stats, "getMaximum"));
    }
  }
}
