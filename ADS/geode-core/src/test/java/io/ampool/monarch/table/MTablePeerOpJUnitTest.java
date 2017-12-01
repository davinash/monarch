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

import io.ampool.monarch.types.BasicTypes;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MTablePeerOpJUnitTest extends JUnit4DistributedTestCase {
  @Test
  public void testSimpleOp() {
    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
    InternalDistributedSystem ds = getSystem(props);
    MCacheFactory.create(ds);
    MCacheFactory.getAnyInstance();

    final String tableName = getTestMethodName() + "_MTable";
    MTableDescriptor tableDescriptor1 = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    for (int colIdx = 0; colIdx < 5; colIdx++) {
      tableDescriptor1.addColumn("Column-" + colIdx);
    }
    tableDescriptor1.setRecoveryDelay(0);
    tableDescriptor1.setRedundantCopies(1);
    tableDescriptor1.setTotalNumOfSplits(100);
    MTable table =
        MCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, tableDescriptor1);

    for (int rowKey = 0; rowKey < 1000; rowKey++) {
      Put row = new Put(Bytes.toBytes(rowKey));
      for (int colIdx = 0; colIdx < 5; colIdx++) {
        row.addColumn("Column-" + colIdx, Bytes.toBytes("Value-" + colIdx));
      }
      table.put(row);
    }

    for (int rowKey = 0; rowKey < 1000; rowKey++) {
      Get get = new Get(Bytes.toBytes(rowKey));
      Row row = table.get(get);
      assertFalse(row.isEmpty());
      List<Cell> cells = row.getCells();
      assertEquals(6, cells.size());

      for (int colIdx = 0; colIdx < cells.size() - 1; colIdx++) {
        Cell cell = cells.get(colIdx);
        assertEquals(0, Bytes.compareTo(cell.getColumnName(), Bytes.toBytes("Column-" + colIdx)));
        assertEquals(0,
            Bytes.compareTo((byte[]) cell.getColumnValue(), Bytes.toBytes("Value-" + colIdx)));
      }
    }
    MCacheFactory.getAnyInstance().getAdmin().deleteMTable(tableName);
  }

  @Test
  public void testMultiVersionGet() {
    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
    InternalDistributedSystem ds = getSystem(props);
    MCacheFactory.create(ds);
    MCacheFactory.getAnyInstance();

    final String tableName = getTestMethodName() + "_MTable";
    MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    for (int colIdx = 0; colIdx < 5; colIdx++) {
      tableDescriptor.addColumn("Column-" + colIdx, BasicTypes.STRING);
    }
    tableDescriptor.setRecoveryDelay(0);
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setTotalNumOfSplits(100);
    tableDescriptor.setMaxVersions(3);
    assertNotNull(MCacheFactory.getAnyInstance());
    MTable table =
        MCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, tableDescriptor);

    final int numOfRows = 10;

    for (int rowKey = 0; rowKey < numOfRows; rowKey++) {
      for (int ts = 100; ts <= 300; ts += 100) {
        Put row = new Put(Bytes.toBytes(rowKey));
        row.setTimeStamp(ts);
        for (int colIdx = 0; colIdx < 5; colIdx++) {
          row.addColumn("Column-" + colIdx, "Value-" + ts + " - " + colIdx);
        }
        table.put(row);
      }
    }

    // BUG -> GEN-1995
    // for (int rowKey = 0; rowKey < numOfRows; rowKey++) {
    // Get get = new Get(Bytes.toBytes(rowKey));
    // Row row = table.get(get);
    // get.setTimeStamp(200);
    // assertFalse(row.isEmpty());
    // List<Cell> cells = row.getCells();
    // assertEquals(6, cells.size());
    //
    // int ts = 200;
    // for (int colIdx = 0; colIdx < cells.size() - 1; colIdx++) {
    // Cell cell = cells.get(colIdx);
    // assertEquals(0, Bytes.compareTo(cell.getColumnName(), Bytes.toBytes("Column-" + colIdx)));
    // assertEquals(cell.getColumnValue(), "Value-" + ts + " - " + colIdx);
    // }
    // }

    Scanner scanner = table.getScanner(new Scan());
    Iterator<Row> iterator = scanner.iterator();
    while (iterator.hasNext()) {
      Row row = iterator.next();
      Iterator<Cell> cellIterator = row.getCells().iterator();
      while (cellIterator.hasNext()) {
        Cell cell = cellIterator.next();
        System.out.println(Bytes.toString(cell.getColumnName()) + " --> " + cell.getColumnValue());
      }
    }


    MCacheFactory.getAnyInstance().getAdmin().deleteMTable(tableName);

  }
}
