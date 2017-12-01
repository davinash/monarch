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
package io.ampool.monarch.table.coprocessor;

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.List;
import java.util.Map;

@Category(MonarchTest.class)
public class MTableCoprocessorWithAllMTableConfiguration extends MTableDUnitConfigFramework {

  String[] keys = {"005", "006", "007", "008", "009"};

  public MTableCoprocessorWithAllMTableConfiguration() {
    super();
  }

  @Test
  public void testMTableCoProcessorwithAllMtableConfigs() {

    runAllConfigs(new FrameworkRunnable() {
      @Override
      public void run() {
        MTable table = getTable();
        doPut(table, keys);

        if (table.getTableDescriptor().getCoprocessorList().size() > 0
            && table.getTableDescriptor().getCoprocessorList().get(0)
                .equals("io.ampool.monarch.table.quickstart.SampleRowCountCoprocessor")) {
          testCoProcessor(table);
        }
      }

      private void testCoProcessor(MTable mtable) {
        assertNotNull(mtable);
        assertNotNull(mtable.getTableDescriptor());
        assertEquals(1, mtable.getTableDescriptor().getCoprocessorList().size());
        long rows = 0;
        Scan scan = new Scan();
        MExecutionRequest request = new MExecutionRequest();
        request.setScanner(scan);

        Map<Integer, List<Object>> collector =
            mtable.coprocessorService(mtable.getTableDescriptor().getCoprocessorList().get(0),
                "rowCount", null, null, request);
        rows = collector.values().stream().mapToLong(value -> value.stream().map(val -> (Long) val)
            .reduce(0L, (prev, current) -> prev + current)).sum();

        System.out.println("MTableCoprocessorWithAllMTableConfiguration.testCoProcessor " + rows);
        assertEquals(5, rows);
      }


      private void doPut(MTable table, String[] keys) {
        for (int rowIndex = 0; rowIndex < 5; rowIndex++) {
          Put record = new Put(Bytes.toBytes(keys[rowIndex]));
          for (int columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex++) {
            record.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex),
                Bytes.toBytes(VALUE_PREFIX + columnIndex));
          }
          table.put(record);
        }
      }

      @Override
      public void runAfterRestart() {
        MTable table = getTable();
        assertNotNull(table);
        if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
          if (table.getTableDescriptor().getCoprocessorList().size() > 0
              && table.getTableDescriptor().getCoprocessorList().get(0)
                  .equals("io.ampool.monarch.table.quickstart.SampleRowCountCoprocessor")
              && table.getTableDescriptor().getTableType().equals(MTableType.ORDERED_VERSIONED)) {
            testCoProcessor(table);
          }
        }



      }

    });
  }
}
