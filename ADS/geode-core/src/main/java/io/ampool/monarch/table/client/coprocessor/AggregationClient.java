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
package io.ampool.monarch.table.client.coprocessor;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Aggregation co-processor providing handy methods to get aggregated results on table.<br>
 * <br>
 * As of version 1.0, it provides rowCount implementation.<br>
 * <br>
 * Usage Examples:<br>
 * <br>
 * - Get row count of Table:<br>
 * &nbsp;&nbsp;&nbsp;AggregationClient ac = new AggregationClient();<br>
 * &nbsp;&nbsp;&nbsp;long count = ac.rowCount(TABLE_NAME, new MScan());<br>
 * - Get row count with some conditions like particular range. For more predicates and conditions
 * refer {@link Scan}<br>
 * &nbsp;&nbsp;&nbsp;AggregationClient ac = new AggregationClient();<br>
 * &nbsp;&nbsp;&nbsp;long count = ac.rowCount(TABLE_NAME, new MScan(startRow, stopRow));<br>
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AggregationClient {
  private static final Logger logger = LogManager.getLogger();

  private static final String ROW_COUNT_COPROCESSOR_CLASS =
      "io.ampool.monarch.table.coprocessor.impl.RowCountCoProcessor";

  /**
   * Count the number of rows in the Table specified by the scan object
   * 
   * @param tableName Name of the table
   * @param scan Scan instance see {@link Scan}
   * @return number of rows, Matching to MScan Object
   */
  public long rowCount(final String tableName, Scan scan) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    return rowCount(table, scan);
  }

  /**
   * Count the number of rows in the Table specified by the scan object
   * 
   * @param table MTable Handle for which rows to be counted See {@link MTable}
   * @param scan Scan instance see {@link Scan}
   * @return number of rows, Matching to MScan Object
   */

  public long rowCount(final MTable table, Scan scan) {
    long rows = 0L;
    // call co-processor
    MExecutionRequest request = new MExecutionRequest();
    request.setScanner(scan);

    Map<Integer, List<Object>> collector = table.coprocessorService(ROW_COUNT_COPROCESSOR_CLASS,
        "rowCount", scan.getStartRow(), scan.getStopRow(), request);

    rows = collector.values().stream().mapToLong(value -> value.stream().map(val -> (Long) val)
        .reduce(0L, (prev, current) -> prev + current)).sum();
    logger.info("Row count: " + rows);
    return rows;
  }

  //
  // public <T> T max(final String tableName, MScan scan){
  //
  // }
}
