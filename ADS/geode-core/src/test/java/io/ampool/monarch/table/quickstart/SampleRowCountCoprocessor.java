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
package io.ampool.monarch.table.quickstart;

import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.coprocessor.MCoprocessorContext;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import io.ampool.monarch.table.filter.KeyOnlyFilter;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

public class SampleRowCountCoprocessor extends MCoprocessor {

  public SampleRowCountCoprocessor() {}

  private static final Logger logger = LogService.getLogger();

  public long rowCount(MCoprocessorContext context) {
    MExecutionRequest request = context.getRequest();
    long rowCount = 0L;
    try {
      Scan scan = request.getScanner();
      // since we only need keys
      scan.setFilter(new KeyOnlyFilter());
      Scanner scanner = context.getMTableRegion().getScanner(scan);
      while (scanner.next() != null) {
        rowCount++;
      }
    } catch (Exception e) {
      logger.error("Error in scanning ", e);
      throw new MCoprocessorException("Error in scanning results");
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Row count coprocessor execution completed");
    }
    // context.getResultSender().lastResult(rowCount);
    return rowCount;
  }

  @Override
  public String getId() {
    return this.getClass().getName();
  }
}
