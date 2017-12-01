package io.ampool.examples.mtable;

import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.coprocessor.MCoprocessorContext;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import io.ampool.monarch.table.filter.KeyOnlyFilter;

/**
 * Sample RowCount coprocessor implementation.
 */
public class SampleRowCountCoprocessor extends MCoprocessor {

  public SampleRowCountCoprocessor() {}

  public long rowCount(MCoprocessorContext context) {
    MExecutionRequest request = context.getRequest();

    // Shows how to get user specified arguments
    /*
     * Object arguments = request.getArguments(); if(arguments instanceof ArrayList){ ArrayList list
     * = (ArrayList)arguments; //Do some processing }
     */

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
      throw new MCoprocessorException("Error in scanning results");
    }
    return rowCount;
  }
}
