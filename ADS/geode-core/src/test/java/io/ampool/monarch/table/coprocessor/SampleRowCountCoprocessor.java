package io.ampool.monarch.table.coprocessor;

import org.apache.geode.internal.logging.LogService;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;

/**

 */
public class SampleRowCountCoprocessor extends MCoprocessor {

  public SampleRowCountCoprocessor() {}


  private static final Logger logger = LogService.getLogger();

  public long rowCount(MCoprocessorContext context) {

    MExecutionRequest request = context.getRequest();
    System.out.println("SampleRowCountCoprocessor.rowCount ------------>");

    long rowCount = 0L;
    try {
      MTable table = context.getTable();

      Scan scan = new Scan();
      Scanner resultScanner = table.getScanner(scan);
      Iterator<Row> resultIterator = resultScanner.iterator();
      while (resultIterator.hasNext()) {
        Row result = resultIterator.next();
        if (result != null) {
          rowCount++;
        }
      }

    } catch (Exception e) {
      System.out.println("SampleRowCountCoprocessor.rowCount ");
      e.printStackTrace();
      throw new MCoprocessorException("Error in scanning results");
    }
    if (logger.isDebugEnabled()) {
      System.out.println("SampleRowCountCoprocessor.rowCount");
      logger.debug("Row count coprocessor execution completed");
    }
    // context.getResultSender().lastResult(rowCount);
    System.out.println("SampleRowCountCoprocessor.rowCount " + rowCount);
    return rowCount;
  }

  // @Override
  // public String getId() {
  // return this.getClass().getName();
  // }
}
