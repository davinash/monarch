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
package io.ampool.examples.mtable;

import org.apache.geode.internal.logging.LogService;
import io.ampool.monarch.table.*;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

/**
 * MTableCDCListener
 *
 * An MAsyncEventListener receives callbacks for events that change MTable data. You can use an
 * MAsyncEventListener implementation as a write-behind cache event handler to synchronize MTable
 * updates with a database.
 *
 * Following implemention listens to the events and write a cvs file with all the updates received.
 */

public class MTableCDCListener implements MAsyncEventListener {
  private static final Logger logger = LogService.getLogger();
  // Delimiter used in CSV file
  private static final String NEW_LINE_SEPARATOR = "\n";
  private static final Object[] FILE_HEADER =
      {"EVENTID", "NAME", "ID", "AGE", "SALARY", "DEPT", "OPERATION_TYPE"};

  PrintWriter writer = null;

  public MTableCDCListener() throws IOException {
    File file = new File("/tmp/EmployeeData.csv");
    if (file.exists()) {
      file.delete();
    }
    writer = new PrintWriter(file);
    writer.println("EVENTID, NAME, ID, AGE, SALARY, DEPT, OPERATION_TYPE");
  }


  /**
   * Process the list of <code>CDCEvent</code>s. This method will asynchronously be called when
   * events are queued to be processed.
   */
  @Override
  public boolean processEvents(List<CDCEvent> events) {
    try {
      for (CDCEvent event : events) {
        MEventSequenceID eventSequenceID = event.getEventSequenceID();
        writer.print(eventSequenceID.getSequenceID() + ",");

        Row row = event.getRow();
        if (event.getOperation().equals(MEventOperation.CREATE)) {

          // Create Operation Event
          String res = "";
          for (Iterator<Cell> iterator = row.getCells().iterator(); iterator.hasNext();) {
            res += iterator.next().getColumnValue() + (iterator.hasNext() ? "," : "");
          }
          writer.print(res);

          writer.print("," + event.getOperation());
        } else if (event.getOperation().equals(MEventOperation.UPDATE)) {
          // Update Operation Event
          String res = "";
          for (Iterator<Cell> iterator = row.getCells().iterator(); iterator.hasNext();) {
            res += iterator.next().getColumnValue() + (iterator.hasNext() ? "," : "");
          }
          writer.print(res);
          writer.print("," + event.getOperation());
        } else if (event.getOperation().equals(MEventOperation.DELETE)) {
          // Delete Operation Event.
          writer.print(new String(event.getKey()));
          writer.print("," + event.getOperation());
        } else {
          // Unknown Operation Event.
        }
        writer.println();
        writer.flush();
      }
    } catch (Exception e) {
      logger.error("MTableCDCListener Exception : " + e);
    }
    return true;
  }

  @Override
  public void close() {
    logger.info("MTableCDCListener close is called");
    writer.close();
  }
}
