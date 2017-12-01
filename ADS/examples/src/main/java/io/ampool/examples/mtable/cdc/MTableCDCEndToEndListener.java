/*
 * ========================================================================= * Copyright (c) 2015
 * Ampool, Inc. All Rights Reserved. * This product is protected by U.S. and international copyright
 * * and intellectual property laws.
 * *=========================================================================
 *
 */

package io.ampool.examples.mtable.cdc;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.CDCEvent;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MAsyncEventListener;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.Record;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * An MAsyncEventListener receives callbacks for events that change MTable data. You can use an
 * MAsyncEventListener implementation as a write-behind cache event handler to synchronize MTable
 * updates with a database.
 *
 * Following implementation listens to the events and appends that event into the FTable.
 */

public class MTableCDCEndToEndListener implements MAsyncEventListener {
  private static final Logger logger = LogService.getLogger();

  private int fileIndex = 0;

  public MTableCDCEndToEndListener() {}


  /**
   * Process the list of <code>CDCEvent</code>s. This method will asynchronously be called when
   * events are queued to be processed.
   */
  @Override
  public boolean processEvents(List<CDCEvent> events) {
    MTable mTable = null;
    MTableDescriptor tableDescriptor = null;
    MCache serverCache = MCacheFactory.getAnyInstance();
    FTable fTable = null;
    try {
      for (CDCEvent event : events) {
        if (event == null) {
          logger.error("Skipping record...");
          continue;
        }
        logger.error("OP " + event.getOperation());
        if (mTable == null) {
          mTable = event.getMTable();
          tableDescriptor = mTable.getTableDescriptor();
          fTable = serverCache.getFTable(
              MTableCDCEndToEndExample.EVENTS_TABLE_PREFIX + mTable.getName().split("_")[1]);
        }
        Record record = getFTableRecord(event, tableDescriptor);
        fTable.append(record);
      }
    } catch (Exception e) {
      logger.error("MTableCDCEndToEndListener Exception : ", e);
    }
    return true;
  }

  @Override
  public void close() {
    logger.info("MTableCDCEndToEndListener close is called");
  }

  private Record getFTableRecord(CDCEvent event, MTableDescriptor tableDescriptor) {
    Record record = new Record();
    Row row = event.getRow();

    // EVENTID
    record.add("EVENTID", String.valueOf(event.getEventSequenceID().getSequenceID()));

    // OPERATION_TYPE
    record.add("OPERATION_TYPE", String.valueOf(event.getOperation()));

    // RowKey
    record.add("RowKey", row.getRowId());

    // VersionID
    if (row.getRowTimeStamp() != null) {
      record.add("VersionID", row.getRowTimeStamp());
    }

    // add col values
    List<Cell> cells = row.getCells();
    cells.forEach(cell -> {
      if (cell.getColumnValue() != null
          && !Bytes.toString(cell.getColumnName()).equalsIgnoreCase("__ROW__KEY__COLUMN__")) {
        record.add(Bytes.toString(cell.getColumnName()), cell.getColumnValue());
      }
    });
    return record;
  }
}
