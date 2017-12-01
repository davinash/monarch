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

import io.ampool.monarch.table.CDCEvent;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MAsyncEventListener;
import io.ampool.monarch.table.MEventOperation;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.internal.logging.LogService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An MAsyncEventListener receives callbacks for events that change MTable data. You can use an
 * MAsyncEventListener implementation as a write-behind cache event handler to synchronize MTable
 * updates with a database.
 *
 * Following implementation listens to the events and write a csv file with all the events received
 * on kerberized HDFS.
 */

public class MTableCDCHDFSListener implements MAsyncEventListener {
  private static final Logger logger = LogService.getLogger();
  // Delimiter used in CSV file
  private static final String[] FILE_HEADER =
      {"EVENTID", "OPERATION_TYPE", "RowKey", "VersionID", "NAME", "ID", "AGE", "SALARY", "DEPT"};

  private int fileIndex = 0;

  public MTableCDCHDFSListener() throws IOException {}


  /**
   * Process the list of <code>CDCEvent</code>s. This method will asynchronously be called when
   * events are queued to be processed.
   */
  @Override
  public boolean processEvents(List<CDCEvent> events) {
    try {
      FSDataOutputStream fsDataOutputStream = getFileStream();
      List<String> header = Arrays.asList(FILE_HEADER);

      fsDataOutputStream.writeBytes(String.join(",", header));
      fsDataOutputStream.writeBytes("\n");

      for (CDCEvent event : events) {
        if (event == null) {
          continue;
        }
        List<String> strings = printRowMeta(event);
        strings.forEach(line -> {
          try {
            fsDataOutputStream.writeBytes(line);
            fsDataOutputStream.writeBytes("\n");
          } catch (IOException e) {
            logger.error("Error in writing hdfs ", e);
          }
        });
      }
      fsDataOutputStream.flush();
      fsDataOutputStream.close();
    } catch (Exception e) {
      logger.error("MTableCDCListener Exception : ", e);
    }
    return true;
  }

  @Override
  public void close() {
    logger.info("MTableCDCListener close is called");
  }

  public FSDataOutputStream getFileStream() throws IOException {
    Configuration conf = new Configuration();
    // Add configurations
    conf.addResource(new Path("file:///opt/conf/core-site.xml"));
    conf.addResource(new Path("file:///opt/conf/hdfs-site.xml"));

    // set config to UGI
    UserGroupInformation.setConfiguration(conf);
    // provide user pricipal and keytab to authenticate using kerberos
    UserGroupInformation.loginUserFromKeytab("ampool@TEST.AMPOOL.IO", "/opt/ampool.keytab");

    FileSystem fs = FileSystem.get(conf);
    Path file = new Path("/tmp/data/EmployeeData" + fileIndex++ + ".csv");

    if (fs.exists(file)) {
      fs.delete(file, false);
    }
    FSDataOutputStream fsDataOutputStream = fs.create(file);
    return fsDataOutputStream;
  }


  private List<String> printRowMeta(CDCEvent event) {
    List<String> rows = new ArrayList<>();

    Row row = event.getRow();

    List<String> values = new ArrayList<>();
    // event id
    values.add(String.valueOf(event.getEventSequenceID().getSequenceID()));

    // operation
    values.add(String.valueOf(event.getOperation()));

    // rowKey
    values.add(Arrays.toString(row.getRowId()));

    // iterate through all version

    // timestamp
    if (row.getRowTimeStamp() != null) {
      values.add(String.valueOf(row.getRowTimeStamp()));
    }

    // add col values
    List<Cell> cells = row.getCells();
    cells.forEach(cell -> {
      if (cell.getColumnType() != BasicTypes.BINARY) {
        values.add(String.valueOf(cell.getColumnValue()));
      } else {
        values.add(Arrays.toString((byte[]) cell.getColumnValue()));
      }
    });
    rows.add(String.join(",", values));
    return rows;
  }
}
