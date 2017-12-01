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

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.CDCEvent;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MAsyncEventListener;
import io.ampool.monarch.table.Row;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.geode.internal.logging.LogService;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An MAsyncEventListener receives callbacks for events that change MTable data. You can use an
 * MAsyncEventListener implementation as a write-behind cache event handler to synchronize MTable
 * updates with a database.
 *
 * Following implementation listens to the events and write a parquet file with all the events
 * received.
 */

public class MTableCDCParquetListener implements MAsyncEventListener {
  private static final Logger logger = LogService.getLogger();
  // Delimiter used in CSV file
  private static final String[] FILE_HEADER =
      {"EVENTID", "OPERATION_TYPE", "RowKey", "VersionID", "NAME", "ID", "AGE", "SALARY", "DEPT"};

  private int fileIndex = 0;

  public MTableCDCParquetListener() {

  }


  /**
   * Process the list of <code>CDCEvent</code>s. This method will asynchronously be called when
   * events are queued to be processed.
   */
  @Override
  public boolean processEvents(List<CDCEvent> events) {
    try {
      ParquetWriter<GenericRecord> parquetWriter = getParquetFileStream();

      for (CDCEvent event : events) {
        if (event == null) {
          continue;
        }
        GenericRecord record = convertToAvroRecord(event);
        parquetWriter.write(record);
      }
      parquetWriter.close();
    } catch (Exception e) {
      logger.error("MTableCDCParquetListener Exception : ", e);
    }
    return true;
  }

  @Override
  public void close() {
    logger.info("MTableCDCListener close is called");
  }

  public ParquetWriter<GenericRecord> getParquetFileStream() throws IOException {
    Schema avroSchema = getAvroSchema();
    Path file = new Path("/tmp/data/EmployeeData" + fileIndex++ + ".parquet");
    // create avro schema
    ParquetWriter<GenericRecord> parquetWriter =
        AvroParquetWriter.<GenericRecord>builder(file).withSchema(avroSchema).build();

    return parquetWriter;
  }

  private Schema getAvroSchema() {
    List<Schema.Field> fields = new ArrayList<>();
    for (int i = 0; i < FILE_HEADER.length; i++) {

      Schema.Field field = new Schema.Field(FILE_HEADER[i],
          Schema.createUnion(
              Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL))),
          null, null);

      if (FILE_HEADER[i].equals("RowKey")) {
        field = new Schema.Field(FILE_HEADER[i],
            Schema.createUnion(
                Arrays.asList(Schema.create(Schema.Type.BYTES), Schema.create(Schema.Type.NULL))),
            null, null);
      }

      if (FILE_HEADER[i].equals("ID")) {
        field = new Schema.Field(FILE_HEADER[i],
            Schema.createUnion(
                Arrays.asList(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL))),
            null, null);
      }

      if (FILE_HEADER[i].equals("AGE")) {
        field = new Schema.Field(FILE_HEADER[i],
            Schema.createUnion(
                Arrays.asList(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL))),
            null, null);
      }

      if (FILE_HEADER[i].equals("SALARY")) {
        field = new Schema.Field(FILE_HEADER[i],
            Schema.createUnion(
                Arrays.asList(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL))),
            null, null);
      }

      if (FILE_HEADER[i].equals("VersionID")) {
        field = new Schema.Field(FILE_HEADER[i],
            Schema.createUnion(
                Arrays.asList(Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL))),
            null, null);
      }

      fields.add(field);
    }
    Schema avrorecord = Schema.createRecord("CDCRecord", null, null, false);
    avrorecord.setFields(fields);
    return avrorecord;
  }


  private GenericRecord convertToAvroRecord(CDCEvent event) {
    GenericRecord avroRec = new GenericData.Record(getAvroSchema());
    Row row = event.getRow();

    // EVENTID
    avroRec.put("EVENTID", String.valueOf(event.getEventSequenceID().getSequenceID()));

    // OPERATION_TYPE
    avroRec.put("OPERATION_TYPE", String.valueOf(event.getOperation()));
    // RowKey
    avroRec.put("RowKey", row.getRowId());

    // VersionID

    if (row.getRowTimeStamp() != null) {
      avroRec.put("VersionID", row.getRowTimeStamp());
    }

    // add col values
    List<Cell> cells = row.getCells();
    cells.forEach(cell -> {
      if (cell.getColumnValue() != null) {
        avroRec.put(Bytes.toString(cell.getColumnName()), cell.getColumnValue());
      }
    });

    return avroRec;
  }
}
