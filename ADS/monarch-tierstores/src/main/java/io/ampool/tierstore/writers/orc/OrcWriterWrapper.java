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
package io.ampool.tierstore.writers.orc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.ColumnConverterDescriptor;
import io.ampool.tierstore.ConverterDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.FTableOrcStruct;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.internal.logging.LogService;


public class OrcWriterWrapper {
  private static final Logger logger = LogService.getLogger();
  private Path file;
  private Writer writer;
  private AtomicInteger numRecords;
  private int recordLimit;
  private Configuration conf;
  private ConverterDescriptor converterDescriptor;


  public OrcWriterWrapper(final FileSystem fileSystem, final Path path, final Configuration conf,
      final ConverterDescriptor converterDescriptor, final int stripeSize,
      final CompressionKind compression, final int bufferSize, final int newIndexStride,
      final int walFileRecordLimit) throws IOException {
    numRecords = new AtomicInteger(0);
    this.conf = conf;
    this.converterDescriptor = converterDescriptor;
    this.file = file;
    this.recordLimit = walFileRecordLimit;
    initWriter(path, walFileRecordLimit, fileSystem, conf, stripeSize, compression, bufferSize,
        newIndexStride);
  }

  private OrcWriterWrapper initWriter(Path file, int recordLimit, final FileSystem fileSystem,
      final Configuration conf, final int stripeSize, final CompressionKind compression,
      final int bufferSize, final int newIndexStride) throws IOException {
    numRecords.set(0);

    writer = OrcFile.createWriter(fileSystem, file, conf, getObjectInspector(), stripeSize,
        compression, bufferSize, newIndexStride);
    return this;
  }

  private ObjectInspector getObjectInspector() {
    final List<ColumnConverterDescriptor> columnConverters =
        converterDescriptor.getColumnConverters();
    List<String> names = new ArrayList<>();
    List<TypeInfo> typeInfos = new ArrayList<>();
    for (int i = 0; i < columnConverters.size(); i++) {
      final String columnName = columnConverters.get(i).getColumnName();
      names.add(i, columnName);
      final TypeInfo typeInfo = (TypeInfo) columnConverters.get(i).getTypeDescriptor();
      typeInfos.add(i, typeInfo);
    }
    TypeInfo rowTypeInfo = TypeInfoFactory.getStructTypeInfo(names, typeInfos);
    ObjectInspector inspector = OrcStruct.createObjectInspector(rowTypeInfo);
    return inspector;
  }



  /**
   * Return the path associated with the opened file.
   * 
   * @return path
   */
  public Path getPath() {
    return file;
  }

  /**
   * Write one OrcRecord to Orc file
   * 
   * @param record
   * @throws IOException
   */
  protected void writeOneRecord(StoreRecord record) throws IOException {
    writer.addRow(getORCWritableObject(record.getValues()));
    numRecords.incrementAndGet();
  }

  /**
   * Write one OrcRecord to Orc file
   *
   * @param row
   * @throws IOException
   */
  public void writeOneRow(final Row row) throws IOException {
    writer.addRow(getORCWritableObject(row.getLatestRow()));
    numRecords.incrementAndGet();
  }

  private Object getORCWritableObject(final SingleVersionRow singleVersionRow) {
    final List<ColumnConverterDescriptor> columnConverters =
        this.converterDescriptor.getColumnConverters();
    List<Cell> cells = singleVersionRow.getCells();
    FTableOrcStruct structOut = new FTableOrcStruct(cells.size());
    for (int i = 0; i < cells.size(); i++) {
      final ColumnConverterDescriptor columnConverterDescriptor = columnConverters.get(i);
      structOut.setFieldValue(i,
          columnConverterDescriptor.getWritable(cells.get(i).getColumnValue()));
    }
    return structOut.getOrcStruct();
  }

  private Object getORCWritableObject(final Object[] valueArray) {
    final List<ColumnConverterDescriptor> columnConverters =
        this.converterDescriptor.getColumnConverters();
    FTableOrcStruct structOut = new FTableOrcStruct(valueArray.length);
    for (int i = 0; i < valueArray.length; i++) {
      final ColumnConverterDescriptor columnConverterDescriptor = columnConverters.get(i);
      structOut.setFieldValue(i, columnConverterDescriptor.getWritable(valueArray[i]));
    }
    return structOut.getOrcStruct();
  }

  /**
   * Close a writer
   */
  public void close() throws IOException {
    if (writer == null) {
      return;
    } else {
      synchronized (this) {
        if (writer != null) {
          writer.close();
          writer = null;
        }
      }
    }
  }

}
