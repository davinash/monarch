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
package io.ampool.tierstore.writers.parquet;

import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.ColumnConverterDescriptor;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.config.CommonConfig;
import io.ampool.tierstore.parquet.utils.ParquetUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.geode.internal.logging.LogService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wrapper implementation for parquet writer
 */
public class ParquetWriterWrapper {
  private static final Logger logger = LogService.getLogger();
  private final Properties props;
  private Path file;
  private ParquetWriter<GenericRecord> writer;
  private AtomicInteger numRecords;
  private int recordLimit;
  private Configuration conf;
  private ConverterDescriptor converterDescriptor;
  private org.apache.avro.Schema avroRecord;

  public ParquetWriterWrapper(final Properties props, final FileSystem fileSystem, final Path path,
      final Configuration conf, final ConverterDescriptor converterDescriptor,
      final CompressionCodecName compression, final int blockSize, final int pageSize,
      final int walFileRecordLimit) throws IOException {
    this.props = props;
    numRecords = new AtomicInteger(0);
    this.conf = conf;
    this.converterDescriptor = converterDescriptor;
    this.file = file;
    this.recordLimit = walFileRecordLimit;
    initWriter(path, walFileRecordLimit, fileSystem, conf, compression, blockSize, pageSize);
  }

  private ParquetWriterWrapper initWriter(Path file, int recordLimit, final FileSystem fileSystem,
      final Configuration conf, final CompressionCodecName compression, final int blockSize,
      final int pageSize) throws IOException {
    numRecords.set(0);
    String[] columnNames = converterDescriptor.getColumnConverters().stream()
        .map(columnConverterDescriptor -> columnConverterDescriptor.getColumnName())
        .toArray(size -> new String[size]);

    Schema[] columnTypes = converterDescriptor.getColumnConverters().stream()
        .map(columnConverterDescriptor -> columnConverterDescriptor.getTypeDescriptor())
        .toArray(size -> new Schema[size]);

    avroRecord = ParquetUtils.createAvroRecordSchema(getTableName(), columnNames, columnTypes);

    // TODO confirm how without fs will it be able to write on hdfs
    writer = AvroParquetWriter.<GenericRecord>builder(file).withCompressionCodec(compression)
        .withPageSize(pageSize).withConf(conf).withSchema(avroRecord).build();

    return this;
  }

  private String getTableName() {
    if (props != null && props.contains(CommonConfig.TABLE_NAME)) {
      return props.getProperty(CommonConfig.TABLE_NAME);
    }
    return "DUMMY";
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
   */
  protected void writeOneRecord(StoreRecord record) throws IOException {
    GenericRecord genericRecord = convertToAvroRecord(avroRecord, record.getValues());
    writer.write(genericRecord);
    numRecords.incrementAndGet();
  }

  private GenericRecord convertToAvroRecord(Schema avroRecordSchema, Object[] values) {
    // TODO can be improve to create once and reuse
    GenericRecord avroRec = new GenericData.Record(avroRecordSchema);
    List<ColumnConverterDescriptor> columnConverters = converterDescriptor.getColumnConverters();
    if (values.length != columnConverters.size()) {
      // mismatch schema
      // TODO better exception
      throw new RuntimeException("Expecting " + columnConverters.size() + " fields, received "
          + values.length + " values");
    }
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      ColumnConverterDescriptor columnConverterDescriptor = columnConverters.get(i);
      Object valueToWrite = columnConverterDescriptor.getWritable(value);
      avroRec.put(columnConverterDescriptor.getColumnName(), valueToWrite);
    }
    return avroRec;
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
