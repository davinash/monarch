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
package io.ampool.tierstore.parquet;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.ColumnConverterDescriptor;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.FileFormats;
import io.ampool.tierstore.stores.junit.StoreTestUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.geode.test.junit.categories.StoreTest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.List;


/**
 * Standalone test to verify different way to write into Parquet format
 */
@Ignore
@Category(StoreTest.class)
public class SimpleParquetWriterJUnit {

  @Rule
  public TemporaryFolder fileDir = new TemporaryFolder();

  /*
   * Code to see how to write parquet file
   */
  @Test
  public void testAvroToParquet() throws IOException {

    final StoreRecord storeRecord = StoreTestUtils.getStoreRecord();
    final Object[] values = storeRecord.getValues();
    // final List<ColumnConverterDescriptor> columnConverters =
    // converterDescriptor.getColumnConverters();
    // final Object[] convertedValues = new Object[values.length];
    // for (int i = 0; i < values.length; i++) {
    // final Object writable = columnConverters.get(i).getWritable(values[i]);
    // System.out.println("Writable type : " + writable.getClass() + " Value : " + writable);
    // convertedValues[i] = columnConverters.get(i).getReadable(writable);
    // System.out.println(
    // "Readable type : " + convertedValues[i].getClass() + " Value : " + convertedValues[i]);
    // }
    // StoreTestUtils.validateConvertedValues(convertedValues);

    // fTableDescriptor.addColumn("BIN_COL", BasicTypes.BINARY);
    // fTableDescriptor.addColumn("SHORT_COL", BasicTypes.SHORT);
    // fTableDescriptor.addColumn("VARCHAR_COL", BasicTypes.VARCHAR);
    // fTableDescriptor.addColumn("DOUBLE_COL", BasicTypes.DOUBLE);
    // fTableDescriptor.addColumn("DATE_COL", BasicTypes.DATE);
    // fTableDescriptor.addColumn("BIGDEC_COL", BasicTypes.BIG_DECIMAL);
    // fTableDescriptor.addColumn("BOOL_COL", BasicTypes.BOOLEAN);
    // fTableDescriptor.addColumn("BYTE_COL", BasicTypes.BYTE);
    // fTableDescriptor.addColumn("CHAR_COL", BasicTypes.CHAR);
    // fTableDescriptor.addColumn("CHARS_COL", BasicTypes.CHARS);
    // fTableDescriptor.addColumn("FLOAT_COL", BasicTypes.FLOAT);
    // fTableDescriptor.addColumn("INT_COL", BasicTypes.INT);
    // fTableDescriptor.addColumn("LONG_COL", BasicTypes.LONG);
    // fTableDescriptor.addColumn("STRING_COL", BasicTypes.STRING);
    // fTableDescriptor.addColumn("TS_COL", BasicTypes.TIMESTAMP);
    // if (includeTSColumn)
    // fTableDescriptor.addColumn(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, BasicTypes.LONG);

    // dynamically create schema
    String space = "DefaultNameSpace";
    String doc = "DefaultDoc";

    Schema record = Schema.createRecord("recordName", null, null, false);
    // create fields
    List<Schema.Field> fields1 = new ArrayList<>();
    // String field
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema.Field stringField =
        new Schema.Field("stringCol", stringSchema, null, null, Schema.Field.Order.ASCENDING);
    fields1.add(stringField);
    record.setFields(fields1);

    System.out.println("SimpleParquetWriterJUnit.testAvroToParquet :: 123 " + record.toString());

    Schema recordSchema = SchemaBuilder.record("TestRecord").fields()
        // .name("BIN_COL").type().bytesType().noDefault()//
        // .name("SHORT_COL").type().intType().noDefault()//
        // .name("VARCHAR_COL").type().stringType().noDefault()//
        // .name("DOUBLE_COL").type().doubleType().noDefault()//
        // .name("DATE_COL").type().longType().noDefault()//
        // .name("BIGDEC_COL").type().bytesType().noDefault()//
        // .name("BOOL_COL").type().booleanType().noDefault()//
        // .name("BYTE_COL").type().bytesType().noDefault()//
        // .name("CHAR_COL").type().stringType().noDefault()//
        // .name("CHARS_COL").type().stringType().noDefault()//
        // .name("FLOAT_COL").type().floatType().noDefault()//
        // .name("INT_COL").type().intType().noDefault()//
        // .name("LONG_COL").type().longType().noDefault()//
        // .name("STRING_COL").type().stringType().noDefault()//
        // .name(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME).type().longType().noDefault()//
        // .name("array").type().array().items(Schema.create(Schema.Type.INT)).noDefault()//
        // .name("map").type().map().values(Schema.create(Schema.Type.INT)).noDefault()//
        .name("union").type().unionOf().stringType().and().intType().and().floatType()
        /* .and().doubleType() */.endUnion().noDefault()//
        .endRecord();
    System.out
        .println("SimpleParquetWriterJUnit.testAvroToParquet :: 109 " + recordSchema.toString());
    // try to convert to avro schema first

    // generate the corresponding parquet schema
    MessageType parquetSchema = new AvroSchemaConverter().convert(recordSchema);

    // create a WriteSupport object to serialize your Avro objects
    WriteSupport writeSupport =
        new AvroWriteSupport(parquetSchema, recordSchema, GenericData.get());

    // choose compression scheme
    CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;

    // set parquet file block size and page size values
    int blockSize = 256 * 1024 * 1024;
    int pageSize = 64 * 1024;

    // the ParquetWriter object that will consume Avro GenericRecords
    // ParquetWriter parquetWriter = new ParquetWriter(new org.apache.hadoop.fs.Path(outputFile),
    // writeSupport, compressionCodecName, blockSize, pageSize);
    Path path = new Path("/tmp/data");
    FileSystem fileSystem = FileSystem.get(new Configuration());
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true);
    }

    ParquetWriter<GenericRecord> parquetWriter =
        AvroParquetWriter.<GenericRecord>builder(path).withSchema(recordSchema).build();
    // ParquetWriter<GenericRecord> parquetWriter = new ParquetWriter<>(path,
    // writeSupport, compressionCodecName, blockSize, pageSize);

    int numRecords = 0;
    GenericRecord avroRec = new GenericData.Record(recordSchema);

    // avroRec.put("BIN_COL", Bytes.toBytes("BIN"));
    // avroRec.put("SHORT_COL", (short) 1);
    // avroRec.put("VARCHAR_COL", "VARCHAR");
    // avroRec.put("DOUBLE_COL", 10.0);
    // avroRec.put("DATE_COL", 1000l); // TODO verify here
    // avroRec.put("BIGDEC_COL", Bytes.toBytes(new BigDecimal(1000.0)));
    // avroRec.put("BOOL_COL", true);
    // avroRec.put("BYTE_COL", new byte[]{1});
    // avroRec.put("CHAR_COL", "C");
    // avroRec.put("CHARS_COL", "CHARS");
    // avroRec.put("FLOAT_COL", 100.0f);
    // avroRec.put("INT_COL", 10);
    // avroRec.put("LONG_COL", 10000l);
    // avroRec.put("STRING_COL", "STRING");
    // avroRec.put(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, System.nanoTime());
    // ArrayList<Integer> objects = new ArrayList<>();
    // objects.add(1);
    // objects.add(2);
    // objects.add(3);
    // avroRec.put("array", objects);
    //
    // Map<String, Integer> map = new HashMap<>();
    // map.put("key1", 1);
    // avroRec.put("map", map);

    // create union
    // Schema unionSchema = SchemaBuilder.record("union")
    // .fields()
    // .name("INT").type().intType().noDefault()//
    // .name("STRING").type().stringType().noDefault()//
    // .endRecord();
    // GenericRecord union = new GenericData.Record(unionSchema);
    // union.put("INT",1);
    //// union.put("STRING","UNION");
    // avroRec.put("union",union);

    // ArrayList<Object> union = new ArrayList<>();
    //// union.add("1");
    // union.add(1);
    avroRec.put("union", 1.00f);


    // while (dataFileStream.hasNext()) {
    parquetWriter.write(avroRec);
    // if (numRecords%1000 == 0) {
    // System.out.println(numRecords);
    // }
    // numRecords++;
    // }
    parquetWriter.close();

    ReadSupport readSupport = new AvroReadSupport();
    ParquetReader<GenericRecord> parquetReader =
        AvroParquetReader.builder(readSupport, path).build();

    ParquetReader build = AvroParquetReader.<GenericRecord>builder(path).build();


    List<Schema> unionSchema = new ArrayList<>();
    unionSchema.add(Schema.create(Schema.Type.NULL));
    unionSchema.add(Schema.create(Schema.Type.STRING));
    unionSchema.add(Schema.create(Schema.Type.INT));
    unionSchema.add(Schema.create(Schema.Type.FLOAT));

    // ParquetReader<GenericRecord> parquetReader = new ParquetReader<>(path, readSupport);
    GenericRecord read = parquetReader.read();
    List<Schema.Field> fields = read.getSchema().getFields();
    fields.forEach(field -> {
      Object obj = read.get(field.name());
      System.out.println(field.name() + " : " + obj);
      int index = GenericData.get().resolveUnion(Schema.createUnion(unionSchema), obj);
      if (field.name().equalsIgnoreCase("union")) {
        System.out.println("SimpleParquetWriterJUnit.testAvroToParquet :: 248 " + obj.getClass());
      }
    });

    // }
    // Schema.createRecord()
  }

  /*
   * Code to see how to write parquet file
   */
  @Test
  public void testAmpoolValuesToParquet() throws IOException {

    final StoreRecord storeRecord = StoreTestUtils.getStoreRecord();
    final Object[] values = storeRecord.getValues();
    // final List<ColumnConverterDescriptor> columnConverters =
    // converterDescriptor.getColumnConverters();
    // final Object[] convertedValues = new Object[values.length];
    // for (int i = 0; i < values.length; i++) {
    // final Object writable = columnConverters.get(i).getWritable(values[i]);
    // System.out.println("Writable type : " + writable.getClass() + " Value : " + writable);
    // convertedValues[i] = columnConverters.get(i).getReadable(writable);
    // System.out.println(
    // "Readable type : " + convertedValues[i].getClass() + " Value : " + convertedValues[i]);
    // }
    // StoreTestUtils.validateConvertedValues(convertedValues);

    // fTableDescriptor.addColumn("BIN_COL", BasicTypes.BINARY);
    // fTableDescriptor.addColumn("SHORT_COL", BasicTypes.SHORT);
    // fTableDescriptor.addColumn("VARCHAR_COL", BasicTypes.VARCHAR);
    // fTableDescriptor.addColumn("DOUBLE_COL", BasicTypes.DOUBLE);
    // fTableDescriptor.addColumn("DATE_COL", BasicTypes.DATE);
    // fTableDescriptor.addColumn("BIGDEC_COL", BasicTypes.BIG_DECIMAL);
    // fTableDescriptor.addColumn("BOOL_COL", BasicTypes.BOOLEAN);
    // fTableDescriptor.addColumn("BYTE_COL", BasicTypes.BYTE);
    // fTableDescriptor.addColumn("CHAR_COL", BasicTypes.CHAR);
    // fTableDescriptor.addColumn("CHARS_COL", BasicTypes.CHARS);
    // fTableDescriptor.addColumn("FLOAT_COL", BasicTypes.FLOAT);
    // fTableDescriptor.addColumn("INT_COL", BasicTypes.INT);
    // fTableDescriptor.addColumn("LONG_COL", BasicTypes.LONG);
    // fTableDescriptor.addColumn("STRING_COL", BasicTypes.STRING);
    // fTableDescriptor.addColumn("TS_COL", BasicTypes.TIMESTAMP);
    // if (includeTSColumn)
    // fTableDescriptor.addColumn(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, BasicTypes.LONG);

    Schema recordSchema = SchemaBuilder.record("TestRecord").fields().name("BIN_COL").type()
        .bytesType().noDefault().name("SHORT_COL").type().intType().noDefault().name("VARCHAR_COL")
        .type().stringType().noDefault().name("DOUBLE_COL").type().doubleType().noDefault()
        .name("DATE_COL").type().longType().noDefault().name("BIGDEC_COL").type().bytesType()
        .noDefault().name("BOOL_COL").type().booleanType().noDefault().name("BYTE_COL").type()
        .bytesType().noDefault().name("CHAR_COL").type().stringType().noDefault().name("CHARS_COL")
        .type().stringType().noDefault().name("FLOAT_COL").type().floatType().noDefault()
        .name("INT_COL").type().intType().noDefault().name("LONG_COL").type().longType().noDefault()
        .name("STRING_COL").type().stringType().noDefault()
        .name(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME).type().longType().noDefault()
        .endRecord();
    System.out
        .println("SimpleParquetWriterJUnit.testAvroToParquet :: 109 " + recordSchema.toString());
    // try to convert to avro schema first

    // generate the corresponding parquet schema
    MessageType parquetSchema = new AvroSchemaConverter().convert(recordSchema);

    // create a WriteSupport object to serialize your Avro objects
    WriteSupport writeSupport =
        new AvroWriteSupport(parquetSchema, recordSchema, GenericData.get());

    // choose compression scheme
    CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;

    // set parquet file block size and page size values
    int blockSize = 256 * 1024 * 1024;
    int pageSize = 64 * 1024;

    // the ParquetWriter object that will consume Avro GenericRecords
    // ParquetWriter parquetWriter = new ParquetWriter(new org.apache.hadoop.fs.Path(outputFile),
    // writeSupport, compressionCodecName, blockSize, pageSize);
    Path path = new Path("/tmp/data");
    FileSystem fileSystem = FileSystem.get(new Configuration());
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true);
    }

    ParquetWriter<GenericRecord> parquetWriter =
        AvroParquetWriter.<GenericRecord>builder(path).withSchema(recordSchema).build();
    // ParquetWriter<GenericRecord> parquetWriter = new ParquetWriter<>(path,
    // writeSupport, compressionCodecName, blockSize, pageSize);

    int numRecords = 0;
    GenericRecord avroRec = new GenericData.Record(recordSchema);

    avroRec.put("BIN_COL", Bytes.toBytes("BIN"));
    avroRec.put("SHORT_COL", (short) 1);
    avroRec.put("VARCHAR_COL", "VARCHAR");
    avroRec.put("DOUBLE_COL", 10.0);
    avroRec.put("DATE_COL", 1000l); // TODO verify here
    avroRec.put("BIGDEC_COL", Bytes.toBytes(new BigDecimal(1000.0)));
    avroRec.put("BOOL_COL", true);
    avroRec.put("BYTE_COL", new byte[] {1});
    avroRec.put("CHAR_COL", "C");
    avroRec.put("CHARS_COL", "CHARS");
    avroRec.put("FLOAT_COL", 100.0f);
    avroRec.put("INT_COL", 10);
    avroRec.put("LONG_COL", 10000l);
    avroRec.put("STRING_COL", "STRING");
    avroRec.put(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, System.nanoTime());

    // while (dataFileStream.hasNext()) {
    parquetWriter.write(avroRec);
    // if (numRecords%1000 == 0) {
    // System.out.println(numRecords);
    // }
    // numRecords++;
    // }
    parquetWriter.close();

    ReadSupport readSupport = new AvroReadSupport();
    ParquetReader<GenericRecord> parquetReader =
        AvroParquetReader.builder(readSupport, path).build();
    // ParquetReader<GenericRecord> parquetReader = new ParquetReader<>(path, readSupport);
    GenericRecord read = parquetReader.read();
    List<Schema.Field> fields = read.getSchema().getFields();
    fields.forEach(field -> {
      System.out.println(field.name() + " : " + read.get(field.name()));
    });

    // }
    // Schema.createRecord()
  }


  @Test
  public void testComplexTypes() {
    final ConverterDescriptor converterDescriptor =
        StoreTestUtils.getConverterDescriptorComplexTypes("testComplexTypes", FileFormats.ORC);
    final StoreRecord storeRecord = StoreTestUtils.getStoreRecordWithComplexTypes();
    final Object[] values = storeRecord.getValues();
    final List<ColumnConverterDescriptor> columnConverters =
        converterDescriptor.getColumnConverters();
    final Object[] convertedValues = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      final Object writable = columnConverters.get(i).getWritable(values[i]);
      if (writable != null) {
        System.out.println("Writable type : " + writable.getClass() + " Value : " + writable);
      }
      convertedValues[i] = columnConverters.get(i).getReadable(writable);
      if (convertedValues[i] != null) {
        System.out.println(
            "Readable type : " + convertedValues[i].getClass() + " Value : " + convertedValues[i]);
      }
    }
    // StoreTestUtils.validateConvertedValues(convertedValues);
  }

}
