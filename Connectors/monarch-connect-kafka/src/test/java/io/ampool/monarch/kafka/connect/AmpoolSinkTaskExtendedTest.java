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

package io.ampool.monarch.kafka.connect;

import io.ampool.monarch.RMIException;
import org.apache.kafka.connect.data.Date;
import org.testng.annotations.Test;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import io.ampool.monarch.common.Constants;
import io.ampool.monarch.SerializableRunnable;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.internal.InternalTable;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;


import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.testng.Assert.assertEquals;

public class AmpoolSinkTaskExtendedTest extends TestBase implements Serializable {
  private static final String regionName = "mtable";
  private static final String regionName2 = "fTable";

  private static final int TOTAL_RECORDS = 10;
  private final Map<String, String> configProps = new HashMap<>();

  public static final String[] regionNames = {"mtable1","ftable1","ftable2","mtable2"};
  public static final String FTABLE_WITH_LOGICAL_TYPES = "ftable_with_logical_types";
  public static final String TOPIC_WITH_LOGICAL_TYPES = "topic_with_logical_types";
  public static final String[] topicNames = {"topic1","topic2","topic3"};

  @BeforeTest
  public void setUpBeforeMethod() throws Exception {
    //kafka sink-connector/task config.

    configProps.clear();

    configProps.put("locator.port", testBase.getLocatorPort());
    configProps.put("locator.host", MonarchUtils.LOCATOR_HOST);
    configProps.put("batch.size", String.valueOf(2));
    String topics = String.join(",", regionNames);
    configProps.put("ampool.tables", topics);
    configProps.put("topics", topics);
    configProps.put("tasks.max", String.valueOf(3));

    final String localPort = testBase.getLocatorPort();
    testBase.getClientVm().invoke(new SerializableRunnable() {
      @Override
      public void run() {

        try {
          MClientCache connection = MonarchUtils.getConnection(regionNames[0], localPort);
          MonarchUtils.createMTable(regionNames[0], connection, MTableType.UNORDERED);
          MonarchUtils.createFTable(regionNames[1], connection);
          MonarchUtils.createFTable(regionNames[2], connection);
          MonarchUtils.createMTable(regionNames[3], connection, MTableType.ORDERED_VERSIONED);
          MonarchUtils.createConnectionAndMTable_2(regionName, localPort);
          MonarchUtils.createConnectionAndFTable_2(regionName2, localPort);

          MonarchUtils.createMTable(topicNames[0], connection, MTableType.UNORDERED);
          MonarchUtils.createMTable(topicNames[1], connection, MTableType.ORDERED_VERSIONED);
          MonarchUtils.createFTable(topicNames[2], connection);
          MonarchUtils.createFTableWithLogicalTypes(FTABLE_WITH_LOGICAL_TYPES, connection);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

  @AfterTest
  public void setUpAfterMethod() throws Exception {
  }

  @Test
  public void testAmpoolSinkTaskWithAllTypes(){
    AmpoolSinkTask sinkTask = new AmpoolSinkTask();
    configProps.clear();
    configProps.put("locator.port", testBase.getLocatorPort());
    configProps.put("locator.host", MonarchUtils.LOCATOR_HOST);
    configProps.put("batch.size", String.valueOf(100));
    configProps.put("ampool.tables", regionName);
    configProps.put("topics", regionName);
    configProps.put("max.retries", Integer.toString(5));
    configProps.put("retry.interval.ms", Integer.toString(30000));
    sinkTask.start(configProps);

    Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
      .field("the_byte", Schema.INT8_SCHEMA)
      .field("the_short", Schema.INT16_SCHEMA)
      .field("the_int", Schema.INT32_SCHEMA)
      .field("the_long", Schema.INT64_SCHEMA)
      .field("the_float", Schema.FLOAT32_SCHEMA)
      .field("the_double", Schema.FLOAT64_SCHEMA)
      .field("the_bool", Schema.BOOLEAN_SCHEMA)
      .field("the_string", Schema.STRING_SCHEMA)
      .field("the_bytes", Schema.BYTES_SCHEMA);
      //.field("the_decimal", Decimal.schema(2).schema());
      //.field("the_date", org.apache.kafka.connect.data.Date.SCHEMA);
    //.field("the_time", Time.SCHEMA)
    //.field("the_timestamp", Timestamp.SCHEMA);

    final java.util.Date instant = new java.util.Date(1474661402123L);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    int noOfRecords = 1001;
    for (int i = 1; i <= noOfRecords; i++) {
      final Struct record = new Struct(valueSchema)
        .put("the_byte", (byte) -32)
        .put("the_short", (short) 1234)
        .put("the_int", 42 + i)
        .put("the_long", 12425436L)
        .put("the_float", 2356.3f)
        .put("the_double", -2436546.56457d)
        .put("the_bool", true)
        .put("the_string", "foo" + i)
        .put("the_bytes", new byte[]{-32, 124});
        //.put("the_decimal", new BigDecimal("1234.567"));
        //.put("the_date", instant);
      //.put("the_time", instant)
      //.put("the_timestamp", instant);

      SinkRecord sinkRecord = new SinkRecord(regionName, i, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }

    sinkTask.put(sinkRecords);

    //MTable scan and verify records.
    final String locatorPort = testBase.getLocatorPort();
    //testBase.getClientVm().invoke(new SerializableRunnable() {
    //  @Override
    //  public void run() {

    MClientCache connection = MonarchUtils.getConnection(regionName, locatorPort);
    MTable table = connection.getMTable(regionName);
    io.ampool.monarch.table.Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    int totalCount = 0;
    while (itr.hasNext()) {
      Row result = (Row) itr.next();
      System.out.print("[ Key = " + Bytes.toString(result.getRowId()) + " ]");

      List<Cell> selectedRow = result.getCells();

      for (Cell cell : selectedRow) {
        System.out.print("  [ColumnName: " + Bytes.toString(cell.getColumnName()) + " ColumnValue:"
          + cell.getColumnValue() + "], ");

      }
      totalCount++;
//      System.out.println();
    }

    assertEquals(totalCount, noOfRecords);
//    System.out.println("XXX Total records found in scan = " + totalCount);

    connection.getAdmin().deleteMTable(regionName);
    //close the sink-task
    sinkTask.stop();
  }


  @Test
  public void testAmpoolSinkTaskWithAllTypesFTable(){
    AmpoolSinkTask sinkTask = new AmpoolSinkTask();
    configProps.clear();
    configProps.put("locator.port", testBase.getLocatorPort());
    configProps.put("locator.host", MonarchUtils.LOCATOR_HOST);
    configProps.put("batch.size", String.valueOf(100));
    configProps.put("ampool.tables", regionName2);
    configProps.put("topics", regionName2);
    configProps.put("max.retries", Integer.toString(5));
    configProps.put("retry.interval.ms", Integer.toString(30000));
    sinkTask.start(configProps);

    Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
      .field("the_byte", Schema.INT8_SCHEMA)
      .field("the_short", Schema.INT16_SCHEMA)
      .field("the_int", Schema.INT32_SCHEMA)
      .field("the_long", Schema.INT64_SCHEMA)
      .field("the_float", Schema.FLOAT32_SCHEMA)
      .field("the_double", Schema.FLOAT64_SCHEMA)
      .field("the_bool", Schema.BOOLEAN_SCHEMA)
      .field("the_string", Schema.STRING_SCHEMA)
      .field("the_bytes", Schema.BYTES_SCHEMA);
    //.field("the_decimal", Decimal.schema(2).schema());
    //.field("the_date", org.apache.kafka.connect.data.Date.SCHEMA);
    //.field("the_time", Time.SCHEMA)
    //.field("the_timestamp", Timestamp.SCHEMA);

    final java.util.Date instant = new java.util.Date(1474661402123L);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    int noOfRecords = 2002;
    for (int i = 1; i <= noOfRecords; i++) {
      final Struct record = new Struct(valueSchema)
        .put("the_byte", (byte) -32)
        .put("the_short", (short) 1234)
        .put("the_int", 42 + i)
        .put("the_long", 12425436L)
        .put("the_float", 2356.3f)
        .put("the_double", -2436546.56457d)
        .put("the_bool", true)
        .put("the_string", "foo" + i)
        .put("the_bytes", new byte[]{-32, 124});
      //.put("the_decimal", new BigDecimal("1234.567"));
      //.put("the_date", instant);
      //.put("the_time", instant)
      //.put("the_timestamp", instant);

      SinkRecord sinkRecord = new SinkRecord(regionName2, i, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }

    sinkTask.put(sinkRecords);

    //MTable scan and verify records.
    final String locatorPort = testBase.getLocatorPort();
    //testBase.getClientVm().invoke(new SerializableRunnable() {
    //  @Override
    //  public void run() {

    MClientCache connection = MonarchUtils.getConnection(regionName2, locatorPort);
    FTable table = connection.getFTable(regionName2);
    io.ampool.monarch.table.Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    int totalCount = 0;
    while (itr.hasNext()) {
      Row result = (Row) itr.next();
      System.out.print("[ Key = " + Bytes.toString(result.getRowId()) + " ]");

      List<Cell> selectedRow = result.getCells();

      for (Cell cell : selectedRow) {
        System.out.print("  [ColumnName: " + Bytes.toString(cell.getColumnName()) + " ColumnValue:"
          + cell.getColumnValue() + "], ");

      }
      totalCount++;
//      System.out.println();
    }

    assertEquals(totalCount, noOfRecords);
//    System.out.println("XXX Total records found in scan = " + totalCount);

    connection.getAdmin().deleteFTable(regionName2);
    //close the sink-task
    sinkTask.stop();
  }

  @Test
  public void testAmpoolSinkTaskWithLogicalTypesFTable(){
    AmpoolSinkTask sinkTask = new AmpoolSinkTask();
    configProps.clear();
    configProps.put("locator.port", testBase.getLocatorPort());
    configProps.put("locator.host", MonarchUtils.LOCATOR_HOST);
    configProps.put("batch.size", String.valueOf(100));
    configProps.put("ampool.tables", FTABLE_WITH_LOGICAL_TYPES);
    configProps.put("topics", TOPIC_WITH_LOGICAL_TYPES);
    configProps.put("max.retries", Integer.toString(5));
    configProps.put("retry.interval.ms", Integer.toString(30000));
    sinkTask.start(configProps);

    Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
        .field("the_int", Schema.INT32_SCHEMA)
        .field("the_decimal", Decimal.schema(2).schema())
        .field("the_date", Date.SCHEMA)
        .field("the_time", Time.SCHEMA)
        .field("the_timestamp", Timestamp.SCHEMA);

    //final java.util.Date instant = new java.util.Date(1474661402123L);
    java.util.Date dateInstance = java.util.Date.from(ZonedDateTime.of(LocalDate.of(1970, 1, 1), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()).toInstant());

    //DecimalInstance
    BigDecimal decimalInstance =  new BigDecimal("12345678");

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    int noOfRecords = 2002;
    for (int i = 1; i <= noOfRecords; i++) {
      final Struct record = new Struct(valueSchema)
          .put("the_int", 42 + i)
          .put("the_decimal", decimalInstance)
          .put("the_date", dateInstance)
          .put("the_time", dateInstance)
          .put("the_timestamp", dateInstance);

      SinkRecord sinkRecord = new SinkRecord(TOPIC_WITH_LOGICAL_TYPES, 0, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }

    sinkTask.put(sinkRecords);

    //FTable scan and verify records.
    final String locatorPort = testBase.getLocatorPort();
    //testBase.getClientVm().invoke(new SerializableRunnable() {
    //  @Override
    //  public void run() {

    MClientCache connection = MonarchUtils.getConnection(FTABLE_WITH_LOGICAL_TYPES, locatorPort);
    FTable table = connection.getFTable(FTABLE_WITH_LOGICAL_TYPES);
    io.ampool.monarch.table.Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    int totalCount = 0;
    while (itr.hasNext()) {
      Row result = (Row) itr.next();
      System.out.print("[ Key = " + Bytes.toString(result.getRowId()) + " ]");

      List<Cell> selectedRow = result.getCells();

      for (Cell cell : selectedRow) {
        System.out.print("  [NNN ColumnName: " + Bytes.toString(cell.getColumnName()) + " ColumnValue:"
            + cell.getColumnValue() + "], ");

      }
      totalCount++;
//      System.out.println();
    }

    assertEquals(totalCount, noOfRecords);
//    System.out.println("XXX Total records found in scan = " + totalCount);

    connection.getAdmin().deleteFTable(FTABLE_WITH_LOGICAL_TYPES);
    //close the sink-task
    sinkTask.stop();
  }

  @Test
  public void testAmpoolSinkTaskWithTableTypes() throws Exception {
    testConnector(regionNames);
  }

  private void testScanResults(InternalTable table, int expected) {
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    int totalCount = 0;
    while (itr.hasNext()) {
      Row result = (Row) itr.next();
      System.out.print("[ Key = " + Bytes.toString(result.getRowId()) + " ]");

      List<Cell> selectedRow = result.getCells();

      for (Cell cell : selectedRow) {
        System.out.print("  [ColumnName: " + Bytes.toString(cell.getColumnName()) + " ColumnValue:"
          + cell.getColumnValue() + "], ");

      }
      totalCount++;
//      System.out.println();
    }
    assertEquals(totalCount, expected);
//    System.out.println("XXX Total records found in scan = " + totalCount);
  }

  private void testConnector(String[] regionNames) {

    AmpoolSinkTask sinkTask = new AmpoolSinkTask();
    configProps.clear();

    configProps.put("locator.port", testBase.getLocatorPort());
    configProps.put("locator.host", MonarchUtils.LOCATOR_HOST);
    configProps.put("batch.size", String.valueOf(2));
    String topics = String.join(",", regionNames);
    configProps.put("ampool.tables", topics);
    configProps.put("topics", topics);
    configProps.put("tasks.max", String.valueOf(3));
    configProps.put("max.retries", Integer.toString(5));
    configProps.put("retry.interval.ms", Integer.toString(30000));
    sinkTask.start(configProps);

    final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
      .field("url", Schema.STRING_SCHEMA)
      .field("id", Schema.INT32_SCHEMA)
      .field("zipcode", Schema.INT32_SCHEMA)
      .field("status", Schema.INT32_SCHEMA)
      .build();

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    int noOfRecords = TOTAL_RECORDS;
    for (int i = 1; i <= noOfRecords; i++) {
      final Struct record = new Struct(valueSchema)
        .put("url", "google.com")
        .put("id", i)
        .put("zipcode", 95050 + i)
        .put("status", 400 + i);
      SinkRecord sinkRecord = null;
      if (i == 1 || i == 2) {
//        System.out.println("AmpoolSinkTaskTest.testConnector 1 ");
        sinkRecord = new SinkRecord(regionNames[0], i, null, null, valueSchema, record, i);
      }
      else if (i == 3 || i == 4) {
//        System.out.println("AmpoolSinkTaskTest.testConnector 2 ");
        sinkRecord = new SinkRecord(regionNames[1], i, null, null, valueSchema, record, i);
      }
      else if (i == 5 || i == 6) {
//        System.out.println("AmpoolSinkTaskTest.testConnector 3 ");
        sinkRecord = new SinkRecord(regionNames[2], i, null, null, valueSchema, record, i);
      }
      else if (i > 6) {
//        System.out.println("AmpoolSinkTaskTest.testConnector 4 ");
        sinkRecord = new SinkRecord(regionNames[3], i, null, null, valueSchema, record, i);
      }
      sinkRecords.add(sinkRecord);
    }

    sinkTask.put(sinkRecords);

    MConfiguration mConf = MConfiguration.create();
    mConf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, "localhost");
    mConf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, Integer.parseInt(testBase.getLocatorPort()));
    MClientCache clientCache = MClientCacheFactory.getOrCreate(mConf);
    testScanResults((InternalTable) clientCache.getMTable(regionNames[0]), 2);
    testScanResults((InternalTable) clientCache.getFTable(regionNames[1]), 2);
    testScanResults((InternalTable) clientCache.getFTable(regionNames[2]), 2);
    testScanResults((InternalTable) clientCache.getMTable(regionNames[3]), 4);

    clientCache.getAdmin().deleteMTable(regionNames[0]);
    clientCache.getAdmin().deleteFTable(regionNames[1]);
    clientCache.getAdmin().deleteFTable(regionNames[2]);
    clientCache.getAdmin().deleteMTable(regionNames[3]);
    sinkTask.stop();
  }

  class ProducerTask implements Runnable {

    private String tableName = null;
    private AmpoolSinkTask sinkTask = null;

    public ProducerTask(AmpoolSinkTask task, String topicName){
      tableName = topicName;
      sinkTask =  task;
    }

    @Override
    public void run() {
//      System.out.println("ProducerTask.run tableName = " + tableName);
      if (tableName.compareTo(topicNames[0]) == 0) {
        final Schema valueSchema = SchemaBuilder.struct().name("record1").version(1)
          .field("url", Schema.STRING_SCHEMA)
          .field("id", Schema.INT32_SCHEMA)
          .field("zipcode", Schema.INT32_SCHEMA)
          .field("status", Schema.INT32_SCHEMA)
          .build();

        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (int i = 1; i <= TOTAL_RECORDS; i++) {
          final Struct record = new Struct(valueSchema)
            .put("url", "google.com")
            .put("id", i)
            .put("zipcode", 95050 + i)
            .put("status", 400 + i);
          SinkRecord sinkRecord = new SinkRecord(tableName, i, null, null, valueSchema, record, i);
          sinkRecords.add(sinkRecord);
        }
        sinkTask.put(sinkRecords);

      } else if (tableName.compareTo(topicNames[1]) == 0) {
        final Schema valueSchema = SchemaBuilder.struct().name("record2").version(1)
          .field("url", Schema.STRING_SCHEMA)
          .field("id", Schema.INT32_SCHEMA)
          .field("zipcode", Schema.INT32_SCHEMA)
          .build();

        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (int i = 1; i <= TOTAL_RECORDS; i++) {
          final Struct record = new Struct(valueSchema)
            .put("url", "google.com")
            .put("id", i)
            .put("zipcode", 95050 + i);
          SinkRecord sinkRecord = new SinkRecord(tableName, i, null, null, valueSchema, record, i);
          sinkRecords.add(sinkRecord);
        }
        sinkTask.put(sinkRecords);

      } else if (tableName.compareTo(topicNames[2]) == 0) {
        final Schema valueSchema = SchemaBuilder.struct().name("record3").version(1)
          .field("url", Schema.STRING_SCHEMA)
          .field("id", Schema.INT32_SCHEMA)
          .build();

        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (int i = 1; i <= TOTAL_RECORDS; i++) {
          final Struct record = new Struct(valueSchema)
            .put("url", "google.com")
            .put("id", i);
          SinkRecord sinkRecord = new SinkRecord(tableName, i, null, null, valueSchema, record, i);
          sinkRecords.add(sinkRecord);
        }
        sinkTask.put(sinkRecords);
      }
    }
  }

  @org.testng.annotations.Test
  public void testAmpoolSinkTaskWithMultipleThreadsWithAllTableTypes() throws RMIException, InterruptedException {

    AmpoolSinkTask sinkTask = new AmpoolSinkTask();

    configProps.clear();
    configProps.put("locator.port", testBase.getLocatorPort());
    configProps.put("locator.host", MonarchUtils.LOCATOR_HOST);
    configProps.put("batch.size", String.valueOf(100));
    String topics = String.join(",", topicNames);
    configProps.put("ampool.tables", topics);
    configProps.put("topics", topics);
    configProps.put("max.retries", Integer.toString(5));
    configProps.put("retry.interval.ms", Integer.toString(30000));
    sinkTask.start(configProps);

    ExecutorService executor = Executors.newFixedThreadPool(3);
    for (int i = 1; i <= 3; i++) {
      Runnable worker = new AmpoolSinkTaskExtendedTest.ProducerTask(sinkTask, "topic"+i);
      executor.execute(worker);
    }

    executor.shutdown();
    while (!executor.isTerminated()) {
    }

    MConfiguration mConf = MConfiguration.create();
    mConf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, "localhost");
    mConf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, Integer.parseInt(testBase.getLocatorPort()));
    MClientCache clientCache = MClientCacheFactory.getOrCreate(mConf);
    testScanResults((InternalTable) clientCache.getMTable(topicNames[0]), 10);
    testScanResults((InternalTable) clientCache.getMTable(topicNames[1]), 10);
    testScanResults((InternalTable) clientCache.getFTable(topicNames[2]), 10);

    sinkTask.stop();
  }
}

