package io.ampool.monarch.kafka.connect;

import io.ampool.monarch.RMIException;
import io.ampool.monarch.SerializableRunnable;
import io.ampool.monarch.VM;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AmpoolSinkTaskTest extends TestBase implements Serializable {
  private static final String TABLE1 = "test1  ";
  private static final String TABLE2 = " test2";
  private static final String TABLE3 = "test3 ";
  private static final String TRANSACTION_TABLE = " trx_table";
  private static final String TABLE_ALL_TYPES = "test_allSupportedTypes";
  private static final String TABLE_LOGICAL_TYPES = "Test_Logical_Types";
  private static final String TOPIC1 = "test1  ";
  private static final String TOPIC2 = " test2";
  private static final String TOPIC3 = " test3";
  private static final String TOPIC2_ALL_TYPES = "test_allSupportedTypes";
  private static final String TOPIC_LOGICAL_TYPES = "Test_Logical_Types";
  private static final String TRANSACTION_TOPIC = " trx_topic";

  private static final String resourceFile = "data/sample1.txt";
  public static final String COLUMNS = "c1,c2,c3,c4";
  public static final List<String> COLUMN_LIST = Arrays.asList(COLUMNS.split(","));

  private long readLineCount;
  private static final int TOTAL_RECORDS = 1000;
  private final Map<String, String> configProps = new HashMap<>();

  @BeforeTest
  /*public void setUpBeforeMethod() throws Exception {
    //kafka sink-connector/task config.
    configProps.put("locator.port", testBase.getLocatorPort());
    configProps.put("locator.host", MonarchUtils.LOCATOR_HOST);
    configProps.put("batch.size", String.valueOf(1));
    configProps.put("ampool.tables", regionName);
    configProps.put("topics", regionName);
    final String localPort = testBase.getLocatorPort();
    //create ampool table
    testBase.getClientVm().invoke(new SerializableRunnable() {
      @Override
      public void run() {
        final Map<String, String> map = new HashMap<>();
        try {
          MonarchUtils.createConnectionAndMTable(regionName, localPort);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }*/
  public void setUpBeforeMethod() throws Exception {
    //kafka sink-connector/task config.
    configProps.put("locator.port", testBase.getLocatorPort());
    configProps.put("locator.host", MonarchUtils.LOCATOR_HOST);
    configProps.put("batch.size", String.valueOf(1));
    configProps.put("ampool.tables", TABLE1+","+TABLE2+","+TABLE3+","+TABLE_ALL_TYPES+","+TABLE1+"_Unknown"+","+TRANSACTION_TABLE+","+TABLE_LOGICAL_TYPES);
    configProps.put("topics", TOPIC1+","+TOPIC2+","+TOPIC3+","+TOPIC2_ALL_TYPES+","+TABLE1+"_Unknown"+","+TRANSACTION_TOPIC+","+TABLE_LOGICAL_TYPES);
    configProps.put("max.retries", Integer.toString(5));
    configProps.put("retry.interval.ms", Integer.toString(30000));

    //create ampool table

    //final Map<String, String> map = new HashMap<>();
    //map.put(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());
    //map.put(MonarchUtils.REGION, regionName);
    final String localPort = testBase.getLocatorPort();
    testBase.getClientVm().invoke(new SerializableRunnable() {
      @Override
      public void run() {

        try {
          MonarchUtils.createConnectionAndMTable(TABLE1.trim(), localPort);
          MonarchUtils.createConnectionAndMTable(TABLE2.trim(), localPort);
          MonarchUtils.createConnectionAndMTable(TABLE3.trim(), localPort);
          MonarchUtils.createConnectionAndMTable_2(TABLE_ALL_TYPES.trim(), localPort);
          MonarchUtils.createConnectionAndMTable_LogicalTypes(TABLE_LOGICAL_TYPES.trim(), localPort);
          MonarchUtils.createConnectionAndMTable(TRANSACTION_TABLE.trim(), localPort);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

  @AfterTest
  /*public void setUpAfterMethod() throws RMIException, Exception {
    String locatorPort = testBase.getLocatorPort();
    testBase.getClientVm().invoke(new SerializableRunnable() {
      @Override
      public void run() {
        try {
          MonarchUtils.destroyTable(regionName, locatorPort);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }*/
  public void setUpAfterMethod() throws Exception {

    //final Map<String, String> map = new HashMap<>();
    //map.put(MonarchUtils.LOCATOR_PORT, testBase.getLocatorPort());
    //map.put(MonarchUtils.REGION, regionName);
    final String locatorPort = testBase.getLocatorPort();
    try {
      MonarchUtils.destroyTable(TABLE1.trim(), locatorPort);
      MonarchUtils.destroyTable(TABLE2.trim(), locatorPort);
      MonarchUtils.destroyTable(TABLE3.trim(), locatorPort);
      MonarchUtils.destroyTable(TABLE_ALL_TYPES.trim(), locatorPort);
      MonarchUtils.destroyTable(TABLE_LOGICAL_TYPES.trim(), locatorPort);
      MonarchUtils.destroyTable(TRANSACTION_TABLE.trim(), locatorPort);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  /**
   * Perform write through kafka-connect and verify it in ampool table
   */
  @org.testng.annotations.Test
  public void testAmpoolSinkTask() throws RMIException {
    final VM clientVm = testBase.getClientVm();

    AmpoolSinkTask sinkTask = new AmpoolSinkTask();
    sinkTask.start(configProps);

    final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
        .field("url", Schema.STRING_SCHEMA)
        .field("id", Schema.INT32_SCHEMA)
        .field("zipcode", Schema.INT32_SCHEMA)
        .field("status", Schema.INT32_SCHEMA)
        .build();

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    int noOfRecords = 10;
    for (int i = 1; i <= TOTAL_RECORDS; i++) {
      final Struct record = new Struct(valueSchema)
          .put("url", "google.com")
          .put("id", i)
          .put("zipcode", 95050 + i)
          .put("status", 400 + i);
      SinkRecord sinkRecord = new SinkRecord(TABLE1.trim(), 0, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }

    sinkTask.put(sinkRecords);

    //MTable scan and verify records.
    final String locatorPort = testBase.getLocatorPort();
    //clientVm.invoke(new SerializableRunnable() {
    //  @Override
    //  public void run() {

    MTable table = MonarchUtils.getConnection(TABLE1.trim(), locatorPort).getMTable(TABLE1.trim());
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
    assertEquals(totalCount, TOTAL_RECORDS);
//    System.out.println("XXX Total records found in scan = " + totalCount);

    sinkTask.stop();
  }

  @org.testng.annotations.Test
  public void testAmpoolSinkTaskWithRowKey() throws RMIException {
    final VM clientVm = testBase.getClientVm();
    configProps.put(AmpoolSinkConnectorConfig.TABLES_ROWKEY_COLUMNS, TRANSACTION_TOPIC.trim() + ":" + "id");

    AmpoolSinkTask sinkTask = new AmpoolSinkTask();
    sinkTask.start(configProps);

    final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
        .field("url", Schema.STRING_SCHEMA)
        .field("id", Schema.INT32_SCHEMA)
        .field("zipcode", Schema.INT32_SCHEMA)
        .field("status", Schema.INT32_SCHEMA)
        .build();

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (int i = 1; i <= TOTAL_RECORDS; i++) {
      final Struct record = new Struct(valueSchema)
          .put("url", "ampool.io")
          .put("id", i)
          .put("zipcode", 411014 + i)
          .put("status", 500 + i);
      SinkRecord sinkRecord = new SinkRecord(TRANSACTION_TOPIC.trim(), 0, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }
    sinkTask.put(sinkRecords);

    final String locatorPort = testBase.getLocatorPort();
    MTable table = MonarchUtils.getConnection(TRANSACTION_TABLE.trim(), locatorPort).getMTable(TRANSACTION_TABLE.trim());
    int scanCount1 = getScanCount(table, locatorPort);
    assertEquals(scanCount1, TOTAL_RECORDS);


    //update records
    for (int i = 1; i <= TOTAL_RECORDS; i++) {
      final Struct record = new Struct(valueSchema)
          .put("url", "ampool-updated.io")
          .put("id", i)
          .put("zipcode", 411014 + TOTAL_RECORDS + i)
          .put("status", 500 + TOTAL_RECORDS + i);
      SinkRecord sinkRecord = new SinkRecord(TRANSACTION_TOPIC.trim(), 0, null, null, valueSchema, record, i + TOTAL_RECORDS);
      sinkRecords.add(sinkRecord);
    }
    sinkTask.put(sinkRecords);

    int scanCount2 =  getScanCount(table, locatorPort);
    assertEquals(scanCount2, TOTAL_RECORDS);
    //MTable scan and verify records.
    //clientVm.invoke(new SerializableRunnable() {
    //  @Override
    //  public void run() {

    if(table != null){
      MonarchUtils.getConnection(TRANSACTION_TABLE.trim(), locatorPort).getAdmin().deleteMTable(TRANSACTION_TABLE.trim());
    }
    sinkTask.stop();
  }

  private int getScanCount(MTable table, String locatorPort){
      Scanner scanner = table.getScanner(new Scan());
      Iterator itr = scanner.iterator();
      int totalCount = 0;
      while (itr.hasNext()) {
        Row result = (Row) itr.next();

        List<Cell> selectedRow = result.getCells();

        for (Cell cell : selectedRow) {
          if(Arrays.equals(Bytes.toBytes("id"), cell.getColumnName())) {
            System.out.print("  [ColumnName: " + Bytes.toString(cell.getColumnName()) + " ColumnValue:"
                + cell.getColumnValue() + "], ");

            assertTrue(Arrays.equals(result.getRowId(), Bytes.toBytes((int)cell.getColumnValue())));
          }
        }
        totalCount++;
//        System.out.println();
      }
//      System.out.println("XXX Total records found in scan = " + totalCount);

    return totalCount;
  }

  private int getScanCount_2(MTable table, String locatorPort) {
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    int totalCount = 0;
    while (itr.hasNext()) {
      Row result = (Row) itr.next();

      List<Cell> selectedRow = result.getCells();

//      for (Cell cell : selectedRow) {
//        System.out.print("  [ColumnName: " + Bytes.toString(cell.getColumnName()) + " ColumnValue:"
//            + cell.getColumnValue() + "], ");
//
//      }
      totalCount++;
    }
//    System.out.println();
    return totalCount;
  }

  /**
   * Verify sinkTask throws RetriableException when servers are not running.
   */
  @org.testng.annotations.Test
  public void testAmpoolSinkTaskWithServerDown() throws RMIException {

    testBase.stopServerOn(testBase.getVm(0));
    testBase.stopServerOn(testBase.getVm(2));
    testBase.stopServerOn(testBase.getVm(3));

    AmpoolSinkTask sinkTask = new AmpoolSinkTask();

    try {
      sinkTask.start(configProps);
    }catch (Exception e) {
    }

    final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
      .field("url", Schema.STRING_SCHEMA)
      .field("id", Schema.INT32_SCHEMA)
      .field("zipcode", Schema.INT32_SCHEMA)
      .field("status", Schema.INT32_SCHEMA)
      .build();

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    int noOfRecords = 10;
    for (int i = 1; i <= noOfRecords; i++) {
      final Struct record = new Struct(valueSchema)
        .put("url", "google.com")
        .put("id", i)
        .put("zipcode", 95050 + i)
        .put("status", 400 + i);
      SinkRecord sinkRecord = new SinkRecord(TABLE1, 0, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }

    Exception exception = null;
    try {
      sinkTask.put(sinkRecords);
    }catch (Exception re) {
      //verify retry mechanism
      if(re instanceof RetriableException){
        exception = re;
      }
    }
    assertTrue(exception instanceof RetriableException);

    sinkTask.stop();

    testBase.startServerOn(testBase.getVm(0), testBase.getLocatorString());
    testBase.startServerOn(testBase.getVm(2), testBase.getLocatorString());
    testBase.startServerOn(testBase.getVm(3), testBase.getLocatorString());
  }


  /**
   * Testcase - Test ampool sink task with multiple server restart
   * Steps in a testcase
   * 1. Ampool cluster setup (1 locator, 3 servers)
   * 2. create a ampool sinkTask
   * 3. create a table - durableTable - from a client vm.
   * 4. create a sink-records and ingest them using sinkTask.put(sinkRecords).
   * 5. verify that no exception has occurred and records are ingested correctly.
   * 6. Create a scenario where sink-task is running but servers are down - stop all running servers.
   * 7. restart all servers and simulate replay by ingesting sink-records again using sinkTask.put(sinkRecords)
   * 8. verify that records gets ingested after server restart.
   *
   */
  @org.testng.annotations.Test
  public void testAmpoolSinkTaskWithServerRestart() throws RMIException, InterruptedException {

    final String TABLE_NAME = "durableTable";
    final String TOPIC_NAME = "durableTopic";

    AmpoolSinkTask sinkTask = new AmpoolSinkTask();

    //create required table - durableTable.
    final String locatorPort = testBase.getLocatorPort();
    testBase.getClientVm().invoke(new SerializableRunnable() {
      @Override
      public void run() {

        try {
          MonarchUtils.createConnectionAndMTable(TABLE_NAME, locatorPort);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    //kafka sink-connector/task config.
    final Map<String, String> config = new HashMap<>();
    config.put("locator.port", testBase.getLocatorPort());
    config.put("locator.host", MonarchUtils.LOCATOR_HOST);
    config.put("batch.size", String.valueOf(100));
    config.put("ampool.tables", TABLE_NAME);
    config.put("topics", TOPIC_NAME);
    config.put("max.retries", Integer.toString(5));
    config.put("retry.interval.ms", Integer.toString(30000));

    try {
      sinkTask.start(config);
    }catch (Exception e) {
      System.out.println("start() All ServerDown Exception = " + e.getMessage());
      e.printStackTrace();
    }

    final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
        .field("url", Schema.STRING_SCHEMA)
        .field("id", Schema.INT32_SCHEMA)
        .field("zipcode", Schema.INT32_SCHEMA)
        .field("status", Schema.INT32_SCHEMA)
        .build();

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    int noOfRecords = 10;
    for (int i = 1; i <= noOfRecords; i++) {
      final Struct record = new Struct(valueSchema)
          .put("url", "google.com")
          .put("id", i)
          .put("zipcode", 95050 + i)
          .put("status", 400 + i);
      SinkRecord sinkRecord = new SinkRecord(TOPIC_NAME, 0, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }

    //verify that if table is not created, sinkTask throws RetriableException
    //Exception exception = null;
    try {
      sinkTask.put(sinkRecords);
    }catch (Exception re) {
      org.testng.Assert.fail("Error unexpected exception cought - " + re.getMessage());
    }
    //verify that records are ingested correctly
    MTable table = MonarchUtils.getConnection(TABLE_NAME, locatorPort).getMTable(TABLE_NAME);
    int scanCount1 = getScanCount_2(table, locatorPort);
    //System.out.println("NNNN AmpoolSinkTaskTest scan count = "+ scanCount1);
    assertEquals(scanCount1, noOfRecords);

    //Restart servers.
    //Create a scenario where sink-task is running but servers are down.
    testBase.stopServerOn(testBase.getVm(0));
    testBase.stopServerOn(testBase.getVm(2));
    testBase.stopServerOn(testBase.getVm(3));

    //wait for proper shutdown
    Thread.sleep(2000);

    //restart servers
    testBase.startServerOn(testBase.getVm(0), testBase.getLocatorString());
    testBase.startServerOn(testBase.getVm(2), testBase.getLocatorString());
    testBase.startServerOn(testBase.getVm(3), testBase.getLocatorString());

    //test what happens when sinktask tries to ingest records.
    sinkRecords.clear();
    noOfRecords = 100;
    for (int i = 1; i <= noOfRecords; i++) {
      final Struct record = new Struct(valueSchema)
          .put("url", "ampool.io")
          .put("id", i)
          .put("zipcode", 100000 + i)
          .put("status", 1000 + i);
      SinkRecord sinkRecord = new SinkRecord(TOPIC_NAME, 0, null, null, valueSchema, record, noOfRecords+ i);
      sinkRecords.add(sinkRecord);
    }

    Exception exception = null;
    try {
      sinkTask.put(sinkRecords);
    }catch (Exception re) {
      org.testng.Assert.fail("Error unexpected exception cought - " + re.getMessage());
    }

    //MTable table = MonarchUtils.getConnection(TABLE_NAME, locatorPort).getMTable(TABLE_NAME);
    int scanCount2 = getScanCount_2(table, locatorPort);
    //System.out.println("NNNN AmpoolSinkTaskTest scan count = "+ scanCount2);

    assertEquals(scanCount2, noOfRecords);

    sinkTask.stop();
  }

  /**
   * Testcase - When servers (cluster) are up and running but required table is not created.
   * Expectation :sinkTask should throw RetriableException, till #retry-attempts or table is available.
   */
  @org.testng.annotations.Test
  public void testAmpoolSinkTaskWhenTableNotCreated() throws RMIException {

    AmpoolSinkTask sinkTask = new AmpoolSinkTask();

    final String TABLE_NAME = "NotExistingTable";
    final String TOPIC_NAME = "dummyTopic";

    //kafka sink-connector/task config.
    final Map<String, String> config = new HashMap<>();
    config.put("locator.port", testBase.getLocatorPort());
    config.put("locator.host", MonarchUtils.LOCATOR_HOST);
    config.put("batch.size", String.valueOf(100));
    config.put("ampool.tables", TABLE_NAME);
    config.put("topics", TOPIC_NAME);
    config.put("max.retries", Integer.toString(5));
    config.put("retry.interval.ms", Integer.toString(30000));

    try {
      sinkTask.start(config);
    }catch (Exception e) {
      System.out.println("start() All ServerDown Exception = " + e.getMessage());
      e.printStackTrace();
    }

    final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
        .field("url", Schema.STRING_SCHEMA)
        .field("id", Schema.INT32_SCHEMA)
        .field("zipcode", Schema.INT32_SCHEMA)
        .field("status", Schema.INT32_SCHEMA)
        .build();

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    int noOfRecords = 10;
    for (int i = 1; i <= noOfRecords; i++) {
      final Struct record = new Struct(valueSchema)
          .put("url", "google.com")
          .put("id", i)
          .put("zipcode", 95050 + i)
          .put("status", 400 + i);
      SinkRecord sinkRecord = new SinkRecord(TOPIC_NAME, 0, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }

    //verify that if table is not created, sinkTask throws RetriableException
    Exception exception = null;
    try {
      sinkTask.put(sinkRecords);
    }catch (Exception re) {
      if(re instanceof RetriableException){
        exception = re;
      }
    }
    assertTrue(exception instanceof RetriableException);

    sinkTask.stop();
  }

  /**
   * Testcase steps (Simulate kafka connect retry mechanism)
   * 1. Ampool servers are down
   * 2. try to ingest data using sinktask.put() verify for the RetriableException.
   * 3. Start ampool servers and create required region.
   * 4. replay data ingestion, verify that this time no RetriableException occurs
   * 5. scan the table and verify #records.
   */
  @org.testng.annotations.Test
  public void testAmpoolSinkTaskWithDelayedServerStart() throws RMIException {

    testBase.stopServerOn(testBase.getVm(0));
    testBase.stopServerOn(testBase.getVm(2));
    testBase.stopServerOn(testBase.getVm(3));

    final String TABLE_NAME = "exampleTable";
    final String TOPIC_NAME = "exampleTopic";

    //kafka sink-connector/task config.
    final Map<String, String> config = new HashMap<>();
    config.put("locator.port", testBase.getLocatorPort());
    config.put("locator.host", MonarchUtils.LOCATOR_HOST);
    config.put("batch.size", String.valueOf(100));
    config.put("ampool.tables", TABLE_NAME);
    config.put("topics", TOPIC_NAME);
    config.put("max.retries", Integer.toString(5));
    config.put("retry.interval.ms", Integer.toString(30000));

    AmpoolSinkTask sinkTask = new AmpoolSinkTask();

    try {
      sinkTask.start(config);
    }catch (Exception e) {
      System.out.println("start() All ServerDown Exception = " + e.getMessage());
      e.printStackTrace();
    }

    final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
        .field("url", Schema.STRING_SCHEMA)
        .field("id", Schema.INT32_SCHEMA)
        .field("zipcode", Schema.INT32_SCHEMA)
        .field("status", Schema.INT32_SCHEMA)
        .build();

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    //int noOfRecords = 10;
    for (int i = 1; i <= TOTAL_RECORDS; i++) {
      final Struct record = new Struct(valueSchema)
          .put("url", "google.com")
          .put("id", i)
          .put("zipcode", 95050 + i)
          .put("status", 400 + i);
      SinkRecord sinkRecord = new SinkRecord(TOPIC_NAME, 0, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }

    Exception exception = null;
    try {
      sinkTask.put(sinkRecords);
    }catch (Exception re) {
      //verify retry mechanism
      if(re instanceof RetriableException){
        //System.out.println("Cought exception of type RetriableException..!");
        exception = re;
      }
    }
    assertTrue(exception instanceof RetriableException);

    //ampool servers - delayed start
    testBase.startServerOn(testBase.getVm(0), testBase.getLocatorString());
    testBase.startServerOn(testBase.getVm(2), testBase.getLocatorString());
    testBase.startServerOn(testBase.getVm(3), testBase.getLocatorString());

    //create required table.
    final String locatorPort = testBase.getLocatorPort();
    testBase.getClientVm().invoke(new SerializableRunnable() {
      @Override
      public void run() {

        try {
          MonarchUtils.createConnectionAndMTable(TABLE_NAME, locatorPort);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    //replay sinkTask.put(SinkRecords)
    try {
      sinkTask.put(sinkRecords);
    }catch (Exception re) {
      //verify retry mechanism
      if(re instanceof RetriableException){
        org.testng.Assert.fail("Error: Should not get RetriableException as cluster is up and table is created!");
      }
    }

    //MTable scan and verify records.
    MTable table = MonarchUtils.getConnection(TABLE_NAME, locatorPort).getMTable(TABLE_NAME);
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    int totalCount = 0;
    while (itr.hasNext()) {
      Row result = (Row) itr.next();
      System.out.print("[ Key = " + Bytes.toString(result.getRowId()) + " ]");

      List<Cell> selectedRow = result.getCells();

//      for (Cell cell : selectedRow) {
//        System.out.print("  [ColumnName: " + Bytes.toString(cell.getColumnName()) + " ColumnValue:"
//          + cell.getColumnValue() + "], ");
//
//      }
      totalCount++;
//      System.out.println();
    }
    assertEquals(totalCount, TOTAL_RECORDS);
//    System.out.println("XXX Total records found in scan = " + totalCount);

    sinkTask.stop();
  }

  @org.testng.annotations.Test
  public void testAmpoolSinkTaskWithAllSupportedTypes(){
    AmpoolSinkTask sinkTask = new AmpoolSinkTask();
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
        .field("the_bytes", Schema.BYTES_SCHEMA
        //.field("the_decimal", Decimal.schema(2).schema());
        //.field("the_date", Date.SCHEMA);
        //.field("the_time", Time.SCHEMA)
        //.field("the_timestamp", Timestamp.SCHEMA
        );

    final java.util.Date instant = new java.util.Date(1474661402123L);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (int i = 1; i <= TOTAL_RECORDS; i++) {
      final Struct record = new Struct(valueSchema)
          .put("the_byte", (byte) -32)
          .put("the_short", (short) 1234)
          .put("the_int", 42 + i)
          .put("the_long", 12425436L)
          .put("the_float", 2356.3f)
          .put("the_double", -2436546.56457d)
          .put("the_bool", true)
          .put("the_string", "foo" + i)
          .put("the_bytes", new byte[] { -32, 124 });
              //.put("the_decimal", new BigDecimal("1234.567"))
              //.put("the_date", instant)
              //.put("the_time", instant)
              //.put("the_timestamp", instant);

      SinkRecord sinkRecord = new SinkRecord(TABLE_ALL_TYPES, 0, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }

    sinkTask.put(sinkRecords);

    //MTable scan and verify records.
    final String locatorPort = testBase.getLocatorPort();
    //testBase.getClientVm().invoke(new SerializableRunnable() {
    //  @Override
    //  public void run() {

    MTable table = MonarchUtils.getConnection(TABLE_ALL_TYPES, locatorPort).getMTable(TABLE_ALL_TYPES);
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    int totalCount = 0;
    while (itr.hasNext()) {
      Row result = (Row) itr.next();
      System.out.print("[ Key = " + Bytes.toString(result.getRowId()) + " ]");

      List<Cell> selectedRow = result.getCells();

//      for (Cell cell : selectedRow) {
//        System.out.print("  [ColumnName: " + Bytes.toString(cell.getColumnName()) + " ColumnValue:"
//            + cell.getColumnValue() + "], ");
//
//      }
      totalCount++;
//      System.out.println();
    }

    assertEquals(totalCount, TOTAL_RECORDS);
//    System.out.println("XXX Total records found in scan = " + totalCount);
    //  }
    //});

    //close the sink-task
    sinkTask.stop();
  }

  @org.testng.annotations.Test
  public void testAmpoolSinkTaskWithLogicalTypes(){
    AmpoolSinkTask sinkTask = new AmpoolSinkTask();
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
    for (int i = 1; i <= TOTAL_RECORDS; i++) {
      final Struct record = new Struct(valueSchema)
          .put("the_int", 42 + i)
          .put("the_decimal", decimalInstance)
          .put("the_date", dateInstance)
          .put("the_time", dateInstance)
          .put("the_timestamp", dateInstance);

      SinkRecord sinkRecord = new SinkRecord(TABLE_LOGICAL_TYPES, 0, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }

    sinkTask.put(sinkRecords);

    //MTable scan and verify records.
    final String locatorPort = testBase.getLocatorPort();
    //testBase.getClientVm().invoke(new SerializableRunnable() {
    //  @Override
    //  public void run() {

    MTable table = MonarchUtils.getConnection(TABLE_LOGICAL_TYPES, locatorPort).getMTable(TABLE_LOGICAL_TYPES);
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    int totalCount = 0;
    while (itr.hasNext()) {
      Row result = (Row) itr.next();
      System.out.print("[ Key = " + Bytes.toString(result.getRowId()) + " ]");

      List<Cell> selectedRow = result.getCells();

//      for (Cell cell : selectedRow) {
//        System.out.print("  [ColumnName: " + Bytes.toString(cell.getColumnName()) + " ColumnValue:"
//            + cell.getColumnValue() + "], ");
//
//      }
      totalCount++;
//      System.out.println();
    }

    assertEquals(totalCount, TOTAL_RECORDS);
//    System.out.println("XXX Total records found in scan = " + totalCount);
    //  }
    //});

    //close the sink-task
    sinkTask.stop();
  }

  /**
   * Testcase: table corresponding to topic is configured in sink.properties file but not created in ampool explicitly.
   * @throws RMIException
   */
  @org.testng.annotations.Test
  public void testAmpoolSinkTaskWithNonExistingTable() throws RMIException {
    final VM clientVm = testBase.getClientVm();

    AmpoolSinkTask sinkTask = new AmpoolSinkTask();
    sinkTask.start(configProps);

    final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
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
      SinkRecord sinkRecord = new SinkRecord(TABLE1.trim()+"_Unknown", 0, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }

    Exception exception = null;
    try {
      sinkTask.put(sinkRecords);
    }catch (Exception re){
      if(re instanceof RetriableException){
        exception = re;
      }
    }
    assertTrue(exception instanceof RetriableException);

    //cleanup
    final String locatorPort = testBase.getLocatorPort();
    MTable table = MonarchUtils.getConnection(TABLE1.trim()+"_Unknown", locatorPort).getMTable(TABLE1.trim()+"_Unknown");

    //close the sink-task
    sinkTask.stop();
  }

  class ProducerTask implements Runnable{

    private String tableName = null;
    private AmpoolSinkTask sinkTask = null;
    private String locatorPort = null;

    public ProducerTask(AmpoolSinkTask task, String topicName){
      tableName = topicName.trim();
      sinkTask =  task;
    }

    @Override
    public void run() {
      //create and update records.
      if(TOPIC3.trim().equalsIgnoreCase(tableName)){
        final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
            .field("url", Schema.STRING_SCHEMA)
            .field("id", Schema.INT32_SCHEMA)
            .field("zipcode", Schema.INT32_SCHEMA)
            .field("status", Schema.INT32_SCHEMA)
            .build();

        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (int i = 1; i <= TOTAL_RECORDS; i++) {
          final Struct record = new Struct(valueSchema)
              .put("url", "ampool.io")
              .put("id", i)
              .put("zipcode", 95050 + i)
              .put("status", 400 + i);
          SinkRecord sinkRecord = new SinkRecord(tableName, 0, null, null, valueSchema, record, i);
          sinkRecords.add(sinkRecord);
        }
        sinkTask.put(sinkRecords);

        sinkRecords.clear();
        for (int i = 1; i <= TOTAL_RECORDS; i++) {
          final Struct record = new Struct(valueSchema)
              .put("url", "ampool-updated.io")
              .put("id", i)
              .put("zipcode", 411015 + TOTAL_RECORDS + i)
              .put("status", 500 + TOTAL_RECORDS+ i);
          SinkRecord sinkRecord = new SinkRecord(tableName, 0, null, null, valueSchema, record, i);
          sinkRecords.add(sinkRecord);
        }
        sinkTask.put(sinkRecords);


      }else {
        final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
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
          SinkRecord sinkRecord = new SinkRecord(tableName, 0, null, null, valueSchema, record, i);
          sinkRecords.add(sinkRecord);
        }
        sinkTask.put(sinkRecords);
      }
    }
  }

  @org.testng.annotations.Test
  public void testAmpoolSinkTaskWithMultipleThreads() throws RMIException, InterruptedException {

    AmpoolSinkTask sinkTask = new AmpoolSinkTask();
    configProps.put(AmpoolSinkConnectorConfig.TABLES_ROWKEY_COLUMNS, TOPIC3.trim() + ":" +"id");
    sinkTask.start(configProps);
    final String locatorPort = testBase.getLocatorPort();

    ExecutorService executor = Executors.newFixedThreadPool(3);
    for (int i = 1; i <= 3; i++) {
      Runnable worker = new ProducerTask(sinkTask, "test"+i);
      executor.execute(worker);
    }

    executor.shutdown();
    while (!executor.isTerminated()) {
    }


    //MTable scan and verify records.

    verifyTableRecords(MonarchUtils.getConnection(TABLE1.trim(), locatorPort).getMTable(TABLE1.trim()), null, null);
    verifyTableRecords(MonarchUtils.getConnection(TABLE2.trim(), locatorPort).getMTable(TABLE2.trim()), null, null);
    verifyTableRecords(MonarchUtils.getConnection(TABLE3.trim(), locatorPort).getMTable(TABLE3.trim()), "id", "url");

    sinkTask.stop();
  }

  private void verifyTableRecords(MTable table, String keyColumn, String columnToValidate) {
    //MTable table = MonarchUtils.getConnection(regionName, locatorPort).getMTable("test");
//    System.out.println("NNNN TEST Scanning table -- "+ table.getName());
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    int totalCount = 0;
    while (itr.hasNext()) {
      Row result = (Row) itr.next();
      /*
      if(TABLE3.equals(table.getName())){
        System.out.print("[ Key = " + Bytes.toInt(result.getRowId()) + " ]");
      }else {
        System.out.print("[ Key = " + Bytes.toString(result.getRowId()) + " ]");
      }
      */

      List<Cell> selectedRow = result.getCells();

      for (Cell cell : selectedRow) {
        /*System.out.print("  [ColumnName: " + Bytes.toString(cell.getColumnName()) + " ColumnValue:"
            + cell.getColumnValue() + "], ");
        */
        if(keyColumn != null && Bytes.toString(cell.getColumnName()).equals(keyColumn)){
//          System.out.println("RowID = "+ Bytes.toInt(result.getRowId())  +  " AND " + "KeyColumn Value = " + cell.getColumnValue());
          assertEquals(Bytes.toInt(result.getRowId()), cell.getColumnValue());
        }

        if(columnToValidate != null && Bytes.toString(cell.getColumnName()).equals(columnToValidate)){
          assertTrue(cell.getColumnValue().toString().contains("updated"));
        }
      }
      totalCount++;
//      System.out.println();
    }
    assertEquals(totalCount, TOTAL_RECORDS);
//    System.out.println("NNNN Table [" + table.getName() + "] Total records found in scan = " + totalCount);
  }

}
