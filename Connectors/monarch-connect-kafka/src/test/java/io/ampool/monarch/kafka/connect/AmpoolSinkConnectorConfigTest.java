package io.ampool.monarch.kafka.connect;

import org.apache.geode.cache.client.NoAvailableLocatorsException;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class AmpoolSinkConnectorConfigTest {
  private AmpoolSinkTask sinkTask;
  private Map<String, String> sinkProperties;

  @Test
  public void doc() {
//    System.out.println(AmpoolSinkConnectorConfig.getConfig().toRst());
  }

  @Before
  public void setup(){
    sinkTask = new AmpoolSinkTask();
    sinkProperties = new HashMap<>();
    sinkProperties.put("locator.host", "localhost");
    sinkProperties.put("locator.port", Integer.toString(12345));
    sinkProperties.put("batch.size", Integer.toString(100));
    sinkProperties.put("topics", "topic1, topic2, topic3");
    sinkProperties.put("ampool.tables", "table1, table2, table3");
  }

  @Test
  public void testValidConfig() {
    sinkProperties.put("max.retries", Integer.toString(5));
    sinkProperties.put("retry.interval.ms", Integer.toString(30000));
    try {
      sinkTask.start(sinkProperties);
    }catch (Exception e){
      if(e instanceof NoAvailableLocatorsException){
       //As we do not start locator here this exception is expected.
        //It is expected that we start ampool cluster externally.
      }else {
        e.printStackTrace();
        Assert.fail("Error encountered in starting SinkTask!");
      }
    }
    sinkTask.stop();
  }

  //new AmpoolSinkConnectorConfig(props)
  /*@Test
  public void testConfigWithDefaultBatchSize() {
    Map<String, String> config = new HashMap<>();
    config.put("locator.host", "localhost");
    config.put("locator.port", Integer.toString(12345));
    config.put("batch.size", Integer.toString(100));
    config.put("topics", "topic1, topic2, topic3");
    config.put("ampool.tables", "table1, table2, table3");
    AmpoolSinkConnectorConfig connectorConfig = new AmpoolSinkConnectorConfig(config);

    SinkTask

    connector.start(new HashMap<String, String>() {{
      put("locator.host", "localhost");
      put("locator.port", Integer.toString(12345));
      put("topics", "topic1, topic2, topic3");
      put("ampool.tables", "table1, table2, table3");
    }});
    List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

    System.out.println(taskConfigs.get(0).get("batch.size"));

    PowerMock.verifyAll();
  }*/

  @Test
  public void testMissingConfig() {
    AmpoolSinkTask taskWithMissingConfig = new AmpoolSinkTask();
    Exception expectedException = null;
    try {
      taskWithMissingConfig.start(new HashMap<String, String>() {{
        put("locator.host", "invalidHost");
        put("locator.port", Integer.toString(12345));
      }});
    }catch (ConfigException ex){
      //org.apache.kafka.common.config.ConfigException: Missing required configuration "topics" which has no default value
      expectedException = ex;
    }
    Assert.assertTrue(expectedException instanceof ConfigException);
  }

  @Test
  public void testInvalidConfig() {
    AmpoolSinkTask taskWithInvalidConfig = new AmpoolSinkTask();
    Exception expectedException = null;

    //Test disabled, in start() Now we are not throwing NoAvailableLocatorsException, instead log the warning.
    /*try {
      taskWithInvalidConfig.start(new HashMap<String, String>() {{
        put("locator.host", "invalidHost");
        put("locator.port", Integer.toString(12345));
        put("batch.size", Integer.toString(100));
        put("topics", "topic1, topic2, topic3");
        put("ampool.tables", "table1, table2, table3");
        put("max.retries", Integer.toString(5));
        put("retry.interval.ms", Integer.toString(30000));
      }});
    }catch (NoAvailableLocatorsException ex){
      //org.apache.geode.cache.client.NoAvailableLocatorsException: Unable to connect to any locators in the list [invalidHost:12345]
      expectedException = ex;
      taskWithInvalidConfig.stop();
    }
    Assert.assertTrue(expectedException instanceof NoAvailableLocatorsException);*/

    expectedException = null;
    try {
    taskWithInvalidConfig.start(new HashMap<String, String>() {{
      put("locator.host", "localhost");
      put("locator.port", "invalidPort");
      put("batch.size", Integer.toString(100));
      put("topics", "topic1, topic2, topic3");
      put("ampool.tables", "table1, table2, table3");
    }});
    }catch (ConfigException ex){
      //org.apache.kafka.common.config.ConfigException: Invalid value invalidPort for configuration locator.port: Not a number of type INT
      expectedException = ex;
      taskWithInvalidConfig.stop();
    }
    Assert.assertTrue(expectedException instanceof ConfigException);


    expectedException = null;
    try {
      taskWithInvalidConfig.start(new HashMap<String, String>() {{
        put("locator.host", "localhost");
        put("locator.port",  Integer.toString(12345));
        put("batch.size", "batchSize");
        put("topics", "topic1, topic2, topic3");
        put("ampool.tables", "table1, table2, table3");
      }});
    }catch (ConfigException ex){
      //org.apache.kafka.common.config.ConfigException: Invalid value batchSize for configuration batch.size: Not a number of type INT
      expectedException = ex;
      taskWithInvalidConfig.stop();
    }
    Assert.assertTrue(expectedException instanceof ConfigException);

    expectedException = null;
    try {
      taskWithInvalidConfig.start(new HashMap<String, String>() {{
        put("locator.host", "localhost");
        put("locator.port", Integer.toString(12345));
        put("batch.size", Integer.toString(100));
        put("topics", "topic1");
        put("ampool.tables", "table1, table2, table3");
      }});
    }catch(ConfigException ex){
      //org.apache.kafka.common.config.ConfigException: Number of topics and ampool tables must be same!
      expectedException = ex;
      taskWithInvalidConfig.stop();
    }
    Assert.assertTrue(expectedException instanceof ConfigException);
  }

  @Test
  public void testInvalidRowKeyConfig() {
    List<String> invalidRowKeyList = new ArrayList<>(Arrays.asList(
        "table1-key1,table-key2",
        "table1, table2, table3",
        "",
        ":,:,:,:",
        "table1:key1:,,:,,"));

    for(String rowKey : invalidRowKeyList) {
      _testInvalidRowKeyProperty(rowKey);
    }
  }

  private void _testInvalidRowKeyProperty(String rowKey) {
    AmpoolSinkTask taskWithInvalidConfig = new AmpoolSinkTask();
    Exception expectedException = null;
    expectedException = null;
    try {
      taskWithInvalidConfig.start(new HashMap<String, String>() {{
        put("locator.host", "localhost");
        put("locator.port", Integer.toString(12345));
        put("batch.size", Integer.toString(100));
        put("topics", "topic1, topic2, topic3");
        put("ampool.tables", "table1, table2, table3");
        put(AmpoolSinkConnectorConfig.TABLES_ROWKEY_COLUMNS, rowKey);
      }});
    }catch (ConfigException ex){
      System.out.println("rowKey = [ " + rowKey + " ], Cought expected exception = "+ ex.getMessage());
      ex.printStackTrace();

      expectedException = ex;
      taskWithInvalidConfig.stop();
    }
    Assert.assertTrue(expectedException instanceof ConfigException);
  }

  @Test
  public void testInvalidRetryConfig() {
    AmpoolSinkTask taskWithInvalidConfig = new AmpoolSinkTask();
    Exception expectedException = null;
    try {
      taskWithInvalidConfig.start(new HashMap<String, String>() {{
        put("locator.host", "invalidHost");
        put("locator.port", Integer.toString(12345));
        put("batch.size", Integer.toString(100));
        put("topics", "topic1, topic2, topic3");
        put("ampool.tables", "table1, table2, table3");
        put("max.retries", "invalidParam");
        put("retry.interval.ms", Integer.toString(30000));
      }});
    } catch (ConfigException ex) {
      //ConfigException: Invalid configuration specified for retry-interval or max-retries!
      expectedException = ex;
      taskWithInvalidConfig.stop();
    }
    Assert.assertTrue(expectedException instanceof ConfigException);
  }
}
