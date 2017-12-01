package io.ampool.monarch.kafka.connect;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AmpoolSinkConnectorTest {
  private AmpoolSinkConnector connector;
  private ConnectorContext context;
  private Map<String, String> sinkProperties;

  @Before
  public void setup(){
    connector = new AmpoolSinkConnector();
    context = PowerMock.createMock(ConnectorContext.class);
    connector.initialize(context);

    sinkProperties = new HashMap<>();
    sinkProperties.put("locator.host", "localhost");
    sinkProperties.put("locator.port", Integer.toString(12345));
    sinkProperties.put("batch.size", Integer.toString(100));
    sinkProperties.put("topics", "topic1, topic2, topic3");
    sinkProperties.put("ampool.tables", "table1, table2, table3");
  }

  @Test
  public void testSinkTasks() {
    PowerMock.replayAll();
    connector.start(sinkProperties);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
    Assert.assertEquals(1, taskConfigs.size());
    Assert.assertEquals("localhost", taskConfigs.get(0).get("locator.host"));
    Assert.assertEquals("12345", taskConfigs.get(0).get("locator.port"));
    Assert.assertEquals("100", taskConfigs.get(0).get("batch.size"));;
    Assert.assertEquals("topic1, topic2, topic3", taskConfigs.get(0).get("topics"));
    Assert.assertEquals("table1, table2, table3", taskConfigs.get(0).get("ampool.tables"));

//    System.out.println(taskConfigs.get(0).get("locator.host"));
//    System.out.println(taskConfigs.get(0).get("locator.port"));
//    System.out.println(taskConfigs.get(0).get("batch.size"));
//    System.out.println(taskConfigs.get(0).get("topics"));
//    System.out.println(taskConfigs.get(0).get("ampool.tables"));
    PowerMock.verifyAll();
  }

  @Test
  public void testConfigWithDefaultBatchSize() {
    PowerMock.replayAll();
    connector.start(new HashMap<String, String>() {{
      put("locator.host", "localhost");
      put("locator.port", Integer.toString(12345));
      put("topics", "topic1, topic2, topic3");
      put("ampool.tables", "table1, table2, table3");
    }});
    List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

//    System.out.println("TEST batch_size=" + taskConfigs.get(0).get("batch.size"));
    Assert.assertEquals(String.valueOf(AmpoolSinkConnectorConfig.DEFALUT_BATCH_SIZE), taskConfigs.get(0).get("batch.size"));;
    PowerMock.verifyAll();
  }

  @Test
  public void testMultipleTasks() {
    PowerMock.replayAll();
    connector.start(sinkProperties);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(2);

//    System.out.println("TEST Task-1 topics = " +  taskConfigs.get(0).get("topics"));
//    System.out.println("TEST Task-1 tables = " +  taskConfigs.get(0).get("ampool.tables"));
//
//    System.out.println("TEST Task-2 topics = " + taskConfigs.get(1).get("topics"));
//    System.out.println("TEST Task-2 tables = " + taskConfigs.get(1).get("ampool.tables"));

    Assert.assertEquals(2, taskConfigs.size());
    Assert.assertEquals("localhost", taskConfigs.get(0).get("locator.host"));
    Assert.assertEquals("12345", taskConfigs.get(0).get("locator.port"));
    Assert.assertEquals("100", taskConfigs.get(0).get("batch.size"));;
    Assert.assertEquals("topic1, topic2, topic3", taskConfigs.get(0).get("topics"));
    Assert.assertEquals("table1, table2, table3", taskConfigs.get(0).get("ampool.tables"));

    Assert.assertEquals("localhost", taskConfigs.get(1).get("locator.host"));
    Assert.assertEquals("12345", taskConfigs.get(1).get("locator.port"));
    Assert.assertEquals("100", taskConfigs.get(1).get("batch.size"));;
    Assert.assertEquals("topic1, topic2, topic3", taskConfigs.get(1).get("topics"));
    Assert.assertEquals("table1, table2, table3", taskConfigs.get(1).get("ampool.tables"));

    PowerMock.verifyAll();
  }

  @Test
  public void testTaskClass() {
    PowerMock.replayAll();
    connector.start(sinkProperties);
    Assert.assertEquals(AmpoolSinkTask.class, connector.taskClass());
    PowerMock.verifyAll();
  }

  @Test
  public void testSinkTasksWithRowKeyConfig() {
    PowerMock.replayAll();
    sinkProperties.put("ampool.tables.rowkey.column", "topic3:id");
    connector.start(sinkProperties);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
    Assert.assertEquals(1, taskConfigs.size());
    Assert.assertEquals("localhost", taskConfigs.get(0).get("locator.host"));
    Assert.assertEquals("12345", taskConfigs.get(0).get("locator.port"));
    Assert.assertEquals("100", taskConfigs.get(0).get("batch.size"));;
    Assert.assertEquals("topic1, topic2, topic3", taskConfigs.get(0).get("topics"));
    Assert.assertEquals("table1, table2, table3", taskConfigs.get(0).get("ampool.tables"));
    Assert.assertEquals("topic3:id", taskConfigs.get(0).get("ampool.tables.rowkey.column"));

//    System.out.println(taskConfigs.get(0).get("locator.host"));
//    System.out.println(taskConfigs.get(0).get("locator.port"));
//    System.out.println(taskConfigs.get(0).get("batch.size"));
//    System.out.println(taskConfigs.get(0).get("topics"));
//    System.out.println(taskConfigs.get(0).get("ampool.tables"));
//    System.out.println(taskConfigs.get(0).get("ampool.tables.rowkey.column"));
    PowerMock.verifyAll();
  }

}