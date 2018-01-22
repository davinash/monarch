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
package io.ampool.monarch.table.internal;

import static io.ampool.monarch.cli.MashCliStrings.*;
import static io.ampool.monarch.table.internal.TableType.IMMUTABLE;
import static io.ampool.monarch.table.internal.TableType.ORDERED_VERSIONED;
import static io.ampool.monarch.table.internal.TableType.UNORDERED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Stream;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.DescMTableFunction;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.commands.MTableCommands;
import org.apache.geode.management.internal.cli.domain.DiskStoreDetails;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.DescribeDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.DestroyDiskStoreFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.json.GfJsonArray;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MExpirationAction;
import io.ampool.monarch.types.TypeUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class MTableCommandsCreateJUnitTest {
  private static MCache cache;
  private static Method createTable = null;
  private static final MTableCommands COMMANDS = new MTableCommands();
  private static final String TABLE_NAME = "MTableCommandsCreateJUnitTest_CreateTable";
  /** the sequence of arguments for create table command **/
  private static final String[] ARGS_SEQUENCE = new String[] {CREATE_MTABLE__NAME,
      CREATE_MTABLE__TYPE, CREATE_MTABLE_COLUMNS, CREATE_MTABLE_SCHEMA,
      CREATE_MTABLE__REDUNDANT_COPIES, CREATE_MTABLE__RECOVERY_DELAY,
      CREATE_MTABLE__STARTUP_RECOVERY_DELAY, CREATE_MTABLE__MAX_VERSIONS,
      CREATE_MTABLE__DISK_PERSISTENCE, CREATE_MTABLE__DISK_WRITE_POLICY, CREATE_MTABLE__DISKSTORE,
      CREATE_MTABLE__SPLITS, CREATE_MTABLE__EVICTION_POLICY, CREATE_MTABLE__EXPIRATION_TIMEOUT,
      CREATE_MTABLE__EXPIRATION_ACTION, CREATE_MTABLE__BLOCK_SIZE, CREATE_TABLE__LOCAL_MAX_MEMORY,
      CREATE_TABLE__LOCAL_MAX_MEMORY_PCT, CREATE_MTABLE__TIERSTORES, TIER1_TIME_TO_EXPIRE,
      TIER2_TIME_TO_EXPIRE, TIER1_TIME_PARTITION_INTERVAL, TIER2_TIME_PARTITION_INTERVAL,
      CREATE_MTABLE__OBSERVER_COPROCESSORS, CREATE_MTABLE__FTABLE_PARTITION_COLUMN_NAME,
      CREATE_MTABLE__CACHE_LOADER_CLASS, CREATE_MTABLE__FTABLE_BLOCK_FORMAT};

  @BeforeClass
  public static void setUpClass() {
    final Properties props = new Properties();
    props.put("log-level", "none");
    props.put("mcast-port", "0");
    props.put("jmx-manager", "true");
    props.put("jmx-manager-start", "true");
    props.put("jmx-manager-port", "12345");
    cache = new MCacheFactory(props).create();
    cache.setIsServer(true);
    /** keep method reference for easy execution later.. **/
    for (final Method method : MTableCommands.class.getDeclaredMethods()) {
      if ("createMtable".equals(method.getName())) {
        createTable = method;
        break;
      }
    }
    assertNotNull(createTable);
  }

  @AfterClass
  public static void cleanUpClass() {
    COMMANDS.deleteMtable(TABLE_NAME);
    cache.close();
  }

  @After
  public void cleanUpMethod() {
    COMMANDS.deleteMtable(TABLE_NAME);
  }

  /**
   * Helper to execute method and return result.
   *
   * @param map inputs to be passed to the command
   * @return the command result
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   */
  private CommandResult executeCommand(final Map<String, Object> map)
      throws IllegalAccessException, InvocationTargetException {

    Objects.requireNonNull(map);
    Object[] args = Stream.of(ARGS_SEQUENCE).map(type -> {
      Object value = null;
      switch (type) {
        case CREATE_MTABLE__NAME:
          value = TABLE_NAME;
          break;
        case CREATE_MTABLE__TYPE:
          value = map.getOrDefault(type, ORDERED_VERSIONED);
          break;
        case CREATE_MTABLE__DISK_PERSISTENCE:
          value = map.getOrDefault(CREATE_MTABLE__DISK_PERSISTENCE, false);
          break;
        default:
          value = map.get(type);
      }
      return value;
    }).toArray();

    return (CommandResult) createTable.invoke(COMMANDS, args);
  }

  /**
   * Give an error if neither columns or schema options were provided (both null).
   *
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  @Test
  public void testCreateColumnsAndSchema_BothNull()
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    CommandResult result = executeCommand(Collections.emptyMap());
    assertEquals(Result.Status.ERROR, result.getStatus());
    assertTrue(result.getContent().getString("message").contains(CREATE_MTABLE___HELP));

    /** assert that table is not created **/
    CommandResult result1 = (CommandResult) COMMANDS.describeMtable(TABLE_NAME);
    assertEquals(Result.Status.ERROR, result1.getStatus());
  }

  /**
   * Give an error if neither columns or schema options were provided (both empty).
   *
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  @Test
  public void testCreateColumnsAndSchema_BothEmpty()
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    Map<String, Object> map = new HashMap<String, Object>(2) {
      {
        put("schema-json", "");
        put("columns", new String[0]);
      }
    };
    CommandResult result = executeCommand(map);
    assertEquals(Result.Status.ERROR, result.getStatus());
    assertTrue(result.getContent().getString("message").contains(CREATE_MTABLE___HELP));

    /** assert that table is not created **/
    CommandResult result1 = (CommandResult) COMMANDS.describeMtable(TABLE_NAME);
    assertEquals(Result.Status.ERROR, result1.getStatus());
  }

  /**
   * Assert on the error thrown for invalid JSON.
   *
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  @Test
  public void testCreateTableInvalidSchema()
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    Map<String, Object> inputs = new HashMap<String, Object>(1) {
      {
        put("schema-json", "abc");
      }
    };
    CommandResult result = executeCommand(inputs);
    assertEquals(Result.Status.ERROR, result.getStatus());
    assertTrue("Invalid schema.",
        result.getContent().getString("message").contains("Invalid schema"));
  }

  /**
   * Assert that --columns takes precedence over --schema if both are provided.
   *
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  @Test
  public void testCreateTableColumnsAndSchemaTogether()
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    /** offset in the result from where column information starts.. **/
    final int OFFSET = 6;
    final String[][] COLUMNS = new String[][] {{"c1", "INT"}, {"c2", "map<DOUBLE,array<LONG>>"}};
    final String schema = TypeUtils.getJsonSchema(COLUMNS);

    Map<String, Object> inputs = new HashMap<String, Object>(1) {
      {
        put("columns", new String[] {"c1", "c2"});
        put("schema-json", schema);
      }
    };
    CommandResult result = executeCommand(inputs);
    assertEquals(Result.Status.ERROR, result.getStatus());
    assertTrue(result.getContent().getString("message").contains(CREATE_MTABLE___HELP_1));

    /** assert that table is not created **/
    CommandResult result1 = (CommandResult) COMMANDS.describeMtable(TABLE_NAME);
    assertEquals(Result.Status.ERROR, result1.getStatus());
  }

  /**
   * Assert the basic table creation with types..
   *
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  @Test
  public void testCreateTableWithSchema_1()
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    /** offset in the result from where column information starts.. **/
    final int OFFSET = 5;
    final String[][] COLUMNS = new String[][] {{"c1", "INT"}, {"c2", "map<DOUBLE,array<LONG>>"}};
    final String schema = TypeUtils.getJsonSchema(COLUMNS);

    Map<String, Object> inputs = new HashMap<String, Object>(1) {
      {
        put("schema-json", schema);
      }
    };
    CommandResult result = executeCommand(inputs);
    assertEquals(Result.Status.OK, result.getStatus());

    /** assert that table is not created **/
    CommandResult result1 = (CommandResult) COMMANDS.describeMtable(TABLE_NAME);
    assertEquals(Result.Status.OK, result1.getStatus());

    /** assert on columns **/
    GfJsonObject details = result1.getContent().getJSONObject("__sections__-0")
        .getJSONObject("__sections__-0").getJSONObject("__tables__-0").getJSONObject("content");

    final GfJsonArray typeArray = details.getJSONArray("Type");
    int offset = -1;
    for (int i = 0; i < typeArray.size(); i++) {
      if (DescMTableFunction.SCHEMA.equals(typeArray.get(i))) {
        offset = i;
        break;
      }
    }
    assertNotEquals("Table Attributes section must be present in output.", -1, offset);
    final GfJsonArray names = details.getJSONArray("Name");
    final GfJsonArray types = details.getJSONArray("Value");
    for (int i = 0; i < COLUMNS.length; i++) {
      assertEquals("Incorrect column-name.", COLUMNS[i][0], names.get(offset + i));
      assertEquals("Incorrect column-type.", COLUMNS[i][1], types.get(offset + i));
    }
  }

  private static final String SCHEMA =
      TypeUtils.getJsonSchema(new String[][] {{"c1", "INT"}, {"c2", "map<DOUBLE,array<LONG>>"}});

  /**
   * A simple helper method to assert that the table creation is successful with specified options.
   *
   * @param type the table type
   * @param inputs the arguments to be passed to table creation
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  private void assertCreateTableSuccess(final TableType type, final Map<String, Object> inputs)
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    inputs.put(CREATE_MTABLE__TYPE, type);

    CommandResult result = executeCommand(inputs);
    assertEquals("Incorrect status for " + inputs.get(CREATE_MTABLE__TYPE), Result.Status.OK,
        result.getStatus());

    COMMANDS.deleteMtable(TABLE_NAME);
  }

  /**
   * A simple helper to assert that the table creation failed; also assert the error message if
   * provided.
   *
   * @param type the table type
   * @param inputs the arguments to be passed to table creation
   * @param expectedMessage the expected error message
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  private void assertCreateTableError(final TableType type, final Map<String, Object> inputs,
      final String expectedMessage)
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    inputs.put(CREATE_MTABLE__TYPE, type);

    CommandResult result = executeCommand(inputs);
    assertEquals("Incorrect status for " + inputs.get(CREATE_MTABLE__TYPE), Result.Status.ERROR,
        result.getStatus());
    if (expectedMessage != null) {
      assertEquals("Incorrect error message for " + inputs.get(CREATE_MTABLE__TYPE),
          expectedMessage, result.getContent().getJSONArray("message").get(0));
    }
  }

  /**
   * Test option max-versions for different table types.
   *
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  @Test
  public void testMaxVersions()
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    Map<String, Object> inputs = new HashMap<String, Object>(1) {
      {
        put("schema-json", SCHEMA);
        put(CREATE_MTABLE__MAX_VERSIONS, 10);
      }
    };

    assertCreateTableSuccess(ORDERED_VERSIONED, inputs);

    assertCreateTableSuccess(UNORDERED, inputs);

    assertCreateTableError(IMMUTABLE, inputs,
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__MAX_VERSIONS));
  }

  /**
   * Test option disk-persistence with different table types and variations.
   *
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  @Test
  public void testEnableDiskPersistence()
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    Map<String, Object> inputs = new HashMap<String, Object>(1) {
      {
        put("schema-json", SCHEMA);
        put(CREATE_MTABLE__DISK_PERSISTENCE, true);
      }
    };

    assertCreateTableSuccess(ORDERED_VERSIONED, inputs);

    assertCreateTableSuccess(UNORDERED, inputs);

    assertCreateTableError(IMMUTABLE, inputs,
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__DISK_PERSISTENCE));

    /** with disk-persistence=false **/
    inputs.put(CREATE_MTABLE__DISK_PERSISTENCE, false);

    assertCreateTableSuccess(ORDERED_VERSIONED, inputs);

    assertCreateTableSuccess(UNORDERED, inputs);

    assertCreateTableError(IMMUTABLE, inputs,
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__DISK_PERSISTENCE));
  }

  /**
   * Test option eviction-policy with different table types and variations.
   *
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  @Test
  public void testEvictionPolicy()
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    Map<String, Object> inputs = new HashMap<String, Object>(1) {
      {
        put("schema-json", SCHEMA);
        put(CREATE_MTABLE__EVICTION_POLICY, MEvictionPolicy.OVERFLOW_TO_DISK);
      }
    };

    /** with eviction-policy=OVERFLOW_TO_DISK **/
    assertCreateTableSuccess(ORDERED_VERSIONED, inputs);

    assertCreateTableSuccess(UNORDERED, inputs);

    assertCreateTableError(IMMUTABLE, inputs,
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__EVICTION_POLICY));

    /** with eviction-policy=OVERFLOW_TO_TIER **/
    inputs.put(CREATE_MTABLE__EVICTION_POLICY, MEvictionPolicy.OVERFLOW_TO_TIER);
    assertCreateTableError(ORDERED_VERSIONED, inputs, null);

    assertCreateTableError(UNORDERED, inputs, null);

    assertCreateTableError(IMMUTABLE, inputs,
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__EVICTION_POLICY));
  }

  /**
   * Test options expiration-action and expiration-timeout for different table types.
   *
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  @Test
  public void testExpirationActionAndTimeout()
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    Map<String, Object> inputs = new HashMap<String, Object>(2) {
      {
        put("schema-json", SCHEMA);
        put(CREATE_MTABLE__EXPIRATION_ACTION, MExpirationAction.DESTROY);
        put(CREATE_MTABLE__DISK_PERSISTENCE, null);
      }
    };

    /** expiration-action=DESTROY **/
    assertCreateTableSuccess(ORDERED_VERSIONED, inputs);

    assertCreateTableSuccess(UNORDERED, inputs);

    assertCreateTableError(IMMUTABLE, inputs,
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__EXPIRATION_ACTION));

    /** with expiration-timeout=10 **/
    inputs.remove(CREATE_MTABLE__EXPIRATION_ACTION);
    inputs.put(CREATE_MTABLE__EXPIRATION_TIMEOUT, 10);
    assertCreateTableSuccess(ORDERED_VERSIONED, inputs);

    assertCreateTableSuccess(UNORDERED, inputs);

    assertCreateTableError(IMMUTABLE, inputs,
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__EXPIRATION_TIMEOUT));
  }

  /**
   * Test option disk-write-policy for different table types.
   *
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  @Test
  public void testDiskWritePolicy()
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    Map<String, Object> inputs = new HashMap<String, Object>(2) {
      {
        put("schema-json", SCHEMA);
        put(CREATE_MTABLE__DISK_WRITE_POLICY, MDiskWritePolicy.ASYNCHRONOUS);
      }
    };

    /** with disk-write-policy=ASYNCHRONOUS **/
    assertCreateTableSuccess(ORDERED_VERSIONED, inputs);

    assertCreateTableSuccess(UNORDERED, inputs);

    assertCreateTableError(IMMUTABLE, inputs,
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__DISK_WRITE_POLICY));

    /** with disk-write-policy=SYNCHRONOUS **/
    inputs.put(CREATE_MTABLE__DISK_WRITE_POLICY, MDiskWritePolicy.SYNCHRONOUS);
    assertCreateTableSuccess(ORDERED_VERSIONED, inputs);

    assertCreateTableSuccess(UNORDERED, inputs);

    assertCreateTableError(IMMUTABLE, inputs,
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__DISK_WRITE_POLICY));
  }

  /**
   * Test option block-size for different table types.
   *
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  @Test
  public void testBlockSize()
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    Map<String, Object> inputs = new HashMap<String, Object>(2) {
      {
        put("schema-json", SCHEMA);
        put(CREATE_MTABLE__BLOCK_SIZE, 10_000);
        put(CREATE_MTABLE__DISK_PERSISTENCE, null);
      }
    };

    assertCreateTableError(ORDERED_VERSIONED, inputs,
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_FOR, CREATE_MTABLE__BLOCK_SIZE));

    assertCreateTableError(UNORDERED, inputs,
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_FOR, CREATE_MTABLE__BLOCK_SIZE));

    assertCreateTableSuccess(IMMUTABLE, inputs);
  }

  /**
   * Test option block-size for different table types.
   *
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws GfJsonException
   */
  @Test
  public void testDiskStore()
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    final String dsName = "disk_store_abc";
    Map<String, Object> inputs = new HashMap<String, Object>(2) {
      {
        put("schema-json", SCHEMA);
        put(CREATE_MTABLE__DISKSTORE, dsName);
        put(CREATE_MTABLE__DISK_PERSISTENCE, true);
      }
    };

    DistributedMember dm = cache.getDistributedSystem().getDistributedMember();
    DiskStoreAttributes dsa = new DiskStoreAttributes();
    CliUtil.executeFunction(new CreateDiskStoreFunction(), new Object[] {dsName, dsa}, dm);

    inputs.put(CREATE_MTABLE__TYPE, ORDERED_VERSIONED);
    CommandResult result = executeCommand(inputs);
    assertEquals(Result.Status.OK, result.getStatus());
    assertProperty("disk-store-name", dsName);

    inputs.put(CREATE_MTABLE__TYPE, UNORDERED);
    result = executeCommand(inputs);
    assertEquals(Result.Status.OK, result.getStatus());
    assertProperty("disk-store-name", dsName);

    inputs.put(CREATE_MTABLE__TYPE, IMMUTABLE);
    inputs.put(CREATE_MTABLE__DISK_PERSISTENCE, null);
    result = executeCommand(inputs);
    assertEquals(Result.Status.OK, result.getStatus());
    assertProperty("disk-store-name", dsName);

    CliUtil.executeFunction(new DestroyDiskStoreFunction(), new Object[] {dsName}, dm);
  }


  /**
   * Test for disk store attributes
   */
  @Test
  public void testDiskStoreWithDiskAttributes() {
    final String dsName = "disk_store_abc";
    DistributedMember dm = cache.getDistributedSystem().getDistributedMember();
    DiskStoreAttributes dsa = new DiskStoreAttributes();
    dsa.enableDeltaPersistence = true;
    dsa.queueSize = 99;
    dsa.autoCompact = false;
    ResultCollector<?, ?> resultCollector =
        CliUtil.executeFunction(new CreateDiskStoreFunction(), new Object[] {dsName, dsa}, dm);
    ArrayList resultList = ((ArrayList) resultCollector.getResult());
    XmlEntity xmlEntity = ((CliFunctionResult) resultList.get(0)).getXmlEntity();
    System.out.println(xmlEntity.getXmlDefinition());
    assertTrue(xmlEntity.getXmlDefinition().contains("enable-delta-persistence=\"true\""));
    assertTrue(xmlEntity.getXmlDefinition().contains("queue-size=\"99\""));

    resultCollector = CliUtil.executeFunction(new DescribeDiskStoreFunction(), dsName, dm);
    DiskStoreDetails diskStoreDetails =
        (DiskStoreDetails) ((ArrayList) resultCollector.getResult()).get(0);
    assertTrue(diskStoreDetails.getEnableDeltaPersistence());
  }

  /**
   * Test for local-max-memory and total-max-memory
   */
  @Test
  public void testTotalAndLocalMaxMemory()
      throws InvocationTargetException, IllegalAccessException, GfJsonException {
    TableType[] tableTypesEnum = {ORDERED_VERSIONED, UNORDERED, IMMUTABLE};
    for (int i = 0; i < tableTypesEnum.length; i++) {
      /* Set local max memory */
      /* Set local max memory */
      Map<String, Object> inputs = new HashMap<String, Object>(2) {
        {
          put("schema-json", SCHEMA);
          put(CREATE_MTABLE__DISK_PERSISTENCE, null);
          put(CREATE_TABLE__LOCAL_MAX_MEMORY, 53);
        }
      };
      inputs.put(CREATE_MTABLE__TYPE, tableTypesEnum[i]);
      CommandResult result = executeCommand(inputs);
      assertEquals(Result.Status.OK, result.getStatus());
      assertProperty("local-max-memory", "53");
      /* Set total max memory */
      inputs.remove(CREATE_TABLE__LOCAL_MAX_MEMORY);
      // REFER TO GEN-1625 why this is commented ( Avinash )
      // inputs.put(CREATE_TABLE__TOTAL_MAX_MEMORY, 103l);
      // result = executeCommand(inputs);
      // assertEquals(Result.Status.OK, result.getStatus());
      // assertProperty("total-max-memory", "103");
      /* Set total and local max memory */
      inputs.put(CREATE_TABLE__LOCAL_MAX_MEMORY, 56);
      result = executeCommand(inputs);
      assertEquals(Result.Status.OK, result.getStatus());
      // assertProperty("total-max-memory", "103", false);
      assertProperty("local-max-memory", "56");
    }
  }

  /**
   * Assert on the value of the specified property via "describe table" command.
   *
   * @param name the property name
   * @param expectedValue expected value for the specified property
   * @throws GfJsonException
   */
  private void assertProperty(final String name, final String expectedValue)
      throws GfJsonException {
    assertProperty(name, expectedValue, true);
  }

  private void assertProperty(final String name, final String expectedValue, boolean deleteTable)
      throws GfJsonException {
    CommandResult result = (CommandResult) COMMANDS.describeMtable(TABLE_NAME);
    assertEquals(Result.Status.OK, result.getStatus());

    if (deleteTable) {
      COMMANDS.deleteMtable(TABLE_NAME);
    }

    GfJsonObject content = result.getContent().getJSONObject("__sections__-0")
        .getJSONObject("__sections__-0").getJSONObject("__tables__-0").getJSONObject("content");
    GfJsonArray names = content.getJSONArray("Name");
    GfJsonArray values = content.getJSONArray("Value");

    String actualName = null;
    String actualValue = null;
    for (int i = 0; i < names.size(); i++) {
      if (names.get(i).toString().equals(name)) {
        actualName = names.get(i).toString();
        actualValue = values.get(i).toString();
        break;
      }
    }
    assertEquals("Incorrect value for property: " + actualName, expectedValue, actualValue);
  }
}
