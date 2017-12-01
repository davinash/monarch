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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import io.ampool.monarch.cli.MashCliStrings;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.types.TypeHelper;
import io.ampool.monarch.types.TypeUtils;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.MTableCommands;
import org.apache.geode.management.internal.cli.commands.MTableDataCommands;
import org.apache.geode.management.internal.cli.json.GfJsonArray;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class MTableDataCommandsPutJUnitTest {
  private static MCache cache;
  private static final MTableCommands COMMANDS = new MTableCommands();
  private static final MTableDataCommands CMD_CTX = new MTableDataCommands();
  private static final String TABLE_NAME = "MTableDataCommandsPutJUnitTest_PutTable";

  @BeforeClass
  public static void setUpClass() {
    final Properties props = new Properties();
    props.put("log-level", "none");
    props.put("mcast-port", "0");
    cache = new MCacheFactory(props).create();
  }

  @AfterClass
  public static void cleanUpClass() {
    COMMANDS.deleteMtable(TABLE_NAME);
    cache.close();
  }

  @After
  public void cleanUpMethod() {}

  /** helpers for creating and deleting tables **/
  private static void createTable(final String name, final String schema, final TableType type) {
    Integer maxVersions = null;
    if (type == TableType.ORDERED_VERSIONED) {
      maxVersions = 1;
    }
    Result result = COMMANDS.createMtable(name, type, null, schema, 1, null, null, maxVersions,
        false, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null);
    assertEquals(Result.Status.OK, result.getStatus());
  }

  private static void deleteTable(final String name) {
    Result result = COMMANDS.deleteMtable(name);
    assertEquals(Result.Status.OK, result.getStatus());
  }

  /**
   * Create table and then PUT and assert on the data using GET.
   *
   * @param COLUMNS the schema
   * @param VALUES the values to put
   * @throws GfJsonException
   */
  private void createPutAndAssertData(final String[][] COLUMNS, final Object[][] VALUES)
      throws GfJsonException {
    createPutAndAssertData(COLUMNS, VALUES, null, null, true);
  }

  /**
   *
   * @param COLUMNS : the schema
   * @param VALUES the values to put
   * @param useJson use Json or simple value
   * @throws GfJsonException
   */
  private void createPutAndAssertData(final String[][] COLUMNS, final Object[][] VALUES,
      final Object[][] EXP_VALUES, Result.Status[] results, boolean useJson)
      throws GfJsonException {
    /** create and put data with JSON value **/
    createTable(TABLE_NAME, TypeUtils.getJsonSchema(COLUMNS), TableType.UNORDERED);

    Map<String, Object> valueMap = new LinkedHashMap<>(5);
    int counter = 0;
    Result result = null;
    int i = 0;
    for (i = 0; i < VALUES.length; i++) {
      Object[] values = VALUES[i];
      if (useJson) {
        for (int j = 0; j < values.length; j++) {
          valueMap.put(COLUMNS[j][0], values[j]);
        }
        StringBuilder sb = new StringBuilder(100);
        TypeHelper.deepToString(valueMap, sb, true);
        result = CMD_CTX.put("key_" + counter++, null, sb.toString(), 0, TABLE_NAME);
      } else {
        StringBuilder sb = new StringBuilder(100);
        String delim = "";
        for (int j = 0; j < values.length; j++) {
          sb.append(delim + COLUMNS[j][0] + "=" + values[j]);
          delim = ",";
        }
        result = CMD_CTX.put("key_" + counter++, sb.toString(), null, 0, TABLE_NAME);
      }
      assertNotNull(result);
      if (results != null) {
        assertEquals(results[i], result.getStatus());
        if (results[i] == Result.Status.ERROR) {
          GfJsonArray resultArr = ((CommandResult) result).getContent().getJSONArray("message");
          assertEquals(0, resultArr.get(0).toString().compareTo(
              "Failed to execute mash command.Error processing Row: Invalid data for column=ID, type=LONG: For input string: \"ABC\""));
        }
      } else {
        assertEquals(Result.Status.OK, result.getStatus());
      }
    }

    /** assert the values via GET **/
    for (i = 0; i < counter; i++) {
      final CommandResult result1 = (CommandResult) CMD_CTX.get("key_" + i, TABLE_NAME, 0);
      if (results == null) {
        assertEquals(Result.Status.OK, result1.getStatus());
      } else {
        assertEquals(results[i], result1.getStatus());
      }

      /** assert on values **/
      if (results == null || (results != null && results[i] == Result.Status.OK)) {
        GfJsonObject resultMap = result1.getContent().getJSONObject("__sections__-0");
        for (int j = 0; j < COLUMNS.length; j++) {
          if (EXP_VALUES != null) {
            assertEquals("Incorrect value for: " + COLUMNS[j][0], String.valueOf(EXP_VALUES[i][j]),
                resultMap.getString(COLUMNS[j][0]));
          } else {
            assertEquals("Incorrect value for: " + COLUMNS[j][0], String.valueOf(VALUES[i][j]),
                resultMap.getString(COLUMNS[j][0]));
          }
        }
      }
    }
  }

  /***********************************************/

  @Test
  public void testPutValueAndJson_BothNull() {
    final Result result = CMD_CTX.put("key", null, null, 0, TABLE_NAME);
    assertEquals(Result.Status.OK, result.getStatus());
    assertTrue(result.toString().contains(MashCliStrings.MPUT__MSG___VALUE_EMPTY));
  }

  @Test
  public void testPutValueAndJson_BothEmpty() {
    final Result result = CMD_CTX.put("key", "", "", 0, TABLE_NAME);
    assertEquals(Result.Status.OK, result.getStatus());
    assertTrue(result.toString().contains(MashCliStrings.MPUT__MSG___VALUE_EMPTY));
  }

  /**
   * Test both --value and --value-json together.
   */
  @Test
  public void testPutValueAndJsonTogether() {
    final Result result = CMD_CTX.put("key", "abc", "pqr", 0, TABLE_NAME);
    assertEquals(Result.Status.OK, result.getStatus());
    assertTrue(result.toString().contains(MashCliStrings.MPUT__VALUE_JSON__HELP_1));
  }

  /**
   * Simple test with two columns
   * 
   * @throws GfJsonException
   */
  @Test
  public void testPutWithJson_1() throws GfJsonException {
    final String schema = "{'c1':'INT','c2':'array<DOUBLE>'}";
    final String value = "{'c1':'1', 'c2':['11', '12', '13']}";
    final String key = "key_1";

    /** create and put data with JSON value **/
    createTable(TABLE_NAME, schema, TableType.ORDERED_VERSIONED);
    final Result result = CMD_CTX.put(key, null, value, 0, TABLE_NAME);
    assertEquals(Result.Status.OK, result.getStatus());

    /** assert on the data that was PUT via GET **/
    final CommandResult result1 = (CommandResult) CMD_CTX.get("key_1", TABLE_NAME, 0);
    assertEquals(Result.Status.OK, result1.getStatus());
    GfJsonObject resultMap = result1.getContent().getJSONObject("__sections__-0");
    assertEquals("Incorrect key.", key, resultMap.getString("KEY"));
    assertEquals("Incorrect value for: c1", "1", resultMap.getString("c1"));
    assertEquals("Incorrect value for: c2", "[11.0, 12.0, 13.0]", resultMap.getString("c2"));
    deleteTable(TABLE_NAME);
  }

  @Test
  public void testPutWithJson_3() throws GfJsonException {
    final String schema = "{'c1':'INT','c2':'BINARY'}";
    final String value = "{'c1':'1', 'c2':['11', '12', '13']}";
    final String key = "key_1";

    /** create and put data with JSON value **/
    createTable(TABLE_NAME, schema, TableType.ORDERED_VERSIONED);
    final Result result = CMD_CTX.put(key, null, value, 0, TABLE_NAME);
    assertEquals(Result.Status.OK, result.getStatus());

    /** assert on the data that was PUT via GET **/
    final CommandResult result1 = (CommandResult) CMD_CTX.get("key_1", TABLE_NAME, 0);
    assertEquals(Result.Status.OK, result1.getStatus());
    GfJsonObject resultMap = result1.getContent().getJSONObject("__sections__-0");
    assertEquals("Incorrect key.", key, resultMap.getString("KEY"));
    assertEquals("Incorrect value for: c1", "1", resultMap.getString("c1"));
    assertEquals("Incorrect value for: c2", "[11, 12, 13]", resultMap.getString("c2"));
    deleteTable(TABLE_NAME);
  }

  /**
   * Another test multiple columns with different types.
   *
   * @throws GfJsonException
   */
  @Test
  public void testPutWithJson_2() throws GfJsonException {
    final String[][] COLUMNS =
        {{"ID", "LONG"}, {"NAME", "STRING"}, {"AGE", "INT"}, {"SEX", "CHAR"}, {"SALARY", "DOUBLE"}};
    final Object[][] VALUES = {{11L, "ABC", 1, 'M', 11.111D}, {22L, "PQR", 2, 'F', 22.222D},};

    createPutAndAssertData(COLUMNS, VALUES);
    deleteTable(TABLE_NAME);
  }

  /**
   * Test for validating successful partial puts (subset of columns).
   *
   * @throws GfJsonException
   */
  @Test
  public void testPutWithJson_Partial() throws GfJsonException {
    final String[][] COLUMNS =
        {{"ID", "LONG"}, {"NAME", "STRING"}, {"AGE", "INT"}, {"SEX", "CHAR"}, {"SALARY", "DOUBLE"}};
    final Object[][] VALUES = {{11L, "ABC", 1, 'M', 11.111D}, {22L, "PQR", 2, 'F', 22.222D},};
    createPutAndAssertData(COLUMNS, VALUES);

    /** modify the data using partial put **/
    final String json = "{'ID':'1111', 'SALARY':'1111.2222'}";
    Result result = CMD_CTX.put("key_0", null, json, 0, TABLE_NAME);
    assertEquals("Partial put failed.", Result.Status.OK, result.getStatus());

    /** verify via GET.. the modified and un-modified columns **/
    final CommandResult result1 = (CommandResult) CMD_CTX.get("key_0", TABLE_NAME, 0);
    assertEquals(Result.Status.OK, result1.getStatus());
    GfJsonObject resultMap = result1.getContent().getJSONObject("__sections__-0");
    assertEquals("Incorrect key.", "key_0", resultMap.getString("KEY"));
    assertEquals("Incorrect value for: AGE", "1", resultMap.getString("AGE"));
    assertEquals("Incorrect value for: ID", "1111", resultMap.getString("ID"));
    assertEquals("Incorrect value for: SALARY", "1111.2222", resultMap.getString("SALARY"));

    deleteTable(TABLE_NAME);
  }

  /**
   * Simple test for JSON value with invalid data for some column values.
   */
  @Test
  public void testPutWithJson_InvalidData() {
    final String[][] COLUMNS = {{"C1", "LONG"}, {"C2", "array<INT>"},};
    createTable(TABLE_NAME, TypeUtils.getJsonSchema(COLUMNS), TableType.ORDERED_VERSIONED);

    Result result;

    result = CMD_CTX.put("key_0", null, "{'C1':'abc'}", 0, TABLE_NAME);
    assertEquals(Result.Status.ERROR, result.getStatus());
    assertTrue("Invalid exception while processing data.",
        result.toString().contains("Error processing Row"));

    result = CMD_CTX.put("key_0", null, "{'C2':'abc'}", 0, TABLE_NAME);
    assertEquals(Result.Status.ERROR, result.getStatus());
    assertTrue("Invalid exception while processing data.",
        result.toString().contains("Error processing Row"));
    deleteTable(TABLE_NAME);
  }

  /** tests for simpler --value options **/
  @Test
  public void testPutWithValue() throws GfJsonException {
    final String[][] COLUMNS =
        {{"ID", "LONG"}, {"NAME", "STRING"}, {"AGE", "INT"}, {"SEX", "CHAR"}, {"SALARY", "DOUBLE"}};
    final Object[][] VALUES = {{11L, "ABC", 1, 'M', 11.111D}, {22L, "PQR", 2, 'F', 22.222D},
        {"ABC", "PQR", 2, 'F', 22.222D}, /* INVALID */
    };
    final Result.Status[] results = {Result.Status.OK, Result.Status.OK, Result.Status.ERROR};
    createPutAndAssertData(COLUMNS, VALUES, null, results, false);
    /** TODO: add handling.. **/
    deleteTable(TABLE_NAME);
  }

  @Test
  public void testPutWithValue1() throws GfJsonException {
    final String[][] COLUMNS = {{"ID", "BINARY"}, {"NAME", "BINARY"}, {"AGE", "BINARY"},
        {"SEX", "BINARY"}, {"SALARY", "BINARY"}};
    final Object[][] VALUES = {{11L, "ABC", 1, 'M', 11.111D}, {22L, "PQR", 2, 'F', 22.222D},
        {"ABC", "PQR", 2, 'F', 22.222D}, /* INVALID */
    };
    final Object[][] EXP_VALUES = {
        {"[49, 49]", "[65, 66, 67]", "[49]", "[77]", "[49, 49, 46, 49, 49, 49]"},
        {"[50, 50]", "[80, 81, 82]", "[50]", "[70]", "[50, 50, 46, 50, 50, 50]"},
        {"[65, 66, 67]", "[80, 81, 82]", "[50]", "[70]", "[50, 50, 46, 50, 50, 50]"}, /* INVALID */
    };

    final Result.Status[] results = {Result.Status.OK, Result.Status.OK, Result.Status.OK};
    createPutAndAssertData(COLUMNS, VALUES, EXP_VALUES, results, false);
    /** TODO: add handling.. **/
    deleteTable(TABLE_NAME);
  }
}
