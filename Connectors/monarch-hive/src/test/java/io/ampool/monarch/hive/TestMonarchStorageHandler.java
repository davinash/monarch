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
package io.ampool.monarch.hive;

import io.ampool.monarch.TestBase;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.ftable.FTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.ampool.monarch.hive.MonarchUtils.ENABLE_KERBEROS_AUTHC;
import static io.ampool.monarch.hive.MonarchUtils.KERBEROS_SERVICE_PRINCIPAL;
import static io.ampool.monarch.hive.MonarchUtils.KERBEROS_USER_KEYTAB;
import static io.ampool.monarch.hive.MonarchUtils.KERBEROS_USER_PRINCIPAL;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestMonarchStorageHandler extends TestBase {
    protected MonarchStorageHandler storageHandler = new MonarchStorageHandler();

    @BeforeClass
    public void setUp() {
    }

    @AfterClass
    public void cleanUp() {
    }

    @Test
    public void testTablePropertiesPassedToOutputJobProperties() {
        TableDesc tableDesc = mock(TableDesc.class);
        Properties props = new Properties();
        Map<String,String> jobProperties = new HashMap<String,String>();

        props.setProperty(MonarchUtils.LOCATOR_HOST, "localhost");
        props.setProperty(MonarchUtils.LOCATOR_PORT, "10334");
        props.setProperty(MonarchUtils.REGION, "testTablePropertiesPassedToOutputJobProperties");
        props.setProperty(MonarchUtils.REDUNDANCY, "2");
        props.setProperty(MonarchUtils.BUCKETS, "57");
        props.setProperty(MonarchUtils.PERSIST, "async");
        props.setProperty(MonarchUtils.MONARCH_TABLE_TYPE, MonarchUtils.TABLE_TYPE_UNORDERED);
        props.setProperty(MonarchUtils.READ_TIMEOUT, "1000");
        props.setProperty(ENABLE_KERBEROS_AUTHC, "true");
        props.setProperty(KERBEROS_SERVICE_PRINCIPAL, "ampoolservice");
        props.setProperty(KERBEROS_USER_PRINCIPAL, "ampoolclient");
        props.setProperty(KERBEROS_USER_KEYTAB, "ampoolclient.keytab");

        Mockito.when(tableDesc.getProperties()).thenReturn(props);

        storageHandler.configureOutputJobProperties(tableDesc, jobProperties);

        assertEquals(jobProperties.size(), 15);
        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.LOCATOR_HOST));
        assertEquals(props.getProperty(MonarchUtils.LOCATOR_HOST),
                jobProperties.get(MonarchUtils.LOCATOR_HOST));

        Assert.assertTrue(
                jobProperties.containsKey(MonarchUtils.LOCATOR_PORT));
        assertEquals(props.getProperty(MonarchUtils.LOCATOR_PORT),
                jobProperties.get(MonarchUtils.LOCATOR_PORT));

        Assert.assertTrue(
                jobProperties.containsKey(MonarchUtils.REGION));
        assertEquals(props.getProperty(MonarchUtils.REGION),
                jobProperties.get(MonarchUtils.REGION));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.REDUNDANCY));
        assertEquals(props.getProperty(MonarchUtils.REDUNDANCY),
          jobProperties.get(MonarchUtils.REDUNDANCY));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.PERSIST));
        assertEquals(props.getProperty(MonarchUtils.PERSIST),
          jobProperties.get(MonarchUtils.PERSIST));
        assertEquals(jobProperties.get(MonarchUtils.READ_TIMEOUT),
          props.getProperty(MonarchUtils.READ_TIMEOUT));

        assertEquals(jobProperties.get(ENABLE_KERBEROS_AUTHC),
            props.getProperty(ENABLE_KERBEROS_AUTHC));
        assertEquals(jobProperties.get(KERBEROS_SERVICE_PRINCIPAL),
            props.getProperty(KERBEROS_SERVICE_PRINCIPAL));
        assertEquals(jobProperties.get(KERBEROS_USER_PRINCIPAL),
            props.getProperty(KERBEROS_USER_PRINCIPAL));
        assertEquals(jobProperties.get(KERBEROS_USER_KEYTAB),
            props.getProperty(KERBEROS_USER_KEYTAB));

    }

    @Test
    public void testTablePropertiesPassedToOutputJobProperties_Ordered() {
        TableDesc tableDesc = mock(TableDesc.class);
        Properties props = new Properties();
        Map<String,String> jobProperties = new HashMap<String,String>();

        props.setProperty(MonarchUtils.LOCATOR_HOST, "localhost");
        props.setProperty(MonarchUtils.LOCATOR_PORT, "10334");
        props.setProperty(MonarchUtils.REGION, "testTablePropertiesPassedToOutputJobProperties");
        props.setProperty(MonarchUtils.REDUNDANCY, "2");
        props.setProperty(MonarchUtils.BUCKETS, "57");
        props.setProperty(MonarchUtils.PERSIST, "async");
        props.setProperty(MonarchUtils.MONARCH_TABLE_TYPE, MonarchUtils.TABLE_TYPE_ORDERED);
        props.setProperty(MonarchUtils.READ_TIMEOUT, "1000");
        props.setProperty(MonarchUtils.MAX_VERSIONS, "999");
        props.setProperty(MonarchUtils.KEY_RANGE_START, "a");
        props.setProperty(MonarchUtils.KEY_RANGE_STOP, "zzzzzzzzzzzzzz");
        props.setProperty(ENABLE_KERBEROS_AUTHC, "true");
        props.setProperty(KERBEROS_SERVICE_PRINCIPAL, "ampoolservice");
        props.setProperty(KERBEROS_USER_PRINCIPAL, "ampoolclient");
        props.setProperty(KERBEROS_USER_KEYTAB, "ampoolclient.keytab");

        Mockito.when(tableDesc.getProperties()).thenReturn(props);

        storageHandler.configureOutputJobProperties(tableDesc, jobProperties);

        assertEquals(jobProperties.size(), 17);
        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.LOCATOR_HOST));
        assertEquals(props.getProperty(MonarchUtils.LOCATOR_HOST),
                jobProperties.get(MonarchUtils.LOCATOR_HOST));

        Assert.assertTrue(
                jobProperties.containsKey(MonarchUtils.LOCATOR_PORT));
        assertEquals(props.getProperty(MonarchUtils.LOCATOR_PORT),
                jobProperties.get(MonarchUtils.LOCATOR_PORT));

        Assert.assertTrue(
                jobProperties.containsKey(MonarchUtils.REGION));
        assertEquals(props.getProperty(MonarchUtils.REGION),
                jobProperties.get(MonarchUtils.REGION));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.REDUNDANCY));
        assertEquals(props.getProperty(MonarchUtils.REDUNDANCY),
          jobProperties.get(MonarchUtils.REDUNDANCY));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.PERSIST));
        assertEquals(props.getProperty(MonarchUtils.PERSIST),
          jobProperties.get(MonarchUtils.PERSIST));
        assertEquals(jobProperties.get(MonarchUtils.READ_TIMEOUT),
          props.getProperty(MonarchUtils.READ_TIMEOUT));
        assertEquals(jobProperties.get(MonarchUtils.MAX_VERSIONS),
          props.getProperty(MonarchUtils.MAX_VERSIONS));
        assertEquals(jobProperties.get(MonarchUtils.KEY_RANGE_START),
          props.getProperty(MonarchUtils.KEY_RANGE_START));
        assertEquals(jobProperties.get(MonarchUtils.KEY_RANGE_STOP),
          props.getProperty(MonarchUtils.KEY_RANGE_STOP));

        assertEquals(jobProperties.get(ENABLE_KERBEROS_AUTHC),
            props.getProperty(ENABLE_KERBEROS_AUTHC));
        assertEquals(jobProperties.get(KERBEROS_SERVICE_PRINCIPAL),
            props.getProperty(KERBEROS_SERVICE_PRINCIPAL));
        assertEquals(jobProperties.get(KERBEROS_USER_PRINCIPAL),
            props.getProperty(KERBEROS_USER_PRINCIPAL));
        assertEquals(jobProperties.get(KERBEROS_USER_KEYTAB),
            props.getProperty(KERBEROS_USER_KEYTAB));
    }

    @Test
    public void testTablePropertiesPassedToOutputJobPropertiesNoSecurity() {
        TableDesc tableDesc = mock(TableDesc.class);
        Properties props = new Properties();
        Map<String,String> jobProperties = new HashMap<String,String>();

        props.setProperty(MonarchUtils.LOCATOR_HOST, "localhost");
        props.setProperty(MonarchUtils.LOCATOR_PORT, "10334");
        props.setProperty(MonarchUtils.REGION, "testTablePropertiesPassedToOutputJobProperties");
        props.setProperty(MonarchUtils.REDUNDANCY, "2");
        props.setProperty(MonarchUtils.BUCKETS, "57");
        props.setProperty(MonarchUtils.PERSIST, "async");
        props.setProperty(MonarchUtils.MONARCH_TABLE_TYPE, MonarchUtils.TABLE_TYPE_UNORDERED);
        props.setProperty(MonarchUtils.READ_TIMEOUT, "1000");
        props.setProperty(MonarchUtils.MAX_VERSIONS, "999");
        props.setProperty(ENABLE_KERBEROS_AUTHC, "false");
        props.setProperty(KERBEROS_SERVICE_PRINCIPAL, "ampoolservice");
        props.setProperty(KERBEROS_USER_PRINCIPAL, "ampoolclient");
        props.setProperty(KERBEROS_USER_KEYTAB, "ampoolclient.keytab");

        Mockito.when(tableDesc.getProperties()).thenReturn(props);

        storageHandler.configureOutputJobProperties(tableDesc, jobProperties);

        assertEquals(jobProperties.size(), 11);
        Assert.assertTrue(
            jobProperties.containsKey(MonarchUtils.LOCATOR_HOST));
        assertEquals(props.getProperty(MonarchUtils.LOCATOR_HOST),
            jobProperties.get(MonarchUtils.LOCATOR_HOST));

        Assert.assertTrue(
            jobProperties.containsKey(MonarchUtils.LOCATOR_PORT));
        assertEquals(props.getProperty(MonarchUtils.LOCATOR_PORT),
            jobProperties.get(MonarchUtils.LOCATOR_PORT));

        Assert.assertTrue(
            jobProperties.containsKey(MonarchUtils.REGION));
        assertEquals(props.getProperty(MonarchUtils.REGION),
            jobProperties.get(MonarchUtils.REGION));

        Assert.assertTrue(
            jobProperties.containsKey(MonarchUtils.REDUNDANCY));
        assertEquals(props.getProperty(MonarchUtils.REDUNDANCY),
            jobProperties.get(MonarchUtils.REDUNDANCY));

        Assert.assertTrue(
            jobProperties.containsKey(MonarchUtils.PERSIST));
        assertEquals(props.getProperty(MonarchUtils.PERSIST),
            jobProperties.get(MonarchUtils.PERSIST));
        assertEquals(jobProperties.get(MonarchUtils.READ_TIMEOUT),
            props.getProperty(MonarchUtils.READ_TIMEOUT));
        assertEquals(jobProperties.get(MonarchUtils.MAX_VERSIONS),
            props.getProperty(MonarchUtils.MAX_VERSIONS));

        assertFalse(jobProperties.containsKey(ENABLE_KERBEROS_AUTHC));
        assertFalse(jobProperties.containsKey(KERBEROS_SERVICE_PRINCIPAL));
        assertFalse(jobProperties.containsKey(KERBEROS_USER_PRINCIPAL));
        assertFalse(jobProperties.containsKey(KERBEROS_USER_KEYTAB));

    }

    @Test
    public void testTablePropertiesPassedToOutputJobPropertiesFTable() {
        TableDesc tableDesc = mock(TableDesc.class);
        Properties props = new Properties();
        Map<String,String> jobProperties = new HashMap<String,String>();

        props.setProperty(MonarchUtils.LOCATOR_HOST, "localhost");
        props.setProperty(MonarchUtils.LOCATOR_PORT, "10334");
        props.setProperty(MonarchUtils.REGION, "testTablePropertiesPassedToOutputJobPropertiesFTable");
        props.setProperty(MonarchUtils.REDUNDANCY, "2");
        props.setProperty(MonarchUtils.BUCKETS, "57");
        props.setProperty(MonarchUtils.MONARCH_PARTITIONING_COLUMN, "C1");
        props.setProperty(ENABLE_KERBEROS_AUTHC, "true");
        props.setProperty(KERBEROS_SERVICE_PRINCIPAL, "ampoolservice");
        props.setProperty(KERBEROS_USER_PRINCIPAL, "ampoolclient");
        props.setProperty(KERBEROS_USER_KEYTAB, "ampoolclient.keytab");

        Mockito.when(tableDesc.getProperties()).thenReturn(props);

        storageHandler.configureOutputJobProperties(tableDesc, jobProperties);

        assertEquals(jobProperties.size(), 15);
        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.LOCATOR_HOST));
        assertEquals(props.getProperty(MonarchUtils.LOCATOR_HOST),
          jobProperties.get(MonarchUtils.LOCATOR_HOST));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.LOCATOR_PORT));
        assertEquals(props.getProperty(MonarchUtils.LOCATOR_PORT),
          jobProperties.get(MonarchUtils.LOCATOR_PORT));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.REGION));
        assertEquals(props.getProperty(MonarchUtils.REGION),
          jobProperties.get(MonarchUtils.REGION));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.REDUNDANCY));
        assertEquals(props.getProperty(MonarchUtils.REDUNDANCY),
          jobProperties.get(MonarchUtils.REDUNDANCY));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.MONARCH_PARTITIONING_COLUMN));
        assertEquals(props.getProperty(MonarchUtils.MONARCH_PARTITIONING_COLUMN),
          jobProperties.get(MonarchUtils.MONARCH_PARTITIONING_COLUMN));
        assertEquals(jobProperties.get(MonarchUtils.READ_TIMEOUT),
          MonarchUtils.READ_TIMEOUT_DEFAULT);
        assertEquals(jobProperties.get(MonarchUtils.MAX_VERSIONS),
          MonarchUtils.MAX_VERSIONS_DEFAULT);

        assertEquals(jobProperties.get(ENABLE_KERBEROS_AUTHC),
            props.getProperty(ENABLE_KERBEROS_AUTHC));
        assertEquals(jobProperties.get(KERBEROS_SERVICE_PRINCIPAL),
            props.getProperty(KERBEROS_SERVICE_PRINCIPAL));
        assertEquals(jobProperties.get(KERBEROS_USER_PRINCIPAL),
            props.getProperty(KERBEROS_USER_PRINCIPAL));
        assertEquals(jobProperties.get(KERBEROS_USER_KEYTAB),
            props.getProperty(KERBEROS_USER_KEYTAB));
    }

    @Test
    public void testTablePropertiesPassedToInputJobProperties() {
        TableDesc tableDesc = mock(TableDesc.class);
        Properties props = new Properties();
        Map<String,String> jobProperties = new HashMap<String,String>();

        props.setProperty(MonarchUtils.LOCATOR_HOST, "localhost");
        props.setProperty(MonarchUtils.LOCATOR_PORT, "10334");
        props.setProperty(MonarchUtils.REGION, "testTablePropertiesPassedToInputJobProperties");
        props.setProperty(MonarchUtils.REDUNDANCY, "2");
        props.setProperty(MonarchUtils.BUCKETS, "57");
        props.setProperty(MonarchUtils.PERSIST, "async");
        props.setProperty(MonarchUtils.MONARCH_TABLE_TYPE, MonarchUtils.TABLE_TYPE_UNORDERED);
        props.setProperty(MonarchUtils.READ_TIMEOUT, "99999");
        props.setProperty(MonarchUtils.MAX_VERSIONS, "99");
        props.setProperty(ENABLE_KERBEROS_AUTHC, "true");
        props.setProperty(KERBEROS_SERVICE_PRINCIPAL, "ampoolservice");
        props.setProperty(KERBEROS_USER_PRINCIPAL, "ampoolclient");
        props.setProperty(KERBEROS_USER_KEYTAB, "ampoolclient.keytab");

        Mockito.when(tableDesc.getProperties()).thenReturn(props);

        storageHandler.configureInputJobProperties(tableDesc, jobProperties);

        assertEquals(jobProperties.size(), 15);
        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.LOCATOR_HOST));
        assertEquals(props.getProperty(MonarchUtils.LOCATOR_HOST),
                jobProperties.get(MonarchUtils.LOCATOR_HOST));

        Assert.assertTrue(
                jobProperties.containsKey(MonarchUtils.LOCATOR_PORT));
        assertEquals(props.getProperty(MonarchUtils.LOCATOR_PORT),
                jobProperties.get(MonarchUtils.LOCATOR_PORT));

        Assert.assertTrue(
                jobProperties.containsKey(MonarchUtils.REGION));
        assertEquals(props.getProperty(MonarchUtils.REGION),
                jobProperties.get(MonarchUtils.REGION));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.REDUNDANCY));
        assertEquals(props.getProperty(MonarchUtils.REDUNDANCY),
          jobProperties.get(MonarchUtils.REDUNDANCY));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.PERSIST));
        assertEquals(props.getProperty(MonarchUtils.PERSIST),
          jobProperties.get(MonarchUtils.PERSIST));
        assertEquals(jobProperties.get(MonarchUtils.READ_TIMEOUT),
          props.getProperty(MonarchUtils.READ_TIMEOUT));
        assertEquals(jobProperties.get(MonarchUtils.MAX_VERSIONS),
          props.getProperty(MonarchUtils.MAX_VERSIONS));

        assertEquals(jobProperties.get(ENABLE_KERBEROS_AUTHC),
            props.getProperty(ENABLE_KERBEROS_AUTHC));
        assertEquals(jobProperties.get(KERBEROS_SERVICE_PRINCIPAL),
            props.getProperty(KERBEROS_SERVICE_PRINCIPAL));
        assertEquals(jobProperties.get(KERBEROS_USER_PRINCIPAL),
            props.getProperty(KERBEROS_USER_PRINCIPAL));
        assertEquals(jobProperties.get(KERBEROS_USER_KEYTAB),
            props.getProperty(KERBEROS_USER_KEYTAB));
    }

    @Test
    public void testTablePropertiesPassedToInputJobPropertiesFTable() {
        TableDesc tableDesc = mock(TableDesc.class);
        Properties props = new Properties();
        Map<String,String> jobProperties = new HashMap<String,String>();

        props.setProperty(MonarchUtils.LOCATOR_HOST, "localhost");
        props.setProperty(MonarchUtils.LOCATOR_PORT, "10334");
        props.setProperty(MonarchUtils.REGION, "testTablePropertiesPassedToInputJobPropertiesFTable");
        props.setProperty(MonarchUtils.REDUNDANCY, "2");
        props.setProperty(MonarchUtils.BUCKETS, "57");
        props.setProperty(MonarchUtils.MONARCH_PARTITIONING_COLUMN, "C2");
        props.setProperty(ENABLE_KERBEROS_AUTHC, "true");
        props.setProperty(KERBEROS_SERVICE_PRINCIPAL, "ampoolservice");
        props.setProperty(KERBEROS_USER_PRINCIPAL, "ampoolclient");
        props.setProperty(KERBEROS_USER_KEYTAB, "ampoolclient.keytab");
        props.setProperty(MonarchUtils.BLOCK_SIZE, "9999");
        props.setProperty(MonarchUtils.BLOCK_FORMAT, "ORC_BYTES");

        Mockito.when(tableDesc.getProperties()).thenReturn(props);

        storageHandler.configureInputJobProperties(tableDesc, jobProperties);

        assertEquals(jobProperties.size(), 17);
        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.LOCATOR_HOST));
        assertEquals(props.getProperty(MonarchUtils.LOCATOR_HOST),
          jobProperties.get(MonarchUtils.LOCATOR_HOST));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.LOCATOR_PORT));
        assertEquals(props.getProperty(MonarchUtils.LOCATOR_PORT),
          jobProperties.get(MonarchUtils.LOCATOR_PORT));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.REGION));
        assertEquals(props.getProperty(MonarchUtils.REGION),
          jobProperties.get(MonarchUtils.REGION));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.REDUNDANCY));
        assertEquals(props.getProperty(MonarchUtils.REDUNDANCY),
          jobProperties.get(MonarchUtils.REDUNDANCY));

        Assert.assertTrue(
          jobProperties.containsKey(MonarchUtils.MONARCH_PARTITIONING_COLUMN));
        assertEquals(props.getProperty(MonarchUtils.MONARCH_PARTITIONING_COLUMN),
          jobProperties.get(MonarchUtils.MONARCH_PARTITIONING_COLUMN));
        assertEquals(jobProperties.get(MonarchUtils.READ_TIMEOUT),
          MonarchUtils.READ_TIMEOUT_DEFAULT);
        assertEquals(jobProperties.get(MonarchUtils.MAX_VERSIONS),
          MonarchUtils.MAX_VERSIONS_DEFAULT);
        assertEquals(jobProperties.get(MonarchUtils.BLOCK_FORMAT),
          props.getProperty(MonarchUtils.BLOCK_FORMAT));
        assertEquals(jobProperties.get(MonarchUtils.BLOCK_SIZE),
          props.getProperty(MonarchUtils.BLOCK_SIZE));

        assertEquals(jobProperties.get(ENABLE_KERBEROS_AUTHC),
            props.getProperty(ENABLE_KERBEROS_AUTHC));
        assertEquals(jobProperties.get(KERBEROS_SERVICE_PRINCIPAL),
            props.getProperty(KERBEROS_SERVICE_PRINCIPAL));
        assertEquals(jobProperties.get(KERBEROS_USER_PRINCIPAL),
            props.getProperty(KERBEROS_USER_PRINCIPAL));
        assertEquals(jobProperties.get(KERBEROS_USER_KEYTAB),
            props.getProperty(KERBEROS_USER_KEYTAB));
    }

    @Test
    public void testTableJobPropertiesCallsInputAndOutputMethods() {
        MonarchStorageHandler mockStorageHandler = mock(MonarchStorageHandler.class);
        TableDesc tableDesc = mock(TableDesc.class);
        Map<String,String> jobProperties = new HashMap<String,String>();

        Mockito.doCallRealMethod().when(mockStorageHandler)
                .configureTableJobProperties(tableDesc, jobProperties);

        // configureTableJobProperties shouldn't be getting called by Hive, but, if it somehow does,
        // we should just set all of the configurations for input and output.
        mockStorageHandler.configureTableJobProperties(tableDesc, jobProperties);

        Mockito.verify(mockStorageHandler).configureInputJobProperties(tableDesc, jobProperties);
        Mockito.verify(mockStorageHandler).configureOutputJobProperties(tableDesc, jobProperties);
    }

    private Type createType(String typeName, Map<String, String> fields) throws Throwable {
        Type typ1 = new Type();
        typ1.setName(typeName);
        typ1.setFields(new ArrayList<FieldSchema>(fields.size()));
        for(String fieldName : fields.keySet()) {
            typ1.getFields().add(
                    new FieldSchema(fieldName, fields.get(fieldName), ""));
        }

        return typ1;
    }

    @Test
    public void testPreCreateTable() throws Exception, Throwable{
//        System.out.println("Running testPreCreateTable. ");
        String tableName = "testPreCreateTable";

        Configuration conf = new Configuration();
        conf.set("monarch.region.name", tableName);
        conf.set("monarch.locator.port", testBase.getLocatorPort());

        MonarchStorageHandler storageHandler = mock(MonarchStorageHandler.class);

        StorageDescriptor sd = mock(StorageDescriptor.class);
        Table table = mock(Table.class);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);

        // Call the real preCreateTable method
        Mockito.doCallRealMethod().when(storageHandler).getMetaHook();
        Mockito.when(table.getSd()).thenReturn(sd);
        // Return our known table name
        Mockito.when(storageHandler.getMonarchTableName(table)).thenReturn(tableName);

        // This is creation of dummy schema for columns
        Map<String, String> fields = new HashMap<String, String>();
        fields.put("name", serdeConstants.STRING_TYPE_NAME);
        fields.put("income", serdeConstants.INT_TYPE_NAME);
        Type typ1 = createType("MyType", fields);
        Mockito.when(table.getSd().getColsIterator()).thenReturn(typ1.getFields().iterator());

        // Not an EXTERNAL table
        Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);
        // No location expected with MonarchStorageHandler
        Mockito.when(sd.getLocation()).thenReturn(null);
        // Return mocked SerDeInfo
        Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);


        HashMap<String, String> map = new HashMap<>();
        map.put("monarch.locator.port", testBase.getLocatorPort());
        map.put("monarch.region.name", tableName);
        map.put("monarch.table.type", MonarchUtils.TABLE_TYPE_UNORDERED);
        Mockito.when(table.getParameters()).thenReturn(map);

        storageHandler.getMetaHook().preCreateTable(table);
        MTable mtable = MonarchUtils.getTableInstance(conf, fields.keySet().toArray(new String[fields.keySet().size()]));
        Assert.assertNotNull(mtable);
        assertEquals(tableName, mtable.getName());
        /** assert on meta-table **/
        conf.set("monarch.region.name", tableName+MonarchUtils.META_TABLE_SFX);
        mtable = MonarchUtils.getTableInstance(conf, fields.keySet().toArray(new String[fields.keySet().size()]));
        Assert.assertNotNull(mtable);
        assertEquals(tableName+MonarchUtils.META_TABLE_SFX, mtable.getName());

        try {
            storageHandler.getMetaHook().preCreateTable(table);
        } catch (MetaException ex) {
            assertEquals("Table " + tableName + " already exists" + " within Monarch; use CREATE EXTERNAL TABLE instead to" + " register it in Hive.",
                    ex.getMessage());
        }

        MonarchUtils.destroyTable(tableName, map, false, true);
    }

    @Test
    public void testPreCreateFTable() throws Exception, Throwable{
//        System.out.println("Running testPreCreateFTable. ");
        String tableName = "testPreCreateFTable";

        Configuration conf = new Configuration();
        conf.set("monarch.region.name", tableName);
        conf.set("monarch.locator.port", testBase.getLocatorPort());

        MonarchStorageHandler storageHandler = mock(MonarchStorageHandler.class);

        StorageDescriptor sd = mock(StorageDescriptor.class);
        Table table = mock(Table.class);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);

        // Call the real preCreateTable method
        Mockito.doCallRealMethod().when(storageHandler).getMetaHook();
        Mockito.when(table.getSd()).thenReturn(sd);
        // Return our known table name
        Mockito.when(storageHandler.getMonarchTableName(table)).thenReturn(tableName);

        // This is creation of dummy schema for columns
        Map<String, String> fields = new HashMap<String, String>();
        fields.put("name", serdeConstants.STRING_TYPE_NAME);
        fields.put("income", serdeConstants.INT_TYPE_NAME);
        Type typ1 = createType("MyType", fields);
        Mockito.when(table.getSd().getColsIterator()).thenReturn(typ1.getFields().iterator());

        // Not an EXTERNAL table
        Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);
        // No location expected with MonarchStorageHandler
        Mockito.when(sd.getLocation()).thenReturn(null);
        // Return mocked SerDeInfo
        Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);


        HashMap<String, String> map = new HashMap<>();
        map.put("monarch.locator.port", testBase.getLocatorPort());
        map.put("monarch.region.name", tableName);
        Mockito.when(table.getParameters()).thenReturn(map);

        storageHandler.getMetaHook().preCreateTable(table);
        FTable fTable = MonarchUtils.getFTableInstance(conf, fields.keySet().toArray(new String[fields.keySet().size()]));
        Assert.assertNotNull(fTable);
        assertEquals(tableName, fTable.getName());
        /** assert on meta-table **/
        conf.set("monarch.region.name", tableName+MonarchUtils.META_TABLE_SFX);
        MTable mtable = MonarchUtils.getTableInstance(conf, fields.keySet().toArray(new String[fields.keySet().size()]));
        Assert.assertNotNull(mtable);
        assertEquals(tableName+MonarchUtils.META_TABLE_SFX, mtable.getName());

        try {
            storageHandler.getMetaHook().preCreateTable(table);
        } catch (MetaException ex) {
            assertEquals("Table " + tableName + " already exists" + " within Monarch; use CREATE EXTERNAL TABLE instead to" + " register it in Hive.",
              ex.getMessage());
        }

        MonarchUtils.destroyFTable(tableName, map, false, true);
    }

    //@Test(expected = MetaException.class)
    public void testNonNullLocation() throws Exception {

        String tableName = "testNonNullLocation";
        MonarchStorageHandler storageHandler = mock(MonarchStorageHandler.class);

        StorageDescriptor sd = mock(StorageDescriptor.class);
        Table table = mock(Table.class);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);

        // Call the real preCreateTable method
        Mockito.doCallRealMethod().when(storageHandler).getMetaHook();
        Mockito.when(table.getSd()).thenReturn(sd);
        Mockito.when(storageHandler.getMonarchTableName(table)).thenReturn(tableName);

        // Not an EXTERNAL table
        Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);
        // No location expected with MonarchStorageHandler
        Mockito.when(sd.getLocation()).thenReturn("foobar");
        // Return mocked SerDeInfo
        Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);

        try {
            storageHandler.getMetaHook().preCreateTable(table);
        } catch (MetaException expected) {
            assertEquals("Location can't be specified for Monarch", expected.getMessage());
        }
    }


    @Test
    public void testExternalNonExistentTableFails() throws Exception, Throwable {

        String tableName = "testExternalNonExistentTableFails";
        MonarchStorageHandler storageHandler = mock(MonarchStorageHandler.class);

        HashMap<String, String> map = new HashMap<>();
        map.put("monarch.region.name", tableName);
        map.put("monarch.locator.port", testBase.getLocatorPort());

        StorageDescriptor sd = mock(StorageDescriptor.class);
        Table table = mock(Table.class);
        Mockito.when(table.getParameters()).thenReturn(map);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);

        // Call the real preCreateTable method
        Mockito.doCallRealMethod().when(storageHandler).getMetaHook();
        Mockito.when(table.getSd()).thenReturn(sd);
        Mockito.when(storageHandler.getMonarchTableName(table)).thenReturn(tableName);

        // Not an EXTERNAL table
        Mockito.when(storageHandler.isExternalTable(table)).thenReturn(true);
        // No location expected with MonarchStorageHandler
        Mockito.when(sd.getLocation()).thenReturn(null);
        // Return mocked SerDeInfo
        Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);

        // This is creation of dummy schema for columns
        Map<String, String> fields = new HashMap<String, String>();
        fields.put("name", serdeConstants.STRING_TYPE_NAME);
        fields.put("income", serdeConstants.INT_TYPE_NAME);
        Type typ1 = createType("MyType", fields);
        Mockito.when(table.getSd().getColsIterator()).thenReturn(typ1.getFields().iterator());

        try {
            storageHandler.getMetaHook().preCreateTable(table);
        } catch (MetaException expected) {
            assertEquals("Monarch table " + tableName + " doesn't exist while the table is declared as an external table.",
                    expected.getMessage());
        }

        Configuration conf = new Configuration();
        conf.set("monarch.region.name", tableName);
        conf.set("monarch.locator.port", testBase.getLocatorPort());

        MonarchUtils.destroyTable(tableName, map, false, true);
    }

    @Test
    public void testNonExternalExistentTable() throws Exception, Throwable {
        String tableName = "testNonExternalExistentTable";

        Configuration conf = new Configuration();
        conf.set("monarch.region.name", tableName);

        HashMap<String, String> map = new HashMap<>();
        map.put("monarch.locator.port", testBase.getLocatorPort());

        Map<String, String> fields = new HashMap<String, String>();
        fields.put("name", serdeConstants.STRING_TYPE_NAME);
        fields.put("income", serdeConstants.INT_TYPE_NAME);
        Type typ1 = createType("MyType", fields);

        MonarchUtils.createConnectionAndFTable(tableName, map, false, tableName, fields);
        FTable tbl = MonarchUtils.getFTableInstance(conf, fields.keySet().toArray(new String[fields.keySet().size()]));
        Assert.assertNotNull(tbl);
        assertEquals(tableName, tbl.getName());

        MonarchStorageHandler storageHandler = mock(MonarchStorageHandler.class);

        StorageDescriptor sd = mock(StorageDescriptor.class);
        Table table = mock(Table.class);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);

        // Call the real preCreateTable method
        Mockito.doCallRealMethod().when(storageHandler).getMetaHook();
        Mockito.when(table.getSd()).thenReturn(sd);
        Mockito.when(storageHandler.getMonarchTableName(table)).thenReturn(tableName);

        // Not an EXTERNAL table
        Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);
        // No location expected with MonarchStorageHandler
        Mockito.when(sd.getLocation()).thenReturn(null);
        // Return mocked SerDeInfo
        Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);
        Mockito.when(table.getSd().getColsIterator()).thenReturn(typ1.getFields().iterator());

        // This is done purposefully so as to create a scenario.
        // With Hive a new gemfire gets invoked and hence above region creation is not useful.
        // Once createRegion and getRegion gives region on server, remove 1 call to preCreateTable.
        try {
            storageHandler.getMetaHook().preCreateTable(table);
        } catch (MetaException expected) {
            assertEquals("Table " + tableName + " already exists" + " within Monarch; use CREATE EXTERNAL TABLE instead to" + " register it in Hive.",
                    expected.getMessage());
        }

        MonarchUtils.destroyTable(tableName, map, false, true);
     }

    @Test
    public void testRollbackCreateTableOnNonExistentTable() throws Exception, Throwable {

        String tableName = "testRollbackCreateTableOnNonExistentTable";
        MonarchStorageHandler storageHandler = mock(MonarchStorageHandler.class);

        StorageDescriptor sd = mock(StorageDescriptor.class);
        Table table = mock(Table.class);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);

        // Call the real preCreateTable method
        Mockito.doCallRealMethod().when(storageHandler).getMetaHook();
        Mockito.when(table.getSd()).thenReturn(sd);
        Mockito.when(storageHandler.getMonarchTableName(table)).thenReturn(tableName);

        // Not an EXTERNAL table
        Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);
        // No location expected with MonarchStorageHandler
        Mockito.when(sd.getLocation()).thenReturn(null);
        // Return mocked SerDeInfo
        Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);

        HashMap<String, String> map = new HashMap<>();
        map.put("monarch.locator.port", testBase.getLocatorPort());
        map.put("monarch.table.type", MonarchUtils.TABLE_TYPE_UNORDERED);
//        System.out.println("Locator port =  " + testBase.getLocatorPort());

        Mockito.when(table.getParameters()).thenReturn(map);

        // This is creation of dummy schema for columns
        Map<String, String> fields = new HashMap<String, String>();
        fields.put("name", serdeConstants.STRING_TYPE_NAME);
        fields.put("income", serdeConstants.INT_TYPE_NAME);
        Type typ1 = createType("MyType", fields);
        Mockito.when(table.getSd().getColsIterator()).thenReturn(typ1.getFields().iterator());

        storageHandler.getMetaHook().rollbackCreateTable(table);
        MonarchUtils.destroyTable(tableName, map, false, true);
    }

    @Test
    public void testRollbackCreateFTableOnNonExistentTable() throws Exception, Throwable {

        String tableName = "testRollbackCreateFTableOnNonExistentTable";
        MonarchStorageHandler storageHandler = mock(MonarchStorageHandler.class);

        StorageDescriptor sd = mock(StorageDescriptor.class);
        Table table = mock(Table.class);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);

        // Call the real preCreateTable method
        Mockito.doCallRealMethod().when(storageHandler).getMetaHook();
        Mockito.when(table.getSd()).thenReturn(sd);
        Mockito.when(storageHandler.getMonarchTableName(table)).thenReturn(tableName);

        // Not an EXTERNAL table
        Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);
        // No location expected with MonarchStorageHandler
        Mockito.when(sd.getLocation()).thenReturn(null);
        // Return mocked SerDeInfo
        Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);

        HashMap<String, String> map = new HashMap<>();
        map.put("monarch.locator.port", testBase.getLocatorPort());
        map.put("monarch.table.type", MonarchUtils.DEFAULT_TABLE_TYPE);
//        System.out.println("Locator port =  " + testBase.getLocatorPort());

        Mockito.when(table.getParameters()).thenReturn(map);

        // This is creation of dummy schema for columns
        Map<String, String> fields = new HashMap<String, String>();
        fields.put("name", serdeConstants.STRING_TYPE_NAME);
        fields.put("income", serdeConstants.INT_TYPE_NAME);
        Type typ1 = createType("MyType", fields);
        Mockito.when(table.getSd().getColsIterator()).thenReturn(typ1.getFields().iterator());

        storageHandler.getMetaHook().rollbackCreateTable(table);
        MonarchUtils.destroyFTable(tableName, map, false, true);
    }

    @Test()
    public void testRollbackCreateTableDeletesExistentTable() throws Exception, Throwable {

        MonarchStorageHandler storageHandler = Mockito.mock(MonarchStorageHandler.class);
        String tableName = "testRollbackCreateTableDeletesExistentTable";

        Configuration conf = new Configuration();
        conf.set("monarch.region.name", tableName);
        conf.set("monarch.locator.port", testBase.getLocatorPort());
        conf.set("monarch.table.type", MonarchUtils.TABLE_TYPE_UNORDERED);

        StorageDescriptor sd = mock(StorageDescriptor.class);
        Table table = mock(Table.class);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);

        // Call the real preCreateTable method
        Mockito.doCallRealMethod().when(storageHandler).getMetaHook();
        Mockito.when(table.getSd()).thenReturn(sd);
        // Return our known table name
        Mockito.when(storageHandler.getMonarchTableName(table)).thenReturn(tableName);

        // Not an EXTERNAL table
        Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);
        // No location expected with MonarchStorageHandler
        Mockito.when(sd.getLocation()).thenReturn(null);
        // Return mocked SerDeInfo
        Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);

        HashMap<String, String> map = new HashMap<>();
        map.put("monarch.locator.port", testBase.getLocatorPort());
        map.put("monarch.region.name", tableName);
        map.put("monarch.table.type", MonarchUtils.TABLE_TYPE_UNORDERED);
        Mockito.when(table.getParameters()).thenReturn(map);

        // This is creation of dummy schema for columns
        Map<String, String> fields = new HashMap<String, String>();
        fields.put("name", serdeConstants.STRING_TYPE_NAME);
        fields.put("income", serdeConstants.INT_TYPE_NAME);
        Type typ1 = createType("MyType", fields);
        Mockito.when(table.getSd().getColsIterator()).thenReturn(typ1.getFields().iterator());

        // Use preCreate Instead of separate function from outside, as locator port with this function is 10334.
        storageHandler.getMetaHook().preCreateTable(table);

        MTable mtable = MonarchUtils.getTableInstance(conf, fields.keySet().toArray(new String[fields.keySet().size()]));
        Assert.assertNotNull(mtable);
        assertEquals(tableName, mtable.getName());

        storageHandler.getMetaHook().rollbackCreateTable(table);
        storageHandler.getMetaHook().commitDropTable(table, true);

        // Region should not be found on server.
        testBase.assertOnServer(tableName, false);

        // This is creation of dummy schema for columns
        MonarchUtils.destroyTable(tableName, map, false, true);
    }

    @Test()
    public void testRollbackCreateFTableDeletesExistentTable() throws Exception, Throwable {

        MonarchStorageHandler storageHandler = Mockito.mock(MonarchStorageHandler.class);
        String tableName = "testRollbackCreateTableDeletesExistentTable";

        Configuration conf = new Configuration();
        conf.set("monarch.region.name", tableName);
        conf.set("monarch.locator.port", testBase.getLocatorPort());

        StorageDescriptor sd = mock(StorageDescriptor.class);
        Table table = mock(Table.class);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);

        // Call the real preCreateTable method
        Mockito.doCallRealMethod().when(storageHandler).getMetaHook();
        Mockito.when(table.getSd()).thenReturn(sd);
        // Return our known table name
        Mockito.when(storageHandler.getMonarchTableName(table)).thenReturn(tableName);

        // Not an EXTERNAL table
        Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);
        // No location expected with MonarchStorageHandler
        Mockito.when(sd.getLocation()).thenReturn(null);
        // Return mocked SerDeInfo
        Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);

        HashMap<String, String> map = new HashMap<>();
        map.put("monarch.locator.port", testBase.getLocatorPort());
        map.put("monarch.region.name", tableName);

        Mockito.when(table.getParameters()).thenReturn(map);

        // This is creation of dummy schema for columns
        Map<String, String> fields = new HashMap<String, String>();
        fields.put("name", serdeConstants.STRING_TYPE_NAME);
        fields.put("income", serdeConstants.INT_TYPE_NAME);
        Type typ1 = createType("MyType", fields);
        Mockito.when(table.getSd().getColsIterator()).thenReturn(typ1.getFields().iterator());

        // Use preCreate Instead of separate function from outside, as locator port with this function is 10334.
        storageHandler.getMetaHook().preCreateTable(table);

        FTable fTable = MonarchUtils.getFTableInstance(conf, fields.keySet().toArray(new String[fields.keySet().size()]));
        Assert.assertNotNull(fTable);
        assertEquals(tableName, fTable.getName());

        storageHandler.getMetaHook().rollbackCreateTable(table);
        storageHandler.getMetaHook().commitDropTable(table, true);

        // Region should not be found on server.
        testBase.assertOnServer(tableName, false);

        // This is creation of dummy schema for columns
        MonarchUtils.destroyFTable(tableName, map, false, true);
    }

    @Test
    public void testRollbackCreateTableDoesntDeleteExternalExistentTable() throws Exception, Throwable {

        MonarchStorageHandler storageHandler = Mockito.mock(MonarchStorageHandler.class);
        String tableName = "testRollbackCreateTableDoesntDeleteExternalExistentTable";

        Configuration conf = new Configuration();
        conf.set("monarch.region.name", tableName);
        conf.set("monarch.locator.port", testBase.getLocatorPort());

        StorageDescriptor sd = mock(StorageDescriptor.class);
        Table table = mock(Table.class);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);

        // Call the real preCreateTable method
        Mockito.doCallRealMethod().when(storageHandler).getMetaHook();
        Mockito.when(table.getSd()).thenReturn(sd);
        // Return our known table name
        Mockito.when(storageHandler.getMonarchTableName(table)).thenReturn(tableName);

        // EXTERNAL table
        Mockito.when(storageHandler.isExternalTable(table)).thenReturn(true);
        // No location expected with MonarchStorageHandler
        Mockito.when(sd.getLocation()).thenReturn(null);
        // Return mocked SerDeInfo
        Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);

        HashMap<String, String> map = new HashMap<>();
        map.put("monarch.locator.port", testBase.getLocatorPort());
        map.put("monarch.region.name", tableName);
        Mockito.when(table.getParameters()).thenReturn(map);

        Map<String, String> fields = new HashMap<String, String>();
        fields.put("name", serdeConstants.STRING_TYPE_NAME);
        fields.put("income", serdeConstants.INT_TYPE_NAME);
        Type typ1 = createType("MyType", fields);

        MonarchUtils.createConnectionAndFTable(tableName, map, false, tableName, fields);
        FTable tbl = MonarchUtils.getFTableInstance(conf, fields.keySet().toArray(new String[fields.keySet().size()]));
        Assert.assertNotNull(tbl);
        assertEquals(tableName, tbl.getName());

        Mockito.when(table.getSd().getColsIterator()).thenReturn(typ1.getFields().iterator());

        // Use preCreate Instead of separate function from outside, as locator port with this function is 10334.
        storageHandler.getMetaHook().preCreateTable(table);

        storageHandler.getMetaHook().rollbackCreateTable(table);
        storageHandler.getMetaHook().commitDropTable(table, true);

        // Region should be found on server.
        testBase.assertOnServer(tableName, true);

        //MonarchUtils.getCache(conf).close();
        MonarchUtils.destroyTable(tableName, map, false, true);
    }

    @Test
    public void testDropTableWithoutDeleteLeavesTableIntact() throws Exception, Throwable {

        MonarchStorageHandler storageHandler = Mockito.mock(MonarchStorageHandler.class);
        String tableName = "testDropTableWithoutDeleteLeavesTableIntact";

        HashMap<String, String> map = new HashMap<>();
        map.put("monarch.locator.port", testBase.getLocatorPort());
        map.put("monarch.region.name", tableName);
//        System.out.println("Locator port =  " + testBase.getLocatorPort());

        Configuration conf = new Configuration();
        conf.set("monarch.region.name", tableName);
        conf.set("monarch.locator.port", testBase.getLocatorPort());

        StorageDescriptor sd = mock(StorageDescriptor.class);
        Table table = mock(Table.class);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);

        // Call the real preCreateTable method
        Mockito.doCallRealMethod().when(storageHandler).getMetaHook();
        Mockito.when(table.getSd()).thenReturn(sd);
        // Return our known table name
        Mockito.when(storageHandler.getMonarchTableName(table)).thenReturn(tableName);

        // Not an EXTERNAL table
        Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);
        // No location expected with MonarchStorageHandler
        Mockito.when(sd.getLocation()).thenReturn(null);
        // Return mocked SerDeInfo
        Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);

        Mockito.when(table.getParameters()).thenReturn(map);

        // This is creation of dummy schema for columns
        Map<String, String> fields = new HashMap<String, String>();
        fields.put("name", serdeConstants.STRING_TYPE_NAME);
        fields.put("income", serdeConstants.INT_TYPE_NAME);
        Type typ1 = createType("MyType", fields);
        Mockito.when(table.getSd().getColsIterator()).thenReturn(typ1.getFields().iterator());

        // Use preCreate Instead of separate function from outside, as locator port with this function is 10334.
        storageHandler.getMetaHook().preCreateTable(table);

        storageHandler.getMetaHook().commitDropTable(table, false);
        //storageHandler.getMetaHook().rollbackCreateTable(table);

        // Region should be found on server.
        testBase.assertOnServer(tableName, true);

        MonarchUtils.destroyTable(tableName, map, false, true);
    }

    @Test
    public void testPreCreateTableWithRedundancy() throws Exception, Throwable{
//        System.out.println("Running testPreCreateTableWithRedundancy. ");
        String tableName = "testPreCreateTableWithRedundancy";

        MonarchStorageHandler storageHandler = mock(MonarchStorageHandler.class);

        StorageDescriptor sd = mock(StorageDescriptor.class);
        Table table = mock(Table.class);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);

        // Call the real preCreateTable method
        Mockito.doCallRealMethod().when(storageHandler).getMetaHook();
        Mockito.when(table.getSd()).thenReturn(sd);
        // Return our known table name
        Mockito.when(storageHandler.getMonarchTableName(table)).thenReturn(tableName);

        // This is creation of dummy schema for columns
        Map<String, String> fields = new HashMap<String, String>();
        fields.put("name", serdeConstants.STRING_TYPE_NAME);
        fields.put("income", serdeConstants.INT_TYPE_NAME);
        Type typ1 = createType("MyType", fields);
        Mockito.when(table.getSd().getColsIterator()).thenReturn(typ1.getFields().iterator());

        // Not an EXTERNAL table
        Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);
        // No location expected with MonarchStorageHandler
        Mockito.when(sd.getLocation()).thenReturn(null);
        // Return mocked SerDeInfo
        Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);


        HashMap<String, String> map = new HashMap<>();
        map.put("monarch.locator.port", testBase.getLocatorPort());
        map.put("monarch.region.name", tableName);
        map.put("monarch.redundancy", "100");
        Mockito.when(table.getParameters()).thenReturn(map);

        try {
            storageHandler.getMetaHook().preCreateTable(table);
        } catch (MetaException ex) {
//            System.out.println("TestMonarchStorageHandler.testPreCreateTableWithRedundancy " + ex.getMessage());
            assertEquals("io.ampool.monarch.table.exceptions.MCacheInternalErrorException: Table Creation Failed",
              ex.getMessage());
        }
    }
}
