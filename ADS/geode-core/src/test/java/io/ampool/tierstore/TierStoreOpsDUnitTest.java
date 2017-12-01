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
package io.ampool.tierstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.internal.AdminImpl;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.store.StoreHandler;

import org.apache.geode.cache.CacheClosedException;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Category(FTableTest.class)
public class TierStoreOpsDUnitTest extends MTableDUnitHelper {

  private static Properties storeProps = new Properties();
  private static Properties readerProps = new Properties();
  private static Properties writerProps = new Properties();
  private static final String storeClass = "io.ampool.tierstore.SampleTierStoreImpl";
  private static final String storeReader = "io.ampool.tierstore.SampleTierStoreReaderImpl";
  private static final String storeWriter = "io.ampool.tierstore.SampleTierStoreWriterImpl";


  private static final int NUM_OF_COLUMNS = 10;
  private static final String COLUMN_NAME_PREFIX = "COL";

  static {
    storeProps.setProperty("store.test.prop1", "value1");
    storeProps.setProperty("store.test.prop3", "value3");
    storeProps.setProperty("store.test.prop7", "value7");

    readerProps.setProperty("store.reader.test.prop2", "value2");
    readerProps.setProperty("store.reader.test.prop4", "value4");
    readerProps.setProperty("store.reader.test.prop8", "value8");

    writerProps.setProperty("store.writer.test.prop11", "value11");
    writerProps.setProperty("store.writer.test.prop17", "value17");
    writerProps.setProperty("store.writer.test.prop19", "value19");
  }

  List<VM> allServers = null;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);

    createClientCache();
    allServers = Arrays.asList(vm0, vm1, vm2);
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);
    allServers.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    }));

    super.tearDown2();
  }

  private void createTierStoreOnAllServers(String name, String handler, Properties storeProps,
      String reader, String writer, Properties readerProps, Properties writerProps)
      throws Exception {
    VM vm = allServers.get(0);
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ((AdminImpl) MCacheFactory.getAnyInstance().getAdmin()).createTierStore(name, handler,
            storeProps, writer, writerProps, reader, readerProps);
        return null;
      }
    });
  }

  private void verifyTierStoreOnAllServers(String storeName, String storeClass,
      Properties storeProps, String storeReader, String storeWriter, Properties readerProps,
      Properties writerProps) {
    for (VM vm : allServers) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() {
          verifyTierStore(storeName, storeClass, storeProps, storeReader, storeWriter, readerProps,
              writerProps);
          return null;
        }
      });
    }
  }

  private void verifyTableOnAllServers(String tableName, Properties storeProps, String storeName) {
    for (VM vm : allServers) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() {
          verifyTable(tableName, storeProps, storeName);
          return null;
        }
      });
    }
  }

  private void verifyTable(String tableName, Properties storeProps, String storeName) {
    MCache anyInstance = MCacheFactory.getAnyInstance();
    Admin admin = anyInstance.getAdmin();
    Map<String, FTableDescriptor> stringFTableDescriptorMap = admin.listFTables();

    assertEquals(1, stringFTableDescriptorMap.size());
    for (Map.Entry<String, FTableDescriptor> entry : stringFTableDescriptorMap.entrySet()) {
      System.out.println(entry.getKey() + "/" + entry.getValue());
      assertEquals(tableName, entry.getKey());
      FTableDescriptor value = entry.getValue();

      LinkedHashMap<String, TierStoreConfiguration> tierStores = value.getTierStores();
      assertEquals(1, tierStores.size());

      for (Map.Entry<String, TierStoreConfiguration> entry2 : tierStores.entrySet()) {
        assertEquals(storeName, entry2.getKey());

        TierStoreConfiguration tierStoreConfiguration = entry2.getValue();
        assertNotNull(tierStoreConfiguration);

        Properties tierProperties = tierStoreConfiguration.getTierProperties();
        compareProperties(storeProps, tierProperties);
      }
    }
  }

  private void verifyTierStore(String storeName, String storeClass, Properties storeProps,
      String storeReader, String storeWriter, Properties readerProps, Properties writerProps) {
    Exception e = null;
    TierStore store = StoreHandler.getInstance().getTierStore(storeName);
    assertNotNull(store);
    Class clazz = null;
    try {
      clazz = Class.forName(storeClass);
    } catch (ClassNotFoundException e1) {
      e = e1;
      e.printStackTrace();
    }
    assertNull(e);
    e = null;
    assertTrue(clazz.isInstance(store));


    TierStoreReader reader = store.getReader("dummy", 0, null);
    try {
      clazz = Class.forName(storeReader);
    } catch (ClassNotFoundException e1) {
      e = e1;
      e.printStackTrace();
    }
    assertNull(e);
    e = null;
    assertTrue(clazz.isInstance(reader));

    TierStoreWriter writer = store.getWriter("dummy", 0, null);
    try {
      clazz = Class.forName(storeWriter);
    } catch (ClassNotFoundException e1) {
      e = e1;
      e.printStackTrace();
    }
    assertNull(e);
    e = null;
    assertTrue(clazz.isInstance(writer));

    // Check Properties of store, reader and writer in instances.
    SampleTierStoreImpl sampleStore = (SampleTierStoreImpl) store;
    SampleTierStoreReaderImpl sampleReader = (SampleTierStoreReaderImpl) reader;
    SampleTierStoreWriterImpl sampleWriter = (SampleTierStoreWriterImpl) writer;

    compareProperties(storeProps, sampleStore.getProperties());
    compareProperties(readerProps, sampleReader.getProperties());
    compareProperties(writerProps, sampleWriter.getProperties());

    // Check the meta region
    Region<String, Map<String, Object>> region =
        MCacheFactory.getAnyInstance().getRegion(MTableUtils.AMPL_STORE_META_REGION_NAME);
    assertNotNull(region);
    Map<String, Object> rootProps = region.get(storeName);
    assertNotNull(rootProps);

    assertTrue(storeName.compareTo((String) rootProps.get(TierStoreFactory.STORE_NAME_PROP)) == 0);
    assertTrue(
        storeClass.compareTo((String) rootProps.get(TierStoreFactory.STORE_HANDLER_PROP)) == 0);
    compareProperties(storeProps, (Properties) rootProps.get(TierStoreFactory.STORE_OPTS_PROP));

    assertTrue(
        storeReader.compareTo((String) rootProps.get(TierStoreFactory.STORE_READER_PROP)) == 0);
    compareProperties(readerProps,
        (Properties) rootProps.get(TierStoreFactory.STORE_READER_PROPS_PROP));

    assertTrue(
        storeWriter.compareTo((String) rootProps.get(TierStoreFactory.STORE_WRITER_PROP)) == 0);
    compareProperties(writerProps,
        (Properties) rootProps.get(TierStoreFactory.STORE_WRITER_PROPS_PROP));
  }

  private void compareProperties(Properties exp, Properties act) {
    assertTrue((exp == null && act == null) || (exp != null && act != null));

    if (act == null) {
      return;
    }

    assertEquals(exp.size(), act.size());

    for (Map.Entry entry : exp.entrySet()) {
      String expKey = (String) entry.getKey();
      Object expValue = entry.getValue();
      if (expKey.equalsIgnoreCase("time-to-expire")
          || expKey.equalsIgnoreCase("partition-interval")) {
        // convert to nano
        expValue = TierStoreConfiguration.convertToMS(Double.parseDouble(String.valueOf(expValue)));
      }
      // assertTrue(((String)act.get(expKey)).compareTo(expValue) == 0);
      assertEquals("Incorrect value for: " + expKey, String.valueOf(expValue),
          String.valueOf(act.get(expKey)));
    }
  }

  private void restartServerOn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache c = null;
        try {
          c = MCacheFactory.getAnyInstance();
          int serverRestarted = 0;
          assertTrue(c.isServer());
          for (CacheServer server : c.getCacheServers()) {
            assertTrue(server.isRunning());
            server.stop();
            server.start();
            assertTrue(server.isRunning());
          }
          assertTrue(c.isServer());
        } catch (CacheClosedException cce) {
        }
        return null;
      }
    });
  }

  @Test
  public void testTierStoreCreate() {

    String storeName = "testTierStoreCreate";

    Exception e = null;

    try {
      createTierStoreOnAllServers(storeName, storeClass, new Properties(), storeReader, storeWriter,
          new Properties(), new Properties());
    } catch (Exception e1) {
      e = e1;
      e.printStackTrace();
    }
    assertNull(e);
    verifyTierStoreOnAllServers(storeName, storeClass, new Properties(), storeReader, storeWriter,
        new Properties(), new Properties());
  }

  @Test
  public void testTierStoreCreateWithProperties() {

    String storeName = "testTierStoreCreateWithProperties";

    Exception e = null;

    try {
      createTierStoreOnAllServers(storeName, storeClass, storeProps, storeReader, storeWriter,
          readerProps, writerProps);
    } catch (Exception e1) {
      e = e1;
    }
    assertNull(e);
    verifyTierStoreOnAllServers(storeName, storeClass, storeProps, storeReader, storeWriter,
        readerProps, writerProps);
  }

  @Test
  public void testTierStoreRecoveryWithProperties() {

    String storeName = "testTierStoreCreateWithProperties";

    Exception e = null;

    try {
      createTierStoreOnAllServers(storeName, storeClass, storeProps, storeReader, storeWriter,
          readerProps, writerProps);
    } catch (Exception e1) {
      e = e1;
    }
    assertNull(e);
    verifyTierStoreOnAllServers(storeName, storeClass, storeProps, storeReader, storeWriter,
        readerProps, writerProps);

    for (VM vm : allServers) {
      restartServerOn(vm);
    }

    verifyTierStoreOnAllServers(storeName, storeClass, storeProps, storeReader, storeWriter,
        readerProps, writerProps);
  }

  @Test
  public void testTierStoreCreateWithPropertiesAndTableWithProperties() {

    String storeName = "testTierStoreCreateWithPropertiesAndTableWithProperties";

    Exception e = null;

    try {
      createTierStoreOnAllServers(storeName, storeClass, storeProps, storeReader, storeWriter,
          readerProps, writerProps);
    } catch (Exception e1) {
      e = e1;
    }
    assertNull(e);
    verifyTierStoreOnAllServers(storeName, storeClass, storeProps, storeReader, storeWriter,
        readerProps, writerProps);

    String tableName = getTestMethodName();
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      if (colmnIndex == 0) {
        tableDescriptor.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    tableDescriptor.setRedundantCopies(2);
    LinkedHashMap<String, TierStoreConfiguration> storeConfigurationLinkedHashMap =
        new LinkedHashMap<>();
    TierStoreConfiguration tierStoreConfiguration = new TierStoreConfiguration();
    Properties properties = new Properties();
    properties.setProperty(TierStoreConfiguration.TIER_PARTITION_INTERVAL, "10");
    properties.setProperty(TierStoreConfiguration.TIER_TIME_TO_EXPIRE, "5");
    tierStoreConfiguration.setTierProperties(properties);
    storeConfigurationLinkedHashMap.put(storeName, tierStoreConfiguration);
    tableDescriptor.addTierStores(storeConfigurationLinkedHashMap);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    FTable fTable = null;
    try {
      fTable = clientCache.getAdmin().createFTable(tableName, tableDescriptor);
    } catch (Exception ex) {
      System.out.println("CreateMTableDUnitTest.createFTable :: " + "Throwing from test");
      throw ex;
    }

    assertNotNull(fTable);
    verifyTableOnAllServers(tableName, properties, storeName);
  }
}
