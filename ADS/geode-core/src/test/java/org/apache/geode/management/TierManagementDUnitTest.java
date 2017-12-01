/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management;

import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.internal.AdminImpl;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.store.StoreHandler;
import io.ampool.tierstore.SampleTierStoreImpl;
import io.ampool.tierstore.SampleTierStoreReaderImpl;
import io.ampool.tierstore.SampleTierStoreWriterImpl;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreFactory;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.TierStoreWriter;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test cases to cover all test cases which pertains to tier from Management layer
 * 
 * 
 */
@Category(DistributedTest.class)
public class TierManagementDUnitTest extends ManagementTestBase {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  // This must be bigger than the dunit ack-wait-threshold for the revoke
  // tests. The command line is setting the ack-wait-threshold to be
  // 60 seconds.
  private static final int MAX_WAIT = 70 * 1000;

  boolean testFailed = false;

  String failureCause = "";
  static final String REGION_NAME = "region";

  private File diskDir;

  protected static LogWriter logWriter;


  private static Properties storeProps = new Properties();
  private static Properties readerProps = new Properties();
  private static Properties writerProps = new Properties();
  private static final String storeClass = "io.ampool.tierstore.SampleTierStoreImpl";
  private static final String storeReader = "io.ampool.tierstore.SampleTierStoreReaderImpl";
  private static final String storeWriter = "io.ampool.tierstore.SampleTierStoreWriterImpl";


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

  public TierManagementDUnitTest() throws Exception {
    super();

    diskDir = new File("diskDir-" + getName()).getAbsoluteFile();
    org.apache.geode.internal.FileUtil.delete(diskDir);
    diskDir.mkdir();
    diskDir.deleteOnExit();
  }

  @Override
  protected final void postSetUpManagementTestBase() throws Exception {
    failureCause = "";
    testFailed = false;
  }

  @Override
  protected final void postTearDownManagementTestBase() throws Exception {
    org.apache.geode.internal.FileUtil.delete(diskDir);
  }



  /**
   * Tests Tier URI from a MemberMbean.
   *
   * @throws Exception
   */


  @Test
  @Ignore
  public void testTierURI() throws Throwable {
    initManagement(false);

    //


    String storeName = "testTier";

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


  public Object startServerOn(VM vm, final String locators) {
    System.out.println("Starting server on VM: " + vm.toString());
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
        Cache c = null;
        try {
          c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        c = CacheFactory.create(getSystem(props));
        CacheServer s = c.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        s.setPort(port);
        s.start();
        MCacheFactory.getAnyInstance();
        // registerFunction();
        return port;
      }
    });
  }



  private void createTierStoreOnAllServers(String name, String handler, Properties storeProps,
      String reader, String writer, Properties readerProps, Properties writerProps)
      throws Exception {
    VM vm = getManagedNodeList().get(0);
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
    for (VM vm : getManagedNodeList()) {
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
        CacheFactory.getAnyInstance().getRegion(MTableUtils.AMPL_STORE_META_REGION_NAME);
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


  /**
   * get Distributed member for a given vm
   */
  @SuppressWarnings("serial")
  protected static DistributedMember getMember() throws Exception {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    return cache.getDistributedSystem().getDistributedMember();
  }



  /**
   * Close the given region REGION_NAME
   * 
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  protected void closeRegion(final VM vm) {
    SerializableRunnable closeRegion = new SerializableRunnable("Close persistent region") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.close();
      }
    };
    vm.invoke(closeRegion);
  }

}
