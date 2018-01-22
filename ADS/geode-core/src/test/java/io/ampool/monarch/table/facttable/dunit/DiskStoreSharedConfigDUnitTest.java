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
package io.ampool.monarch.table.facttable.dunit;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.CliCommandTestBase;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.*;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.Wait.waitForCriterion;
import static org.junit.Assert.*;

@Category(FTableTest.class)

public class DiskStoreSharedConfigDUnitTest extends CliCommandTestBase {

  final List<String> filesToBeDeleted = new CopyOnWriteArrayList<String>();

  /**
   * Asserts that creating and destroying disk stores correctly updates the shared configuration.
   */
  @Test
  public void testCreateDestroyUpdatesSharedConfig() {
    disconnectAllFromDS();
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    jmxPort = ports[0];
    httpPort = ports[1];
    try {
      jmxHost = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ignore) {
      jmxHost = "localhost";
    }

    final String groupName = "testDiskStoreSharedConfigGroup";
    final String diskStoreName = "testDiskStoreSharedConfigDiskStore";

    // Start the Locator and wait for shared configuration to be available
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final Properties locatorProps = new Properties();
    locatorProps.setProperty(NAME, "Locator");
    locatorProps.setProperty(MCAST_PORT, "0");
    locatorProps.setProperty(LOG_LEVEL, "fine");
    locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    locatorProps.setProperty(JMX_MANAGER, "true");
    locatorProps.setProperty(JMX_MANAGER_START, "true");
    locatorProps.setProperty(JMX_MANAGER_BIND_ADDRESS, String.valueOf(jmxHost));
    locatorProps.setProperty(JMX_MANAGER_PORT, String.valueOf(jmxPort));
    locatorProps.setProperty(HTTP_SERVICE_PORT, String.valueOf(httpPort));

    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        final File locatorLogFile = new File("locator-" + locatorPort + ".log");
        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locatorPort,
              locatorLogFile, null, locatorProps);

          WaitCriterion wc = new WaitCriterion() {
            @Override
            public boolean done() {
              return locator.isSharedConfigurationRunning();
            }

            @Override
            public String description() {
              return "Waiting for shared configuration to be started";
            }
          };
          waitForCriterion(wc, 5000, 500, true);
        } catch (IOException ioex) {
          fail("Unable to create a locator with a shared configuration");
        }
      }
    });

    connect(jmxHost, jmxPort, httpPort, getDefaultShell());

    // Create a cache in VM 1
    final File diskStoreDir = new File(new File(".").getAbsolutePath(), diskStoreName);
    this.filesToBeDeleted.add(diskStoreDir.getAbsolutePath());
    VM vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        diskStoreDir.mkdirs();

        Properties localProps = new Properties();
        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        localProps.setProperty(GROUPS, groupName);
        getSystem(localProps);
        assertNotNull(getCache());
      }
    });

    // Test creating the disk store
    CommandStringBuilder commandStringBuilder =
        new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__NAME, diskStoreName);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__GROUP, groupName);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE,
        diskStoreDir.getAbsolutePath());
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__ENABLE_DELTA_PERSISTENCE, "true");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure the disk store exists in the shared config
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        ClusterConfigurationService sharedConfig =
            ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        String xmlFromConfig;
        try {
          xmlFromConfig = sharedConfig.getConfiguration(groupName).getCacheXmlContent();
          assertTrue(xmlFromConfig.contains(diskStoreName));
          assertTrue(
              xmlFromConfig.contains(CliStrings.CREATE_DISK_STORE__ENABLE_DELTA_PERSISTENCE));
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service", e);
        }
      }
    });

    // Restart the cache and make sure it has the diskstore
    vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        getCache().close();
        Properties localProps = new Properties();
        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        localProps.setProperty(GROUPS, groupName);
        localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
        getSystem(localProps);
        Cache cache = getCache();
        assertNotNull(cache);

        GemFireCacheImpl gfc = (GemFireCacheImpl) cache;
        Collection<DiskStoreImpl> diskStoreList = gfc.listDiskStores();
        assertNotNull(diskStoreList);
        assertFalse(diskStoreList.isEmpty());
        assertTrue(diskStoreList.size() == 1);

        for (DiskStoreImpl diskStore : diskStoreList) {
          assertTrue(diskStore.getName().equals(diskStoreName));
          break;
        }
        return null;
      }
    });

    // Test destroying the disk store
    commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.DESTROY_DISK_STORE__NAME, diskStoreName);
    commandStringBuilder.addOption(CliStrings.DESTROY_DISK_STORE__GROUP, groupName);
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure the disk store was removed from the shared config
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        ClusterConfigurationService sharedConfig =
            ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        String xmlFromConfig;
        try {
          xmlFromConfig = sharedConfig.getConfiguration(groupName).getCacheXmlContent();
          assertFalse(xmlFromConfig.contains(diskStoreName));
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service", e);
        }
      }
    });


    // Restart the cache and make sure it DOES NOT have the diskstore
    vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        getCache().close();
        Properties localProps = new Properties();
        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        localProps.setProperty(GROUPS, groupName);
        localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
        getSystem(localProps);
        Cache cache = getCache();
        assertNotNull(cache);
        GemFireCacheImpl gfc = (GemFireCacheImpl) cache;
        Collection<DiskStoreImpl> diskStores = gfc.listDiskStores();
        assertNotNull(diskStores);
        assertTrue(diskStores.isEmpty());
        return null;
      }
    });
  }

}
