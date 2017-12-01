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
package io.ampool.monarch.table;

import static org.apache.geode.test.dunit.DistributedTestUtils.getDUnitLocatorPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.AMPLJUnit4CacheTestCase;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Category(MonarchTest.class)
public class MTableFunExecNonBlockingReadDunitTest extends AMPLJUnit4CacheTestCase {
  protected static final String REGION_NAME = "testRegion";
  private static final int numOfEntries = 20;
  Host host = null;
  VM vm_0 = null;
  VM vm_1 = null;
  VM vm_2 = null;
  VM client = null;

  public MTableFunExecNonBlockingReadDunitTest() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm_0 = host.getVM(0);
    vm_1 = host.getVM(1);
    vm_2 = host.getVM(2);
    client = host.getVM(3);
    initVM(vm_0, DUnitLauncher.getLocatorString());
    initVM(vm_1, DUnitLauncher.getLocatorString());
    initVM(vm_2, DUnitLauncher.getLocatorString());

    startClientOn(client);
    startClientOn();
  }

  public void cleanUpAllVMs() throws Exception {
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache c = null;
        try {
          c = MCacheFactory.getAnyInstance();
          if (c != null) {
            c.close();
          }
        } catch (CacheClosedException e) {
        }
        return null;
      }
    });
  }

  public Object initVM(VM vm, final String locators) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
        MCache c = null;
        try {
          c = MCacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        c = MCacheFactory.create(getSystem(props));
        CacheServer s = c.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        s.setPort(port);
        s.start();

        // registerFunction();
        return port;
      }
    });
  }

  @Override
  public void preTearDownCacheTestCase() throws Exception {
    closeClientCache();
    closeClientCache(client);
    super.preTearDownCacheTestCase();
  }

  private void closeClientCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        closeClientCache();
        return null;
      }
    });

  }

  private void closeClientCache() {
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    if (cache != null) {
      cache.close();
    }
  }

  private void createRegionOnServer(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        try {
          MCache c = MCacheFactory.getAnyInstance();
          RegionFactory rf = c.createRegionFactory(RegionShortcut.PARTITION);
          Region r = rf.create(REGION_NAME);
          assertNotNull(r);
        } catch (Exception cce) {
          throw cce;
        }
        return null;
      }
    });
  }

  public void startClientOn(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        startClientOn();
        return null;
      }
    });
  }

  public void startClientOn() {
    MClientCache cache =
        new MClientCacheFactory().addPoolLocator("127.0.0.1", getLocatorPort()).create();
  }

  public void verifyRegionOnClient(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          ClientCache c = ClientCacheFactory.getAnyInstance();
          Region r = c.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
          assertNotNull(r);
        } catch (Exception cce) {
          throw cce;
        }
        return null;
      }
    });
  }

  public void doPuts() {
    ClientCache c = ClientCacheFactory.getAnyInstance();
    Region region = c.getRegion(REGION_NAME);
    String rowKey = "key";
    String value = "value";
    for (int i = 0; i < numOfEntries; i++) {
      region.put(rowKey + i, value + i);
    }
  }

  /**
   * This test doesn't include any asserts. It verifies function execution client can receive
   * results as they are generated using custom collector without waiting for entire execution to
   * get completed
   */
  public void executeNonBlockingGetKeysFun() {
    CustomResultCollector rc = new CustomResultCollector();

    // starting functio execution in separate thread.
    Thread t = new Thread() {
      public void run() {
        ClientCache c = ClientCacheFactory.getAnyInstance();
        Region region = c.getRegion(REGION_NAME);
        assertNotNull(region);
        Function regionGetFun = new RegionGetKeysStreamingFunction();
        FunctionService.registerFunction(regionGetFun);
        Object[] dsInputList = new Object[3];
        dsInputList[0] = REGION_NAME;
        dsInputList[1] = true;
        dsInputList[2] = 500;

        // default result collector
        Execution exe = FunctionService.onRegion(region).withArgs(dsInputList).withCollector(rc);

        exe.execute(regionGetFun);
      }
    };
    t.start();
    while (!rc.isCompleted()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void executeBlockingGetKeysFun() {
    ClientCache c = ClientCacheFactory.getAnyInstance();
    Region region = c.getRegion(REGION_NAME);

    Function regionGetFun = new RegionGetKeysStreamingFunction();
    FunctionService.registerFunction(regionGetFun);
    Object[] dsInputList = new Object[3];
    dsInputList[0] = REGION_NAME;
    dsInputList[1] = true;
    dsInputList[2] = 500;

    // default result collector
    List<String> result = (List<String>) FunctionService.onRegion(region).withArgs(dsInputList)
        .execute(regionGetFun.getId()).getResult();

    int resultCount = 0;
    for (String res : result) {
      System.out.println("Client received -" + res);
      resultCount++;
    }
    assertEquals(numOfEntries + 3, resultCount);

  }

  public void getKeysFromClient(VM vm, boolean nonBlocking) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        getKeys(nonBlocking);
        return null;
      }
    });

  }

  public void getKeys(boolean nonBlocking) {
    ClientCache cache = ClientCacheFactory.getAnyInstance();
    Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);

    if (nonBlocking) {
      executeNonBlockingGetKeysFun();
    } else {
      executeBlockingGetKeysFun();
    }
  }

  public void doPutsFromClient(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        doPuts();
        return null;
      }
    });

  }

  /**
   * Test description Creates a region Put data in region Register a function to execute on region
   * members to get keys - Fun arg is provided to add delay after each send key call from server
   * verify it reaches to client before all keys are processed
   */
  @Test
  public void testFunctionExecution() {
    createRegionOnServer(vm_0);
    createRegionOnServer(vm_1);
    createRegionOnServer(vm_2);
    registerFunctionOn(this.vm_0);
    registerFunctionOn(this.vm_1);
    registerFunctionOn(this.vm_2);
    verifyRegionOnClient(client);
    doPutsFromClient(client);
    verifySizeOfRegionOnServer(vm_0);
    verifySizeOfRegionOnServer(vm_1);
    verifySizeOfRegionOnServer(vm_2);
    getKeys(true);
  }

  private void registerFunctionOn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        FunctionService.registerFunction(new RegionGetKeysStreamingFunction());
        return null;
      }
    });
  }

  private void verifySizeOfRegionOnServer(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          MCache c = MCacheFactory.getAnyInstance();
          Region r = c.getRegion(REGION_NAME);
          assertNotNull(r);
          assertTrue(r.size() > 5);

        } catch (CacheClosedException cce) {
        }
        return null;
      }
    });
  }

  public int getLocatorPort() {
    if (DUnitLauncher.isLaunched()) {
      String locatorString = DUnitLauncher.getLocatorString();
      int index = locatorString.indexOf("[");
      return Integer.parseInt(locatorString.substring(index + 1, locatorString.length() - 1));
    }
    // Running in hydra
    else {
      return getDUnitLocatorPort();
    }
  }

  class RegionGetKeysStreamingFunction extends FunctionAdapter implements InternalEntity {

    @Override
    public void execute(FunctionContext context) {
      Object[] args = (Object[]) context.getArguments();
      String regionName = (String) args[0];
      boolean delayProcessing = (boolean) args[1];
      int millisToDelay = (int) args[2];
      String result = "last";
      try {
        MCache cache = MCacheFactory.getAnyInstance();
        PartitionedRegion pr = (PartitionedRegion) cache.getRegion(regionName);
        if (pr != null) {
          Set<BucketRegion> allLocalBuckets = pr.getDataStore().getAllLocalBucketRegions();
          for (BucketRegion eachLocalBucket : allLocalBuckets) {
            for (Object key : eachLocalBucket.keys()) {
              if (eachLocalBucket.getBucketAdvisor().isPrimary() && eachLocalBucket.size() != 0) {
                System.out.println("Server sending - " + key);
                context.getResultSender().sendResult(key);
              }
              if (delayProcessing) {
                try {
                  Thread.sleep(millisToDelay);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
            }

          }
        }
      } catch (GemFireException re) {
      }
      context.getResultSender().lastResult(result);
    }

    @Override
    public String getId() {
      return this.getClass().getName();
    }
  }

  class CustomResultCollector implements ResultCollector<String, String> {
    private boolean completed;

    public boolean isCompleted() {
      return completed;
    }

    public void setCompleted(boolean completed) {
      this.completed = completed;
    }

    @Override
    public String getResult() throws FunctionException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getResult(long timeout, TimeUnit unit)
        throws FunctionException, InterruptedException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void addResult(DistributedMember memberID, String resultOfSingleExecution) {
      System.out.println("Client received -" + resultOfSingleExecution);
    }

    @Override
    public void endResults() {
      completed = true;
    }

    @Override
    public void clearResults() {
      // TODO Auto-generated method stub

    };
  }

}
