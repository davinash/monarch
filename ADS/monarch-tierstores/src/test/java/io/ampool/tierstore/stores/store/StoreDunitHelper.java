package io.ampool.tierstore.stores.store;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MConfiguration;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.AMPLJUnit4CacheTestCase;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;

public class StoreDunitHelper extends AMPLJUnit4CacheTestCase {
  @Override
  public void preSetUp() throws Exception {
    disconnectAllFromDS();
    super.preSetUp();
  }

  public void tearDown2() throws Exception {}

  @Override
  public void preTearDownCacheTestCase() throws Exception {
    tearDown2();
    super.preTearDownCacheTestCase();
  }

  public StoreDunitHelper() {
    super();
  }

  public Object startServerOn(VM vm, final String locators) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
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
        return port;
      }
    });
  }

  public void createClientCache() {
    String testName = getTestMethodName();
    String logFileName = testName + "-client.log";

    assertNotNull(new MClientCacheFactory().set("log-file", logFileName)
        .addPoolLocator("127.0.0.1", getLocatorPort()).create());
  }

  public void createClientCache(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        createClientCache();
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

  /**
   * @return
   */
  private int getDUnitLocatorPort() {
    return DistributedTestUtils.getDUnitLocatorPort();
  }


  public void closeMClientCache() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    if (clientCache != null) {
      clientCache.close();
      clientCache = null;
    }
  }

  public void closeMClientCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        closeMClientCache();
        return null;
      }
    });
  }
}
