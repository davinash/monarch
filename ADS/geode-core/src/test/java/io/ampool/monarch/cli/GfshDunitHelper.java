package io.ampool.monarch.cli;

import static org.apache.geode.test.dunit.DistributedTestUtils.getDUnitLocatorPort;
import static org.junit.Assert.assertNotNull;

import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.AMPLJUnit4CacheTestCase;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.GfshConfig;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class GfshDunitHelper extends AMPLJUnit4CacheTestCase {

  public GfshDunitHelper() {
    super();
  }

  protected String regionName;
  protected Host host = null;
  protected VM vm_0 = null;
  protected VM vm_1 = null;
  protected VM vm_2 = null;
  protected VM client = null;

  protected Gfsh shell;

  private static final File HISTORY_FILE = new File(getHomeGemFireDirectory(), ".mash.history");
  private static final int MAX_HISTORY_SIZE = 500;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm_0 = host.getVM(0);
    vm_1 = host.getVM(1);
    vm_2 = host.getVM(2);
    client = host.getVM(3);
    initVM(this.vm_0, DUnitLauncher.getLocatorString());
    initVM(this.vm_1, DUnitLauncher.getLocatorString());
    initVM(this.vm_2, DUnitLauncher.getLocatorString());
    startClientOn();
  }

  public void tearDown2() throws Exception {
    super.preTearDown();
  }

  protected void initVM(VM vm, final String locators) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("info"));
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
        MCache c = null;
        try {
          c = MCacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        c = MCacheFactory.create(getSystem(props));
        CacheServer s = c.addCacheServer();
        s.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
        s.start();
        return null;
      }
    });
  }

  protected Object startServerOn(VM vm, final String locators) {
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

  protected void startMashShell() throws ClassNotFoundException, IOException {
    shell = Gfsh.getInstance(true, false, null, new GfshConfig(HISTORY_FILE.getAbsolutePath(),
        "{0}mash{1}>", MAX_HISTORY_SIZE, null, null, null, null, null));
  }

  public Gfsh getMashShell() throws ClassNotFoundException, IOException {
    if (shell == null) {
      startMashShell();
    }
    return shell;
  }

  @Test
  public void testClientSetup() {
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
  }

  protected void startClientOn() {
    MClientCache cache =
        new MClientCacheFactory().addPoolLocator("127.0.0.1", getLocatorPort()).create();
    cache = MClientCacheFactory.getAnyInstance();
  }

  protected int getLocatorPort() {
    if (DUnitLauncher.isLaunched()) {
      String locatorString = DUnitLauncher.getLocatorString();
      int index = locatorString.indexOf("[");
      return Integer.parseInt(locatorString.substring(index + 1, locatorString.length() - 1));
    } else {
      return getDUnitLocatorPort();
    }
  }

  /**
   * 
   */
  protected static final long serialVersionUID = 1L;

  public static String getHomeGemFireDirectory() {
    String userHome = System.getProperty("user.home");
    String homeDirPath = userHome + "/.gemfire";
    File alternateDir = new File(homeDirPath);
    if (!alternateDir.exists()) {
      if (!alternateDir.mkdirs()) {
        homeDirPath = ".";
      }
    }
    return homeDirPath;
  }
}
