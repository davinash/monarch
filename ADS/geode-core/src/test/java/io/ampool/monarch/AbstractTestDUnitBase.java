package io.ampool.monarch;


import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.functions.TestDUnitBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class AbstractTestDUnitBase {

  public static final TestDUnitBase TEST_BASE = new TestDUnitBase();

  protected static MClientCache clientCache;
  protected static MCache geodeCache;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_BASE.preSetUp();
    TEST_BASE.postSetUp();
    clientCache = TEST_BASE.getClientCache();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    TEST_BASE.preTearDownCacheTestCase();
  }

}
