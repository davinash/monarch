package io.ampool.monarch.table;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Category(MonarchTest.class)
public class MTableGetAnyInstanceDUnitTest extends MTableDUnitHelper {
  public MTableGetAnyInstanceDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  private List<VM> allVMList = null;


  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    allVMList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);

    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);
    super.tearDown2();
  }

  @Test
  public void testServerSideGetAnyInstance() {
    allVMList.forEach((VM) -> {
      VM.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          MCacheFactory.getAnyInstance();
          MCacheFactory.getAnyInstance();
          MCacheFactory.getAnyInstance();
          MCacheFactory.getAnyInstance();
          return null;
        }
      });

    });
  }

  @Test
  public void testClientSideGetAnyInstance() {
    this.client1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MClientCacheFactory.getAnyInstance();
        MClientCacheFactory.getAnyInstance();
        MClientCacheFactory.getAnyInstance();
        return null;
      }
    });
    MClientCacheFactory.getAnyInstance();
    MClientCacheFactory.getAnyInstance();
    MClientCacheFactory.getAnyInstance();
  }
}
