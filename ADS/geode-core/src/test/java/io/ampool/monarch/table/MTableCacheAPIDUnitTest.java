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

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Category(MonarchTest.class)
public class MTableCacheAPIDUnitTest extends MTableDUnitHelper {
  public MTableCacheAPIDUnitTest() {
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
  public void testCacheGetAnyIntanceAPI() {
    allVMList.forEach((VM) -> {
      VM.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          assertNotNull(MCacheFactory.getAnyInstance());
          assertNotNull(MCacheFactory.getAnyInstance());
          assertNotNull(MCacheFactory.getAnyInstance());
          assertNotNull(MCacheFactory.getAnyInstance());

          return null;
        }
      });
    });
  }

  @Test
  public void testClientCacheGetAnyIntanceAPI() {
    this.client1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        assertNotNull(MClientCacheFactory.getAnyInstance());
        assertNotNull(MClientCacheFactory.getAnyInstance());
        assertNotNull(MClientCacheFactory.getAnyInstance());
        assertNotNull(MClientCacheFactory.getAnyInstance());

        return null;
      }
    });
    assertNotNull(MClientCacheFactory.getAnyInstance());
    assertNotNull(MClientCacheFactory.getAnyInstance());
    assertNotNull(MClientCacheFactory.getAnyInstance());
  }


  @Test
  public void testClientCacheCloseAPI() {
    this.client1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        assertFalse(clientCache.isClosed());
        clientCache.close();
        assertTrue(clientCache.isClosed());
        return null;
      }
    });

    createClientCache(this.client1);

  }
}
