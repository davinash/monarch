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



import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.Arrays;

/*
 * TEST DESCRIPTION : 1. START 3 servers and 1 client 2. Make sure that Default Regions are created
 */
@Category(MonarchTest.class)
public class MTableDefaultRegionCreationOpDUnitTest extends MTableDUnitHelper {
  public MTableDefaultRegionCreationOpDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
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

    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            MCacheFactory.getAnyInstance().close();
            return null;
          }
        }));

    super.tearDown2();
  }

  private void testVerifyDefaultRegionCreationOn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        Region r = cache.getRegion(MTableUtils.AMPL_META_REGION_NAME);
        assertNotNull(r);
        RegionAttributes attributes = r.getAttributes();
        assertNotNull(attributes);
        return null;
      }
    });
  }


  @Test
  public void testVerifyDefaultRegionCreation() {
    testVerifyDefaultRegionCreationOn(this.vm0);
    testVerifyDefaultRegionCreationOn(this.vm0);
    testVerifyDefaultRegionCreationOn(this.vm0);
  }

}
