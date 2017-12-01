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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.commons.io.FileUtils;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

@Category(MonarchTest.class)
public class MTableMetaDataRegionRecoveryDUnitTest extends MTableDUnitHelper {
  public MTableMetaDataRegionRecoveryDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();
  private static final String[] MTABLE_NAMES = {"MTABLE_ORDERED", "MTABLE_ORDERED_PERSISTENT",
      "MTABLE_UNORDERED", "MTABLE_UNORDERED_PERSISTENT"};
  private static final String snapshotName = "metaregion/v1/metaregion.gfd";
  private static final String snapshotName_V11 = "metaregion/v11/metaregion.gfd";

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
  }

  public void copyMetaRegionPersistedData() throws Exception {
    final Iterator<VM> iterator = new ArrayList<VM>(Arrays.asList(vm0, vm1, vm2)).iterator();
    while (iterator.hasNext()) {
      File srcDir = new File(this.getClass().getResource("metaregion/v1").toURI());
      File destDir = iterator.next().getWorkingDirectory();
      FileUtils.copyDirectory(srcDir, destDir);
    }

  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }

  private void closeAllMCache() {
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
  }

  // public void verifyMetaRegionOnAllServers() {
  // new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
  // .forEach((VM) -> VM.invoke(new SerializableCallable() {
  // @Override
  // public Object call() throws Exception {
  // final Region metaRegion = MTableUtils.getMetaRegion(false);
  // assertNotNull(metaRegion);
  // assertEquals(MTABLE_NAMES.length, metaRegion.size());
  // verifyMetaRegionRecovered(metaRegion);
  // for (int i = 0; i < MTABLE_NAMES.length; i++) {
  // assertTrue(MCacheFactory.getAnyInstance().getAdmin().tableExists(MTABLE_NAMES[i]));
  // }
  // return null;
  // }
  // }));
  // }

  private void verifyMetaRegionRecovered(final Region metaRegion) {

    // ORDERED MTABLE
    final MTableDescriptor ordered = (MTableDescriptor) metaRegion.get(MTABLE_NAMES[0]);
    assertNotNull(ordered);
    assertEquals(MTableType.ORDERED_VERSIONED, ordered.getTableType());
    assertFalse(ordered.isDiskPersistenceEnabled());

    // ORDERED MTABLE_PERSISTENT
    final MTableDescriptor orderedPersistent = (MTableDescriptor) metaRegion.get(MTABLE_NAMES[1]);
    assertNotNull(orderedPersistent);
    assertEquals(MTableType.ORDERED_VERSIONED, orderedPersistent.getTableType());
    assertTrue(orderedPersistent.isDiskPersistenceEnabled());

    // UNORDERED MTABLE
    final MTableDescriptor unordered = (MTableDescriptor) metaRegion.get(MTABLE_NAMES[2]);
    assertNotNull(unordered);
    assertEquals(MTableType.UNORDERED, unordered.getTableType());
    assertFalse(unordered.isDiskPersistenceEnabled());

    // ORDERED MTABLE
    final MTableDescriptor unorderedPersistent = (MTableDescriptor) metaRegion.get(MTABLE_NAMES[3]);
    assertNotNull(unorderedPersistent);
    assertEquals(MTableType.UNORDERED, unorderedPersistent.getTableType());
    assertTrue(unorderedPersistent.isDiskPersistenceEnabled());

  }

  private void verifyMetaRegionOnClient() {
    final Region metaRegion =
        ClientCacheFactory.getAnyInstance().getRegion(MTableUtils.AMPL_META_REGION_NAME);
    assertNotNull(metaRegion);
    assertEquals(MTABLE_NAMES.length, metaRegion.keySetOnServer().size());
    verifyMetaRegionRecovered(metaRegion);
  }

  public void verifyMetaRegionOnClientVM(VM clientVM) {
    clientVM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyMetaRegionOnClient();
        return null;
      }
    });
  }

  public void setSnapshotNameOnAllServers() {
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            MTableUtils.setMetaRegionSnapshot(this.getClass().getResource(snapshotName).getPath());
            return null;
          }
        }));
  }

  public void setSnapshotNameOnAllServers_V11() {
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            MTableUtils
                .setMetaRegionSnapshot(this.getClass().getResource(snapshotName_V11).getPath());
            return null;
          }
        }));
  }

  public void unsetSnapshotNameOnAllServers() {
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            MTableUtils.resetMetaRegionSnapshot();
            return null;
          }
        }));
  }

  private void startServersAndClients() {
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
    createClientCache();
  }

  // @Test
  // public void testMetaRegionRecoveryFromV1toV11() {
  // setSnapshotNameOnAllServers();
  // startServersAndClients();
  // unsetSnapshotNameOnAllServers();
  // verifyMetaRegionOnAllServers();
  // verifyMetaRegionOnClient();
  // verifyMetaRegionOnClientVM(this.client1);
  // closeAllMCache();
  // }
  //
  // @Test
  // public void testMetaRegionRecoveryFromV11toV111() {
  // setSnapshotNameOnAllServers_V11();
  // startServersAndClients();
  // unsetSnapshotNameOnAllServers();
  // verifyMetaRegionOnAllServers();
  // verifyMetaRegionOnClient();
  // verifyMetaRegionOnClientVM(this.client1);
  // closeAllMCache();
  // }
  @Test
  public void dummTest() {

  }
}
