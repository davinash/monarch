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

import io.ampool.monarch.types.BasicTypes;

import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.junit.Test;

import java.util.*;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HTTP_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.internal.InternalLocator.FORCE_LOCATOR_DM_TYPE;
import static org.junit.Assert.*;

public class MTablePeerOpJUnitTest2 extends MTableDUnitHelper {

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServer(DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache(client1);
    this.vm0.invoke(() -> MCacheFactory.getAnyInstance().close());
    MCacheFactory.getAnyInstance().close();
  }


  private MTable createTable() {
    final String tableName = getTestMethodName() + "_MTable";
    MTableDescriptor tableDescriptor1 = new MTableDescriptor(MTableType.UNORDERED);
    for (int colIdx = 0; colIdx < 5; colIdx++) {
      tableDescriptor1.addColumn("Column-" + colIdx, BasicTypes.STRING);
    }
    tableDescriptor1.setRecoveryDelay(0);
    tableDescriptor1.setRedundantCopies(1);
    tableDescriptor1.setTotalNumOfSplits(100);
    tableDescriptor1.setMaxVersions(5);
    tableDescriptor1.enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS);
    MTable table =
        MCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, tableDescriptor1);

    return table;
  }

  @Test
  public void testSimplePutWithRedudancy() {
    this.client1.invoke(() -> {
      MTable table = createTable();
      for (int rowKey = 0; rowKey < 1; rowKey++) {
        Put row = new Put(Bytes.toBytes(rowKey));
        for (int colIdx = 0; colIdx < 5; colIdx++) {
          row.addColumn("Column-" + colIdx, "Value-" + colIdx);
        }
        table.put(row);
      }
    });
  }
}
