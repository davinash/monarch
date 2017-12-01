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
package io.ampool.monarch.table.facttable;

import static org.junit.Assert.*;

import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.VM;

public class FTableDUnitHelper {

  private static void verifyTableOnServer(final String tableName) {
    final MCache serverCache = MCacheFactory.getAnyInstance();
    assertNotNull(serverCache);
    FTable ftable = serverCache.getFTable(tableName);
    assertNotNull(ftable);
    final Region<Object, Object> mregion = ((ProxyFTableRegion) ftable).getTableRegion();
    String path = mregion.getFullPath();
    assertTrue(path.contains(tableName));
  }

  public static void verifyFTableONServer(final String tableName, VM... vms) {
    for (final VM vm : vms) {
      vm.invoke(() -> {
        verifyTableOnServer(tableName);
      });
    }
  }

}
