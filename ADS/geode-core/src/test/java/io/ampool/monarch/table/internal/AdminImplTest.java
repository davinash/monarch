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
package io.ampool.monarch.table.internal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.exceptions.MTableNotExistsException;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.exceptions.FTableNotExistsException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(MonarchTest.class)
public class AdminImplTest {
  private static MCache cache;
  private static MCache mCache;

  @BeforeClass
  public static void setUpClass() {
    final Properties props = new Properties();
    props.put("log-level", "none");
    props.put("mcast-port", "0");
    cache = new MCacheFactory(props).create();
    mCache = MCacheFactory.getAnyInstance();
  }

  @AfterClass
  public static void cleanUpClass() {
    cache.close();
  }

  @Test(expected = MTableNotExistsException.class)
  public void testDeleteTable() throws Exception {
    mCache.getAdmin().deleteTable("abc");
  }

  @Test(expected = MTableNotExistsException.class)
  public void testDeleteMTable() throws Exception {
    mCache.getAdmin().deleteMTable("abc");
  }

  @Test(expected = FTableNotExistsException.class)
  public void testDeleteFTable() throws Exception {
    mCache.getAdmin().deleteFTable("abc");
  }

  @Test
  public void testCreateFTableAndExistsCheck() {
    FTableDescriptor td = new FTableDescriptor();
    td.addColumn(new byte[] {0, 1, 2});
    td.setPartitioningColumn(new byte[] {0, 1, 2});
    mCache.getAdmin().createFTable("abc_1", td);
    assertTrue("FTable should not exist.", mCache.getAdmin().tableExists("abc_1"));
    assertTrue("FTable should not exist.", mCache.getAdmin().existsFTable("abc_1"));
    boolean tableExists = mCache.getAdmin().existsMTable("abc_1");
    mCache.getAdmin().deleteFTable("abc_1");
    assertFalse("MTable should not exist.", tableExists);
  }

  @Test
  public void testCreateMTableAndExistsCheck() {
    MTableDescriptor td = new MTableDescriptor();
    td.addColumn(new byte[] {0, 1, 2});
    mCache.getAdmin().createMTable("abc_1", td);
    assertTrue("MTable should not exist.", mCache.getAdmin().tableExists("abc_1"));
    assertTrue("MTable should not exist.", mCache.getAdmin().existsMTable("abc_1"));
    boolean tableExists = mCache.getAdmin().existsFTable("abc_1");
    mCache.getAdmin().deleteMTable("abc_1");
    assertFalse("FTable should not exist.", tableExists);
  }
}
