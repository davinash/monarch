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

import static org.junit.Assert.*;

import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class ScanTest {

  @Test
  public void getDefaultMaxVersions() throws Exception {
    Scan scan = new Scan();
    int maxVersions = scan.getMaxVersions();
    assertEquals(1, maxVersions);
  }

  @Test
  public void isOldestFirst() throws Exception {
    Scan scan = new Scan();
    assertFalse(scan.isOldestFirst());

    scan.setMaxVersions(1, false);
    assertFalse(scan.isOldestFirst());

    scan.setMaxVersions(1, true);
    assertTrue(scan.isOldestFirst());

    scan.setMaxVersions(false);
    assertFalse(scan.isOldestFirst());

    scan.setMaxVersions(true);
    assertTrue(scan.isOldestFirst());
  }

  @Test
  public void isFilterOnLatestVersionOnly() throws Exception {
    Scan scan = new Scan();
    assertTrue(scan.isFilterOnLatestVersionOnly());

    scan.setFilterOnLatestVersionOnly(true);
    assertTrue(scan.isFilterOnLatestVersionOnly());

    scan.setFilterOnLatestVersionOnly(false);
    assertFalse(scan.isFilterOnLatestVersionOnly());
  }

  @Test
  public void setMaxVersions() throws Exception {
    Scan scan = new Scan();
    scan.setMaxVersions();
    int maxVersions = scan.getMaxVersions();
    assertEquals(Integer.MAX_VALUE, maxVersions);
  }

  @Test
  public void setMaxVersionsWithOrder() throws Exception {
    Scan scan = new Scan();

    scan.setMaxVersions(false);
    assertEquals(Integer.MAX_VALUE, scan.getMaxVersions());

    scan.setMaxVersions(true);
    assertEquals(Integer.MAX_VALUE, scan.getMaxVersions());
  }

  @Test(expected = IllegalArgumentException.class)
  public void setMaxVersionsZeroValue() throws Exception {
    Scan scan = new Scan();
    scan.setMaxVersions(0, false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void setMaxVersionsNegativeValue() throws Exception {
    Scan scan = new Scan();
    scan.setMaxVersions(-1, false);
  }

  @Test
  public void setMaxVersionsValidValue() throws Exception {
    Scan scan = new Scan();
    scan.setMaxVersions(1, false);
    assertEquals(1, scan.getMaxVersions());

    scan.setMaxVersions(1, true);
    assertEquals(1, scan.getMaxVersions());
  }

}
