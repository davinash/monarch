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
package com.gemstone.gemfire.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.internal.RegionDataOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for RegionDataOrder.
 *
 */
@Category(MonarchTest.class)
public class RegionDataOrderTest {
  @Test
  public void testLastOrder() throws Exception {
    assertEquals(RegionDataOrder.ORDERS.length, RegionDataOrder.IMMUTABLE.getId() + 1);
    assertEquals(RegionDataOrder.IMMUTABLE,
        RegionDataOrder.ORDERS[RegionDataOrder.ORDERS.length - 1]);
  }

  @Test
  public void testGetters() throws Exception {
    assertEquals(0, RegionDataOrder.DEFAULT.getId());
    assertEquals(1, RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED.getId());
    assertEquals(2, RegionDataOrder.ROW_TUPLE_UNORDERED.getId());
    assertEquals("DEFAULT", RegionDataOrder.DEFAULT.toString());
    assertEquals("ROW_TUPLE_ORDERED_VERSIONED",
        RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED.toString());
    assertEquals("ROW_TUPLE_UNORDERED", RegionDataOrder.ROW_TUPLE_UNORDERED.toString());
  }

  /**
   * Assert that singleton instances are returned after serialization for equality/similarity
   * checks.
   *
   * @throws Exception
   */
  @Test
  public void testSingletonInstance() throws Exception {
    RegionDataOrder order0 = RegionDataOrder.DEFAULT;
    RegionDataOrder order1 = RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED;
    RegionDataOrder order2 = RegionDataOrder.ROW_TUPLE_UNORDERED;

    ByteArrayOutputStream bos = new ByteArrayOutputStream(32);
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(order0);
    oos.writeObject(order1);
    oos.writeObject(order2);
    final byte[] bytes = bos.toByteArray();

    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
    RegionDataOrder newOrder0 = (RegionDataOrder) ois.readObject();
    RegionDataOrder newOrder1 = (RegionDataOrder) ois.readObject();
    RegionDataOrder newOrder2 = (RegionDataOrder) ois.readObject();

    assertSame(order0, newOrder0);
    assertSame(order1, newOrder1);
    assertSame(order2, newOrder2);
    oos.close();
    ois.close();
  }

}
