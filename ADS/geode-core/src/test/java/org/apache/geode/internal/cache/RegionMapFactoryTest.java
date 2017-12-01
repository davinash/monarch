/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import static org.testng.Assert.*;

import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class RegionMapFactoryTest {
  @Test
  public void testGetRegionMapFactory() throws Exception {
    RegionMapFactory regionMapFactory = RegionMapFactory.getRegionMapFactory(null);
    assertNotNull(regionMapFactory);
    assertTrue(regionMapFactory instanceof RegionMapFactoryImpl);
    RegionMapFactory regionMapFactory1 = RegionMapFactory
        .getRegionMapFactory("org.apache.geode.internal.cache.DummyRegionMapFactory");
    assertNotNull(regionMapFactory1);
    assertTrue(regionMapFactory1 instanceof DummyRegionMapFactory);

    try {
      RegionMapFactory.getRegionMapFactory("InvalidClassName");
      fail("Expected exception for the invalid class name");
    } catch (Exception ex) {
      System.out.println("Expected exception " + ex);
      assertEquals(ex.getMessage(),
          "Failed to create region map factory java.lang.ClassNotFoundException: InvalidClassName");
    }
  }
}
