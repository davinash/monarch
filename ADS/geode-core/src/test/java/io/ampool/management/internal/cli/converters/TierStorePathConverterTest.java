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
package io.ampool.management.internal.cli.converters;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.management.internal.cli.converters.TierStorePathConverter;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(UnitTest.class)
public class TierStorePathConverterTest {
  private TierStorePathConverter createMockTierStorePathConverter(
      final String[] allTierStorePaths) {
    Arrays.sort(allTierStorePaths);
    TierStorePathConverter mockTPC = Mockito.mock(TierStorePathConverter.class);
    Mockito.when(mockTPC.getAllTierStorePaths(anyInt()))
        .thenReturn(new HashSet<>(Arrays.asList(allTierStorePaths)));
    return mockTPC;
  }

  @Test
  public void testGetAllTierStorePaths() throws Exception {
    String[] allTierStorePaths = {"tier_store_2", "tier_store_1", "tier_store_1", "tier_store_2"};
    Set<String> expectedPaths = new TreeSet<>(Arrays.asList(allTierStorePaths));

    final TierStorePathConverter tpc = createMockTierStorePathConverter(allTierStorePaths);

    Set<String> actualPaths = tpc.getAllTierStorePaths(0);

    assertEquals("Paths don't match.", expectedPaths, actualPaths);
  }
}
