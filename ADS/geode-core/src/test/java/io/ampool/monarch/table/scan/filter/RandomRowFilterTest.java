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
package io.ampool.monarch.table.scan.filter;

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.filter.RandomRowFilter;
import io.ampool.monarch.table.internal.RowImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category(MonarchTest.class)
public class RandomRowFilterTest {

  private RandomRowFilter halfRowsFilter = null;

  @Before
  public void setup() {
    // Creating filter with 0.5f as chance
    // which means roughly 50% of data will pass through the filter
    halfRowsFilter = new RandomRowFilter(0.5f);
  }

  @Test
  public void testRandomFilter() {
    int included = 0;
    int max = 1000000;

    Row randomRow = new RowImpl(Bytes.toBytes("testKey"));

    for (int index = 0; index < max; index++) {
      if (halfRowsFilter.filterRowKey(randomRow)) {
        included++;
      }
    }

    // Now let's check if the filter included the right number of rows;
    // since we're dealing with randomness, we must have a include an epsilon
    // tolerance.
    int epsilon = max / 100;
    assertTrue("Roughly 50% should pass the filter", Math.abs(included - (max / 2)) < epsilon);
  }
}
