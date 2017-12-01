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

package io.ampool.tierstore.stores.internal;

import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.internal.TierEvictorThread;
import io.codearte.catchexception.shade.mockito.Mockito;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(FTableTest.class)
public class TierEvictorThreadTest {

  /**
   * Assert that the correct value is returned for the condition when to write the data to next
   * tier. - Local tier + Primary : true - Local tier + Secondary: true - Shared tier+ Primary :
   * true - Shared tier+ Secondary: false
   */
  @Test
  public void testShouldWriteToNextTier() throws ClassNotFoundException {
    final String LTS_CLASS = "io.ampool.tierstore.stores.LocalTierStore";
    // final String STS_CLASS = "io.ampool.tierstore.stores.HDFSTierStore";
    TierStore lts = Mockito.mock(Class.forName(LTS_CLASS).asSubclass(TierStore.class));
    // TierStore sts = Mockito.mock(Class.forName(STS_CLASS).asSubclass(TierStore.class));

    assertEquals("Incorrect return value with Local+Primary.", true,
        TierEvictorThread.shouldWriteToNextTier(lts, true));
    assertEquals("Incorrect return value with Local+Secondary.", true,
        TierEvictorThread.shouldWriteToNextTier(lts, false));
    // assertEquals("Incorrect return value with Shared+Primary.", true,
    // TierEvictorThread.shouldWriteToNextTier(sts, true));
    // assertEquals("Incorrect return value with Shared+Secondary.", false,
    // TierEvictorThread.shouldWriteToNextTier(sts, false));
  }
}
