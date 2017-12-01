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

package io.ampool.monarch.types;

import io.ampool.monarch.types.interfaces.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class MPredicateHolderTest {

  /**
   * Dummy class for testing..
   */
  private static final class DummyType implements DataType {
    @Override
    public Object deserialize(byte[] bytes, Integer offset, Integer length) {
      return null;
    }

    @Override
    public byte[] serialize(Object object) {
      return null;
    }

    @Override
    public Category getCategory() {
      return null;
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetPredicateInvalid() throws Exception {
    MPredicateHolder ph = new MPredicateHolder(0, new DummyType(), CompareOp.LESS, 10);
    ph.getPredicate();
  }

  @DataProvider
  public static Object[][] getDataGetPredicate() {
    return new Object[][] {{0, BasicTypes.INT, CompareOp.LESS, 10},};
  }

  @Test(dataProvider = "getDataGetPredicate")
  public void testGetArgObjectType(final int idx, final DataType type, final CompareOp op,
      final Object object) {
    MPredicateHolder ph = new MPredicateHolder(idx, type, op, object);
    final String expected = "argType= " + type.toString() + ", columnIdx= " + idx + ", operation= "
        + op.name() + ", arg2= " + object;
    assertEquals(ph.toString(), expected);
    assertNotNull(ph.getArgObjectType());
    assertNotNull(ph.getPredicate());
  }
}
