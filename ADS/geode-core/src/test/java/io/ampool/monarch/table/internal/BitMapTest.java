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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.BitSet;

@Category(MonarchTest.class)
public class BitMapTest {

  @Test
  public void testSizeofBitMap() {
    BitMap bitMap = new BitMap(10);
    assertEquals(2, bitMap.sizeOfByteArray());

    bitMap = new BitMap(8);
    assertEquals(1, bitMap.sizeOfByteArray());

    bitMap = new BitMap(6);
    assertEquals(1, bitMap.sizeOfByteArray());

    bitMap = new BitMap(0);
    assertEquals(0, bitMap.sizeOfByteArray());

    bitMap = new BitMap(15);
    assertEquals(2, bitMap.sizeOfByteArray());

    bitMap = new BitMap(18);
    assertEquals(3, bitMap.sizeOfByteArray());

    bitMap = new BitMap(new byte[10]);
    assertEquals(10, bitMap.sizeOfByteArray());

    bitMap = new BitMap(new byte[10], 5, 5);
    assertEquals(5, bitMap.sizeOfByteArray());

    bitMap = new BitMap(new byte[10], 1, 3);
    assertEquals(3, bitMap.sizeOfByteArray());

    try {
      bitMap = new BitMap(new byte[10], 1, 0);
      assertEquals(3, bitMap.sizeOfByteArray());
    } catch (Exception e) {
      assertTrue(e instanceof RuntimeException);
    }

    try {
      bitMap = new BitMap(new byte[10], 1, -1);
      assertEquals(3, bitMap.sizeOfByteArray());
    } catch (Exception e) {
      assertTrue(e instanceof RuntimeException);
    }

    try {
      bitMap = new BitMap(new byte[10], -1, 10);
      assertEquals(3, bitMap.sizeOfByteArray());
    } catch (Exception e) {
      assertTrue(e instanceof RuntimeException);
    }

    try {
      bitMap = new BitMap(new byte[10], 7, 5);
      assertEquals(3, bitMap.sizeOfByteArray());
    } catch (Exception e) {
      assertTrue(e instanceof RuntimeException);
    }

  }

  @Test
  public void setBitTest() {
    // temp test byte index test
    BitMap bitMap = new BitMap(8);

    try {
      bitMap.set(0);
      assertTrue(bitMap.get(0));
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }

    try {
      bitMap.unSet(0);
      assertFalse(bitMap.get(0));
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }

    try {
      bitMap.set(0, false);
      assertFalse(bitMap.get(0));
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }

    try {
      bitMap.set(0, true);
      assertTrue(bitMap.get(0));
      assertArrayEquals(new byte[] {1}, bitMap.toByteArray());
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }

    try {
      bitMap.set(0, false);
      assertFalse(bitMap.get(0));
      assertArrayEquals(new byte[] {0}, bitMap.toByteArray());
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }

    try {
      int capacity = 10;
      BitMap bitMap1 = new BitMap(capacity);
      for (int i = 0; i < capacity; i++) {
        if (i % 2 == 0) {
          bitMap1.set(i, true);
        } else {
          bitMap1.set(i, false);
        }
      }
      assertArrayEquals(new byte[] {85, 1}, bitMap1.toByteArray());

      for (int i = 0; i < capacity; i++) {
        if (i % 2 == 0) {
          assertTrue(bitMap1.get(i));
        } else {
          assertFalse(bitMap1.get(i));
        }
      }
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }

    try {
      int capacity = 8;
      BitMap bitMap1 = new BitMap(capacity);
      for (int i = 0; i < capacity; i++) {
        if (i % 2 == 0) {
          bitMap1.set(i, true);
        } else {
          bitMap1.set(i, false);
        }
      }
      assertArrayEquals(new byte[] {85}, bitMap1.toByteArray());
      for (int i = 0; i < capacity; i++) {
        if (i % 2 == 0) {
          assertTrue(bitMap1.get(i));
        } else {
          assertFalse(bitMap1.get(i));
        }
      }
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }

    try {
      int capacity = 10;
      BitMap bitMap1 = new BitMap(capacity);
      for (int i = 0; i < capacity; i++) {
        bitMap1.set(i, true);
      }
      assertArrayEquals(new byte[] {-1, 3}, bitMap1.toByteArray());
      for (int i = 0; i < capacity; i++) {
        assertTrue(bitMap1.get(i));
      }
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }

    try {
      int capacity = 10;
      BitMap bitMap1 = new BitMap(capacity);
      for (int i = 0; i < capacity; i++) {
        bitMap1.set(i);
      }
      assertArrayEquals(new byte[] {-1, 3}, bitMap1.toByteArray());
      for (int i = 0; i < capacity; i++) {
        assertTrue(bitMap1.get(i));
      }
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }

    try {
      int capacity = 10;
      BitMap bitMap1 = new BitMap(capacity);
      for (int i = 0; i < capacity; i++) {
        bitMap1.set(i);
        bitMap1.unSet(i);
      }
      assertArrayEquals(new byte[] {0, 0}, bitMap1.toByteArray());
      for (int i = 0; i < capacity; i++) {
        assertFalse(bitMap1.get(i));
      }
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }

    try {
      int capacity = 10;
      BitMap bitMap1 = new BitMap(capacity);
      for (int i = 0; i < capacity; i++) {
        bitMap1.set(i, false);
      }
      assertArrayEquals(new byte[] {0, 0}, bitMap1.toByteArray());
      for (int i = 0; i < capacity; i++) {
        assertFalse(bitMap1.get(i));
      }
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }

  }

  /**
   * Confirming the implementation
   */
  @Test
  public void setBitTestWRTBitSet() {
    try {
      int capacity = 10;
      BitSet bitSet = new BitSet(capacity);
      BitMap bitMap1 = new BitMap(capacity);
      for (int i = 0; i < capacity; i++) {
        if (i % 2 == 0) {
          bitMap1.set(i, true);
          bitSet.set(i, true);
        } else {
          bitMap1.set(i, false);
          bitSet.set(i, false);
        }
      }
      assertArrayEquals(bitSet.toByteArray(), bitMap1.toByteArray());

      for (int i = 0; i < capacity; i++) {
        assertEquals(bitSet.get(i), bitMap1.get(i));
      }
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }
  }

  @Test
  public void orTest() {
    try {
      int capacity = 8;
      BitSet bitSet = new BitSet(capacity);
      BitMap bitMap = new BitMap(capacity);

      BitSet bitSet1 = new BitSet(capacity);
      BitMap bitMap1 = new BitMap(capacity);
      for (int i = 0; i < capacity; i++) {
        if (i % 2 == 0) {
          bitMap.set(i, true);
          bitSet.set(i, true);

          bitMap1.set(i, false);
          bitSet1.set(i, false);
        } else {
          bitMap.set(i, false);
          bitSet.set(i, false);

          bitMap1.set(i, true);
          bitSet1.set(i, true);
        }
      }
      assertArrayEquals(bitSet.toByteArray(), bitMap.toByteArray());

      bitMap.or(bitMap1);
      bitSet.or(bitSet1);

      assertArrayEquals(bitSet.toByteArray(), bitMap.toByteArray());

      for (int i = 0; i < capacity; i++) {
        assertEquals(bitSet.get(i), bitMap.get(i));
      }
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }
  }

  @Test
  public void andTest() {
    try {
      int capacity = 8;
      BitSet bitSet = new BitSet(capacity);
      BitMap bitMap = new BitMap(capacity);

      BitSet bitSet1 = new BitSet(capacity);
      BitMap bitMap1 = new BitMap(capacity);
      for (int i = 0; i < capacity; i++) {
        if (i % 2 == 0) {
          bitMap.set(i, true);
          bitSet.set(i, true);

          bitMap1.set(i, false);
          bitSet1.set(i, false);
        } else {
          bitMap.set(i, false);
          bitSet.set(i, false);

          bitMap1.set(i, true);
          bitSet1.set(i, true);
        }
      }
      assertArrayEquals(bitSet.toByteArray(), bitMap.toByteArray());

      bitMap.and(bitMap1);
      bitSet.and(bitSet1);

      assertEquals(0, bitSet.toByteArray().length);

      assertArrayEquals(new byte[] {0}, bitMap.toByteArray());

      for (int i = 0; i < capacity; i++) {
        assertEquals(bitSet.get(i), bitMap.get(i));
      }
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }
  }

  @Test
  public void cardinalityTest() {
    try {
      int capacity = 10;
      BitSet bitSet = new BitSet(capacity);
      BitMap bitMap1 = new BitMap(capacity);
      for (int i = 0; i < capacity; i++) {
        if (i % 2 == 0) {
          bitMap1.set(i, true);
          bitSet.set(i, true);
        } else {
          bitMap1.set(i, false);
          bitSet.set(i, false);
        }
      }
      assertEquals(bitSet.cardinality(), bitMap1.cardinality());

      for (int i = 0; i < capacity; i++) {
        if (i % 2 != 0) {
          bitMap1.set(i, true);
          bitSet.set(i, true);
        } else {
          bitMap1.set(i, false);
          bitSet.set(i, false);
        }
      }
      assertEquals(bitSet.cardinality(), bitMap1.cardinality());
    } catch (RuntimeException e) {
      e.printStackTrace();
      fail("Shouldn't get exception here");
    }
  }


  // @Test
  public void andNotTest() {
    try {
      int capacity = 8;
      BitSet bitSet = new BitSet(capacity);
      BitMap bitMap = new BitMap(capacity);

      BitSet bitSet1 = new BitSet(capacity);
      BitMap bitMap1 = new BitMap(capacity);
      for (int i = 0; i < capacity; i++) {
        if (i % 2 == 0) {
          bitMap.set(i, true);
          bitSet.set(i, true);
        } else {
          bitMap.set(i, false);
          bitSet.set(i, false);
        }
      }
      assertArrayEquals(bitSet.toByteArray(), bitMap.toByteArray());

      bitMap.and(bitMap1);
      bitSet.and(bitSet1);

      assertArrayEquals(bitSet.toByteArray(), bitMap.toByteArray());

      for (int i = 0; i < capacity; i++) {
        assertEquals(bitSet.get(i), bitMap.get(i));
      }
    } catch (RuntimeException e) {
      fail("Shouldn't get exception here");
    }
  }
}
