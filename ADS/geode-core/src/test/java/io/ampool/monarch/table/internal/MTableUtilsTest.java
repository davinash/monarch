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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.geode.test.junit.categories.MonarchTest;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.ftable.Record;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(MonarchTest.class)
public class MTableUtilsTest {

  /**
   * Assert that hash-code is correctly detected for map and Record.
   */
  @Test
  public void testGetDeepHashCode() {
    Object[][] values1 = new Object[][] {new Object[] {"k_s", "v_s"},
        new Object[] {"k_b", "v_b".getBytes()}, new Object[] {"k_i", 1}, new Object[] {"k_l", 2L},
        new Object[] {"k_a_i", new Object[] {11, 22, 33}}};
    Object[][] values2 = new Object[][] {new Object[] {"k_s", "v_s"},
        new Object[] {"k_b", "v_b".getBytes()}, new Object[] {"k_i", 1}, new Object[] {"k_l", 2L},
        new Object[] {"k_a_i", new Object[] {11, 22, 33}}};
    Record record1 = new Record();
    Record record2 = new Record();
    Map<String, Object> map1 = new LinkedHashMap<>();
    Map<String, Object> map2 = new HashMap<>();
    for (int i = 0; i < values1.length; i++) {
      record1.getValueMap().put(new ByteArrayKey(((String) values1[i][0]).getBytes()),
          values1[i][1]);
      record2.getValueMap().put(new ByteArrayKey(((String) values2[i][0]).getBytes()),
          values2[i][1]);
      map1.put((String) values1[i][0], values1[i][1]);
      map2.put((String) values2[i][0], values2[i][1]);
    }

    int hash1 = MTableUtils.getDeepHashCode(map1);
    int hash2 = MTableUtils.getDeepHashCode(map2);

    assertEquals("Incorrect hash-code for map.", hash1, hash2);

    int hash11 = MTableUtils.getDeepHashCode(record1.getValueMap());
    int hash22 = MTableUtils.getDeepHashCode(record2.getValueMap());
    assertEquals("Incorrect hash-code for Record.", hash11, hash22);
  }

  @Test
  public void testByteArrayInMap() {
    int[] array = new int[4];
    for (int j = 0; j < 4; j++) {
      Record record = new Record();
      for (int i = 0; i < 10; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + i));
      }
      array[j] = MTableUtils.getDeepHashCode(record.getValueMap());
    }
    assertEquals(array[0], array[1]);
    assertEquals(array[1], array[2]);
    assertEquals(array[2], array[3]);
  }
}
