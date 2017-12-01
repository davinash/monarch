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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Some basic JUnit tests for MKeyBase to make sure that MKeyBase and MTableKey are comparable and
 * allow to retrieve (get) the values stored in a map using their counterparts.
 *
 */
@Category(MonarchTest.class)
public class MKeyBaseTest {

  /**
   * Assert that MTableKey and MKeyBase are comparable as long as the raw bytes are same.
   *
   * @throws Exception
   */
  @Test
  public void testEquals() throws Exception {
    final byte[] keyBytes = "abc".getBytes();
    final IMKey keyBase = new MKeyBase(keyBytes);
    final IMKey keyOther = new MTableKey(keyBytes);

    assertArrayEquals(keyBytes, keyBase.getBytes());
    assertArrayEquals(keyBytes, keyOther.getBytes());

    assertEquals(keyBase, keyOther);
    assertTrue(keyBase.equals(keyOther));
    assertEquals(Arrays.hashCode(keyBytes), keyBase.hashCode());
    assertEquals(Arrays.hashCode(keyBytes), keyOther.hashCode());
    assertEquals(0, keyBase.compareTo(keyOther));
  }

  /**
   * Assert that MTableKey and MKeyBase are comparable even via their setters.
   *
   * @throws Exception
   */
  @Test
  public void testEqualsWithSetter() throws Exception {
    final byte[] keyBytes = "xyz".getBytes();
    final MKeyBase keyBase = new MKeyBase();
    final MTableKey keyOther = new MTableKey();

    keyBase.setBytes(keyBytes);
    keyOther.setKey(keyBytes);

    assertArrayEquals(keyBytes, keyBase.getBytes());
    assertArrayEquals(keyBytes, keyOther.getBytes());

    assertEquals(keyBase, keyOther);
    assertTrue(keyBase.equals(keyOther));
    assertEquals(Arrays.hashCode(keyBytes), keyBase.hashCode());
    assertEquals(Arrays.hashCode(keyBytes), keyOther.hashCode());
    assertEquals(0, keyBase.compareTo(keyOther));
  }

  /**
   * Assert that the keys are different when representing different byte-arrays.
   */
  @Test
  public void testNotEquals() {
    final IMKey keyBase = new MKeyBase("a_1".getBytes());
    final IMKey keyOther = new MTableKey("a_2".getBytes());

    assertNotEquals(keyOther, keyBase);
    assertFalse(keyBase.equals(keyOther));
    assertNotEquals(0, keyBase.compareTo(keyOther));
  }

  /**
   * Make sure that the value stored in a map using MKeyBase can be retrieved via MTableKey and vice
   * versa.
   */
  @Test
  public void testByGetOnMap() {
    final IMKey k_1_1 = new MKeyBase("key_1".getBytes());
    final IMKey k_1_2 = new MTableKey("key_1".getBytes());
    final String MY_VALUE_1 = "value_1";

    Map<Object, Object> myMap_1 = new HashMap<Object, Object>(1) {
      {
        put(k_1_2, MY_VALUE_1);
      }
    };

    assertEquals(MY_VALUE_1, myMap_1.get(k_1_2));
    assertEquals(MY_VALUE_1, myMap_1.get(k_1_1));

    final IMKey k_2_1 = new MKeyBase("key_2".getBytes());
    final IMKey k_2_2 = new MTableKey("key_2".getBytes());
    final String MY_VALUE_2 = "value_2";

    Map<Object, Object> myMap_2 = new HashMap<Object, Object>(1) {
      {
        put(k_2_1, MY_VALUE_2);
      }
    };

    assertEquals(MY_VALUE_2, myMap_2.get(k_2_1));
    assertEquals(MY_VALUE_2, myMap_2.get(k_2_2));
  }

  /**
   * Make sure that toString returns the expected data.
   */
  @Test
  public void testToString() {
    final Object key = new MKeyBase("abc".getBytes());
    assertEquals(MKeyBase.class.getName() + "=[97, 98, 99]", key.toString());
  }

  /**
   * Assert that MKeyBase can be serialized and de-serialized as expected.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @Test
  public void testSerialize() throws IOException, ClassNotFoundException {
    final MKeyBase key = new MKeyBase("abc".getBytes());
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);

    key.toData(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
    final MKeyBase readKey = new MKeyBase();
    readKey.fromData(dis);

    dis.close();
    dos.close();
    bos.close();

    /** assert that two objects are different but are same for equals and compareTo **/
    assertFalse(key == readKey);
    assertEquals(key, readKey);
    assertTrue(key.equals(readKey));
    assertEquals(key.hashCode(), readKey.hashCode());
    assertEquals(0, key.compareTo(key));
  }
}
