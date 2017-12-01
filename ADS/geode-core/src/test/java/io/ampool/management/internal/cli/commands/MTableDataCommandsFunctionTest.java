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
package io.ampool.management.internal.cli.commands;

import static org.junit.Assert.*;

import org.apache.geode.test.junit.categories.MonarchTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.MTableDataCommandsFunction;

@Category(MonarchTest.class)
public class MTableDataCommandsFunctionTest {
  @Test
  public void testConvertKeyToBytes_STRING() {
    final Object[][] dataArr = {{null, null}, {"", null, false}, {"a-9", new byte[] {97, 45, 57}},
        {"a-b", new byte[] {97, 45, 98}}, {"c-d", new byte[] {99, 45, 100}},
        {"9999999", new byte[] {57, 57, 57, 57, 57, 57, 57}}};
    byte[] output;
    for (final Object[] data : dataArr) {
      output = MTableDataCommandsFunction.convertKeyToBytes("STRING", (String) data[0]);
      assertArrayEquals((byte[]) data[1], output);
    }
  }

  @Test
  public void testConvertKeyToBytes_BINARY() {
    /** inputString, expectedOutput, true/false (true if exception is expected) **/
    final Object[][] dataArr = {{null, null, false}, {"", null, false},
        {"10,20,33,48", new byte[] {10, 20, 33, 48}, false}, {"a-b", null, true},
        {"c-d", null, true}};
    byte[] output = null;
    Exception exception = null;
    for (final Object[] data : dataArr) {
      exception = null;
      try {
        output = MTableDataCommandsFunction.convertKeyToBytes("BINARY", (String) data[0]);
      } catch (Exception e) {
        exception = e;
      }
      if ((boolean) data[2]) {
        assertNotNull(exception);
        assertTrue(exception instanceof NumberFormatException);
      } else {
        assertArrayEquals((byte[]) data[1], output);
      }
    }
  }

  @Test
  public void testConvertKeyToBytes_Invalid() {
    try {
      MTableDataCommandsFunction.convertKeyToBytes("XYZ", "abc");
      fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      ///
    } catch (Exception e) {
      fail("Expected IllegalArgumentException.");
    }
  }
}
