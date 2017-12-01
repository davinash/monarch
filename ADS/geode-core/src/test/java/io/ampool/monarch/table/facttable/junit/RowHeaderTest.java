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
package io.ampool.monarch.table.facttable.junit;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.internal.RowHeader;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testng.Assert;

@Category(FTableTest.class)
public class RowHeaderTest {

  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void getMagicNumber() throws Exception {
    Assert.assertEquals(new RowHeader().getMagicNumber(), 3);
    byte[] header = new byte[4];
    Bytes.putByte(header, 0, (byte) 3);
    Bytes.putByte(header, 1, (byte) 1);
    Bytes.putByte(header, 2, (byte) 1);
    Bytes.putByte(header, 3, (byte) 0);
    Assert.assertEquals(RowHeader.getMagicNumber(header), 3);
  }


  @Test
  public void getEncoding() throws Exception {
    Assert.assertEquals(new RowHeader().getEncoding(), 2);
    byte[] header = new byte[4];
    Bytes.putByte(header, 0, (byte) 3);
    Bytes.putByte(header, 1, (byte) 1);
    Bytes.putByte(header, 2, (byte) 1);
    Bytes.putByte(header, 3, (byte) 0);
    Assert.assertEquals(RowHeader.getEncoding(header), 1);
  }


  @Test
  public void getSchemaVersion() throws Exception {
    Assert.assertEquals(new RowHeader().getSchemaVersion(), 1);
    byte[] header = new byte[4];
    Bytes.putByte(header, 0, (byte) 3);
    Bytes.putByte(header, 1, (byte) 1);
    Bytes.putByte(header, 2, (byte) 1);
    Bytes.putByte(header, 3, (byte) 0);
    Assert.assertEquals(RowHeader.getSchemaVersion(header), 1);
  }

  @Test
  public void getReserved() throws Exception {
    Assert.assertEquals(new RowHeader().getReserved(), 0);
    byte[] header = new byte[4];
    Bytes.putByte(header, 0, (byte) 3);
    Bytes.putByte(header, 1, (byte) 1);
    Bytes.putByte(header, 2, (byte) 1);
    Bytes.putByte(header, 3, (byte) 0);
    Assert.assertEquals(RowHeader.getReserved(header), 0);
  }


  @Test
  public void getHeaderBytes() throws Exception {
    byte[] header = new byte[4];
    Bytes.putByte(header, 0, (byte) 3);
    Bytes.putByte(header, 1, (byte) 2);
    Bytes.putByte(header, 2, (byte) 1);
    Bytes.putByte(header, 3, (byte) 0);
    Assert.assertEquals(header, new RowHeader().getHeaderBytes());
  }

}
