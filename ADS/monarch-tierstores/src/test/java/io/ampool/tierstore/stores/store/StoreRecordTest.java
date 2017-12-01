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

package io.ampool.tierstore.stores.store;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;

import io.ampool.store.StoreRecord;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.FTableTest;

@Category(FTableTest.class)
public class StoreRecordTest {

  // @Test
  public void getBytes() throws Exception {
    StoreRecord rec = new StoreRecord(12);

    int colIndex = 1;
    rec.addValue(new Boolean(true));
    colIndex++;

    rec.addValue(new Byte(Byte.MAX_VALUE));
    colIndex++;

    rec.addValue(new Short(Short.MAX_VALUE));
    colIndex++;

    rec.addValue(new Integer(Integer.MAX_VALUE));
    colIndex++;

    rec.addValue(new Long(Long.MAX_VALUE));
    colIndex++;

    rec.addValue(new Float(Float.MAX_VALUE));
    colIndex++;

    rec.addValue(new Double(Double.MAX_VALUE));
    colIndex++;

    rec.addValue(new BigDecimal(100));
    colIndex++;

    rec.addValue(new String("Test"));
    colIndex++;

    rec.addValue(new byte[] {0, 1, 2, 3});
    colIndex++;

    rec.addValue(new Date(2016, 1, 1));
    colIndex++;

    rec.addValue(new Timestamp(2016, 1, 1, 1, 1, 1, 1));
    colIndex++;

    // create expected byte[]
    byte[] expectedBytes = {0, 0, 0, 1, -1, 0, 0, 0, 2, 0, 127, 0, 0, 0, 2, 127, -1, 0, 0, 0, 4,
        127, -1, -1, -1, 0, 0, 0, 8, 127, -1, -1, -1, -1, -1, -1, -1, 0, 0, 0, 4, 127, 127, -1, -1,
        0, 0, 0, 8, 127, -17, -1, -1, -1, -1, -1, -1, 0, 0, 0, 5, 0, 0, 0, 0, 100, 0, 0, 0, 4, 84,
        101, 115, 116, 0, 0, 0, 4, 0, 1, 2, 3, 0, 0, 0, 8, 0, 0, 55, -38, -80, -46, -64, 64, 0, 0,
        0, 8, 0, 0, 55, -38, -79, 10, -99, 8};

    final byte[] actualBytes = rec.getBytes();
    assertTrue(Arrays.equals(expectedBytes, actualBytes));
  }

}
