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
package io.ampool.monarch.table;

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.internal.MTableRow;
import org.apache.geode.internal.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

@Category(MonarchTest.class)
public class MTableColumnValueObjectJUnitTest {

  private final int NUM_OF_COLUMNS = 10;
  private final String KEY_PREFIX = "KEY";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 10;
  private final String COLUMN_NAME_PREFIX = "COLUMN";


  @Test
  public void testMColumnValueObjectGetColumnAtWithTimeStamp() {
    byte[] value = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, /* TimeStamp */
        0, 0, 0, 6, /* Column1 Length */
        0, 0, 0, 6, /* Column2 Length */
        0, 0, 0, 6, /* Column3 Length */
        0, 0, 0, 6, /* Column4 Length */
        0, 0, 0, 6, /* Column5 Length */
        0, 0, 0, 6, /* Column6 Length */
        0, 0, 0, 6, /* Column7 Length */
        0, 0, 0, 6, /* Column8 Length */
        0, 0, 0, 6, /* Column9 Length */
        0, 0, 0, 6, /* Column10 Length */
        86, 65, 76, 85, 69, 48, /* Column1 Value */
        86, 65, 76, 85, 69, 49, /* Column2 Value */
        86, 65, 76, 85, 69, 50, /* Column3 Value */
        86, 65, 76, 85, 69, 51, /* Column4 Value */
        86, 65, 76, 85, 69, 52, /* Column5 Value */
        86, 65, 76, 85, 69, 53, /* Column6 Value */
        86, 65, 76, 85, 69, 54, /* Column7 Value */
        86, 65, 76, 85, 69, 55, /* Column8 Value */
        86, 65, 76, 85, 69, 56, /* Column9 Value */
        86, 65, 76, 85, 69, 57 /* Column10 Value */
    };
    MTableRow mcvo = new MTableRow(value, true, 10);
    for (int i = 0; i < 9; i++) {
      byte[] expectedValue = new byte[6];
      System.arraycopy(value, 48 + (i * 6), expectedValue, 0, 6);

      byte[] actualValue = mcvo.getColumnAt(i);

      System.out.println("ACTUAL   => " + Arrays.toString(actualValue));
      System.out.println("EXPECTED => " + Arrays.toString(expectedValue));

      if (!Bytes.equals(actualValue, expectedValue)) {
        Assert.fail("getColumnAt failed");
      }
    }
  }

  @Test
  public void testMColumnValueObjectGetColumnAtWithNoTimeStamp() {
    byte[] value = new byte[] {0, 0, 0, 6, /* Column1 Length */
        0, 0, 0, 6, /* Column2 Length */
        0, 0, 0, 6, /* Column3 Length */
        0, 0, 0, 6, /* Column4 Length */
        0, 0, 0, 6, /* Column5 Length */
        0, 0, 0, 6, /* Column6 Length */
        0, 0, 0, 6, /* Column7 Length */
        0, 0, 0, 6, /* Column8 Length */
        0, 0, 0, 6, /* Column9 Length */
        0, 0, 0, 6, /* Column10 Length */
        86, 65, 76, 85, 69, 48, /* Column1 Value */
        86, 65, 76, 85, 69, 49, /* Column2 Value */
        86, 65, 76, 85, 69, 50, /* Column3 Value */
        86, 65, 76, 85, 69, 51, /* Column4 Value */
        86, 65, 76, 85, 69, 52, /* Column5 Value */
        86, 65, 76, 85, 69, 53, /* Column6 Value */
        86, 65, 76, 85, 69, 54, /* Column7 Value */
        86, 65, 76, 85, 69, 55, /* Column8 Value */
        86, 65, 76, 85, 69, 56, /* Column9 Value */
        86, 65, 76, 85, 69, 57 /* Column10 Value */
    };
    MTableRow mcvo = new MTableRow(value, false, 10);
    for (int i = 0; i < 9; i++) {
      byte[] expectedValue = new byte[6];
      System.arraycopy(value, 40 + (i * 6), expectedValue, 0, 6);

      byte[] actualValue = mcvo.getColumnAt(i);

      System.out.println("ACTUAL   => " + Arrays.toString(actualValue));
      System.out.println("EXPECTED => " + Arrays.toString(expectedValue));

      if (!Bytes.equals(actualValue, expectedValue)) {
        Assert.fail("getColumnAt failed");
      }
    }
  }

  @Test
  public void testGetColumnValue() {

  }
}
