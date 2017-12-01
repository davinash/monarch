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
package io.ampool.monarch.table.perf;

import java.io.*;
import java.util.Random;

public class MTablePerfUtils {
  private static Random rand = new Random();
  private static char[] chars =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

  private static String getRandomString(final int len) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      char c = chars[rand.nextInt(chars.length)];
      sb.append(c);
    }
    return sb.toString();
  }

  public static void generateDataIfRequired(int numOfRecords, int numOfColumns, int lenOfColumn,
      int lenOfRowKey, String fileName, final boolean force) throws IOException {
    File f = new File("/tmp/" + fileName);
    if (f.exists() && !force) {
      return;
    }
    FileOutputStream fos = new FileOutputStream(f);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

    StringBuilder sb = new StringBuilder();
    for (int r = 0; r < numOfRecords - 1; r++) {
      sb.append(getRandomString(lenOfRowKey)).append(',');
      for (int i = 0; i < numOfColumns; i++) {
        sb.append(getRandomString(lenOfColumn)).append((','));
      }
      bw.write(sb.toString());
      bw.newLine();
      sb.setLength(0);
    }
    bw.close();
  }
}
