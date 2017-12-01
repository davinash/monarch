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

package io.ampool.tierstore.stores.junit;

import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.tierstore.wal.WriteAheadLog;
import io.ampool.tierstore.wal.utils.WALFileDump;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Paths;
import java.util.stream.Collectors;


public class TestWalFileDump {
  WriteAheadLog instance;

  @Before
  public void setup() {
    Exception e = null;
    cleanup();
    instance = WriteAheadLog.getInstance();
    try {
      instance.init(Paths.get("/tmp/WALDIR"), 10, 10);
    } catch (IOException e1) {
      e = e1;
    }
  }

  @After
  public void cleanup() {
    File waldir = new File("/tmp/WALDIR");
    if (waldir != null && waldir.list() != null) {
      for (String s : waldir.list()) {
        new File("/tmp/WALDIR/" + s).delete();
      }
      waldir.delete();
    }
  }

  @Test
  public void testWalFileDump() {
    final ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut));
    byte[] data = new byte[] {0, 0, 0};
    Exception e = null;

    for (int j = 0; j < 19; j++) {
      BlockValue value = new BlockValue(5);
      for (int i = 0; i < 5; i++) {
        data[0] = (byte) j;
        data[1] = (byte) j;
        data[2] = (byte) j;

        value.checkAndAddRecord(data);
      }
      try {
        instance.append("testTable", 1, new BlockKey(j), value);
      } catch (IOException e1) {
        e = e1;
      }
      TestCase.assertNull(e);
    }
    String[] args = new String[2];
    args[0] = "/tmp/WALDIR";
    args[1] = "true";
    try {
      WALFileDump.main(args);
    } catch (IOException e1) {
      e = e1;
    }
    TestCase.assertNull(e);
    String refString = read(getClass().getResourceAsStream("expectedWALDump")) + "\n";
    TestCase.assertEquals(refString, myOut.toString());
  }

  public static String read(InputStream input) {
    try {
      try (BufferedReader buffer = new BufferedReader(new InputStreamReader(input))) {
        return buffer.lines().collect(Collectors.joining("\n"));
      }
    } catch (Exception ex) {

    }
    return null;
  }
}
