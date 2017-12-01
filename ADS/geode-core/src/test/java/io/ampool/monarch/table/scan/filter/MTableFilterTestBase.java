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
package io.ampool.monarch.table.scan.filter;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import io.ampool.monarch.table.*;
import org.apache.logging.log4j.Logger;
import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.Arrays;


public abstract class MTableFilterTestBase extends MTableDUnitHelper {

  protected static final Logger logger = LogService.getLogger();

  public MTableFilterTestBase() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    startServerOn(this.vm3, DUnitLauncher.getLocatorString());

    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();

    new ArrayList<>(Arrays.asList(vm0, vm1, vm2, vm3))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            MCacheFactory.getAnyInstance().close();
            return null;
          }
        }));

    super.tearDown2();
  }

  protected final byte[] ROW = Bytes.toBytes("testRow");
  protected final String VALUE = "testValue";

  byte[][] QUALIFIERS = {Bytes.toBytes("col0-<d2v1>-<d3v2>"), Bytes.toBytes("col1-<d2v1>-<d3v2>"),
      Bytes.toBytes("col2-<d2v1>-<d3v2>"), Bytes.toBytes("col3-<d2v1>-<d3v2>"),
      Bytes.toBytes("col4-<d2v1>-<d3v2>"), Bytes.toBytes("col5-<d2v1>-<d3v2>"),
      Bytes.toBytes("col6-<d2v1>-<d3v2>"), Bytes.toBytes("col7-<d2v1>-<d3v2>"),
      Bytes.toBytes("col8-<d2v1>-<d3v2>"), Bytes.toBytes("col9-<d2v1>-<d3v2>")};

  private byte[][] makeN(byte[] base, int n) {
    if (n > 256) {
      return makeNBig(base, n);
    }
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, new byte[] {(byte) i});
    }
    return ret;
  }

  private byte[][] makeNBig(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      int byteA = (i % 256);
      int byteB = (i >> 8);
      ret[i] = Bytes.add(base, new byte[] {(byte) byteB, (byte) byteA});
    }
    return ret;
  }

  protected MTable createTable(String tableName) {
    return createTable(tableName, -1);
  }

  protected MTable createTable(String tableName, int numSplits) {
    MTableDescriptor mts = new MTableDescriptor();
    if (numSplits != -1) {
      mts.setTotalNumOfSplits(numSplits);
    }
    for (int i = 0; i < QUALIFIERS.length; i++) {
      mts.addColumn(QUALIFIERS[i]);
    }

    return getmClientCache().getAdmin().createTable(tableName, mts);
  }

  protected void deleteTable(String tableName) {
    getmClientCache().getAdmin().deleteTable(tableName);
  }

  protected MTable createTableAndPutRows(String tableName, int numRows) {
    MTable mt = createTable(tableName);
    putRows(numRows, mt);
    return mt;
  }

  protected void putRows(int numRows, MTable mt) {
    byte[][] ROWS = makeN(ROW, numRows);
    for (int i = 0; i < numRows; i++) {
      Put put = new Put(ROWS[i]);
      for (int j = 0; j < QUALIFIERS.length; j++) {
        put.addColumn(QUALIFIERS[j], Bytes.toBytes(VALUE + j));
      }
      mt.put(put);
    }

    for (int i = 0; i < numRows; i++) {
      Get get = new Get(ROWS[i]);
      assertNotNull(mt.get(get));
    }
  }
}
