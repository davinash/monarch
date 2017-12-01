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

package io.ampool.monarch.table.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.internal.MKeyBase;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.ThinRow;
import org.apache.geode.cache.EntryEvent;

import java.util.Arrays;
import java.util.List;

/**
 * Observer to verify that value is not changed before event is triggered
 * 
 * @since 1.5.0
 */

public class SampleRegionObserver5 extends MBaseRegionObserver {

  private int prePut;
  private int postPut;
  private int preDelete;
  private int postDelete;
  private int preGet;
  private int postGet;
  private int postOpen;
  private int preClose;
  private int postClose;

  public SampleRegionObserver5() {
    this.prePut = 0;
    this.postPut = 0;
    this.preDelete = 0;
    this.postDelete = 0;
    this.preGet = 0;
    this.postGet = 0;
    this.postOpen = 0;
    this.preClose = 0;
    this.postClose = 0;
  }

  public int getTotalPrePutCount() {
    return prePut;
  }

  public int getTotalPostPutCount() {
    return postPut;
  }

  public int getTotalPreDeleteCount() {
    return preDelete;
  }

  public int getTotalPostDeleteCount() {
    return postDelete;
  }

  public int getTotalPreGetCount() {
    return preGet;
  }

  public int getTotalPostGetCount() {
    return postGet;
  }

  public int getTotalPreCloseCount() {
    return preClose;
  }

  public int getTotalPostCloseCount() {
    return postClose;
  }

  public int getTotalPostOpenCount() {
    return postOpen;
  }

  @Override
  public void prePut(MObserverContext mObserverContext, Put put) {
    prePut++;

    // STRICT TEST Condition
    if (prePut > MTableObserverDUnitTest.NUM_OF_ENTRIES) {
      // now old value should not be null and should be correct old value not merged one
      EntryEvent entryEvent = mObserverContext.getEntryEvent();
      Object oldValue = entryEvent.getOldValue();
      assertNotNull(oldValue);
      // for single version mtable
      assertTrue(oldValue instanceof byte[]);
      // now verify value
      Object key = entryEvent.getKey();
      byte[] keyData = ((MKeyBase) key).getBytes();
      System.out.println("SampleRegionObserver5.prePut :: 104 keyData " + Arrays.toString(keyData)
          + " string format " + Bytes.toString(keyData));
      // int keyIndex = Integer.parseInt(Bytes.toString(keyData));
      int keyIndex = Bytes.toInt(keyData);
      System.out.println("SampleRegionObserver5.prePut :: 101 key value " + keyIndex);

      MTableDescriptor tableDescriptor = mObserverContext.getTable().getTableDescriptor();
      ThinRow thinRow = ThinRow.create(tableDescriptor, ThinRow.RowFormat.M_FULL_ROW);
      thinRow.reset(keyData, (byte[]) oldValue);

      List<Cell> cells = thinRow.getCells();
      System.out.println("SampleRegionObserver5.prePut :: 120 " + cells);
      cells.forEach(cell -> {
        if (!Bytes.toString(cell.getColumnName()).equalsIgnoreCase(MTableUtils.KEY_COLUMN_NAME)) {
          String colName = Bytes.toString(cell.getColumnName());
          String actualValue = Bytes.toString((byte[]) cell.getColumnValue());
          String expectedValue = colName + keyIndex;
          assertEquals(expectedValue, actualValue);
        }
      });
    }
  }

  @Override
  public void postPut(MObserverContext mObserverContext, Put put) {
    postPut++;
  }

  @Override
  public void preDelete(MObserverContext mObserverContext, Delete delete) {
    preDelete++;
  }

  @Override
  public void postDelete(MObserverContext mObserverContext, Delete delete) {
    postDelete++;
  }

  @Override
  public String toString() {
    return "io.ampool.monarch.table.coprocessor.SampleRegionObserver5";
  }

  @Override
  public void preGet(MObserverContext context, Get get) {
    preGet++;
  }

  @Override
  public void postGet(MObserverContext context, Get get) {
    postGet++;
  }

  @Override
  public void postOpen() {
    postOpen++;
  }

  @Override
  public void preClose(MObserverContext mObserverContext) {
    MTableCoprocessorRegionEventsObserverDUnitTest.beforeRegionDestroyCalled = true;
    preClose++;
  }

  @Override
  public void postClose() {
    postClose++;
  }
}
