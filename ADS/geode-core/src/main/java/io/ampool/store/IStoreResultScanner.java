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
package io.ampool.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.ampool.monarch.table.ftable.internal.ReaderOptions;

/**
 * Interface for store scanning. Use
 * {@link StoreHandler#getStoreScanner(String, int, ReaderOptions)} to obtain an instance.
 */
public interface IStoreResultScanner extends Iterable<StoreRecord> {

  /**
   * Grab the next row's worth of values. The scanner will return a Result.
   *
   * @return Result object if there is another row, null if the scanner is exhausted.
   */
  StoreRecord next();

  /**
   * Get no of rows from table.
   *
   * @param nbRows number of rows to return
   * @return Between zero and number of rows MResult in array. Mscan is done if returned array is of
   *         zero length
   */
  default StoreRecord[] next(int nbRows) {
    List<StoreRecord> resultSets = new ArrayList<>(nbRows);
    for (int i = 0; i < nbRows; i++) {
      StoreRecord next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new StoreRecord[resultSets.size()]);
  }

  /**
   * Return next batch of data if configured for batch mode.
   *
   * @return Next batch of data
   */
  default StoreRecord[] nextBatch() throws IllegalStateException {
    throw new UnsupportedOperationException("batch mode is not implemented in this scanner");
  }

  /**
   * Closes the scanner and releases any resources it has allocated
   */
  void close();

  @Override
  default Iterator iterator() {
    return new Iterator<StoreRecord>() {
      // The next RowMResult, possibly pre-read
      StoreRecord next = null;

      // return true if there is another item pending, false if there isn't.
      // this method is where the actual advancing takes place, but you need
      // to call next() to consume it. hasNext() will only advance if there
      // isn't a pending next().
      @Override
      public boolean hasNext() {
        if (next == null) {
          next = this.next();
          return next != null;
        }
        return true;
      }

      // get the pending next item and advance the iterator. returns null if
      // there is no next item.
      @Override
      public StoreRecord next() {
        // since hasNext() does the real advancing, we call this to determine
        // if there is a next before proceeding.
        // if (!hasNext()) {
        // return null;
        // }

        // if we get to here, then hasNext() has given us an item to return.
        // we want to return the item and then null out the next pointer, so
        // we use a temporary variable.
        StoreRecord temp = next;
        next = null;
        return temp;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
