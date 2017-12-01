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

import java.io.Closeable;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.exceptions.MScanFailedException;
import io.ampool.monarch.table.exceptions.MScanSplitUnavailableException;

/**
 * Interface for client-side scanning. Go to {@link MTable} to obtain instances.
 *
 * @since 0.3.0.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Scanner extends Closeable, Iterable<Row> {

  /**
   * Grab the next row's worth of values. The scanner will return a Result.
   * 
   * @return Result object if there is another row, null if the scanner is exhausted.
   *
   * @throws MScanSplitUnavailableException if split(bucket)scan is in progress and server holding
   *         it goes down
   * @throws MScanFailedException if scan operation failed
   *
   */
  Row next();

  /**
   * @param nbRows number of rows to return
   * @return Between zero and <param>nbRows</param> Results
   *
   * @throws MScanSplitUnavailableException if split(bucket)scan is in progress and server holding
   *         it goes down
   * @throws MScanFailedException if scan operation failed
   *
   */
  Row[] next(int nbRows);

  /**
   * Return next batch of data if configured for batch mode.
   *
   * @return Next batch of data
   *
   * @throws MScanSplitUnavailableException if split(bucket)scan is in progress and server holding
   *         it goes down
   * @throws MScanFailedException if scan operation failed
   *
   */
  Row[] nextBatch() throws IllegalStateException;

  /**
   * Closes the scanner and releases any resources it has allocated
   */
  void close();
}
