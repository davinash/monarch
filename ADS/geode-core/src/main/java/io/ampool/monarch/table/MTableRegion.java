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

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.exceptions.MWrongTableRegionException;

import java.io.IOException;

/**
 * MTableRegion stores data for a certain split of a MTable. A given MTable consists of one or more
 * MTableRegion.
 *
 * <p>
 * Each MTableRegion has a 'startKey' and 'endKey'.
 * <p>
 * The first is inclusive, the second is inclusive.
 *
 * @since 0.3.0.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MTableRegion {
  /**
   * Return an iterator that scans over the MTableRegion, returning the indicated columns and rows
   * specified by the {@link Scan}.
   * <p>
   * This Iterator must be closed by the caller.
   *
   * @param scan configured {@link Scan}
   * @return RegionScanner See {@link Scanner}
   * @throws IOException read exceptions
   */
  Scanner getScanner(Scan scan) throws Exception;

  /**
   * Returns the size i.e. number of rows in this MTableRegion.
   * 
   * @return Returns the size i.e. number of rows in this MTableRegion.
   */
  long size();

  /**
   *
   * @param put See {@link Put}
   * @throws MWrongTableRegionException throws if the range is incorrect
   */
  void put(Put put) throws MWrongTableRegionException;

  /**
   *
   * @param get {@link Get}
   * @return Result See {@link Row}
   * @throws MWrongTableRegionException throws if the range is incorrect
   */
  Row get(Get get) throws MWrongTableRegionException;

  /**
   *
   * @param delete See {@link Delete}
   * @throws MWrongTableRegionException throws if the range is incorrect
   */
  void delete(Delete delete) throws MWrongTableRegionException;

  /**
   * Return the Associated {@link MTableDescriptor}
   * 
   * @return See {@link MTableDescriptor}
   */

  MTableDescriptor getTableDescriptor();


  /** @return start key for this split of the MTable */
  byte[] getStartKey();

  /** @return end key for this split of the MTable */
  byte[] getEndKey();

}
