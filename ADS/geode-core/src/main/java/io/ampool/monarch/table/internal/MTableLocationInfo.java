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
package io.ampool.monarch.table.internal;

import java.util.List;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.MServerLocation;
import io.ampool.monarch.table.Pair;

/**
 * Used to view server location information for a Monarch table.
 *
 * @since 0.3.0.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MTableLocationInfo {

  /**
   * Gets the starting row key for every split in the currently open table.
   *
   * This is mainly useful for the MapReduce integration.
   *
   * @return Array of region starting row keys
   */
  byte[][] getStartKeys();

  /**
   * Gets the ending row key for every split in the currently open table.
   *
   * This is mainly useful for the MapReduce integration.
   *
   * @return Array of region ending row keys
   */
  byte[][] getEndKeys();

  /**
   * Gets the starting and ending row keys for every split in the currently open table.
   *
   * This is mainly useful for the MapReduce integration.
   *
   * @return Pair of arrays of split starting and ending row keys
   */
  Pair<byte[][], byte[][]> getStartEndKeys();

  /**
   * Retrieves all of the monarch servers associated with this table.
   *
   * @return a {@link List} of all monarch servers associated with this table.
   */
  List<MServerLocation> getAllMTableLocations();

  /**
   * Finds the monarch server on which the given row is being served.
   *
   * @param row Row to find.
   * @return Location of the row.
   */
  MServerLocation getMTableLocation(final byte[] row);


}
