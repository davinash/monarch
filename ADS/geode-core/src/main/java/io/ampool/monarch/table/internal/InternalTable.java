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

import java.util.concurrent.BlockingQueue;

import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.exceptions.MException;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.ServerLocation;

public interface InternalTable extends Table {
  /**
   * Internal scanner method that is applicable to MTable as well as FTable.
   *
   * @param scan the scanner object
   * @param location the server location
   * @param queue the result to be propagated via this queue
   * @return the result ent marker
   */
  RowEndMarker scan(Scan scan, ServerLocation location, BlockingQueue<Object> queue);

  /**
   * Provide the internal region.
   *
   * @return the internal Geode region
   */
  Region<Object, Object> getInternalRegion();

  /**
   * Get a internal scanner applicable to MTable and FTable
   *
   * @param scan the scanner object
   * @return the result scanner
   * @throws MException
   */
  Scanner getScanner(final Scan scan) throws MException;
}
