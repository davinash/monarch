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

import java.util.List;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

/**
 * A callback for events passing through the CDCStream to which this listener is attached.
 * Implementers of interface <code>MAsyncEventListener</code> process batches of
 * <code>CDCEvent</code> delivered by the corresponding CDCStream. <br>
 * A sample implementation of this interface is as follows: <br>
 *
 * <pre>
 * public class MyEventListener implements MAsyncEventListener {
 *   public boolean processEvents(List<CDCEvent> events) {
 *     for (Iterator i = events.iterator(); i.hasNext();) {
 *       CDCEvent event = (CDCEvent) i.next();
 *       String originalTableName = event.getMTable().getName();
 *       // For illustration purpose, use the event to update the duplicate of above MTable.
 *       final MTable duplicateTable =
 *           MCacheFactory.getAnyInstance().getTable(originalTableName + "_DUP");
 *
 *       final byte[] key = event.getKey();
 *       final MEventRow value = event.getRow();
 *       final MEventOperation op = event.getOperation();
 *
 *       if (op == MEventOperation.CREATE) {
 *         // create MPut instance
 *         // duplicateTable.put(mput);
 *       } else if (op == MEventOperation.PUT) {
 *         // create MPut instance
 *         // duplicateTable.put(mput);
 *       } else if (op == MEventOperation.DELETE) {
 *         // create MDelete instance
 *         // duplicateTable.delete(mdelete);
 *       }
 *     }
 *   }
 * }
 * </pre>
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MAsyncEventListener {

  /**
   * Process the list of <code>CDCEvent</code>s. This method will asynchronously be called when
   * events are queued to be processed. The size of the list will be up to batch size events where
   * batch size is defined in the <code>CDCConfig</code>.
   *
   * @param events The list of <code>CDCEvent</code> to process
   *
   * @return boolean True represents whether the events were successfully processed, false
   *         otherwise.
   */
  boolean processEvents(List<CDCEvent> events);

  /**
   * Called when the MTable containing this callback is closed or destroyed, when the table is
   * deleted.
   *
   * <p>
   * Implementations should cleanup any external resources such as database connections. Any runtime
   * exceptions this method throws will be logged.
   *
   * <p>
   * It is possible for this method to be called multiple times on a single callback instance, so
   * implementations must be tolerant of this.
   *
   */
  void close();
}
