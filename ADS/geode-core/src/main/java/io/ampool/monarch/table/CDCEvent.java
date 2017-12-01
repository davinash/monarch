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

/**
 * Represents <code>MTable</code> events delivered to <code>MAsyncEventListener</code>.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface CDCEvent {
  /**
   * Returns the <code>MTable</code> associated with this CDCEvent
   *
   * @return the <code>MTable</code> associated with this CDCEvent OR null if <code>MTable</code>
   *         not found (e.g. this can happen if it is deleted).
   */
  MTable getMTable();

  /**
   * Returns the <code>MTableDescriptor</code> associated with this CDCEvent's MTable
   *
   * @return the <code>MTable</code> associated with this CDCEvent CDCEvent's MTable OR null if
   *         <code>MTable</code> not found (e.g. this can happen if it is deleted).
   */
  MTableDescriptor getMTableDescriptor();

  /**
   * Returns the <code>Operation</code> that triggered this event.
   *
   * @return the <code>Operation</code> that triggered this event
   */
  MEventOperation getOperation();

  /**
   * Returns the key associated with this event.
   *
   * @return the key associated with this event
   */
  byte[] getKey();

  /**
   * Returns the MEventRow associated with this event.
   *
   * @return the Row associated with this event
   *
   */
  Row getRow();

  /**
   * Returns the wrapper over the DistributedMembershipID, ThreadID, SequenceID which are used to
   * uniquely identify any mtable operation like insert,set,delete. This helps in sequencing the
   * events belonging to a unique producer. e.g. The EventID can be used to track events received by
   * <code>MAsyncEventListener</code> to avoid processing duplicates.
   */
  MEventSequenceID getEventSequenceID();

  /**
   * Returns whether possibleDuplicate is set for this event.
   */
  boolean getPossibleDuplicate();
}
