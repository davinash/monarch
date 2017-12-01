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
 * This class wraps 1) DistributedMembershipID 2) ThreadID 3) SequenceID attributes which are used
 * to uniquely identify any MTable Operation like insert,set,delete etc. This helps in sequencing
 * the events belonging to a unique producer. As an example, the EventSequenceID can be used to
 * track the events received by <code>MAsyncEventListener</code>. If the event has already been
 * seen, <code>MAsyncEventListener</code> can choose to ignore it.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public final class MEventSequenceID {
  /**
   * Uniquely identifies the distributed member VM in which the Event is produced
   */
  private String membershipID;

  /**
   * Unqiuely identifies the thread producing the event
   */
  private long threadID;

  /**
   * Uniquely identifies individual event produced by a given thread
   */
  private long sequenceID;

  public MEventSequenceID(String membershipID, long threadID, long sequenceID) {
    this.membershipID = membershipID;
    this.threadID = threadID;
    this.sequenceID = sequenceID;
  }

  public String getMembershipID() {
    return this.membershipID;
  }

  public long getThreadID() {
    return this.threadID;
  }

  public long getSequenceID() {
    return this.sequenceID;
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof MEventSequenceID))
      return false;

    MEventSequenceID obj2 = (MEventSequenceID) obj;
    return (this.membershipID.equals(obj2.getMembershipID()) && this.threadID == obj2.getThreadID()
        && this.sequenceID == obj2.getSequenceID());
  }

  public int hashCode() {
    StringBuilder builder = new StringBuilder();
    builder.append(this.membershipID);
    builder.append(this.threadID);
    builder.append(this.sequenceID);
    return builder.toString().hashCode();
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("membershipID: " + membershipID);
    builder.append("; threadID: " + threadID);
    builder.append("; sequenceID: " + sequenceID);
    return builder.toString();
  }
}
