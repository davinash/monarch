/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.cache;

import java.util.Comparator;
import java.util.TreeSet;

public class DeltaPersistentOplogSet extends PersistentOplogSet {
  public DeltaPersistentOplogSet(DiskStoreImpl parent) {
    super(parent);
  }

  @Override
  protected Oplog getOplog(long oplogID, PersistentOplogSet oplogSet) {
    return new DeltaOplog(oplogID, oplogSet);
  }

  @Override
  protected Oplog getOplog(long oplogId, PersistentOplogSet parent, DirectoryHolder dirHolder) {
    return new DeltaOplog(oplogId, parent, dirHolder);
  }

  @Override
  protected TreeSet<Oplog> getSortedOplogs() {
    TreeSet<Oplog> result = new TreeSet<Oplog>(new Comparator() {
      public int compare(Object arg0, Object arg1) {
        return Long.signum((((Oplog) arg1).getOplogId() - ((Oplog) arg0).getOplogId()) * -1);
      }
    });
    for (Oplog oplog : getAllOplogs()) {
      if (oplog != null) {
        result.add(oplog);
      }
    }
    return result;
  }

  @Override
  public void remove(LocalRegion region, DiskEntry entry, boolean async, boolean isClear) {
    getChild().remove(region, entry, async, isClear);
    if (entry.isRemovedPhase2()) {
      for (Oplog log : getSortedOplogs()) {
        log.rmLive(region.getDiskRegion(), entry, true);
        if (log.needsCompaction()) {
          log.getOplogSet().addToBeCompacted(log);
        }
      }
    }
  }

  @Override
  protected boolean recoverValues() {
    return true;
  }


}
