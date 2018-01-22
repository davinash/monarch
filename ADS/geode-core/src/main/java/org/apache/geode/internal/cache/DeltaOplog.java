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

import io.ampool.monarch.table.ftable.PersistenceDelta;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DeltaOplog extends Oplog {
  private static final Logger logger = LogService.getLogger();

  DeltaOplog(long oplogId, PersistentOplogSet parent, DirectoryHolder dirHolder) {
    super(oplogId, parent, dirHolder);
  }

  protected DeltaOplog(long oplogId, DirectoryHolder dirHolder, Oplog prevOplog) {
    super(oplogId, dirHolder, prevOplog);
  }

  DeltaOplog(long oplogId, PersistentOplogSet parent) {
    super(oplogId, parent);
  }


  protected Object getKey(long oplogKeyId, Version version, ByteArrayDataInput in) {
    Object key = getRecoveryMap().get(oplogKeyId);

    // if the key is not in the recover map, it's possible it
    // was previously skipped. Check the skipped bytes map for the key.
    if (key == null) {
      byte[] keyBytes = (byte[]) skippedKeyBytes.get(oplogKeyId);
      if (keyBytes != null) {
        key = deserializeKey(keyBytes, version, in);
      }
    }
    return key;
  }


  protected DiskEntry.RecoveredEntry handleToken(byte userBits, long oplogId, long offsetInOplog,
      long oplogKeyId) {
    DiskEntry.RecoveredEntry re = null;

    boolean isInvalid = EntryBits.isInvalid(userBits);
    boolean isTombstone = EntryBits.isTombstone(userBits);
    boolean isLocalInvalid = EntryBits.isLocalInvalid(userBits);
    Object value = null;
    int valueLength = 0;
    if (isInvalid || isTombstone || isLocalInvalid) {
      if (isLocalInvalid) {
        value = Token.LOCAL_INVALID;
        valueLength = 0;
      } else if (isInvalid) {
        value = Token.INVALID;
        valueLength = 0;
      } else if (isTombstone) {
        value = Token.TOMBSTONE;
        valueLength = 0;
      }
      re = new DiskEntry.RecoveredEntry(oplogKeyId, oplogId, offsetInOplog, userBits, valueLength,
          value);
    }
    return re;
  }

  @Override
  public DiskEntry.RecoveredEntry createRecoveredEntry(byte[] valueBytes, int valueLength,
      byte userBits, long oplogId, long offsetInOplog, long oplogKeyId, boolean recoverValue,
      long drid, Version version, ByteArrayDataInput in) {
    Object key = getKey(oplogKeyId, version, in);
    DiskRecoveryStore drs = getOplogSet().getCurrentlyRecovering(drid);
    DiskEntry de = drs.getDiskEntry(key);
    DiskEntry.RecoveredEntry re = null;
    PersistenceDelta oldValue = null;

    re = handleToken(userBits, oplogId, offsetInOplog, oplogKeyId);
    if (re != null) {
      return re;
    } else if (de != null && !Token.isTombstone(de._getValue())
        && EntryBits.isPersistenceDelta(userBits)) {
      if (de._getValue() instanceof VMCachedDeserializable) {
        oldValue = (PersistenceDelta) ((VMCachedDeserializable) de._getValue())
            .getDeserializedValue(null, null);
      } else {
        oldValue = (PersistenceDelta) de._getValue();
      }
      oldValue.fromPersistenceDelta(new DataInputStream(new ByteArrayInputStream(valueBytes)));
      EntryBits.setSerialized(userBits, false);
      EntryBits.setPersistenceDelta(userBits, false);
      re = new DiskEntry.RecoveredEntry(de.getDiskId().getKeyId(), oplogId,
          de.getDiskId().getOffsetInOplog(), userBits, de.getDiskId().getValueLength(), oldValue);
      getStats().incDeltaReads();
      getStats().incDeltaBytesRead(valueBytes.length, false);
      return re;
    } else {
      return super.createRecoveredEntry(valueBytes, valueLength, userBits, oplogId, offsetInOplog,
          oplogKeyId, recoverValue, drid, version, in);
    }

  }

  @Override
  protected boolean checkForEviction(boolean recoverValue, boolean lruLimitExceeded,
      boolean isOfflineCompacting) {
    return false;
  }

  @Override
  protected void basicModify(DiskRegionView dr, DiskEntry entry,
      DiskEntry.Helper.ValueWrapper value, byte userBits, boolean async, boolean calledByCompactor)
      throws IOException, InterruptedException {
    super.basicModify(dr, entry, value, userBits, async, calledByCompactor);
    if (EntryBits.isPersistenceDelta(userBits)) {
      getStats().incDeltaWrites();
      getStats().incDeltaBytesWritten(value.getLength(), false);
    }
  }

  @Override
  protected Oplog getOplog(long oplogId, DirectoryHolder dirHolder) {
    return new DeltaOplog(oplogId, dirHolder, this);
  }

  @Override
  boolean needsCompaction() {
    if (this.hasNoLiveValues() && this.doneAppending) {
      return true;
    }
    return false;
  }


  @Override
  protected DiskRegionInfo getOrCreateDRI(DiskRegionView dr) {
    DiskRegionInfo dri = getDRI(dr);
    if (dri == null) {
      dri = new DeltaDiskRegionInfoWithSet(dr);
      DiskRegionInfo oldDri = this.regionMap.putIfAbsent(dr.getId(), dri);
      if (oldDri != null) {
        dri = oldDri;
      }
    }
    return dri;
  }

  protected DiskRegionInfo getOrCreateDRI(long drId) {
    DiskRegionInfo dri = getDRI(drId);
    // SRI:: no getDRI(long)
    if (dri == null) {
      // TODO: passing null here?
      dri = new DeltaDiskRegionInfoWithSet(null);
      DiskRegionInfo oldDri = this.regionMap.putIfAbsent(drId, dri);
      if (oldDri != null) {
        dri = oldDri;
      }
    }
    return dri;
  }



  @Override
  protected void updateRecoveredEntryWithKey(byte[] valueBytes, int valueLength, byte userBits,
      long oplogId, long offsetInOplog, long oplogKeyId, boolean recoverValue, long drid,
      Version version, ByteArrayDataInput in, VersionTag tag, DiskEntry de, Object key) {
    DiskRecoveryStore drs = getOplogSet().getCurrentlyRecovering(drid);
    DiskEntry.RecoveredEntry re = null;
    PersistenceDelta oldValue = null;
    re = handleToken(userBits, oplogId, offsetInOplog, oplogKeyId);
    if (re != null) {
      if (tag != null) {
        re.setVersionTag(tag);
      }
      de = drs.updateRecoveredEntry(key, re);
      updateRecoveredEntry(drs.getDiskRegionView(), de, re);
      getStats().incRecoveredEntryUpdates();
    } else if (de != null && !Token.isTombstone(de._getValue())
        && EntryBits.isPersistenceDelta(userBits)) {
      if (de._getValue() instanceof VMCachedDeserializable) {
        oldValue = (PersistenceDelta) ((VMCachedDeserializable) de._getValue())
            .getDeserializedForReading();
      } else {
        oldValue = (PersistenceDelta) de._getValue();
      }
      oldValue.fromPersistenceDelta(new DataInputStream(new ByteArrayInputStream(valueBytes)));
      oldValue.resetPersistenceDelta();
      EntryBits.setSerialized(userBits, false);
      EntryBits.setPersistenceDelta(userBits, false);
      re = new DiskEntry.RecoveredEntry(de.getDiskId().getKeyId(), oplogId,
          de.getDiskId().getOffsetInOplog(), userBits, de.getDiskId().getValueLength(), oldValue);
      getStats().incDeltaReads();
      getStats().incDeltaBytesRead(valueBytes.length, false);
      if (tag != null) {
        re.setVersionTag(tag);
      }
      de = drs.updateRecoveredEntry(key, re);
      updateRecoveredEntry(drs.getDiskRegionView(), de, re);
      getStats().incRecoveredEntryUpdates();
    } else {
      re = super.createRecoveredEntry(valueBytes, valueLength, userBits, oplogId, offsetInOplog,
          oplogKeyId, recoverValue, drid, version, in);
      if (tag != null) {
        re.setVersionTag(tag);
      }
      de = drs.updateRecoveredEntry(key, re);
      updateRecoveredEntry(drs.getDiskRegionView(), de, re);
      getStats().incRecoveredEntryCreates();
    }
  }

  protected void decLiveCount(int delta) {
    this.totalLiveCount.addAndGet(-delta);
  }

  public static class DeltaDiskRegionInfoWithMap extends AbstractDiskRegionInfo {
    public DeltaDiskRegionInfoWithMap(DiskRegionView dr) {
      super(dr);
    }

    /**
     * A Map of live entries in this oplog. The map value will have the count of instances of an
     * entry in this oplog.
     */
    protected final Map<DiskEntry, AtomicInteger> liveEntries = new LinkedHashMap<>();

    @Override
    public void addLive(DiskEntry de) {
      synchronized (liveEntries) {
        AtomicInteger prevCount = liveEntries.get(de);
        if (prevCount == null) {
          liveEntries.put(de, new AtomicInteger(1));
        } else {
          prevCount.incrementAndGet();
        }
      }
    }

    @Override
    public void update(DiskEntry de) {
      //
    }

    @Override
    public void replaceLive(DiskEntry old, DiskEntry de) {
      synchronized (liveEntries) {
        AtomicInteger prevCount = liveEntries.get(old);
        if (prevCount == null) {
          liveEntries.put(de, new AtomicInteger(1));
        } else {
          this.liveEntries.remove(old);
          this.liveEntries.put(de, prevCount);
        }
      }
    }

    public boolean rmLive(DiskEntry de, Oplog oplog, boolean removeLiveCount) {
      if (removeLiveCount) {
        return rmLive(de, oplog);
      }
      return false;
    }


    @Override
    public boolean rmLive(DiskEntry de, Oplog oplog) {
      boolean removed = false;
      synchronized (liveEntries) {
        AtomicInteger prevCount = liveEntries.get(de);
        if (prevCount != null) {
          removed = true;
          ((DeltaOplog) oplog).decLiveCount(prevCount.get());
          liveEntries.remove(de);
        }
        return removed;
      }
    }

    @Override
    public DiskEntry getNextLiveEntry() {
      Iterator<DiskEntry> itr = this.liveEntries.keySet().iterator();
      return itr.next();
    }

    @Override
    public long clear(RegionVersionVector rvv) {
      synchronized (this.liveEntries) {
        long size = this.liveEntries.size();
        this.liveEntries.clear();
        return size;
      }
    }

    /**
     * Return true if we are the first guy to set it to true
     */
    @Override
    synchronized public boolean testAndSetUnrecovered() {
      boolean result = super.testAndSetUnrecovered();
      if (result) {
        this.liveEntries.clear();
      }
      return result;
    }

    public int addLiveEntriesToList(Oplog.KRFEntry[] liveEntries, int idx) {
      return liveEntries.length;
    }

    public void afterKrfCreated() {}
  }

  ///
  public static class DeltaDiskRegionInfoWithSet extends AbstractDiskRegionInfo {
    public DeltaDiskRegionInfoWithSet(DiskRegionView dr) {
      super(dr);
    }

    /**
     * A Map of live entries in this oplog. The map value will have the count of instances of an
     * entry in this oplog.
     */
    protected final Set<DiskEntry> liveEntries = new LinkedHashSet<>();

    @Override
    public void addLive(DiskEntry de) {
      synchronized (liveEntries) {
        liveEntries.add(de);
      }
    }

    @Override
    public void update(DiskEntry de) {
      //
    }

    @Override
    public void replaceLive(DiskEntry old, DiskEntry de) {
      synchronized (liveEntries) {
        liveEntries.add(de);
      }
    }

    public boolean rmLive(DiskEntry de, Oplog oplog, boolean removeLiveCount) {
      if (removeLiveCount) {
        return rmLive(de, oplog);
      }
      return false;
    }


    @Override
    public boolean rmLive(DiskEntry de, Oplog oplog) {
      synchronized (liveEntries) {
        boolean removed = liveEntries.remove(de);
        if (removed) {
          oplog.decLiveCount();
        }
        return removed;
      }
    }

    @Override
    public DiskEntry getNextLiveEntry() {
      Iterator<DiskEntry> itr = this.liveEntries.iterator();
      return itr.next();
    }

    @Override
    public long clear(RegionVersionVector rvv) {
      synchronized (this.liveEntries) {
        long size = this.liveEntries.size();
        this.liveEntries.clear();
        return size;
      }
    }

    /**
     * Return true if we are the first guy to set it to true
     */
    @Override
    synchronized public boolean testAndSetUnrecovered() {
      boolean result = super.testAndSetUnrecovered();
      if (result) {
        this.liveEntries.clear();
      }
      return result;
    }

    public int addLiveEntriesToList(Oplog.KRFEntry[] liveEntries, int idx) {
      return liveEntries.length;
    }

    public void afterKrfCreated() {}
  }
}
