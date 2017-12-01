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

package io.ampool.monarch.table.coprocessor.internal;

import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.coprocessor.MObserverContext;
import io.ampool.monarch.table.coprocessor.MTableObserver;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.MOpInfo;
import io.ampool.monarch.table.internal.MOperation;
import io.ampool.monarch.table.internal.MTableStorageFormatter;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.StorageFormatter;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * PostOpRegionObserver
 *
 * @since 0.2.0.0
 */

public class PostOpRegionObserver implements CacheListener {
  private static final Logger logger = LogService.getLogger();

  public List<MTableObserver> getmObserverInstanceList(String regionName) {
    return MCoprocessorManager.getObserversList(regionName);
  }

  public PostOpRegionObserver() {}

  @Override
  public void afterCreate(EntryEvent event) {
    afterUpdate(event);
  }

  @Override
  public void afterUpdate(EntryEvent event) {
    final MOpInfo opInfo = ((EntryEventImpl) event).getOpInfo();
    if (opInfo == null) {
      logger.debug("No operation information found; nothing to process.. leaving.");
      return;
    }
    String regionName = event.getRegion().getName();
    logger.debug("AfterUpdate: Table= {}, OpInfo= {}", regionName, opInfo);

    MObserverContext mObserverContext =
        MCoprocessorUtils.getMObserverContextWithEvent(regionName, event);
    List<MTableObserver> observers = MCoprocessorManager.getObserversList(regionName);
    Object key = event.getKey();
    byte[] keyBytes = IMKey.getBytes(key);
    switch (opInfo.getOp()) {
      case CHECK_AND_PUT:
        beforeUpdateCheckAndPutOp(keyBytes, opInfo, event, mObserverContext, observers);
        break;
      case CHECK_AND_DELETE:
        beforeUpdateCheckAndDeleteOp(keyBytes, opInfo, event, mObserverContext, observers);
        break;
      case DELETE:
        beforeUpdateDeleteOp(keyBytes, opInfo, event, mObserverContext, observers);
        break;
      case PUT:
        beforeUpdatePutOp(keyBytes, opInfo, event, mObserverContext, observers);
        break;
      default:
        logger.warn("Incorrect operation: {}", opInfo.getOp());
    }
  }

  @Override
  public void afterInvalidate(EntryEvent event) {
    // Do nothing
  }

  @Override
  public void afterDestroy(EntryEvent event) {
    final MOpInfo opInfo = ((EntryEventImpl) event).getOpInfo();
    if (opInfo == null) {
      logger.debug("No operation information found; nothing to process.. leaving.");
      return;
    }
    String regionName = event.getRegion().getName();
    logger.debug("AfterDestroy: Table= {}, OpInfo= {}", regionName, opInfo);

    MObserverContext mObserverContext =
        MCoprocessorUtils.getMObserverContextWithEvent(regionName, event);
    List<MTableObserver> observers = MCoprocessorManager.getObserversList(regionName);
    Object key = event.getKey();
    byte[] keyBytes = IMKey.getBytes(key);

    if (opInfo.getOp() == MOperation.CHECK_AND_DELETE) {
      beforeUpdateCheckAndDeleteOp(keyBytes, opInfo, event, mObserverContext, observers);
    } else {
      beforeUpdateDeleteOp(keyBytes, opInfo, event, mObserverContext, observers);
    }
  }

  @Override
  public void afterRegionInvalidate(RegionEvent event) {
    // DO Nothing
  }

  @Override
  public void afterRegionDestroy(RegionEvent event) {
    String regionName = event.getRegion().getName();
    List<MTableObserver> mObserverInstanceList = MCoprocessorManager.getObserversList(regionName);
    for (MTableObserver observer : mObserverInstanceList) {
      if (observer != null) {
        observer.postClose();
      }
    }
    // Remove these observers from MCoprocessorManagers list
    MCoprocessorManager.unRegisterCoprocessors(regionName);
  }

  @Override
  public void afterRegionClear(RegionEvent event) {

  }

  @Override
  public void afterRegionCreate(RegionEvent event) {
    String regionName = event.getRegion().getName();
    List<MTableObserver> mObserverInstanceList = MCoprocessorManager.getObserversList(regionName);
    for (MTableObserver observer : mObserverInstanceList) {
      if (observer != null)
        observer.postOpen();
    }
  }

  @Override
  public void afterRegionLive(RegionEvent event) {

  }

  @Override
  public void close() {

  }

  /** Utility helper methods.. **/
  private void beforeUpdateCheckAndPutOp(byte[] keyBytes, MOpInfo opInfo, EntryEvent event,
      MObserverContext mObserverContext, List<MTableObserver> observers) {
    String regionName = event.getRegion().getName();
    MTableDescriptor tableDescriptor =
        MCacheFactory.getAnyInstance().getMTableDescriptor(regionName);

    final MOpInfo.MCondition condition = opInfo.getCondition();
    byte[] checkColumnValue = (byte[]) condition.getColumnValue();
    MColumnDescriptor columnDescriptorByIndex =
        tableDescriptor.getColumnDescriptorByIndex(condition.getColumnId());

    StorageFormatter storageFormatter = MTableUtils.getStorageFormatter(tableDescriptor);

    boolean isCheckPassed = ((MTableStorageFormatter) storageFormatter).checkValue(tableDescriptor,
        event.getOldValue(), checkColumnValue, condition.getColumnId());

    Put put = MCoprocessorUtils.constructMPut(keyBytes, opInfo.getColumnList(), event.getNewValue(),
        event.getRegion().getName(), true);
    for (MTableObserver observer : observers) {
      if (observer != null) {
        observer.postCheckAndPut(mObserverContext, keyBytes,
            columnDescriptorByIndex.getColumnName(), checkColumnValue, put, isCheckPassed);
      }
    }
  }

  private void beforeUpdatePutOp(byte[] keyBytes, MOpInfo opInfo, EntryEvent event,
      MObserverContext mObserverContext, List<MTableObserver> observers) {
    for (MTableObserver observer : observers) {
      if (observer != null) {
        Object newValue = event.getNewValue();
        List data = null;
        if (newValue instanceof List) {
          data = ((List) newValue);
        } else if (newValue instanceof VMCachedDeserializable) {
          Object deserializedValue =
              ((VMCachedDeserializable) newValue).getDeserializedForReading();
          if (deserializedValue instanceof List) {
            data = (List) deserializedValue;
          }
        } else {
          Put put = MCoprocessorUtils.constructMPut(keyBytes, opInfo.getColumnList(), newValue,
              event.getRegion().getName(), true);
          observer.postPut(mObserverContext, put);
        }

        if (data != null) {
          // data is list so iterate and call put n times
          data.forEach(value -> {
            Put put = MCoprocessorUtils.constructMPut(keyBytes, opInfo.getColumnList(), value,
                event.getRegion().getName(), true);
            observer.postPut(mObserverContext, put);
          });
        }
      }
    }
  }

  private void beforeUpdateDeleteOp(byte[] keyBytes, MOpInfo opInfo, EntryEvent event,
      MObserverContext mObserverContext, List<MTableObserver> observers) {
    Delete del = MCoprocessorUtils.constructMDelete(keyBytes, opInfo.getColumnList(),
        event.getRegion().getName());
    for (MTableObserver observer : observers) {
      if (observer != null) {
        observer.postDelete(mObserverContext, del);
      }
    }
  }

  private void beforeUpdateCheckAndDeleteOp(byte[] keyBytes, MOpInfo opInfo, EntryEvent event,
      MObserverContext mObserverContext, List<MTableObserver> observers) {
    String regionName = event.getRegion().getName();
    MTableDescriptor tableDescriptor =
        MCacheFactory.getAnyInstance().getMTableDescriptor(regionName);

    final MOpInfo.MCondition condition = opInfo.getCondition();
    byte[] checkColumnValue = (byte[]) condition.getColumnValue();
    MColumnDescriptor columnDescriptorByIndex =
        tableDescriptor.getColumnDescriptorByIndex(condition.getColumnId());

    StorageFormatter storageFormatter = MTableUtils.getStorageFormatter(tableDescriptor);

    boolean isCheckPassed = ((MTableStorageFormatter) storageFormatter).checkValue(tableDescriptor,
        event.getOldValue(), checkColumnValue, condition.getColumnId());

    Delete del = MCoprocessorUtils.constructMDelete(keyBytes, opInfo.getColumnList(),
        event.getRegion().getName());
    for (MTableObserver observer : observers) {
      if (observer != null) {
        observer.postCheckAndDelete(mObserverContext, keyBytes,
            columnDescriptorByIndex.getColumnName(), checkColumnValue, del, isCheckPassed);
      }
    }
  }
}
