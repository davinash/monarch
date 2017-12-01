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
package org.apache.geode.internal.cache;

import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.*;
import org.apache.geode.internal.InternalEntity;

import java.io.Serializable;
import java.util.Set;

public final class TableIsEmptyFunction extends FunctionAdapter implements InternalEntity {

  /**
   * Serial version UID.
   */
  private static final long serialVersionUID = 2874806981897896300L;

  @Override
  public String getId() {
    return TableIsEmptyFunction.class.getName();
  }

  /**
   * NOTE: This method should return true so that the function would not be sent to nodes which
   * don't have any primary data.
   */
  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public void execute(FunctionContext ctx) {
    ResultSender<Serializable> sender = ctx.getResultSender();

    if (!(ctx instanceof RegionFunctionContext)) {
      sender.sendException(new FunctionException("Invalid input. execution context not valid."));

      return;
    }

    RegionFunctionContext regionCtx = (RegionFunctionContext) ctx;

    Region<Object, Object> region = regionCtx.getDataSet();

    Region<Object, Object> localRegion = region;

    PartitionedRegion pr = (PartitionedRegion) region;
    boolean isEmpty = true;

    Set<BucketRegion> allLocalPrimaryBucketRegions =
        pr.getDataStore().getAllLocalPrimaryBucketRegions();

    for (BucketRegion bucket : allLocalPrimaryBucketRegions) {
      if (bucket.getBucketAdvisor().isPrimary()) {
        Object iMap = bucket.getRegionMap().getInternalMap();
        if (iMap instanceof RowTupleConcurrentSkipListMap) {
          RowTupleConcurrentSkipListMap realMap = (RowTupleConcurrentSkipListMap) iMap;
          if (!realMap.isEmpty()) {
            isEmpty = false;
            break;
          }
        } else {
          if (!bucket.isEmpty()) {
            isEmpty = false;
            break;
          }
        }
      }
    }
    sender.lastResult(isEmpty);
  }
}

