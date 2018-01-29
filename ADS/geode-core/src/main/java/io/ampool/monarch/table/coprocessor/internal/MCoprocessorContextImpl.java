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

import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableRegion;
import io.ampool.monarch.table.coprocessor.MCoprocessorContext;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.coprocessor.MResultSender;
import io.ampool.monarch.table.internal.MTableImpl;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;

/**
 * MCoprocessorContextImpl
 *
 * @since 0.2.0.0
 */

public class MCoprocessorContextImpl implements MCoprocessorContext {

  private FunctionContext context;

  public MCoprocessorContextImpl(FunctionContext functionContext) {
    context = functionContext;
  }

  public final MExecutionRequest getRequest() {
    return (MExecutionRequest) context.getArguments();
  }

  public String getCoprocessorId() {
    return context.getFunctionId();
  }

  public String toString() {
    return context.toString();
  }

  public <T> MResultSender<T> getResultSender() {
    return new MResultSenderImpl(context.getResultSender());
  }

  public boolean isPossibleDuplicate() {
    return context.isPossibleDuplicate();
  }

  public LogWriter getLogger() {
    return InternalDistributedSystem.getAnyInstance().getLogWriter();
  }

  public <K, V> Region<K, V> getDataSet() {
    if (context instanceof RegionFunctionContextImpl) {
      return ((RegionFunctionContextImpl) this.context).getDataSet();
    }
    return null;
  }

  @Override
  public MTable getTable() {
    RegionFunctionContextImpl rfci = (RegionFunctionContextImpl) this.context;
    MTableImpl inernalTableImpl =
        (MTableImpl) MCacheFactory.getAnyInstance().getTable(rfci.getDataSet().getName());
    // GEN-639 Removed setting bucketid since we are anyway not using this API
    // and it should be handled by other stuff
    // inernalTableImpl.setScannerBucketId(rfci.getTargetBucketId());
    return inernalTableImpl;
  }

  @Override
  public MTableRegion getMTableRegion() {
    RegionFunctionContextImpl rfci = (RegionFunctionContextImpl) this.context;
    MonarchCacheImpl cache = (MonarchCacheImpl) MCacheFactory.getAnyInstance();
    return cache.getTableRegion(rfci.getDataSet().getName(), rfci.getTargetBucketId());
  }

  // only for test Use
  public RegionFunctionContextImpl getNativeContext() {
    return (RegionFunctionContextImpl) this.context;
  }
}
