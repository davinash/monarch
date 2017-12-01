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

package io.ampool.monarch.table.coprocessor;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.Put;

/**
 * This class provide empty implementation of {@link MTableObserver}
 *
 * @since 0.2.0.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class MBaseRegionObserver implements MTableObserver {

  @Override
  public void postOpen() {

  }

  @Override
  public void preClose(MObserverContext mObserverContext) {

  }

  @Override
  public void postClose() {

  }

  @Override
  public void preGet(MObserverContext mObserverContext, Get get) {

  }

  @Override
  public void postGet(MObserverContext mObserverContext, Get get) {

  }

  @Override
  public void prePut(MObserverContext mObserverContext, Put put) {

  }

  @Override
  public void postPut(MObserverContext mObserverContext, Put put) {

  }

  @Override
  public void preDelete(MObserverContext mObserverContext, Delete delete) {

  }

  @Override
  public void postDelete(MObserverContext mObserverContext, Delete delete) {

  }

  @Override
  public void preCheckAndPut(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Put put, boolean isCheckSuccess) {

  }

  @Override
  public void postCheckAndPut(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Put put, boolean isCheckSuccessful) {

  }

  @Override
  public void preCheckAndDelete(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Delete delete, boolean isCheckSuccessful) {

  }

  @Override
  public void postCheckAndDelete(MObserverContext mObserverContext, byte[] rowKey,
      byte[] columnName, byte[] columnValue, Delete delete, boolean isCheckSuccessful) {

  }
}
