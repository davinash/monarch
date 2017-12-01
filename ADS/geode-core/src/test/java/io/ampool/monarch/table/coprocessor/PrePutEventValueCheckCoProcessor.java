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

import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.Put;

public class PrePutEventValueCheckCoProcessor extends MBaseRegionObserver {
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};

  @Override
  public void prePut(MObserverContext mObserverContext, Put put) {}

  @Override
  public void postPut(MObserverContext mObserverContext, Put put) {
    MCache geodeCache = MCacheFactory.getAnyInstance();
    if (mObserverContext.getEntryEvent().getOldValue() != null) {
      throw new RuntimeException("Event Value is changed !!!");
    }
  }
}
