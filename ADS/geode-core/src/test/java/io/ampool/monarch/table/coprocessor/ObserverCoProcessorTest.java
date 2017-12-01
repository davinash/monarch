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

import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.Put;

public class ObserverCoProcessorTest extends MBaseRegionObserver {
  @Override
  public void preGet(MObserverContext mObserverContext, Get get) {
    System.out.println("ObserverCoProcessorTest.preGet");
  }

  @Override
  public void prePut(MObserverContext mObserverContext, Put put) {
    System.out.println("ObserverCoProcessorTest.prePut");
  }

  @Override
  public void postGet(MObserverContext mObserverContext, Get get) {
    System.out.println("ObserverCoProcessorTest.postGet");
  }

  @Override
  public void postPut(MObserverContext mObserverContext, Put put) {
    System.out.println("ObserverCoProcessorTest.postPut");
  }
}
