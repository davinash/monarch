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

import io.ampool.monarch.table.Put;

/**
 * Sample Put observer
 *
 * @since 0.2.0.0
 */

public class SamplePutObserver extends MBaseRegionObserver {

  private int prePut;
  private int postPut;
  private int preDelete;
  private int postDelete;

  public SamplePutObserver() {
    this.prePut = 0;
    this.postPut = 0;
    this.preDelete = 0;
    this.postDelete = 0;
  }

  public int getTotalPrePutCount() {
    return prePut;
  }

  public int getTotalPostPutCount() {
    return postPut;
  }

  public int getTotalPreDeleteCount() {
    return preDelete;
  }

  public int getTotalPostDeleteCount() {
    return postDelete;
  }

  @Override
  public void prePut(MObserverContext mObserverContext, Put put) {
    prePut++;
  }

  @Override
  public void postPut(MObserverContext mObserverContext, Put put) {
    postPut++;
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }
}
