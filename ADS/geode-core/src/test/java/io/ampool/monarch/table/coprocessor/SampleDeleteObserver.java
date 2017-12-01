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

import io.ampool.monarch.table.Delete;

/**
 * Sample Put observer
 *
 * @since 0.2.0.0
 */

public class SampleDeleteObserver extends MBaseRegionObserver {

  private int preDelete;
  private int postDelete;

  public SampleDeleteObserver() {
    this.preDelete = 0;
    this.postDelete = 0;
  }


  public int getTotalPreDeleteCount() {
    return preDelete;
  }

  public int getTotalPostDeleteCount() {
    return postDelete;
  }

  @Override
  public void preDelete(MObserverContext mObserverContext, Delete delete) {
    preDelete++;
  }

  @Override
  public void postDelete(MObserverContext mObserverContext, Delete delete) {
    postDelete++;
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }
}
