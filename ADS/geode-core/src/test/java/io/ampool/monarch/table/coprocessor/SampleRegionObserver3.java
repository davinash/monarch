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
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.Put;

/**
 * SampleRegionObserver3
 *
 * @since 0.2.0.0
 */

public class SampleRegionObserver3 extends MBaseRegionObserver {

  private int prePut;
  private int postPut;
  private int preDelete;
  private int postDelete;
  private int preGet;
  private int postGet;

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

  public int getTotalPreGetCount() {
    return preGet;
  }

  public int getTotalPostGetCount() {
    return postGet;
  }

  public SampleRegionObserver3() {
    prePut = 0;
    postPut = 0;
    preDelete = 0;
    postDelete = 0;
    this.preGet = 0;
    this.postGet = 0;
  }

  @Override
  public void prePut(MObserverContext mObserverContext, Put put) {
    prePut++;
    System.out.println("RRRROOOO SampleRegionObserver3 - prePut() called");
  }

  @Override
  public void postPut(MObserverContext mObserverContext, Put put) {
    postPut++;
    System.out.println("RRRROOOO SampleRegionObserver3 - postPut() called");
  }

  @Override
  public void preDelete(MObserverContext mObserverContext, Delete delete) {
    preDelete++;
    System.out.println("RRRROOOO SampleRegionObserver3 - preDelete() called");
  }

  @Override
  public void postDelete(MObserverContext mObserverContext, Delete delete) {
    postDelete++;
    System.out.println("RRRROOOO SampleRegionObserver3 - postDelete() called");
  }

  @Override
  public String toString() {
    return "io.ampool.monarch.table.coprocessor.SampleRegionObserver3";
  }

  @Override
  public void preGet(MObserverContext context, Get get) {
    preGet++;
    System.out.println("RRRROOOO SampleRegionObserver3 - preGet() called, total = " + preGet);
  }

  @Override
  public void postGet(MObserverContext context, Get get) {
    postGet++;
    System.out.println("RRRROOOO SampleRegionObserver3 - preGet() called total = " + postGet);
  }
}
