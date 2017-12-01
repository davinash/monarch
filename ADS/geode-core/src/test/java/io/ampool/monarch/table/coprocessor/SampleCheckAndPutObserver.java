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
import io.ampool.monarch.table.Put;

/**
 * Sample Put observer
 *
 * @since 0.2.0.0
 */

public class SampleCheckAndPutObserver extends MBaseRegionObserver {

  private int prePut;
  private int postPut;
  private int preDelete;
  private int postDelete;
  private int preCheckAndPut;
  private int postCheckAndPut;
  private int failedPostCheckAndPut;
  private int failedPreCheckAndPut;

  private int preCheckAndDelete;
  private int postCheckAndDelete;
  private int failedPostCheckAndDelete;
  private int failedPreCheckAndDelete;

  public SampleCheckAndPutObserver() {
    this.prePut = 0;
    this.postPut = 0;
    this.preDelete = 0;
    this.postDelete = 0;
    this.preCheckAndPut = 0;
    this.postCheckAndPut = 0;
    this.failedPostCheckAndPut = 0;
    this.failedPreCheckAndPut = 0;
    this.failedPostCheckAndDelete = 0;
    this.failedPreCheckAndDelete = 0;
    this.preCheckAndDelete = 0;
    this.postCheckAndDelete = 0;
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

  public int getPreCheckAndPut() {
    return preCheckAndPut;
  }

  public int getPostCheckAndPut() {
    return postCheckAndPut;
  }

  public int getFailedPostCheckAndPut() {
    return failedPostCheckAndPut;
  }

  public int getFailedPreCheckAndPut() {
    return failedPreCheckAndPut;
  }

  public int getFailedPostCheckAndDelete() {
    return failedPostCheckAndDelete;
  }

  public int getFailedPreCheckAndDelete() {
    return failedPreCheckAndDelete;
  }

  public int getPreCheckAndDelete() {
    return preCheckAndDelete;
  }

  public int getPostCheckAndDelete() {
    return postCheckAndDelete;
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
  public void preCheckAndPut(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Put put, boolean isCheckSuccessful) {
    if (isCheckSuccessful)
      preCheckAndPut++;
    else
      failedPreCheckAndPut++;
  }

  @Override
  public void postCheckAndPut(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Put put, boolean isCheckSuccessful) {
    if (isCheckSuccessful)
      postCheckAndPut++;
    else
      failedPostCheckAndPut++;
  }

  @Override
  public void preCheckAndDelete(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Delete delete, boolean isCheckSuccessful) {
    if (isCheckSuccessful)
      preCheckAndDelete++;
    else
      failedPreCheckAndDelete++;
  }

  @Override
  public void postCheckAndDelete(MObserverContext mObserverContext, byte[] rowKey,
      byte[] columnName, byte[] columnValue, Delete delete, boolean isCheckSuccessful) {
    if (isCheckSuccessful)
      postCheckAndDelete++;
    else
      failedPostCheckAndDelete++;
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }
}
