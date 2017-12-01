/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.ampool.monarch.helper;

import org.apache.geode.cache.CacheCallback;

public abstract class TestCacheCallback implements CacheCallback {
  // differentiate between callback being closed and callback
  // event methods being invoked
  private volatile boolean isClosed = false;

  /** Was a callback event method invoked? */
  volatile boolean invoked = false;

  volatile protected Throwable callbackError = null;

  /**
   * Returns whether or not one of this <code>CacheListener</code>
   * methods was invoked.  Before returning, the <code>invoked</code>
   * flag is cleared.
   */
  public boolean wasInvoked() {
    checkForError();
    boolean value = this.invoked;
    if (value) {
      this.invoked = false;
    }
    return value;
  }
  /**
   * Waits up to timeoutMs milliseconds for the listener to be invoked.
   * Calls wasInvoked and returns its value
   */
  public boolean waitForInvocation(int timeoutMs) {
    return waitForInvocation(timeoutMs, 200);
  }
  public boolean waitForInvocation(int timeoutMs, long interval) {
    if (!this.invoked) {
      DistributedTestCase.WaitCriterion ev = new DistributedTestCase.WaitCriterion() {
        public boolean done() {
          return invoked;
        }
        public String description() {
          return "listener was never invoked";
        }
      };
      DistributedTestCase.waitForCriterion(ev, timeoutMs, interval, true);
    }
    return wasInvoked();
  }

  public boolean isClosed() {
    checkForError();
    return this.isClosed;
  }

  public final void close() {
    this.isClosed = true;
    close2();
  }

  /**
   * This method will do nothing.  Note that it will not throw an
   * exception.
   */
  public void close2() {

  }

  private void checkForError() {
    if (this.callbackError != null) {
      AssertionError  error = new AssertionError("Exception occurred in callback");
      error.initCause(this.callbackError);
      throw error;
    }
  }
}

