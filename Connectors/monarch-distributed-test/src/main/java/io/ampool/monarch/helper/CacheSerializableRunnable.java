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

import io.ampool.monarch.RepeatableRunnable;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheRuntimeException;
import io.ampool.monarch.SerializableRunnable;
import junit.framework.AssertionFailedError;

public abstract class CacheSerializableRunnable
    extends SerializableRunnable
    implements RepeatableRunnable
{

  /**
   * Creates a new <code>CacheSerializableRunnable</code> with the
   * given name
   */
  public CacheSerializableRunnable(String name) {
    super(name);
  }

  /**
   * Invokes the {@link #run2} method and will wrap any {@link
   * CacheException} thrown by <code>run2</code> in a {@link
   * CacheSerializableRunnableException}.
   */
  public final void run() {
    try {
      run2();

    } catch (CacheException ex) {
      String s = "While invoking \"" + this + "\"";
      throw new CacheSerializableRunnableException(s, ex);
    }
  }

  /**
   * Invokes the {@link #run} method.  If AssertionFailedError is thrown,
   * and repeatTimeoutMs is >0, then repeat the {@link #run} method until
   * it either succeeds or repeatTimeoutMs milliseconds have passed.  The
   * AssertionFailedError is only thrown to the caller if the last run
   * still throws it.
   */
  public final void runRepeatingIfNecessary(long repeatTimeoutMs) {
    long start = System.currentTimeMillis();
    AssertionFailedError lastErr = null;
    do {
      try {
        lastErr = null;
        this.run();
        CacheFactory.getAnyInstance().getLogger().fine("Completed " + this);
      } catch (AssertionFailedError err) {
        CacheFactory.getAnyInstance().getLogger().fine("Repeating " + this);
        lastErr = err;
        try {
          Thread.sleep(50);
        } catch (InterruptedException ex) {
          throw new TestException("interrupted", ex);
        }
      }
    } while (lastErr != null && System.currentTimeMillis() - start < repeatTimeoutMs);
    if (lastErr != null) throw lastErr;
  }

  /**
   * A {@link SerializableRunnable#run run} method that may throw a
   * {@link CacheException}.
   */
  public abstract void run2() throws CacheException;

  /////////////////////////  Inner Classes  /////////////////////////

  /**
   * An exception that wraps a {@link CacheException}
   */
  public static class CacheSerializableRunnableException
      extends CacheRuntimeException {

    public CacheSerializableRunnableException(String message,
                                              Throwable cause) {
      super(message, cause);
    }

  }

}

