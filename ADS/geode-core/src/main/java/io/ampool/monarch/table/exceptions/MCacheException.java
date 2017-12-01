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
package io.ampool.monarch.table.exceptions;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

/**
 * A generic exception indicating that a failure has happened while operating with Monarch cache
 *
 * @since 1.0.0.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class MCacheException extends MException {

  private static final long serialVersionUID = 6157641775524694873L;

  public MCacheException() {
    super();
  }

  /**
   * Creates a new <code>MCacheException</code> with the given detail message.
   */
  public MCacheException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>MCacheException</code> with the given detail message and cause.
   * 
   * @param message message
   * @param cause cause
   */
  public MCacheException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new <code>MCacheException</code> with the given cause and no detail message
   * 
   * @param cause cause
   */
  public MCacheException(Throwable cause) {
    super(cause);
  }

  /**
   * Returns the root cause of this <code>GemFireException</code> or <code>null</code> if the cause
   * is nonexistent or unknown.
   * 
   * @return Exception
   */
  public Throwable getRootCause() {
    if (this.getCause() == null) {
      return null;
    }
    Throwable root = this.getCause();
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root;
  }

}
