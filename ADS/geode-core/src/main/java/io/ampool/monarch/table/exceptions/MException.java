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
 * A generic exception indicating that a failure has happened while operating with Monarch system
 * 
 * @since 0.2.0.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class MException extends RuntimeException {

  private static final long serialVersionUID = -5205644901262051330L;

  /**
   * Create a new instance of MException without a detail message or cause.
   */
  public MException() {}

  /**
   *
   * Create a new instance of MException with a detail message
   * 
   * @param message the detail message
   */
  public MException(String message) {
    super(message);
  }

  /**
   * Create a new instance of MException with a detail message and cause
   * 
   * @param message the detail message
   * @param cause the cause
   */
  public MException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create a new instance of MException with a cause
   * 
   * @param cause the cause
   */
  public MException(Throwable cause) {
    super(cause);
  }

}
