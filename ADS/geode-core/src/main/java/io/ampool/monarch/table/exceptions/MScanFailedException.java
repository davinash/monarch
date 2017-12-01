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
 * A generic exception indicating that scan has failed
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MScanFailedException extends MException {

  public MScanFailedException(Throwable cause) {
    super(cause);
  }

  /**
   * Get the root cause of the exception.
   *
   * @return the root cause, if any; self otherwise
   */
  public Throwable getRootCause() {
    Throwable throwable = getCause();
    if (throwable.getCause() == null) {
      return throwable;
    }

    Throwable cause;
    while ((cause = throwable.getCause()) != null) {
      throwable = cause;
    }
    return throwable;
  }
}
