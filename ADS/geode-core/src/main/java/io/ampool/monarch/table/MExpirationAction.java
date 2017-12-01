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
package io.ampool.monarch.table;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

/**
 * Enum for expiration actions.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum MExpirationAction {
  /** When the row expires, it is invalidated. */
  INVALIDATE("INVALIDATE"),

  /** When the row expires, it is destroyed. */
  DESTROY("DESTROY");

  private String action;

  MExpirationAction(String action) {
    this.action = action;
  }

  public String getActionName() {
    return this.action;
  }

  public MExpirationAction getExpirationAction() {
    return MExpirationAction.valueOf(this.action);
  }
}
