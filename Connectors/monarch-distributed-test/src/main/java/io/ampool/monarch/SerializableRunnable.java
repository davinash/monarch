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

package io.ampool.monarch;

import java.io.Serializable;

public abstract class SerializableRunnable
    implements Serializable, Runnable {

  private static final long serialVersionUID = 7584289978241650456L;

  private String name;

  public SerializableRunnable() {
    this.name = null;
  }

  /**
   * This constructor lets you do the following:
   * <p/>
   * <PRE>
   * vm.invoke(new SerializableRunnable("Do some work") {
   * public void run() {
   * // ...
   * }
   * });
   * </PRE>
   */
  public SerializableRunnable(String name) {
    this.name = name;
  }

  public void setName(String newName) {
    this.name = newName;
  }

  public String getName() {
    return this.name;
  }

  public String toString() {
    if (this.name != null) {
      return "\"" + this.name + "\"";

    } else {
      return super.toString();
    }
  }

}
