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

import java.io.Serializable;

class CustomType implements Serializable {
  int x = 0;
  char ch = 'c';

  public CustomType() {
    this.x = 0;
    this.ch = 'c';
  }

  /*
   * @Override public void toData(final DataOutput out) throws IOException { out.writeInt(x);
   * out.writeChar(ch); }
   * 
   * @Override public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
   * this.x = in.readInt(); this.ch = in.readChar(); }
   */

  enum Category {
    Basic, List, Map, Struct, Union
  }
}
