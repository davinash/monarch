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

package io.ampool.monarch.table.ftable;

import java.io.DataInput;
import java.io.DataOutput;

public interface PersistenceDelta {

  /**
   * Returns true if this object has pending changes which need to be written to persistence layer.
   */
  boolean hasPersistenceDelta();

  /**
   * This method is invoked by the persistence to get the delta of the object which need to be
   * written to persistence layer.
   */
  void toPersistenceDelta(DataOutput out);


  /**
   * This method applies delta to the object
   */
  void fromPersistenceDelta(DataInput in);

  /**
   * This method resets the delta read and
   */
  void resetPersistenceDelta();

}
