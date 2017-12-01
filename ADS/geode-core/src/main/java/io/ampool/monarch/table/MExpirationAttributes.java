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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataSerializable;

/**
 * Immutable parameter object for accessing and setting the attributes associated with expiration
 * for the MTable rows. If the expiration action is not specified, it defaults to
 * {@link MExpirationAction#DESTROY}. If the timeout is not specified, it defaults to zero (which
 * means to never timeout).
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MExpirationAttributes implements VersionedDataSerializable {

  /**
   * The number of seconds since this row was created before it expires.
   */
  private int timeout;

  /**
   * The action that should take place when this row expires.
   */
  private MExpirationAction action;

  /**
   * Constructs a default <code>MExpirationAttributes</code>, which indicates no expiration will
   * take place.
   */
  public MExpirationAttributes() {
    this(0, MExpirationAction.DESTROY);
  }

  public MExpirationAttributes(int timeout) {
    this(timeout, MExpirationAction.DESTROY);
  }

  public MExpirationAttributes(int timeout, MExpirationAction action) {
    this.timeout = timeout;
    this.action = action;
  }

  public int getTimeout() {
    return timeout;
  }

  public MExpirationAction getAction() {
    return action;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeInteger(timeout, out);
    DataSerializer.writeEnum(action, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.timeout = DataSerializer.readInteger(in);
    this.action = DataSerializer.readEnum(MExpirationAction.class, in);
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
