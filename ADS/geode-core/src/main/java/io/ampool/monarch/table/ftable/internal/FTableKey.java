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

package io.ampool.monarch.table.ftable.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;

public class FTableKey implements DataSerializableFixedID, Serializable {

  public FTableKey() {

  }

  @Override
  public int hashCode() {
    return this.distributionHash;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof FTableKey && ((FTableKey) obj).distributionHash == this.distributionHash;
  }

  /**
   * The hash-code to be used for key distribution in Geode is different from the actual key. This
   * is used for the cases where distribution is done on some column-value rather than the key
   * itself.
   */
  private int distributionHash = 0;

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return AMPL_FTABLEKEY;
  }

  @Override
  public void toData(final DataOutput out) throws IOException {
    out.writeInt(distributionHash);
  }

  @Override
  public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
    this.distributionHash = in.readInt();
  }

  /**
   * Get the hash-code for distribution of the key.
   *
   * @return the hash-code used for key distribution
   */
  public int getDistributionHash() {
    return distributionHash;
  }

  /**
   * Set the hash-code used for distribution of the key.
   *
   * @param distributionHash the hash-code used for key distribution
   */
  public void setDistributionHash(int distributionHash) {
    this.distributionHash = distributionHash;
  }


}
