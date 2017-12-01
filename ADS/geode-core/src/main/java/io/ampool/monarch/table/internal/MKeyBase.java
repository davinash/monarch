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
package io.ampool.monarch.table.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import io.ampool.monarch.table.Bytes;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;

/**
 * The basic (lean) implementation of the key. It contains only bytes of the key and can be compared
 * or equated successfully with any other key implementing {@link IMKey} as long as same byte-arrays
 * are used.
 *
 */
public class MKeyBase implements IMKey, DataSerializableFixedID, Serializable {
  private byte[] data;

  public MKeyBase() {
    //// For DataSerializer..
  }

  public MKeyBase(final byte[] bytes) {
    this.data = bytes;
  }

  public void setBytes(final byte[] bytes) {
    this.data = bytes;
  }

  @Override
  public byte[] getBytes() {
    return this.data;
  }

  @Override
  public String toString() {
    return getClass().getName() + "=" + Arrays.toString(this.data);
  }

  /**
   * Hash-code of the underlying byte-array.
   *
   * @return the hash-code of the bytes data
   */
  @Override
  public int hashCode() {
    return Arrays.hashCode(this.data);
  }

  /**
   * Equality check.. all objects of IMKey type and same byte-array are equal.
   *
   * @param other the other object
   * @return true if other object is of IMKey type and has same bytes; false otherwise
   */
  @Override
  public boolean equals(Object other) {
    return this == other
        || (other instanceof IMKey && Arrays.equals(this.data, ((IMKey) other).getBytes()));
  }

  @Override
  public int getDSFID() {
    return AMPL_MKEYBASE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(data, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.data = DataSerializer.readByteArray(in);
  }

  /**
   * Compares this object with the specified object for order. Returns a negative integer, zero, or
   * a positive integer as this object is less than, equal to, or greater than the specified object.
   * <p>
   * <p>
   * The implementor must ensure <tt>sgn(x.compareTo(y)) ==
   * -sgn(y.compareTo(x))</tt> for all <tt>x</tt> and <tt>y</tt>. (This implies that
   * <tt>x.compareTo(y)</tt> must throw an exception iff <tt>y.compareTo(x)</tt> throws an
   * exception.)
   * <p>
   * <p>
   * The implementor must also ensure that the relation is transitive:
   * <tt>(x.compareTo(y)&gt;0 &amp;&amp; y.compareTo(z)&gt;0)</tt> implies
   * <tt>x.compareTo(z)&gt;0</tt>.
   * <p>
   * <p>
   * Finally, the implementor must ensure that <tt>x.compareTo(y)==0</tt> implies that
   * <tt>sgn(x.compareTo(z)) == sgn(y.compareTo(z))</tt>, for all <tt>z</tt>.
   * <p>
   * <p>
   * It is strongly recommended, but <i>not</i> strictly required that
   * <tt>(x.compareTo(y)==0) == (x.equals(y))</tt>. Generally speaking, any class that implements
   * the <tt>Comparable</tt> interface and violates this condition should clearly indicate this
   * fact. The recommended language is "Note: this class has a natural ordering that is inconsistent
   * with equals."
   * <p>
   * <p>
   * In the foregoing description, the notation <tt>sgn(</tt><i>expression</i><tt>)</tt> designates
   * the mathematical <i>signum</i> function, which is defined to return one of <tt>-1</tt>,
   * <tt>0</tt>, or <tt>1</tt> according to whether the value of <i>expression</i> is negative, zero
   * or positive.
   *
   * @param o the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
   *         or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException if the specified object's type prevents it from being compared to
   *         this object.
   */
  @Override
  public int compareTo(IMKey o) {
    return Bytes.compareTo(this.data, o.getBytes());
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
