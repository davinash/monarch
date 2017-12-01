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

import io.ampool.monarch.table.ftable.PartitionResolver;
import io.ampool.monarch.table.ftable.internal.FTableKey;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
import org.apache.geode.cache.Cache;

public class MPartitionResolver<K, V> extends PartitionResolver<K, V>
    implements DataSerializableFixedID {
  public static final String NAME = MPartitionResolver.class.getSimpleName();
  public static final MPartitionResolver<IMKey, Object> DEFAULT_RESOLVER =
      new MPartitionResolver<>();

  public MPartitionResolver() {
    //// for DataSerializable
  }

  @Override
  public Object getDistributionObject(EntryOperation entryOp) {
    Object key = entryOp.getKey();
    return key instanceof FTableKey ? ((FTableKey) key).getDistributionHash() : key;
  }

  /**
   * Returns the name of the PartitionResolver
   *
   * @return String name
   */
  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Called when the region containing this callback is closed or destroyed, when the cache is
   * closed, or when a callback is removed from a region using an <code>AttributesMutator</code>.
   * <p>
   * <p>
   * Implementations should cleanup any external resources such as database connections. Any runtime
   * exceptions this method throws will be logged.
   * <p>
   * <p>
   * It is possible for this method to be called multiple times on a single callback instance, so
   * implementations must be tolerant of this.
   *
   * @see Cache#close()
   * @see org.apache.geode.cache.Region#close
   * @see org.apache.geode.cache.Region#localDestroyRegion()
   * @see org.apache.geode.cache.Region#destroyRegion()
   * @see org.apache.geode.cache.AttributesMutator
   */
  @Override
  public void close() {

  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  @Override
  public int getDSFID() {
    return AMPL_M_PARTITION_RESOLVER;
  }

  /**
   * Writes the state of this object as primitive data to the given <code>DataOutput</code>.<br>
   * <br>
   * Note: For rolling upgrades, if there is a change in the object format from previous version,
   * add a new toDataPre_GFE_X_X_X_X() method and add an entry for the current {@link Version} in
   * the getSerializationVersions array of the implementing class. e.g. if msg format changed in
   * version 80, create toDataPre_GFE_8_0_0_0, add Version.GFE_80 to the getSerializationVersions
   * array and copy previous toData contents to this newly created toDataPre_GFE_X_X_X_X() method.
   *
   * @param out the data output stream
   * @throws IOException A problem occurs while writing to <code>out</code>
   */
  @Override
  public void toData(DataOutput out) throws IOException {}

  /**
   * Reads the state of this object as primitive data from the given <code>DataInput</code>. <br>
   * <br>
   * Note: For rolling upgrades, if there is a change in the object format from previous version,
   * add a new fromDataPre_GFE_X_X_X_X() method and add an entry for the current
   * {@link org.apache.geode.internal.Version} in the getSerializationVersions array of the
   * implementing class. e.g. if msg format changed in version 80, create fromDataPre_GFE_8_0_0_0,
   * add Version.GFE_80 to the getSerializationVersions array and copy previous fromData contents to
   * this newly created fromDataPre_GFE_X_X_X_X() method.
   *
   * @param in the data input stream
   * @throws IOException A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException A class could not be loaded while reading from <code>in</code>
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {}

  /**
   * Returns the versions where this classes serialized form was modified. Versions returned by this
   * method are expected to be in increasing ordinal order from 0 .. N. For instance,<br>
   * Version.GFE_7_0, Version.GFE_7_0_1, Version.GFE_8_0, Version.GFXD_1_1<br>
   * <p>
   * You are expected to implement toDataPre_GFE_7_0_0_0(), fromDataPre_GFE_7_0_0_0(), ...,
   * toDataPre_GFXD_1_1_0_0, fromDataPre_GFXD_1_1_0_0.
   * <p>
   * The method name is formed with the version's product name and its major, minor, release and
   * patch numbers.
   */
  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
