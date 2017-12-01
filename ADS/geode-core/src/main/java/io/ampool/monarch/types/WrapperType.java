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
package io.ampool.monarch.types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.function.Function;
import java.util.function.Predicate;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.interfaces.Function3;
import io.ampool.monarch.types.interfaces.MFunction;
import io.ampool.monarch.types.interfaces.TypePredicate;
import io.ampool.monarch.types.interfaces.TypePredicateOp;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataSerializable;

/**
 * Since version: 0.2.0
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class WrapperType implements DataType, TypePredicate, VersionedDataSerializable {
  private static final long serialVersionUID = 5713770854309683643L;
  private DataType objectType;
  private MFunction<Object, byte[]> serializeFunction = null;
  private Function3<byte[], Integer, Integer, Object> deserializeFunction = null;
  private String args = null;
  /** TODO: refactor/group below members to inner class **/
  private Function<Object, Object> preSerialize = null;
  private Function<Object, Object> postDeserialize = null;
  private byte[] converterSerialized = new byte[0];
  private String converterDependency = null;

  public WrapperType() {
    /// empty c'tor.. for DataSerializable
  }

  /**
   * Constructor.. create object by wrapping the basic-object-type since that type is singleton and
   * cannot be modified.
   *
   * @param objectType the basic object type that is wrapped by this
   * @param args arguments to the object type, if any
   * @param preSerialize the pre-serialize function
   * @param postDeserialize the post-deserialization function
   */
  public WrapperType(final DataType objectType, final String args,
      final Function<Object, Object> preSerialize, final Function<Object, Object> postDeserialize) {
    this.objectType = objectType;
    this.args = args;
    this.preSerialize = preSerialize;
    this.postDeserialize = postDeserialize;
    initialize();
  }

  /**
   * Initialize the type correctly so that pre and post converters, if specified, execute as
   * expected.
   */
  private void initialize() {
    this.serializeFunction = objectType::serialize;
    if (this.preSerialize != null) {
      this.serializeFunction = this.serializeFunction.compose(this.preSerialize);
    }
    this.deserializeFunction = objectType::deserialize;
    if (this.postDeserialize != null) {
      this.deserializeFunction = this.deserializeFunction.andThen(this.postDeserialize);
    }
  }

  /**
   * Redirect to the predicate of wrapped object-type.
   *
   * @param op the predicate operation
   * @param initialValue the (initial) value to be tested against using this predicate
   * @return a predicate that tests the two arguments for the specified operation
   */
  @Override
  public Predicate getPredicate(TypePredicateOp op, Object initialValue) {
    if (objectType instanceof TypePredicate) {
      return ((TypePredicate) objectType).getPredicate(op, initialValue);
    } else {
      return null;
    }
  }

  /**
   * Construct and get the object with correct type from bytes of specified length from the provided
   * offset.
   *
   * @param bytes an array of bytes representing an object
   * @param offset offset in the byte-array
   * @param length number of bytes, from offset, to read from the byte-array
   * @return the actual Java object with the respective type
   */
  @Override
  public Object deserialize(byte[] bytes, Integer offset, Integer length) {
    return length <= 0 ? null : deserializeFunction.apply(bytes, offset, length);
  }

  /**
   * Convert the Java object into bytes correctly for the specified type.
   *
   * @param object the actual Java object
   * @return the bytes representing object
   */
  @Override
  public byte[] serialize(Object object) {
    return serializeFunction.apply(object);
  }

  @Override
  public Category getCategory() {
    return objectType.getCategory();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(32);
    if (objectType instanceof BasicTypes) {
      sb.append(((BasicTypes) objectType).name());
    } else {
      sb.append(objectType.toString());
    }
    if (this.args != null) {
      sb.append(DataType.TYPE_ARGS_BEGIN_CHAR).append(args).append(DataType.TYPE_ARGS_END_CHAR);
    }
    if (this.converterDependency != null) {
      sb.append(" [ConverterDependency=").append(this.converterDependency).append(']');
    }
    return sb.toString();
  }

  public DataType getBasicObjectType() {
    return objectType;
  }

  public void setArgs(final String args) {
    this.args = args;
  }

  public String getArgs() {
    return args;
  }

  /**
   * Equality operation for the wrapper.. check the type of the object wrapped.. Just checks the
   * type, excluding arguments.
   *
   * @param other the other object to compare
   * @return true if the other is same or other same as the object wrapped; false otherwises
   */
  public boolean equals(final Object other) {
    return this == other || (other instanceof BasicTypes && objectType.equals(other))
        || (other instanceof WrapperType
            // && args.equals(((MBasicObjectTypeWrapper)other).args)
            // && converterDependency.equals(((MBasicObjectTypeWrapper)other).converterDependency)
            && objectType.equals(((WrapperType) other).objectType))
        || (objectType.equals(other));
  }

  public void setConverterDependency(final String converterDependency) {
    this.converterDependency = converterDependency;
  }

  public String getConverterDependency() {
    return this.converterDependency;
  }

  public void setPrePostFunction(final Function<Object, Object> preSerialize,
      final Function<Object, Object> postDeserialize) {
    setPrePostFunction(null, preSerialize, postDeserialize);
  }

  public void setPrePostFunction(final String converterDependency,
      final Function<Object, Object> preSerialize, final Function<Object, Object> postDeserialize) {
    this.converterDependency = converterDependency;
    this.preSerialize = preSerialize;
    this.postDeserialize = postDeserialize;
    initialize();
  }

  /**
   * Serialization logic for this type.. Serializes the converters (basically the serializable
   * functions) along with the other things like objectType, args, and converter-dependency. The
   * pre/post converters are serialized only when the converter-dependency-class is available. This
   * gives a way to use the pre/post converters that are not always available on the server but the
   * respective clients can still utilize these. So, on server (function or co-processor execution)
   * the data is in raw form. In case client like data is required on server side, the respective
   * classes must be made available on servers (via deploy jars).
   *
   * @param out the output stream where class representation is stored
   * @throws IOException
   */
  // private void writeObject(final ObjectOutputStream out) throws IOException {
  // serializeConverters();
  // out.writeObject(objectType);
  // out.writeObject(args);
  // out.writeObject(converterDependency);
  // out.writeObject(preSerialize);
  // out.writeObject(postDeserialize);
  // out.writeInt(this.converterSerialized.length);
  // out.write(this.converterSerialized);
  //// out.writeObject(converterDependency);
  // out.flush();
  // System.out.println("MBOW/RITE ---> " + Arrays.toString(this.converterSerialized));
  // }

  /**
   * Serialize the pre and post converters, only when the respective class is available. The
   * serialized data for these converters is used as is in case the respective class is not
   * available. In case no converter-dependency was specified serialize the pre/post converters
   * assuming that all the necessary dependencies are always available.
   *
   * @throws IOException
   */
  private void serializeConverters() throws IOException {
    try {
      if (this.converterDependency != null) {
        Class.forName(this.converterDependency);
      }
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(preSerialize);
      oos.writeObject(postDeserialize);
      oos.flush();
      bos.close();
      oos.close();
      this.converterSerialized = bos.toByteArray();
    } catch (ClassNotFoundException e) {
      //// nothing to do..
    }
  }

  /**
   * Read the serialized representation of this object from the specified input stream. It reads all
   * the members and then de-serializes the pre/post converters only where the respective class
   * (converter-dependency) is loaded (i.e. clients). Thus, the type definition on servers does not
   * even change with client specific converters, even when the respective classes are not available
   * on the servers.
   *
   * @param in the input stream to read the object representation
   * @throws IOException
   * @throws ClassNotFoundException
   */
  // @SuppressWarnings("unchecked")
  // private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException
  // {
  // this.objectType = (DataType)in.readObject();
  // this.args = (String) in.readObject();
  // this.converterDependency = (String) in.readObject();
  // this.preSerialize = (Function) in.readObject();
  // this.postDeserialize= (Function) in.readObject();
  //// initialize();
  // final int expectedCount = in.readInt();
  // this.converterSerialized = new byte[expectedCount];
  // int actualCount = in.read(this.converterSerialized, 0, expectedCount);
  //// this.converterDependency = (String) in.readObject();
  // System.out.println("MBOW/READ ---> " + Arrays.toString(this.converterSerialized));
  // if (actualCount != expectedCount) {
  // logger.error("Failed to read all data: expected= {}, actual= {}", expectedCount, actualCount);
  //// throw new IOException("Failed to read all data: expected= " + expectedCount + ", actual= " +
  // actualCount
  //// + " --> " + objectType.toString());
  // }
  // deserializeConverters();
  // }

  /**
   * De-serialize the specified pre/post converters where the respective class is loaded. In case no
   * class was specified, it means all the recursive dependencies for these converter methods are
   * available and hence load these. Typically on the servers, where the specified dependencies may
   * not be available, the wrapped types serialization and de-serialization is used. Thus, the data
   * is available in the respective format.
   *
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  private void deserializeConverters() throws IOException {
    try {
      /** load the pre and post converter functions only when the dependent class is available **/
      if (this.converterDependency != null) {
        Class.forName(this.converterDependency);
      }
      ByteArrayInputStream bis = new ByteArrayInputStream(this.converterSerialized);
      ObjectInputStream ois = new ObjectInputStream(bis);
      this.preSerialize = (Function<Object, Object>) ois.readObject();
      this.postDeserialize = (Function<Object, Object>) ois.readObject();
      ois.close();
      bis.close();
    } catch (ClassNotFoundException e) {
      //// catch exception..
    } finally {
      initialize();
    }
  }

  /**
   * Writes the state of this object as primitive data to the given <code>DataOutput</code>.
   * <p>
   * Since 5.7 it is possible for any method call to the specified <code>DataOutput</code> to throw
   * {@link org.apache.geode.GemFireRethrowable}. It should <em>not</em> be caught by user code. If
   * it is it <em>must</em> be rethrown.
   *
   * @param out the data-output
   * @throws IOException A problem occurs while writing to <code>out</code>
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    serializeConverters();
    DataSerializer.writeObject(objectType, out);
    DataSerializer.writeString(args, out);
    DataSerializer.writeString(converterDependency, out);
    DataSerializer.writeByteArray(this.converterSerialized, out);
  }

  /**
   * Reads the state of this object as primitive data from the given <code>DataInput</code>.
   *
   * @param in the data-input
   * @throws IOException A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException A class could not be loaded while reading from <code>in</code>
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.objectType = DataSerializer.readObject(in);
    this.args = DataSerializer.readString(in);
    this.converterDependency = DataSerializer.readString(in);
    this.converterSerialized = DataSerializer.readByteArray(in);
    deserializeConverters();
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
