package io.ampool.monarch.types;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Predicate;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.types.interfaces.TypePredicate;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.interfaces.TypePredicateOp;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataSerializable;

/**
 * Implementation for Union object type.. It takes list of columns types as argument. The value for
 * this object type is an array of values in the order of column types specified. The serialization
 * and deserialization consumes and produces the data in the same order of types that was provided
 * when creating the type.
 * <p>
 * Since version: 0.2.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class UnionType implements DataType, TypePredicate, VersionedDataSerializable {
  public static final String NAME = "union";
  private static final long serialVersionUID = 6839028735356948448L;
  private DataType[] columnTypes;

  public UnionType() {
    ////
  }

  /**
   * Construct the Struct object-type from the provided inputs.
   *
   * @param columnTypes an array of the respective column types
   */
  public UnionType(final DataType[] columnTypes) {
    if (columnTypes == null || columnTypes.length == 0) {
      throw new IllegalArgumentException("MUnionObjectType: Must have at least one column.");
    }
    this.columnTypes = columnTypes;
  }

  /**
   * Provide the required predicate for execution. It takes following things into consideration: -
   * the predicate operation {@link CompareOp} - the initial value, all the values shall be compared
   * against this value
   *
   * @param op the predicate operation
   * @param initialValue the (initial) value to be tested against using this predicate
   * @return a predicate that tests the two arguments for the specified operation
   */
  @Override
  public Predicate getPredicate(TypePredicateOp op, Object initialValue) {
    return null;
  }

  /**
   * Construct and get the object with correct type from bytes of specified length from the provided
   * offset. It returns an array of length 2; first element is the tag (Java type byte) and second
   * element is the Java object respective to the type provided during the creation of this type.
   *
   * @param bytes an array of bytes representing an object
   * @param offset offset in the byte-array
   * @param length number of bytes, from offset, to read from the byte-array
   * @return an array where first element is the tag and second is the respective Java object
   */
  @Override
  public Object deserialize(byte[] bytes, Integer offset, Integer length) {
    if (length <= 0) {
      return null;
    }
    Object[] array = new Object[2];
    byte tag = bytes[offset];
    int len = TypeHelper.readInt(bytes, offset + 1);
    int off = offset + 1 + TypeHelper.IntegerLength;
    array[0] = tag;
    array[1] = this.columnTypes[tag].deserialize(bytes, off, len);
    return array;
  }

  /**
   * Convert the Java object into bytes correctly for the specified type. It expects array of length
   * 2 where first element is the tag and second is the Java object, respective to the types
   * specified, that is to be serialized.
   *
   * @param object the actual Java object
   * @return the bytes representing object
   */
  @Override
  public byte[] serialize(final Object object) {
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    if (object != null && object instanceof Object[]) {
      final Object[] array = (Object[]) object;
      assert array.length == 2;
      byte tag = (byte) array[0];
      assert tag < columnTypes.length;
      bos.write(tag);
      TypeHelper.writeBytes(bos, columnTypes[tag].serialize(array[1]));
    }
    return bos.toByteArray();
  }

  @Override
  public Category getCategory() {
    return Category.Union;
  }

  /**
   * The string representation of the type.
   *
   * @return the string representation of this object
   */
  public String toString() {
    StringBuilder sb = new StringBuilder(32);
    sb.append(NAME).append(COMPLEX_TYPE_BEGIN_CHAR);
    for (final DataType columnType : columnTypes) {
      sb.append(columnType.toString()).append(COMPLEX_TYPE_SEPARATOR);
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.append(COMPLEX_TYPE_END_CHAR);
    return sb.toString();
  }

  /**
   * Return whether the specified object-type is equal-to this object-type.
   *
   * @param other the other object to be compared
   * @return true if the other object is same as this; false otherwise
   */
  public boolean equals(final Object other) {
    return this == other || (other instanceof UnionType
        && Arrays.deepEquals(columnTypes, ((UnionType) other).columnTypes));
  }

  public DataType[] getColumnTypes() {
    return this.columnTypes;
  }

  /**
   * Writes the state of this object as primitive data to the given <code>DataOutput</code>.
   * <p>
   * Since 5.7 it is possible for any method call to the specified <code>DataOutput</code> to throw
   * {@link org.apache.geode.GemFireRethrowable}. It should <em>not</em> be caught by user code. If
   * it is it <em>must</em> be rethrown.
   *
   * @param out the data output stream
   * @throws IOException A problem occurs while writing to <code>out</code>
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObjectArray(this.columnTypes, out);
  }

  /**
   * Reads the state of this object as primitive data from the given <code>DataInput</code>.
   *
   * @param in the data input stream
   * @throws IOException A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException A class could not be loaded while reading from <code>in</code>
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.columnTypes = (DataType[]) DataSerializer.readObjectArray(in);
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
