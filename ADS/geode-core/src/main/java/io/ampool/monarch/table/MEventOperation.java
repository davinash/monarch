package io.ampool.monarch.table;

import java.io.Serializable;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

/**
 * Since Version 1.0.1
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum MEventOperation implements Serializable {
  CREATE, UPDATE, DELETE;

  /**
   * Get the byte value, instead of integer, corresponding to the operation.
   *
   * @return the byte value representing the operation
   */
  public byte ordinalByte() {
    return (byte) ordinal();
  }

  /**
   * Get the respective operation for the specified byte value of ordinal. In case of unsupported
   * value, it throws an ArrayIndexOutOfBoundsException.
   *
   * @param ordinal the ordinal value as byte
   * @return the respective operation
   */
  public static MEventOperation valueOf(byte ordinal) {
    for (final MEventOperation op : MEventOperation.values()) {
      if ((byte) op.ordinal() == ordinal) {
        return op;
      }
    }
    return null;
  }

}
