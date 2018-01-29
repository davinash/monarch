
package io.ampool.internal;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class RegionDataOrder implements Serializable {
  public static final RegionDataOrder DEFAULT = new RegionDataOrder("DEFAULT", 0);
  public static final RegionDataOrder ROW_TUPLE_ORDERED_VERSIONED =
      new RegionDataOrder("ROW_TUPLE_ORDERED_VERSIONED", 1);
  public static final RegionDataOrder ROW_TUPLE_UNORDERED =
      new RegionDataOrder("ROW_TUPLE_UNORDERED", 2);
  public static final RegionDataOrder IMMUTABLE = new RegionDataOrder("IMMUTABLE", 3);
  /** Singleton instances of the RegionDataOrder **/
  public static final RegionDataOrder[] ORDERS =
      new RegionDataOrder[] {DEFAULT, ROW_TUPLE_ORDERED_VERSIONED, ROW_TUPLE_UNORDERED, IMMUTABLE};
  private int id;
  private String name;

  /**
   * Added for MBean
   */
  public RegionDataOrder() {}

  private RegionDataOrder(String name, int id) {
    this.name = name;
    this.id = id;
  }

  public boolean isRowTupleOrderd() {
    return this == ROW_TUPLE_ORDERED_VERSIONED;
  }

  public boolean isRowTupleUnOrdered() {
    return this == ROW_TUPLE_UNORDERED;
  }

  public boolean isImmutable() {
    return this == IMMUTABLE;
  }

  public boolean isDefault() {
    return this == DEFAULT;
  }

  @Override
  public String toString() {
    return this.name;
  }

  public int getId() {
    return this.id;
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj || (obj instanceof RegionDataOrder && this.id == ((RegionDataOrder) obj).id);
  }

  /**
   * Serialize the RegionDataOrder based on <em>id</em>.
   *
   * @param out the object output stream
   * @throws IOException
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeInt(this.id);
  }

  /**
   * Restore the RegionDataOrder based on <em>id</em>.
   *
   * @param in the object input stream
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    this.id = in.readInt();
  }

  /**
   * Resolve the object to singleton instance of RegionDataOrder.
   *
   * @return the region data order
   */
  protected Object readResolve() {
    return RegionDataOrder.ORDERS[this.id];
  }

  public static RegionDataOrder fromId(final int id) {
    return RegionDataOrder.ORDERS[id];
  }
}

