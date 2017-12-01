package io.ampool.monarch.table.region;

import io.ampool.internal.RegionDataOrder;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CustomRegionAttributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AmpoolTableRegionAttributes implements CustomRegionAttributes {
  RegionDataOrder regionDataOrder = RegionDataOrder.DEFAULT;

  public AmpoolTableRegionAttributes() {

  }

  public RegionDataOrder getRegionDataOrder() {
    return regionDataOrder;
  }

  public void setRegionDataOrder(RegionDataOrder regionDataOrder) {
    this.regionDataOrder = regionDataOrder;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(regionDataOrder, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    regionDataOrder = DataSerializer.readObject(in);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AmpoolTableRegionAttributes)) {
      return false;
    }

    AmpoolTableRegionAttributes that = (AmpoolTableRegionAttributes) o;

    return getRegionDataOrder().equals(that.getRegionDataOrder());
  }

  @Override
  public int hashCode() {
    return getRegionDataOrder().hashCode();
  }

  @Override
  public String toString() {
    return "AmpoolTableRegionAttributes{" + "regionDataOrder=" + regionDataOrder + '}';
  }

  public static boolean isAmpoolTable(CustomRegionAttributes customRegionAttributes) {
    if (customRegionAttributes != null && ((AmpoolTableRegionAttributes) customRegionAttributes)
        .getRegionDataOrder() != RegionDataOrder.DEFAULT) {
      return true;
    }
    return false;
  }

  public static boolean isAmpoolMTable(CustomRegionAttributes customRegionAttributes) {
    if (customRegionAttributes != null && (((AmpoolTableRegionAttributes) customRegionAttributes)
        .getRegionDataOrder() == RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED
        || ((AmpoolTableRegionAttributes) customRegionAttributes)
            .getRegionDataOrder() == RegionDataOrder.ROW_TUPLE_UNORDERED)) {
      return true;
    }
    return false;
  }

  public static boolean isAmpoolMTableOrdered(CustomRegionAttributes customRegionAttributes) {
    if (customRegionAttributes != null && ((AmpoolTableRegionAttributes) customRegionAttributes)
        .getRegionDataOrder() == RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED) {
      return true;
    }
    return false;
  }

  public static boolean isAmpoolMTableUnOrdered(CustomRegionAttributes customRegionAttributes) {
    if (customRegionAttributes != null && ((AmpoolTableRegionAttributes) customRegionAttributes)
        .getRegionDataOrder() == RegionDataOrder.ROW_TUPLE_UNORDERED) {
      return true;
    }
    return false;
  }

  public static boolean isAmpoolFTable(CustomRegionAttributes customRegionAttributes) {
    if (customRegionAttributes != null && ((AmpoolTableRegionAttributes) customRegionAttributes)
        .getRegionDataOrder() == RegionDataOrder.IMMUTABLE) {
      return true;
    }
    return false;
  }

  public static boolean isAmpoolTableOrdered(CustomRegionAttributes customRegionAttributes) {
    if (customRegionAttributes != null && (((AmpoolTableRegionAttributes) customRegionAttributes)
        .getRegionDataOrder() == RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED
        || ((AmpoolTableRegionAttributes) customRegionAttributes)
            .getRegionDataOrder() == RegionDataOrder.IMMUTABLE)) {
      return true;
    }
    return false;
  }

  public static RegionDataOrder getRegionDataOrder(
      final CustomRegionAttributes customRegionAttributes) {
    if (customRegionAttributes == null
        && !(customRegionAttributes instanceof AmpoolTableRegionAttributes)) {
      return RegionDataOrder.DEFAULT;
    } else {
      return ((AmpoolTableRegionAttributes) customRegionAttributes).getRegionDataOrder();
    }
  }
}
