package io.ampool.monarch.table.ftable;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.ftable.internal.PartitionResolverAdapter;
import org.apache.geode.cache.EntryOperation;


/**
 * Table descriptor implementation for Fact table.
 *
 * @since 1.1.1
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public abstract class PartitionResolver<K, V> extends PartitionResolverAdapter<K, V> {
  public abstract Object getDistributionObject(EntryOperation entryOp);

  public abstract String getName();
}
