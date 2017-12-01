package io.ampool.internal;

import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.internal.RowEndMarker;
import io.ampool.monarch.table.region.ScanOp;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.distributed.internal.ServerLocation;

import java.util.concurrent.BlockingQueue;

public class ServerTableRegionProxy extends ServerRegionProxy {
  public ServerTableRegionProxy(Region r) {
    super(r);
  }

  public RowEndMarker scan(Scan scan, BlockingQueue<Object> resultQueue,
      TableDescriptor tableDescriptor, ServerLocation serverLocation) {
    return ScanOp.execute(this.pool, super.getRegion(), scan, resultQueue, tableDescriptor,
        serverLocation);
  }
}
