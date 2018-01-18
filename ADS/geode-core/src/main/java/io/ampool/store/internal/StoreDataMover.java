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
package io.ampool.store.internal;

import io.ampool.tierstore.TierStore;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

/**
 * Used for moving the store data from one node to other in case of bucket movememt
 */
public class StoreDataMover {
  private static final Logger logger = LogService.getLogger();

  private final InternalDistributedMember recipient;
  private final int tier;
  private TierStore store;
  private int partitionId;
  private BucketRegion region;

  public StoreDataMover(final TierStore store, final int tier, BucketRegion region, int partitionId,
                        final InternalDistributedMember recipient) {
    this.store = store;
    this.region = region;
    this.partitionId = partitionId;
    this.recipient = recipient;
    this.tier = tier;
    logger.info(
            "StoreDataMover: Moving data to tierId= {} using tierStore= {} for table= {}, "
                    + "partitionId= {} from {}",
            tier, store.getName(), region.getDisplayName(), partitionId, recipient);
  }

  public void start() {
    final DistributionManager dm = (DistributionManager) this.region.getDistributionManager();
    final String tableName = this.region.getPartitionedRegion().getDisplayName();
    RequestStoreImageMessage requestStoreImageMessage = new RequestStoreImageMessage();
    requestStoreImageMessage.setTableName(tableName);
    requestStoreImageMessage.setRecipient(recipient);
    requestStoreImageMessage.setTier(tier);
    requestStoreImageMessage.setPartitionID(this.region.getId());
    StoreImageProcessor processor = new StoreImageProcessor(this.region.getSystem(), recipient,
            tier, tableName, this.partitionId);
    requestStoreImageMessage.setProcessorID(processor.getProcessorId());
    dm.putOutgoing(requestStoreImageMessage);
    processor.waitForRepliesUninterruptibly();
  }
}
