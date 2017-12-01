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
package io.ampool.monarch.table.region;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionListenerAdapter;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketServerLocation66;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.Set;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class RowTuplePartitionListener extends PartitionListenerAdapter {
  protected static final Logger logger = LogService.getLogger();
  private PartitionedRegion pr = null;
  private Region<String, ServerLocation> tlbNameToBIdSLocRegion;
  private String regionName = null;

  public RowTuplePartitionListener() {}

  @Override
  public void afterPrimary(int bucketId) {
    if (this.pr != null && this.pr.getRegionAdvisor() != null) {
      BucketAdvisor.BucketProfile profile =
          this.pr.getRegionAdvisor().getBucketAdvisor(bucketId).getLocalProfile();

      if (profile != null) {
        if ((profile instanceof BucketAdvisor.ServerBucketProfile) && profile.isHosting) {
          BucketAdvisor.ServerBucketProfile cProfile = (BucketAdvisor.ServerBucketProfile) profile;
          Set<BucketServerLocation66> bucketServerLocations = cProfile.getBucketServerLocations();
          for (BucketServerLocation66 esl : bucketServerLocations) {
            String key = MTableUtils.getTableToBucketIdKey(this.regionName, bucketId);
            ServerLocation sl = new ServerLocation(esl.getHostName(), esl.getPort());
            this.tlbNameToBIdSLocRegion.put(key, sl);
          }
        }
      }
    }
  }

  @Override
  public void afterRegionCreate(Region<?, ?> region) {
    /*
     * logger.info(" ----> Region Created " + region.getName()); Cache cache =
     * region.getAmplCache(); this.pr = (PartitionedRegion) region; this.tlbNameToBIdSLocRegion =
     * cache.getRegion(MTableUtils.getMetaRegionBktIdToSLocName()); if (this.tlbNameToBIdSLocRegion
     * == null) { throw new IllegalStateException("Ampool Meta Region not found"); } this.regionName
     * = this.pr.getName();
     */
  }

  @Override
  public void afterBucketRemoved(int bucketId, Iterable<?> keys) {}

  @Override
  public void afterBucketCreated(int bucketId, Iterable<?> keys) {
    logger.info("Bucket Id " + bucketId + " created ");
  }
}
