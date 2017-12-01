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
package org.apache.geode.internal.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.internal.IMKey;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MTableGetStartEndKeysFunction extends FunctionAdapter implements InternalEntity {

  private static final long serialVersionUID = -2917582637260688041L;

  @Override
  public void execute(FunctionContext context) {

    List<Object> args = (List<Object>) context.getArguments();
    String regionName = (String) args.get(0);
    List<Pair<byte[], byte[]>> result = new ArrayList<>();
    try {
      Cache cache = CacheFactory.getAnyInstance();
      PartitionedRegion pr = (PartitionedRegion) cache.getRegion(regionName);
      if (pr != null) {
        Set<BucketRegion> allLocalBuckets = pr.getDataStore().getAllLocalBucketRegions();
        for (BucketRegion eachLocalBucket : allLocalBuckets) {
          if (eachLocalBucket.getBucketAdvisor().isPrimary()) {
            if (eachLocalBucket.size() != 0) {
              Pair<Object, Object> startEndKey =
                  ((TableBucketRegion) eachLocalBucket).getStartEndKeys();
              if (startEndKey != null) {
                result.add(new Pair<byte[], byte[]>(IMKey.getBytes(startEndKey.getFirst()),
                    IMKey.getBytes(startEndKey.getSecond())));
              } else {
                result.add(new Pair<byte[], byte[]>(null, null));
              }
            } else {
              result.add(new Pair<byte[], byte[]>(null, null));
            }
          }
        }
      }
    } catch (GemFireException re) {
    }
    context.getResultSender().lastResult(result);
  }

  @Override
  public String getId() {
    return this.getClass().getName();
  }
}
