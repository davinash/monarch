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
package io.ampool.monarch.table.cdc;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.EventSequenceID;
import io.ampool.monarch.table.*;

import java.util.List;

public class CDCEventListener implements MAsyncEventListener {
  private final String EVENT_STATS_REGION = "CDC_STATS_REGION";

  public CDCEventListener() {}

  @Override
  public boolean processEvents(List<CDCEvent> events) {
    MCache cache = MCacheFactory.getAnyInstance();
    if (cache != null) {
      Region<Object, Object> region = cache.getRegion(EVENT_STATS_REGION);

      synchronized (CDCEventListener.class) {
        if (region != null) {
          for (CDCEvent event : events) {
            MEventSequenceID eventSequenceID = event.getEventSequenceID();
            String key = eventSequenceID.getMembershipID() + "-" + eventSequenceID.getSequenceID()
                + "-" + eventSequenceID.getThreadID();
            region.put(key,
                new EventData(event.getKey(), /* event.getMTable(), */ event.getMTableDescriptor(),
                    event.getOperation(), event.getRow(), event.getPossibleDuplicate()));
          }
        }
      }
    }
    return true;
  }

  @Override
  public void close() {

  }
}
