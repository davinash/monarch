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

package io.ampool.monarch.functions;

import java.util.List;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;

public class MonarchCreateRegionFunction implements Declarable, Function {

    @Override
    public void init(java.util.Properties props) {

    }

    public MonarchCreateRegionFunction() {
        super();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(FunctionContext context) {
        List<Object> args = (List<Object>) context.getArguments();

        String regionName = (String)args.get(0);
        RegionShortcut regionShortCut = (RegionShortcut)args.get(1);

       try {
           Cache cache = CacheFactory.getAnyInstance();
           Region<Object, Object> region = cache.getRegion(regionName);
           if (region != null) {
               context.getResultSender().lastResult(true);
           }
           if (region == null) {
               cache.createRegionFactory(regionShortCut)
                       .create(regionName);

           }
        } catch ( RegionExistsException re) {
            context.getResultSender().lastResult(true);
        } catch (CacheClosedException e) {
            context.getResultSender().lastResult(false);
        }
        context.getResultSender().lastResult(true);
    }

    @Override
    public String getId() {
        return this.getClass().getName();
    }
}
