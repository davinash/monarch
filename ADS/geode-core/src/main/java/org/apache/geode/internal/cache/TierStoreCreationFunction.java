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

import java.util.List;
import java.util.Properties;

import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import io.ampool.monarch.table.MCacheFactory;
import io.ampool.store.StoreHandler;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreFactory;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.TierStoreReaderFactory;
import io.ampool.tierstore.TierStoreWriter;
import io.ampool.tierstore.TierStoreWriterFactory;

public class TierStoreCreationFunction extends FunctionAdapter implements InternalEntity {

  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = -8352523648695192816L;

  public TierStoreCreationFunction() {
    super();
  }

  @Override
  public void execute(FunctionContext context) {
    List<Object> args = (List<Object>) context.getArguments();
    String storeName = (String) args.get(0);
    String storeClass = (String) args.get(1);
    Properties storeProps = (Properties) args.get(2);
    String writerClass = (String) args.get(3);
    Properties writerOpts = (Properties) args.get(4);
    String readerClass = (String) args.get(5);
    Properties readerOpts = (Properties) args.get(6);
    try {
      TierStoreReader reader = new TierStoreReaderFactory().create(readerClass, readerOpts);
      TierStoreWriter writer = new TierStoreWriterFactory().create(writerClass, writerOpts);
      TierStore store = new TierStoreFactory().create(storeClass, storeName, storeProps, writer,
          reader, (MonarchCacheImpl) MCacheFactory.getAnyInstance());
      StoreHandler.getInstance().registerStore(storeName, store);
    } catch (Exception e) {
      logger.error("Exception while creating the store", e);
      context.getResultSender().sendException(e);
    }
    context.getResultSender().lastResult(true);
  }
}
