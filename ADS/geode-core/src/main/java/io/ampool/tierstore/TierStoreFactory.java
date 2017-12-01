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

package io.ampool.tierstore;

import org.apache.geode.internal.cache.MonarchCacheImpl;
import io.ampool.store.DefaultConstructorMissingException;
import io.ampool.store.StoreCreateException;
import io.ampool.store.StoreUtils;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

public class TierStoreFactory {
  private static final Logger logger = LogService.getLogger();

  public static final String STORE_NAME_PROP = "store.name";
  public static final String STORE_HANDLER_PROP = "store.handler";
  public static final String STORE_OPTS_PROP = "store.options";
  public static final String STORE_WRITER_PROP = "store.writer";
  public static final String STORE_WRITER_PROPS_PROP = "store.writer.props";
  public static final String STORE_READER_PROP = "store.reader";
  public static final String STORE_READER_PROPS_PROP = "store.reader.props";

  public TierStoreFactory() {}

  public TierStore create(String clazz, String name, Properties properties, TierStoreWriter writer,
      TierStoreReader reader, final MonarchCacheImpl monarchCacheImpl)
      throws ClassNotFoundException, DefaultConstructorMissingException, StoreCreateException {

    Class storeClazz = ClassPathLoader.getLatest().forName(clazz);
    Constructor constructor = StoreUtils.getTierStoreConstructor(storeClazz);;
    TierStore storeInstance = null;
    try {
      storeInstance = (TierStore) constructor.newInstance(name);
      storeInstance.init(properties, writer, reader);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException
        | IOException e) {
      throw new StoreCreateException(e);
    }
    monarchCacheImpl.getDistributedSystem().handleResourceEvent(ResourceEvent.TIERSTORE_CREATE,
        storeInstance);
    return storeInstance;
  }
}
