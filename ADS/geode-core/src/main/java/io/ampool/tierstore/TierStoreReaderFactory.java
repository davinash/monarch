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

import io.ampool.store.DefaultConstructorMissingException;
import io.ampool.store.StoreUtils;
import org.apache.geode.internal.ClassPathLoader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

public class TierStoreReaderFactory {
  public TierStoreReader create(String clazz, Properties props)
      throws ClassNotFoundException, DefaultConstructorMissingException, IllegalAccessException,
      InvocationTargetException, InstantiationException {
    Class storeReaderClazz = ClassPathLoader.getLatest().forName(clazz);
    Constructor constructor = StoreUtils.hasParameterlessPublicConstructor(storeReaderClazz);
    TierStoreReader instance = null;
    instance = (TierStoreReader) constructor.newInstance();
    instance.init(props);
    return instance;
  }
}
