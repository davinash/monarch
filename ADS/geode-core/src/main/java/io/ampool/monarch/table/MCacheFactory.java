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

package io.ampool.monarch.table;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.GatewayException;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;

import java.util.Properties;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class MCacheFactory extends CacheFactory {

  public static MCache getAnyInstance() {
    MonarchCacheImpl instance = MonarchCacheImpl.getInstance();
    if (instance == null) {
      throw new CacheClosedException(
          LocalizedStrings.CacheFactory_A_CACHE_HAS_NOT_YET_BEEN_CREATED.toLocalizedString());
    } else {
      instance.getCancelCriterion().checkCancelInProgress(null);
      return instance;
    }
  }

  public MCacheFactory() {
    super();
  }

  public MCacheFactory(Properties props) {
    super(props);
  }

  @Override
  public MCacheFactory set(String name, String value) {
    super.set(name, value);
    return this;
  }

  @Override
  public MCacheFactory setPdxReadSerialized(boolean readSerialized) {
    super.setPdxReadSerialized(readSerialized);
    return this;
  }

  @Override
  public MCacheFactory setSecurityManager(SecurityManager securityManager) {
    super.setSecurityManager(securityManager);
    return this;
  }

  @Override
  public MCacheFactory setPostProcessor(PostProcessor postProcessor) {
    super.setPostProcessor(postProcessor);
    return this;
  }

  @Override
  public MCacheFactory setPdxSerializer(PdxSerializer serializer) {
    super.setPdxSerializer(serializer);
    return this;
  }

  @Override
  public MCacheFactory setPdxDiskStore(String diskStoreName) {
    super.setPdxDiskStore(diskStoreName);
    return this;
  }

  @Override
  public MCacheFactory setPdxPersistent(boolean isPersistent) {
    super.setPdxPersistent(isPersistent);
    return this;
  }

  @Override
  public MCacheFactory setPdxIgnoreUnreadFields(boolean ignore) {
    super.setPdxIgnoreUnreadFields(ignore);
    return this;
  }

  @Override
  public MCache create()
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    synchronized (MCacheFactory.class) {
      DistributedSystem ds = null;
      if (this.dsProps.isEmpty()) {
        // any ds will do
        ds = InternalDistributedSystem.getConnectedInstance();
      }
      if (ds == null) {
        ds = DistributedSystem.connect(this.dsProps);
      }
      return create(ds, true, cacheConfig);
    }
  }

  private static synchronized MCache create(DistributedSystem system, boolean existingOk,
      CacheConfig cacheConfig) throws CacheExistsException, TimeoutException, CacheWriterException,
      GatewayException, RegionExistsException {
    // Moved code in this method to GemFireCacheImpl.create
    return MonarchCacheImpl.create(system, existingOk, cacheConfig);
  }

  @Deprecated
  public static synchronized MCache create(DistributedSystem system) throws CacheExistsException,
      TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    return MonarchCacheImpl.create(system, false, new CacheConfig());
  }


  public static synchronized void invalidateInstance(String reason, boolean keepAlive) {
    MonarchCacheImpl instance = MonarchCacheImpl.getInstance();
    if (instance != null) {
      instance = null;
    }
  }
}
