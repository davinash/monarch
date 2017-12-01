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

package io.ampool.monarch.table.client;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.MConfiguration;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.exceptions.MCacheClosedException;
import io.ampool.monarch.table.exceptions.MCacheInvalidConfigurationException;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.pdx.PdxSerializer;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class MClientCacheFactory extends ClientCacheFactory {

  private boolean metaRegionCachingEnabled = false;

  /**
   * Creates a new Monarch Table client cache factory.
   */
  public MClientCacheFactory() {
    super();
  }

  /**
   * Create a new client cache factory given the initial gemfire properties.
   *
   * @param props The initial gemfire properties to be used. These properties can be overridden
   *        using the {@link #set} method For a full list of valid gemfire properties see
   *        {@link ConfigurationProperties}.
   */
  public MClientCacheFactory(Properties props) {
    super(props);
  }


  /**
   * Gets an arbitrary open instance of {@link MClientCache} produced by an earlier call to
   * {@link #create}.
   *
   * @throws org.apache.geode.cache.CacheClosedException if a cache has not been created or the only
   *         created one is {@link org.apache.geode.cache.client.ClientCache#isClosed closed}
   * @throws IllegalStateException if the cache was created by MCacheFactory instead of
   *         MClientCacheFactory
   */
  public static synchronized MClientCache getAnyInstance() {
    MonarchCacheImpl instance = MonarchCacheImpl.getInstance();
    if (instance == null) {
      throw new MCacheClosedException("A cache has not yet been created.");
    } else {
      if (!instance.isClient()) {
        throw new IllegalStateException(
            "The singleton cache was created by MCacheFactory not MClientCacheFactory.");
      }
      instance.getCancelCriterion().checkCancelInProgress(null);
      return instance;
    }
  }

  /**
   * Create a singleton Monarch Table client cache. If a client cache already exists in this vm that
   * is not compatible with this factory's configuration then create will fail.
   * <P>
   * Note that the cache that is produced is a singleton. Before a different instance can be
   * produced the old one must be {@link MClientCache#close closed}.
   *
   * @return the singleton client cache
   * @throws IllegalStateException if a client cache already exists and it is not compatible with
   *         this factory's configuration.
   * @throws MCacheClosedException if a client cannot not make a connection with Locator/Server.
   */
  @Deprecated
  public MClientCache create(MConfiguration conf) {

    final Iterator<Map.Entry<String, String>> entryIterator = conf.iterator();
    while (entryIterator.hasNext()) {
      final Map.Entry<String, String> entry = entryIterator.next();
      if (!MTableUtils.isAmpoolProperty(entry.getKey())) {
        this.set(entry.getKey(), entry.getValue());
      }
    }
    updateUsingConf(conf);
    return create();
  }

  public MClientCache create() {
    return basicCreate();
  }

  private MClientCache basicCreate() {
    synchronized (MClientCacheFactory.class) {
      MonarchCacheImpl instance = MonarchCacheImpl.getInstance();
      {
        String propValue = this.dsProps.getProperty(MCAST_PORT);
        if (propValue != null) {
          int mcastPort = Integer.parseInt(propValue);
          if (mcastPort != 0) {
            throw new IllegalStateException(
                "On a client cache the mcast-port must be set to 0 or not set. It was set to "
                    + mcastPort);
          }
        }
      }
      {
        String propValue = this.dsProps.getProperty(LOCATORS);
        if (propValue != null && !propValue.equals("")) {
          throw new IllegalStateException(
              "On a client cache the locators property must be set to an empty string or not set. It was set to \""
                  + propValue + "\".");
        }
      }
      this.dsProps.setProperty(MCAST_PORT, "0");
      this.dsProps.setProperty(LOCATORS, "");
      DistributedSystem system = DistributedSystem.connect(this.dsProps);

      if (instance != null && !instance.isClosed()) {
        // this is ok; just make sure it is a client cache
        if (!instance.isClient()) {
          throw new IllegalStateException(
              "A client cache can not be created because a non-client cache already exists.");
        }

        // check if pool is compatible
        Pool pool = instance.determineDefaultPool(this.pf);
        if (pool == null) {
          if (instance.getDefaultPool() != null) {
            throw new IllegalStateException("Existing cache's default pool was not compatible");
          }
        }

        // Check if cache configuration matches.
        cacheConfig.validateCacheConfig(instance);

        return instance;
      } else {
        MonarchCacheImpl gfc =
            MonarchCacheImpl.createClient(system, this.pf, cacheConfig, metaRegionCachingEnabled);
        return gfc;
      }
    }
  }

  /**
   * Get the instance of client cache, if already created, or create the one if not created. Added
   * for simplicity so that clients do not need to worry about synchronization and get an existing
   * instance of cache or create, if required.
   *
   * @param props the client cache properties
   * @return the client cache
   */
  public static MClientCache getOrCreate(final Properties props) {
    MClientCache cache = MonarchCacheImpl.getInstance();
    if (cache == null || cache.isClosed()) {
      synchronized (MClientCacheFactory.class) {
        cache = MonarchCacheImpl.getInstance();
        if (cache == null || cache.isClosed()) {
          cache = new MClientCacheFactory().create(props);
        }
      }
    }
    return cache;
  }


  /**
   * Get the instance of client cache, if already created, or create the one if not created. Added
   * for simplicity so that clients do not need to worry about synchronization and get an existing
   * instance of cache or create, if required.
   * <p>
   * Use {@link MClientCacheFactory#getOrCreate(Properties)} instead of this.
   *
   * @param conf the cache configuration
   * @return the client cache
   */
  @Deprecated
  public static MClientCache getOrCreate(final MConfiguration conf) {
    MClientCache cache = MonarchCacheImpl.getInstance();
    if (cache == null || cache.isClosed()) {
      synchronized (MClientCacheFactory.class) {
        cache = MonarchCacheImpl.getInstance();
        if (cache == null || cache.isClosed()) {
          cache = new MClientCacheFactory().create(conf);
        }
      }
    }
    return cache;
  }

  /**
   * Create a singleton Monarch Table client cache. If a client cache already exists in this vm that
   * is not compatible with this factory's configuration then create will fail.
   * <P>
   * Note that the cache that is produced is a singleton. Before a different instance can be
   * produced the old one must be {@link MClientCache#close closed}.
   *
   * @param props the client cache properties
   * @return the singleton client cache
   * @throws IllegalStateException if a client cache already exists and it is not compatible with
   *         this factory's configuration.
   * @throws MCacheClosedException if a client cannot not make a connection with Locator/Server.
   */
  @Deprecated
  public MClientCache create(final Properties props) {
    /* TODO: remove MConfiguration and instead use Properties */
    MConfiguration conf = MConfiguration.create();
    for (final String pName : props.stringPropertyNames()) {
      conf.set(pName, props.getProperty(pName));
    }
    return create(conf);
  }

  private void updateUsingConf(final MConfiguration conf) {
    String locatorAddress = MTableUtils.getLocatorAddress(conf);
    int locatorPort = MTableUtils.getLocatorPort(conf);
    List<Pair<String, Integer>> locators = MTableUtils.getLocators(conf);
    this.setPoolSocketBufferSize(MTableUtils.getClientSocketBufferSize(conf));
    // set pool read timeout.
    this.setPoolReadTimeout(MTableUtils.getClientPoolReadTimeOut(conf));
    if ("true".equals(conf.get(Constants.MClientCacheconfig.ENABLE_POOL_SUBSCRIPTION))) {
      this.setPoolSubscriptionEnabled(true);
    }

    if ((locatorAddress == null || locatorPort == 0) && (locators.size() == 0)) {
      throw new MCacheInvalidConfigurationException("No Locator Specified to connect");
    }

    if (locatorAddress != null && locatorPort != 0) {
      this.addPoolLocator(locatorAddress, locatorPort);
    }
    if (locators.size() != 0) {
      for (Pair<String, Integer> locator : locators) {
        this.addPoolLocator(locator.getFirst(), locator.getSecond());
      }
    }

    String logFileName = conf.get(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG);
    if (logFileName != null) {
      this.set("log-file", logFileName);
    }
    this.set("cache-xml-file", "");

    boolean cachingProxyMetaRegion =
        "true".equals(conf.get(Constants.MClientCacheconfig.ENABLE_META_REGION_CACHING));
    this.enableMetaRegionCaching(cachingProxyMetaRegion);
  }



  @Override
  public MClientCacheFactory set(String name, String value) {
    super.set(name, value);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolFreeConnectionTimeout(int connectionTimeout) {
    super.setPoolFreeConnectionTimeout(connectionTimeout);
    return this;
  }


  @Override
  public MClientCacheFactory setPoolLoadConditioningInterval(int loadConditioningInterval) {
    super.setPoolLoadConditioningInterval(loadConditioningInterval);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolSocketBufferSize(int bufferSize) {
    super.setPoolSocketBufferSize(bufferSize);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolThreadLocalConnections(boolean threadLocalConnections) {
    super.setPoolThreadLocalConnections(threadLocalConnections);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolReadTimeout(int timeout) {
    super.setPoolReadTimeout(timeout);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolMinConnections(int minConnections) {
    super.setPoolMinConnections(minConnections);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolMaxConnections(int maxConnections) {
    super.setPoolMaxConnections(maxConnections);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolIdleTimeout(long idleTimeout) {
    super.setPoolIdleTimeout(idleTimeout);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolRetryAttempts(int retryAttempts) {
    super.setPoolRetryAttempts(retryAttempts);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolPingInterval(long pingInterval) {
    super.setPoolPingInterval(pingInterval);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolStatisticInterval(int statisticInterval) {
    super.setPoolStatisticInterval(statisticInterval);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolServerGroup(String group) {
    super.setPoolServerGroup(group);
    return this;
  }

  @Override
  public MClientCacheFactory addPoolLocator(String host, int port) {
    super.addPoolLocator(host, port);
    return this;
  }

  @Override
  public MClientCacheFactory addPoolServer(String host, int port) {
    super.addPoolServer(host, port);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolSubscriptionEnabled(boolean enabled) {
    super.setPoolSubscriptionEnabled(enabled);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolSubscriptionRedundancy(int redundancy) {
    super.setPoolSubscriptionRedundancy(redundancy);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolSubscriptionMessageTrackingTimeout(int messageTrackingTimeout) {
    super.setPoolSubscriptionMessageTrackingTimeout(messageTrackingTimeout);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolSubscriptionAckInterval(int ackInterval) {
    super.setPoolSubscriptionAckInterval(ackInterval);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolPRSingleHopEnabled(boolean enabled) {
    super.setPoolPRSingleHopEnabled(enabled);
    return this;
  }

  @Override
  public MClientCacheFactory setPoolMultiuserAuthentication(boolean enabled) {
    super.setPoolMultiuserAuthentication(enabled);
    return this;
  }

  @Override
  public MClientCacheFactory setPdxReadSerialized(boolean pdxReadSerialized) {
    super.setPdxReadSerialized(pdxReadSerialized);
    return this;
  }

  @Override
  public MClientCacheFactory setPdxSerializer(PdxSerializer serializer) {
    super.setPdxSerializer(serializer);
    return this;
  }

  @Override
  public MClientCacheFactory setPdxDiskStore(String diskStoreName) {
    super.setPdxDiskStore(diskStoreName);
    return this;
  }

  @Override
  public MClientCacheFactory setPdxPersistent(boolean isPersistent) {
    super.setPdxPersistent(isPersistent);
    return this;
  }

  @Override
  public MClientCacheFactory setPdxIgnoreUnreadFields(boolean ignore) {
    super.setPdxIgnoreUnreadFields(ignore);
    return this;
  }

  public MClientCacheFactory enableMetaRegionCaching(final boolean isMetaRegionCached) {
    this.metaRegionCachingEnabled = isMetaRegionCached;
    return this;
  }

  public boolean isMetaRegionCachingEnabled() {
    return this.metaRegionCachingEnabled;
  }
}


